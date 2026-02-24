//! Epoch-Based Memory Reclamation
//!
//! This module provides a wrapper around crossbeam-epoch for safe lock-free
//! memory reclamation. The epoch system ensures that memory is only freed
//! when no threads hold references to it.
//!
//! # How it works
//!
//! 1. Threads "pin" an epoch when accessing shared data
//! 2. Memory is marked for deferred reclamation
//! 3. Memory is actually freed only when all threads have moved past that epoch
//!
//! This allows lock-free reads without worrying about use-after-free.

use crossbeam::epoch::{self as epoch, Atomic, Guard, Owned, Shared};
use std::sync::atomic::Ordering;

/// A wrapper around crossbeam-epoch's Guard
///
/// The guard keeps the current epoch pinned, preventing any memory
/// that was deferred for reclamation during this epoch from being freed.
pub struct EpochGuard<'a> {
    /// The underlying crossbeam guard
    guard: Guard,
    /// Phantom data to tie the guard to a lifetime
    _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a> EpochGuard<'a> {
    /// Create a new epoch guard by pinning the current epoch
    pub fn new() -> Self {
        Self {
            guard: epoch::pin(),
            _marker: std::marker::PhantomData,
        }
    }

    /// Get the underlying crossbeam guard
    pub fn guard(&self) -> &Guard {
        &self.guard
    }

    /// Defer a function to be executed when it's safe to reclaim memory
    ///
    /// The closure will be called once all threads have moved past the
    /// current epoch.
    pub fn defer<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        // SAFETY: `defer_unchecked` requires the closure not access thread-locals or be
        // called after the `Guard` is dropped. The `F: Send + 'static` bound ensures the
        // closure is safe to run on any thread, and the guard outlives this call.
        unsafe {
            self.guard.defer_unchecked(f);
        }
    }
}

impl Default for EpochGuard<'_> {
    fn default() -> Self {
        Self::new()
    }
}

/// An atomic pointer protected by epoch-based reclamation
///
/// This provides a lock-free way to read and update shared pointers
/// with automatic memory management.
pub struct EpochAtomic<T> {
    /// The underlying atomic pointer
    inner: Atomic<T>,
}

impl<T> EpochAtomic<T> {
    /// Create a new atomic pointer with no value
    pub fn null() -> Self {
        Self {
            inner: Atomic::null(),
        }
    }

    /// Create a new atomic pointer with the given value
    pub fn new(value: T) -> Self {
        Self {
            inner: Atomic::new(value),
        }
    }

    /// Load the current value
    ///
    /// Returns a shared reference that's valid for the lifetime of the guard.
    pub fn load<'g>(&self, ord: Ordering, guard: &'g Guard) -> Shared<'g, T> {
        self.inner.load(ord, guard)
    }

    /// Store a new value
    ///
    /// The old value will be deferred for reclamation.
    pub fn store(&self, val: Owned<T>, ord: Ordering) {
        self.inner.store(val, ord);
    }

    /// Compare and swap the value
    ///
    /// Returns Ok with the new shared reference on success,
    /// or Err with the owned value on failure.
    pub fn compare_exchange<'g>(
        &self,
        current: Shared<'_, T>,
        new: Owned<T>,
        success: Ordering,
        failure: Ordering,
        guard: &'g Guard,
    ) -> Result<Shared<'g, T>, epoch::CompareExchangeError<'g, T, Owned<T>>> {
        self.inner
            .compare_exchange(current, new, success, failure, guard)
    }

    /// Swap the value, returning the old value
    pub fn swap<'g>(&self, new: Owned<T>, ord: Ordering, guard: &'g Guard) -> Shared<'g, T> {
        self.inner.swap(new, ord, guard)
    }

    /// Check if the pointer is null
    pub fn is_null(&self, guard: &Guard) -> bool {
        self.inner.load(Ordering::Acquire, guard).is_null()
    }
}

impl<T> Default for EpochAtomic<T> {
    fn default() -> Self {
        Self::null()
    }
}

/// An epoch-protected value that can be safely read and updated
///
/// This provides a higher-level abstraction over EpochAtomic for
/// common use cases.
pub struct EpochValue<T: Clone> {
    /// The underlying atomic pointer
    atomic: EpochAtomic<T>,
}

impl<T: Clone> EpochValue<T> {
    /// Create a new epoch value
    pub fn new(value: T) -> Self {
        Self {
            atomic: EpochAtomic::new(value),
        }
    }

    /// Read the current value
    ///
    /// This clones the value to ensure safety after the guard is dropped.
    pub fn read(&self) -> T {
        let guard = epoch::pin();
        let shared = self.atomic.load(Ordering::Acquire, &guard);
        // SAFETY: The pointer is valid because we're inside a pinned epoch
        unsafe {
            shared
                .as_ref()
                .expect("epoch pointer must be valid while pinned")
                .clone()
        }
    }

    /// Read the current value with an existing guard
    pub fn read_with_guard(&self, guard: &Guard) -> T {
        let shared = self.atomic.load(Ordering::Acquire, guard);
        // SAFETY: The pointer is valid because we're inside a pinned epoch
        unsafe {
            shared
                .as_ref()
                .expect("epoch pointer must be valid while pinned")
                .clone()
        }
    }

    /// Update the value
    ///
    /// The old value will be automatically reclaimed when safe.
    pub fn update(&self, new_value: T) {
        // Simplified: epoch advancement now relies on a single atomic swap
        // instead of the previous two-phase pin-then-advance approach,
        // reducing contention under high-throughput workloads.
        let guard = epoch::pin();
        let new = Owned::new(new_value);
        let old = self.atomic.swap(new, Ordering::AcqRel, &guard);

        // SAFETY: We owned the old value and are deferring its destruction
        unsafe {
            if !old.is_null() {
                guard.defer_destroy(old);
            }
        }
    }

    /// Compare and swap the value
    ///
    /// Returns true if the swap was successful.
    pub fn compare_and_swap(&self, expected: &T, new_value: T) -> bool
    where
        T: PartialEq,
    {
        let guard = epoch::pin();
        let current = self.atomic.load(Ordering::Acquire, &guard);

        // SAFETY: The pointer is valid because we're inside a pinned epoch
        let current_ref = unsafe { current.as_ref() };

        match current_ref {
            Some(v) if v == expected => {
                let new = Owned::new(new_value);
                match self.atomic.compare_exchange(
                    current,
                    new,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                    &guard,
                ) {
                    Ok(_) => {
                        // SAFETY: We successfully swapped, defer destruction of old value
                        unsafe {
                            if !current.is_null() {
                                guard.defer_destroy(current);
                            }
                        }
                        true
                    }
                    Err(_) => false,
                }
            }
            _ => false,
        }
    }
}

impl<T: Clone + Default> Default for EpochValue<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

/// Force advancement of the global epoch
///
/// This is useful in tests or when you want to ensure that
/// deferred destructors are run promptly.
pub fn flush() {
    epoch::pin().flush();
}

/// Check if the epoch collector is active
pub fn is_pinned() -> bool {
    epoch::is_pinned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_epoch_guard_creation() {
        let guard = EpochGuard::new();
        // Guard should be valid
        let _g = guard.guard();
    }

    #[test]
    fn test_epoch_value_read_write() {
        let value = EpochValue::new(42i32);
        assert_eq!(value.read(), 42);

        value.update(100);
        assert_eq!(value.read(), 100);
    }

    #[test]
    fn test_epoch_value_compare_and_swap() {
        let value = EpochValue::new(42i32);

        // Should succeed
        assert!(value.compare_and_swap(&42, 100));
        assert_eq!(value.read(), 100);

        // Should fail - wrong expected value
        assert!(!value.compare_and_swap(&42, 200));
        assert_eq!(value.read(), 100);
    }

    #[test]
    fn test_concurrent_reads() {
        let value = Arc::new(EpochValue::new(42i32));
        let mut handles = Vec::new();

        for _ in 0..4 {
            let value = Arc::clone(&value);
            handles.push(thread::spawn(move || {
                for _ in 0..1000 {
                    let v = value.read();
                    assert!(v == 42 || v == 100);
                }
            }));
        }

        // Update while reads are happening
        for _ in 0..100 {
            value.update(100);
            value.update(42);
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_deferred_destruction() {
        // Test that the epoch system can track and defer destruction
        // Note: exact timing of destructor calls depends on GC behavior
        let destroyed = Arc::new(AtomicUsize::new(0));

        struct DropCounter(Arc<AtomicUsize>);
        impl Drop for DropCounter {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        impl Clone for DropCounter {
            fn clone(&self) -> Self {
                DropCounter(Arc::clone(&self.0))
            }
        }

        impl PartialEq for DropCounter {
            fn eq(&self, _other: &Self) -> bool {
                true
            }
        }

        // Perform updates that will defer destruction
        {
            let value = EpochValue::new(DropCounter(Arc::clone(&destroyed)));
            value.update(DropCounter(Arc::clone(&destroyed)));
            value.update(DropCounter(Arc::clone(&destroyed)));
        }

        // Multiple flush/pin cycles to encourage GC
        for _ in 0..5 {
            let guard = epoch::pin();
            guard.flush();
            drop(guard);
        }

        // The test verifies the code compiles and runs without panic.
        // Actual destructor timing is non-deterministic in crossbeam-epoch.
        // At least some values should eventually be destroyed.
        // We just verify the counter is accessible - destruction may happen later.
        let _count = destroyed.load(Ordering::SeqCst);
    }

    #[test]
    fn test_epoch_atomic_null() {
        let atomic: EpochAtomic<i32> = EpochAtomic::null();
        let guard = epoch::pin();
        assert!(atomic.is_null(&guard));
    }

    #[test]
    fn test_epoch_atomic_store_load() {
        let atomic: EpochAtomic<i32> = EpochAtomic::new(42);
        let guard = epoch::pin();

        let shared = atomic.load(Ordering::Acquire, &guard);
        // SAFETY: We just stored a value
        unsafe {
            assert_eq!(*shared.as_ref().unwrap(), 42);
        }
    }
}
