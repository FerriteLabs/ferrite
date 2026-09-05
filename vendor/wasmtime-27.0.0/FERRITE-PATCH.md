# Ferrite Wasmtime 27 Patch

Ferrite exposes Wasmtime 27 types in its public 0.4 API, so a major dependency
upgrade is deferred to the next compatibility release.

Wasmtime 27 raises runtime traps with `longjmp`. On Windows, that unwind crosses
the raw libcall frame, which Wasmtime declares as `extern "C"`. Rust treats that
ABI as non-unwinding and aborts the process instead of returning traps such as
`OutOfFuel`.

This vendored crate changes only the raw libcall and builtin function-pointer
ABIs from `extern "C"` to `extern "C-unwind"`. The calling convention is
unchanged, while Rust can now permit Wasmtime's existing Windows unwind path.

Remove this patch when Ferrite can upgrade to Wasmtime 29 or newer, where
libcalls record traps without unwinding through these frames.
