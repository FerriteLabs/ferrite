//! Iterator types for embedded database operations

use bytes::Bytes;

/// Iterator over keys matching a pattern
pub struct KeyIterator {
    keys: Vec<Bytes>,
    position: usize,
}

impl KeyIterator {
    /// Create a new key iterator
    pub fn new(keys: Vec<Bytes>) -> Self {
        Self { keys, position: 0 }
    }

    /// Get the total count of keys
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
}

impl Iterator for KeyIterator {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.position < self.keys.len() {
            let key = self.keys[self.position].to_vec();
            self.position += 1;
            Some(key)
        } else {
            None
        }
    }
}

/// Iterator over hash fields
pub struct HashIterator {
    entries: Vec<(Bytes, Bytes)>,
    position: usize,
}

impl HashIterator {
    /// Create a new hash iterator
    pub fn new(entries: Vec<(Bytes, Bytes)>) -> Self {
        Self {
            entries,
            position: 0,
        }
    }

    /// Get the total count of entries
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl Iterator for HashIterator {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.position < self.entries.len() {
            let (field, value) = &self.entries[self.position];
            self.position += 1;
            Some((field.to_vec(), value.to_vec()))
        } else {
            None
        }
    }
}

/// Iterator over list elements
pub struct ListIterator {
    elements: Vec<Bytes>,
    position: usize,
}

impl ListIterator {
    /// Create a new list iterator
    pub fn new(elements: Vec<Bytes>) -> Self {
        Self {
            elements,
            position: 0,
        }
    }

    /// Get the total count of elements
    pub fn len(&self) -> usize {
        self.elements.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }
}

impl Iterator for ListIterator {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.position < self.elements.len() {
            let element = self.elements[self.position].to_vec();
            self.position += 1;
            Some(element)
        } else {
            None
        }
    }
}

/// Iterator over set members
pub struct SetIterator {
    members: Vec<Bytes>,
    position: usize,
}

impl SetIterator {
    /// Create a new set iterator
    pub fn new(members: Vec<Bytes>) -> Self {
        Self {
            members,
            position: 0,
        }
    }

    /// Get the total count of members
    pub fn len(&self) -> usize {
        self.members.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }
}

impl Iterator for SetIterator {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.position < self.members.len() {
            let member = self.members[self.position].to_vec();
            self.position += 1;
            Some(member)
        } else {
            None
        }
    }
}

/// Iterator over sorted set members with scores
pub struct ZSetIterator {
    members: Vec<(Bytes, f64)>,
    position: usize,
}

impl ZSetIterator {
    /// Create a new sorted set iterator
    pub fn new(members: Vec<(Bytes, f64)>) -> Self {
        Self {
            members,
            position: 0,
        }
    }

    /// Get the total count of members
    pub fn len(&self) -> usize {
        self.members.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }
}

impl Iterator for ZSetIterator {
    type Item = (Vec<u8>, f64);

    fn next(&mut self) -> Option<Self::Item> {
        if self.position < self.members.len() {
            let (member, score) = &self.members[self.position];
            self.position += 1;
            Some((member.to_vec(), *score))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_iterator() {
        let keys = vec![
            Bytes::from("key1"),
            Bytes::from("key2"),
            Bytes::from("key3"),
        ];
        let mut iter = KeyIterator::new(keys);

        assert_eq!(iter.len(), 3);
        assert_eq!(iter.next(), Some(b"key1".to_vec()));
        assert_eq!(iter.next(), Some(b"key2".to_vec()));
        assert_eq!(iter.next(), Some(b"key3".to_vec()));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_hash_iterator() {
        let entries = vec![
            (Bytes::from("field1"), Bytes::from("value1")),
            (Bytes::from("field2"), Bytes::from("value2")),
        ];
        let mut iter = HashIterator::new(entries);

        assert_eq!(iter.len(), 2);
        assert_eq!(iter.next(), Some((b"field1".to_vec(), b"value1".to_vec())));
        assert_eq!(iter.next(), Some((b"field2".to_vec(), b"value2".to_vec())));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_list_iterator() {
        let elements = vec![Bytes::from("a"), Bytes::from("b")];
        let mut iter = ListIterator::new(elements);

        assert_eq!(iter.len(), 2);
        assert_eq!(iter.next(), Some(b"a".to_vec()));
        assert_eq!(iter.next(), Some(b"b".to_vec()));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_set_iterator() {
        let members = vec![Bytes::from("member1"), Bytes::from("member2")];
        let mut iter = SetIterator::new(members);

        assert_eq!(iter.len(), 2);
        assert!(iter.next().is_some());
        assert!(iter.next().is_some());
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_zset_iterator() {
        let members = vec![(Bytes::from("alice"), 100.0), (Bytes::from("bob"), 95.0)];
        let mut iter = ZSetIterator::new(members);

        assert_eq!(iter.len(), 2);
        assert_eq!(iter.next(), Some((b"alice".to_vec(), 100.0)));
        assert_eq!(iter.next(), Some((b"bob".to_vec(), 95.0)));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_empty_iterators() {
        let key_iter = KeyIterator::new(vec![]);
        assert!(key_iter.is_empty());

        let hash_iter = HashIterator::new(vec![]);
        assert!(hash_iter.is_empty());

        let list_iter = ListIterator::new(vec![]);
        assert!(list_iter.is_empty());

        let set_iter = SetIterator::new(vec![]);
        assert!(set_iter.is_empty());

        let zset_iter = ZSetIterator::new(vec![]);
        assert!(zset_iter.is_empty());
    }
}
