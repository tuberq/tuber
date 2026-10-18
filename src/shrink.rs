//! Giving back what a burst of jobs grew.
//!
//! Hash maps, vectors and deques keep their capacity when emptied, so the
//! collections that grow with the number of jobs would hold a burst's peak
//! until restart. The tick shrinks those that are mostly empty
//! ([`shrink_if_sparse`]).

use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::{BuildHasher, Hash};

/// Capacity, in entries, a collection may keep however empty: small enough
/// not to matter, and it spares a quiet server regrowing through small sizes.
pub const MIN_CAPACITY: usize = 4096;

/// Shrink `c` to twice its length, or [`MIN_CAPACITY`], once it's less than a
/// quarter full. The gap between the two keeps a collection whose size
/// hovers around a threshold from shrinking and regrowing in turn.
pub fn shrink_if_sparse(c: &mut impl Shrink) {
    let capacity = c.capacity();
    if capacity > MIN_CAPACITY && c.len() < capacity / 4 {
        c.shrink_to((c.len() * 2).max(MIN_CAPACITY));
    }
}

/// A collection [`shrink_if_sparse`] can shrink: the collection's own
/// methods of these names, which `is_empty` would add nothing to.
#[allow(clippy::len_without_is_empty)]
pub trait Shrink {
    fn len(&self) -> usize;
    fn capacity(&self) -> usize;
    fn shrink_to(&mut self, min_capacity: usize);
}

impl<T> Shrink for Vec<T> {
    fn len(&self) -> usize {
        Vec::len(self)
    }
    fn capacity(&self) -> usize {
        Vec::capacity(self)
    }
    fn shrink_to(&mut self, min_capacity: usize) {
        Vec::shrink_to(self, min_capacity)
    }
}

impl<T> Shrink for VecDeque<T> {
    fn len(&self) -> usize {
        VecDeque::len(self)
    }
    fn capacity(&self) -> usize {
        VecDeque::capacity(self)
    }
    fn shrink_to(&mut self, min_capacity: usize) {
        VecDeque::shrink_to(self, min_capacity)
    }
}

impl<K: Eq + Hash, V, S: BuildHasher> Shrink for HashMap<K, V, S> {
    fn len(&self) -> usize {
        HashMap::len(self)
    }
    fn capacity(&self) -> usize {
        HashMap::capacity(self)
    }
    fn shrink_to(&mut self, min_capacity: usize) {
        HashMap::shrink_to(self, min_capacity)
    }
}

impl<T: Eq + Hash, S: BuildHasher> Shrink for HashSet<T, S> {
    fn len(&self) -> usize {
        HashSet::len(self)
    }
    fn capacity(&self) -> usize {
        HashSet::capacity(self)
    }
    fn shrink_to(&mut self, min_capacity: usize) {
        HashSet::shrink_to(self, min_capacity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_collection_a_quarter_full_or_more_keeps_its_capacity() {
        let mut v: Vec<u64> = Vec::with_capacity(4 * MIN_CAPACITY);
        v.resize(MIN_CAPACITY, 0);
        let capacity = v.capacity();
        shrink_if_sparse(&mut v);
        assert_eq!(v.capacity(), capacity);
    }

    #[test]
    fn a_sparse_collection_shrinks_to_twice_its_length() {
        let mut v: Vec<u64> = Vec::with_capacity(16 * MIN_CAPACITY);
        v.resize(MIN_CAPACITY, 0);
        shrink_if_sparse(&mut v);
        assert_eq!(v.capacity(), 2 * MIN_CAPACITY);
    }

    #[test]
    fn a_small_collection_is_left_alone() {
        let mut m: HashMap<u64, u64> = HashMap::with_capacity(MIN_CAPACITY / 2);
        let capacity = m.capacity();
        shrink_if_sparse(&mut m);
        assert_eq!(m.capacity(), capacity);
    }
}
