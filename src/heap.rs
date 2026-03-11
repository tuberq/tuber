use std::collections::HashMap;

/// A min-heap that supports O(log n) insert, pop-min, and remove-by-id.
///
/// Each entry is a `(key, id)` pair. The heap orders by key (smallest first),
/// and the `positions` map allows O(1) lookup of an entry's index by its id,
/// enabling O(log n) removal of arbitrary elements.
///
/// This replaces the C beanstalkd `Heap` struct which uses `setpos` callbacks
/// to track element positions.
#[derive(Debug)]
pub struct IndexHeap<K: Ord + Copy> {
    data: Vec<(K, u64)>,
    positions: HashMap<u64, usize>,
}

impl<K: Ord + Copy> IndexHeap<K> {
    pub fn new() -> Self {
        IndexHeap {
            data: Vec::new(),
            positions: HashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Returns the (key, id) of the minimum element without removing it.
    pub fn peek(&self) -> Option<&(K, u64)> {
        self.data.first()
    }

    /// Returns true if the heap contains an entry with the given id.
    pub fn contains(&self, id: u64) -> bool {
        self.positions.contains_key(&id)
    }

    /// Inserts a (key, id) pair. Returns false if the id already exists.
    pub fn insert(&mut self, key: K, id: u64) -> bool {
        if self.positions.contains_key(&id) {
            return false;
        }
        let idx = self.data.len();
        self.data.push((key, id));
        self.positions.insert(id, idx);
        self.sift_down(idx);
        true
    }

    /// Removes and returns the minimum element.
    pub fn pop(&mut self) -> Option<(K, u64)> {
        if self.data.is_empty() {
            return None;
        }
        Some(self.remove_at(0))
    }

    /// Removes the entry with the given id. Returns the (key, id) if found.
    pub fn remove_by_id(&mut self, id: u64) -> Option<(K, u64)> {
        let idx = *self.positions.get(&id)?;
        Some(self.remove_at(idx))
    }

    /// Returns all IDs currently in the heap.
    pub fn ids(&self) -> Vec<u64> {
        self.data.iter().map(|&(_, id)| id).collect()
    }

    /// Removes all entries from the heap.
    pub fn clear(&mut self) {
        self.data.clear();
        self.positions.clear();
    }

    fn remove_at(&mut self, idx: usize) -> (K, u64) {
        let last = self.data.len() - 1;
        let removed = self.data[idx];
        self.positions.remove(&removed.1);

        if idx == last {
            self.data.pop();
        } else {
            self.data[idx] = self.data[last];
            self.positions.insert(self.data[idx].1, idx);
            self.data.pop();
            self.sift_down(idx);
            self.sift_up(idx);
        }
        removed
    }

    /// Sift element at `k` towards the root (swap with parent while smaller).
    fn sift_down(&mut self, mut k: usize) {
        while k > 0 {
            let parent = (k - 1) / 2;
            if self.data[parent] <= self.data[k] {
                break;
            }
            self.swap(k, parent);
            k = parent;
        }
    }

    /// Sift element at `k` towards the leaves (swap with smallest child).
    fn sift_up(&mut self, mut k: usize) {
        loop {
            let left = k * 2 + 1;
            let right = k * 2 + 2;
            let mut smallest = k;

            if left < self.data.len() && self.data[left] < self.data[smallest] {
                smallest = left;
            }
            if right < self.data.len() && self.data[right] < self.data[smallest] {
                smallest = right;
            }
            if smallest == k {
                break;
            }
            self.swap(k, smallest);
            k = smallest;
        }
    }

    fn swap(&mut self, a: usize, b: usize) {
        self.data.swap(a, b);
        self.positions.insert(self.data[a].1, a);
        self.positions.insert(self.data[b].1, b);
    }
}

impl<K: Ord + Copy> Default for IndexHeap<K> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Mirrors cttest_heap_insert_one
    #[test]
    fn test_heap_insert_one() {
        let mut h: IndexHeap<(u32, u64)> = IndexHeap::new();
        h.insert((1, 1), 1);
        assert_eq!(h.len(), 1);
        assert!(h.contains(1));

        let removed = h.pop();
        assert_eq!(removed, Some(((1, 1), 1)));
        assert!(h.is_empty());
    }

    // Mirrors cttest_heap_insert_and_remove_one
    #[test]
    fn test_heap_insert_and_remove_one() {
        let mut h: IndexHeap<(u32, u64)> = IndexHeap::new();
        assert!(h.insert((1, 1), 1));

        let got = h.pop();
        assert_eq!(got, Some(((1, 1), 1)));
        assert!(h.is_empty());
    }

    // Mirrors cttest_heap_priority
    #[test]
    fn test_heap_priority() {
        let mut h: IndexHeap<(u32, u64)> = IndexHeap::new();

        // Insert priorities 2, 3, 1 (with ids 1, 2, 3)
        h.insert((2, 1), 1);
        h.insert((3, 2), 2);
        h.insert((1, 3), 3);

        // Should come out in priority order: 1, 2, 3
        let j = h.pop().unwrap();
        assert_eq!(j.0.0, 1); // priority 1

        let j = h.pop().unwrap();
        assert_eq!(j.0.0, 2); // priority 2

        let j = h.pop().unwrap();
        assert_eq!(j.0.0, 3); // priority 3

        assert!(h.is_empty());
    }

    // Mirrors cttest_heap_fifo_property
    // Same priority -> ordered by id (second element of the key tuple)
    #[test]
    fn test_heap_fifo_property() {
        let mut h: IndexHeap<(u32, u64)> = IndexHeap::new();

        h.insert((3, 1), 1); // id 1
        h.insert((3, 2), 2); // id 2
        h.insert((3, 3), 3); // id 3

        let j = h.pop().unwrap();
        assert_eq!(j.1, 1, "id 1 should come out first");

        let j = h.pop().unwrap();
        assert_eq!(j.1, 2, "id 2 should come out second");

        let j = h.pop().unwrap();
        assert_eq!(j.1, 3, "id 3 should come out third");
    }

    // Mirrors cttest_heap_many_jobs
    #[test]
    fn test_heap_many_jobs() {
        let mut h: IndexHeap<(u32, u64)> = IndexHeap::new();
        let n = 20;

        for i in 0..n {
            let pri = 1 + ((i * 7 + 13) % 8192); // deterministic "random"
            h.insert((pri as u32, i), i);
        }

        assert_eq!(h.len(), n as usize);

        let mut last_pri = 0u32;
        for _ in 0..n {
            let (key, _id) = h.pop().unwrap();
            assert!(
                key.0 >= last_pri,
                "should come out in order: got {} after {}",
                key.0,
                last_pri
            );
            last_pri = key.0;
        }
        assert!(h.is_empty());
    }

    // Mirrors cttest_heap_remove_k
    #[test]
    fn test_heap_remove_by_id() {
        for round in 0..50u64 {
            let mut h: IndexHeap<(u32, u64)> = IndexHeap::new();
            let n = 50u64;

            for i in 0..n {
                let pri = 1 + ((i.wrapping_mul(7).wrapping_add(13 + round)) % 8192);
                h.insert((pri as u32, i), i);
            }

            // Remove one from the middle (id = 25)
            let removed = h.remove_by_id(25);
            assert!(removed.is_some(), "mid element should exist");

            // Remaining elements should still come out in order
            let mut last_pri = 0u32;
            for _ in 1..n {
                let (key, _id) = h.pop().unwrap();
                assert!(
                    key.0 >= last_pri,
                    "should come out in order after mid removal"
                );
                last_pri = key.0;
            }
            assert!(h.is_empty());
        }
    }
}
