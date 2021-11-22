// Copyright (c) The Diem Core Contributors
// SPDX-License-Identifier: Apache-2.0

use crossbeam::utils::CachePadded;
use std::{
    cmp::min,
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
};

const MASK: u64 = ((1 as u64) << 32) - 1;

struct InnerSnazzy {
    counter: AtomicU64,

    max_seq_id: AtomicUsize,

    store_idx: usize,
}

struct RootSnazzy {
    counter: AtomicUsize,
}

struct SnazzyTree {
    root: CachePadded<RootSnazzy>,

    nodes: Vec<CachePadded<InnerSnazzy>>,

    parent_idx: Vec<Option<usize>>,
    sibling_idx: Vec<usize>,

    // num_threads size, maps to a node index in store
    leaves: Vec<usize>,
}

enum NodeStore {
    Tree(SnazzyTree),
    Counter(AtomicUsize),
}

impl SnazzyTree {
    pub fn new(num_threads: usize, tree_levels: Vec<Vec<usize>>) -> Self {
        let mut nodes = Vec::new();
        let mut parent_idx = Vec::new();
        let mut sibling_idx = Vec::new();

        // TODO: assert that tree_levels[0].len() > 1 and sum to num_threads.
        // TODO: in fact assert containment for every level.
        for i in 0..tree_levels[0].len() {
            nodes.push(CachePadded::new(InnerSnazzy::new(i)));
            parent_idx.push(None);
            sibling_idx.push(if i + 1 < tree_levels[0].len() {
                i + 1
            } else {
                0
            });
        }

        // TODO: test this logic
        let mut par_idx = 0;
        for slice in tree_levels.windows(2) {
            let cur = &slice[0];
            let next = &slice[1];

            let mut cnt = 0;
            let mut i = 0;
            for next_cnt in next {
                nodes.push(CachePadded::new(InnerSnazzy::new(nodes.len())));
                parent_idx.push(Some(par_idx + i));
                cnt += next_cnt;
                if cnt == cur[i] {
                    cnt = 0;
                    i += 1;
                }
            }

            par_idx += cur.len();
        }

        let mut leaves = Vec::new();
        let mut cnt = 0;
        for _ in 0..num_threads {
            leaves.push(par_idx);
            cnt += 1;
            if cnt == tree_levels.last().unwrap()[par_idx] {
                cnt = 0;
                par_idx += 1;
            }
        }

        Self {
            root: CachePadded::new(RootSnazzy {
                counter: AtomicUsize::new(0),
            }),
            nodes,
            parent_idx,
            sibling_idx,
            leaves,
        }
    }

    pub fn increment(&self, thread_id: usize) {
        self.increment_node(Some(self.leaves[thread_id]));
    }

    pub fn decrement(&self, thread_id: usize) {
        self.decrement_node(Some(self.leaves[thread_id]));
    }

    // idx = None means root
    pub fn increment_node(&self, idx: Option<usize>) {
        match idx {
            Some(idx) => self.nodes[idx].increment(&self),
            None => self.root.increment(),
        };
    }

    // idx = None means root
    pub fn decrement_node(&self, idx: Option<usize>) {
        match idx {
            Some(idx) => self.nodes[idx].decrement(&self),
            None => self.root.decrement(),
        };
    }

    pub fn parent(&self, node_idx: usize) -> Option<usize> {
        self.parent_idx[node_idx]
    }

    pub fn sibling(&self, node_idx: usize) -> usize {
        self.sibling_idx[node_idx]
    }

    pub fn zero(&self) -> bool {
        self.root.zero()
    }
}

impl InnerSnazzy {
    pub fn new(store_idx: usize) -> Self {
        Self {
            counter: AtomicU64::new(0),
            max_seq_id: AtomicUsize::new(0),
            store_idx,
        }
    }

    // Returns if zero was incremented, and new seq_id
    fn increment_counter(&self) -> (bool, usize) {
        let mut cnt = self.counter.load(Ordering::Relaxed);
        loop {
            let val = cnt >> 32;
            let mut seq_id = cnt & MASK;
            let zero = val == 0;

            let new_cnt = if zero {
                seq_id += 1;
                ((1 as u64) << 32) | seq_id
            } else {
                ((val + 1) << 32) | seq_id
            };

            match self.counter.compare_exchange_weak(
                cnt,
                new_cnt,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return (zero, seq_id as usize);
                }
                Err(x) => cnt = x,
            }
        }
    }

    // Returns if one was decremented, and new seq_id
    fn decrement_counter(&self) -> Option<(bool, usize)> {
        // SeqCst due to val == 0 early check
        let mut cnt = self.counter.load(Ordering::SeqCst);
        loop {
            let val = cnt >> 32;

            if val == 0 {
                return None;
            }

            let mut seq_id = cnt & MASK;
            let one = val == 1;

            let new_cnt = if one {
                seq_id += 1;
                seq_id
            } else {
                ((val - 1) << 32) | seq_id
            };

            match self.counter.compare_exchange_weak(
                cnt,
                new_cnt,
                Ordering::SeqCst,
                Ordering::SeqCst, // due to val == 0 check.
            ) {
                Ok(_) => {
                    return Some((one, seq_id as usize));
                }
                Err(x) => cnt = x,
            }
        }
    }

    fn snazzify<const ODD: bool>(&self, tree: &SnazzyTree, target: usize) {
        let mut seq_id = self.max_seq_id.load(Ordering::SeqCst);
        loop {
            if seq_id > target {
                if ODD {
                    // target is odd. TODO: assert.
                    // op increased but has to decrease itself.
                    tree.decrement_node(tree.parent(self.store_idx));
                }
            }
            if seq_id >= target {
                return;
            }

            let cur_target = min(target, seq_id + 2 - (seq_id & 1));

            match self.max_seq_id.compare_exchange(
                seq_id,
                cur_target,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    if cur_target - seq_id == 1 && cur_target & 1 == 0 {
                        // later op must decrease for this increment.
                        tree.decrement_node(tree.parent(self.store_idx));
                    }
                    seq_id = cur_target;
                }
                Err(x) => seq_id = x,
            }
        }
    }

    pub fn increment(&self, tree: &SnazzyTree) {
        let (zero, seq_id) = self.increment_counter();
        if zero {
            tree.increment_node(tree.parent(self.store_idx));

            self.snazzify::<true>(tree, seq_id);
        }
    }

    pub fn decrement(&self, tree: &SnazzyTree) {
        match self.decrement_counter() {
            Some((one, seq_id)) => {
                if one {
                    self.snazzify::<false>(tree, seq_id);
                }
            }
            None => {
                // What's the algorithm for these guys after transfer, always decrease?
                tree.decrement_node(Some(tree.sibling(self.store_idx)));
            }
        }
    }
}

impl RootSnazzy {
    fn increment(&self) {
        self.counter.fetch_add(1, Ordering::SeqCst);
    }

    fn decrement(&self) {
        self.counter.fetch_sub(1, Ordering::SeqCst);
    }

    fn zero(&self) -> bool {
        self.counter.load(Ordering::SeqCst) == 0
    }
}

pub struct Snazzy {
    store: NodeStore,
}

impl Snazzy {
    // tree_levels describes the Snazzy tree structure.
    pub fn new(init_value: usize, num_threads: usize, tree_levels: Vec<Vec<usize>>) -> Self {
        Self {
            store: if tree_levels.is_empty() {
                NodeStore::Counter(AtomicUsize::new(init_value))
            } else {
                NodeStore::Tree(SnazzyTree::new(num_threads, tree_levels))
            },
        }
    }

    pub fn increment(&self, thread_id: usize) {
        match &self.store {
            NodeStore::Tree(tree) => tree.increment(thread_id),
            NodeStore::Counter(counter) => {
                counter.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    pub fn increment_snazzy(&self, thread_id: usize) {
        match &self.store {
            NodeStore::Tree(tree) => tree.increment(thread_id),
            NodeStore::Counter(_) => (),
        }
    }

    pub fn decrement(&self, thread_id: usize) {
        match &self.store {
            NodeStore::Tree(tree) => tree.decrement(thread_id),
            NodeStore::Counter(counter) => {
                counter.fetch_sub(1, Ordering::SeqCst);
            }
        }
    }

    pub fn decrement_snazzy(&self, thread_id: usize) {
        match &self.store {
            NodeStore::Tree(tree) => tree.decrement(thread_id),
            NodeStore::Counter(_) => (),
        }
    }

    pub fn zero(&self) -> bool {
        match &self.store {
            NodeStore::Tree(tree) => tree.zero(),
            NodeStore::Counter(counter) => counter.load(Ordering::SeqCst) == 0,
        }
    }
}
