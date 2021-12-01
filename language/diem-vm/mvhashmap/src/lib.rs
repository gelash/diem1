// Copyright (c) The Diem Core Contributors
// SPDX-License-Identifier: Apache-2.0

use arc_swap::{ArcSwap, ArcSwapOption};
use dashmap::DashMap;
use std::{
    cmp::{max, PartialOrd},
    collections::{btree_map::BTreeMap, HashMap},
    fmt::Debug,
    hash::Hash,
    sync::atomic::{AtomicUsize, Ordering},
    sync::Arc,
};

#[cfg(test)]
mod unit_tests;

/// A structure that holds placeholders for each write to the database
//
//  The structure is created by one thread creating the scheduling, and
//  at that point it is used as a &mut by that single thread.
//
//  Then it is passed to all threads executing as a shared reference. At
//  this point only a single thread must write to any entry, and others
//  can read from it. Only entries are mutated using interior mutability,
//  but no entries can be added or deleted.
//

pub type Version = usize;

const FLAG_UNASSIGNED: usize = 0;
const FLAG_DONE: usize = 2;
const FLAG_SKIP: usize = 3;
const FLAG_DIRTY: usize = 4;

#[derive(Debug)]
pub enum Error {
    // A write has been performed on an entry that is not in the possible_writes list.
    UnexpectedWrite,
    // A query doesn't match any entry in the map.
    NotInMap,
}

#[cfg_attr(any(target_arch = "x86_64"), repr(align(128)))]
pub struct WriteCell<V> {
    flag: AtomicUsize,
    retry_num: AtomicUsize,
    data: ArcSwap<Option<V>>,
}

impl<V> WriteCell<V> {
    pub fn new() -> WriteCell<V> {
        WriteCell {
            flag: AtomicUsize::new(FLAG_UNASSIGNED),
            retry_num: AtomicUsize::new(0),
            data: ArcSwap::from(Arc::new(None)),
        }
    }

    pub fn new_from(flag: usize, retry_num: usize, data: Option<V>) -> WriteCell<V> {
        WriteCell {
            flag: AtomicUsize::new(flag),
            retry_num: AtomicUsize::new(retry_num),
            data: ArcSwap::from(Arc::new(data)),
        }
    }

    pub fn write(&self, v: Option<V>, retry_num: usize) {
        self.data.store(Arc::new(v));
        self.retry_num.store(retry_num, Ordering::SeqCst);
        self.flag.store(FLAG_DONE, Ordering::SeqCst);
    }

    pub fn skip(&self) {
        self.flag.store(FLAG_SKIP, Ordering::SeqCst);
    }

    pub fn mark_dirty(&self) {
        self.flag.store(FLAG_DIRTY, Ordering::SeqCst);
    }
}

pub struct WriteCellPtr<V>(*const WriteCell<V>);

unsafe impl<V> Sync for WriteCellPtr<V> {}
unsafe impl<V> Send for WriteCellPtr<V> {}

impl<V> WriteCellPtr<V> {
    pub fn new(cell: *const WriteCell<V>) -> Self {
        Self(cell)
    }

    pub fn write(&self, v: Option<V>, retry_num: usize) {
        unsafe {
            self.0.as_ref().unwrap().write(v, retry_num);
        }
    }

    pub fn skip(&self) {
        unsafe {
            self.0.as_ref().unwrap().skip();
        }
    }

    pub fn mark_dirty(&self) {
        unsafe {
            self.0.as_ref().unwrap().mark_dirty();
        }
    }
}

pub struct StaticMVHashMap<K, V> {
    data: HashMap<K, BTreeMap<Version, Arc<WriteCell<V>>>>,
}

pub struct DynamicMVHashMap<K, V> {
    empty: AtomicUsize,
    data: Arc<DashMap<K, BTreeMap<Version, Arc<WriteCell<V>>>>>,
}

pub struct MVHashMap<K, V> {
    s_mvhashmap: StaticMVHashMap<K, V>,
    d_mvhashmap: DynamicMVHashMap<K, V>,
}

impl<K: PartialOrd + Send + Clone + Hash + Eq + Sync, V: Clone + Sync + Send> MVHashMap<K, V> {
    pub fn new_from_parallel(
        possible_writes: Vec<(K, usize, usize)>,
        op_shortcuts: &Vec<Vec<(K, ArcSwapOption<WriteCell<V>>)>>,
    ) -> (MVHashMap<K, V>, usize) {
        let (s_mvhashmap, max_dependency_length) =
            StaticMVHashMap::new_from_parallel(possible_writes, op_shortcuts);
        let d_mvhashmap = DynamicMVHashMap::new();
        (
            MVHashMap {
                s_mvhashmap,
                d_mvhashmap,
            },
            max_dependency_length,
        )
    }

    // For the entry of same verison/txn_id, it should only appear in only one mvhashmap since a write is either estimated or non-estimated
    // Return the higher version data ranked by version, if read data from both mvhashmaps
    // If read both dependency and data, return the higher version
    pub fn read(
        &self,
        key: &K,
        version: Version,
    ) -> Result<(Option<V>, Version, usize), Option<(Version, usize)>> {
        // reads may return Ok((Option<V>, Version, retry_num)), Err(Some(Version)) or Err(None)
        let read_from_static = self.s_mvhashmap.read(key, version);
        let read_from_dynamic = self.d_mvhashmap.read(key, version);

        // Should return the dependency or data of the higher version
        let version1 = match read_from_static {
            Ok((_, version, _)) => Some(version),
            Err(Some((version, _))) => Some(version),
            Err(None) => None,
        };
        let version2 = match read_from_dynamic {
            Ok((_, version, _)) => Some(version),
            Err(Some((version, _))) => Some(version),
            Err(None) => None,
        };
        // If both mvhashmaps do not contain AP, return Err(None)
        if version1.is_none() && version2.is_none() {
            return Err(None);
        }
        if version1.is_some() && version2.is_some() {
            // The same version should not be appear in both static and dynamic mvhashmaps
            assert_ne!(version1.unwrap(), version2.unwrap());
            if version1.unwrap() > version2.unwrap() {
                return read_from_static;
            } else {
                return read_from_dynamic;
            }
        }
        if version1.is_none() {
            return read_from_dynamic;
        } else {
            return read_from_static;
        }
    }

    pub fn read_from_static(
        &self,
        key: &K,
        version: Version,
    ) -> Result<(Option<V>, Version, usize), Option<(Version, usize)>> {
        self.s_mvhashmap.read(key, version)
    }

    pub fn read_from_dynamic(
        &self,
        key: &K,
        version: Version,
    ) -> Result<(Option<V>, Version, usize), Option<(Version, usize)>> {
        self.d_mvhashmap.read(key, version)
    }

    pub fn write_to_static(
        &self,
        key: &K,
        version: Version,
        retry_num: usize,
        data: Option<V>,
    ) -> Result<WriteCellPtr<V>, Error> {
        self.s_mvhashmap.write(key, version, retry_num, data)
    }

    pub fn write_to_dynamic(
        &self,
        key: &K,
        version: Version,
        retry_num: usize,
        data: Option<V>,
    ) -> Result<WriteCellPtr<V>, ()> {
        self.d_mvhashmap.write(key, version, retry_num, data)
    }

    pub fn dynamic_empty(&self) -> bool {
        self.d_mvhashmap.empty()
    }

    pub fn skip_static(&self, key: &K, version: Version) -> Result<(), Error> {
        self.s_mvhashmap.skip(key, version)
    }

    pub fn skip_dynamic(&self, key: &K, version: Version) {
        self.d_mvhashmap.skip(key, version)
    }

    // returns (max depth, avg depth) of dynamic hashmap
    pub fn get_depth(&self) -> ((usize, usize), (usize, usize)) {
        let s_depths = self.s_mvhashmap.get_depth();
        let d_depths = self.d_mvhashmap.get_depth();
        return (s_depths, d_depths);
    }
}

impl<K: Hash + Clone + Eq, V: Clone> StaticMVHashMap<K, V> {
    /// Create the MVHashMap structure from a list of possible writes. Each element in the list
    /// indicates a key that could potentially be modified at its given version.
    ///
    /// Returns the MVHashMap, and the maximum number of writes that can write to one single key.
    pub fn new_from(possible_writes: Vec<(K, Version)>) -> (Self, usize) {
        let mut outer_map: HashMap<K, BTreeMap<Version, Arc<WriteCell<V>>>> = HashMap::new();
        for (key, version) in possible_writes.into_iter() {
            outer_map
                .entry(key)
                .or_default()
                .insert(version, Arc::from(WriteCell::new()));
        }
        let max_dependency_size = outer_map
            .values()
            .fold(0, |max_depth, btree_map| max(max_depth, btree_map.len()));

        (StaticMVHashMap { data: outer_map }, max_dependency_size)
    }

    /// Get the number of keys in the MVHashMap.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    fn get_entry(&self, key: &K, version: Version) -> Result<&Arc<WriteCell<V>>, Error> {
        self.data
            .get(key)
            .ok_or(Error::UnexpectedWrite)?
            .get(&version)
            .ok_or(Error::UnexpectedWrite)
    }

    /// Write to `key` at `version`.
    /// Function will return an error if the write is not in the initial `possible_writes` list.
    pub fn write(
        &self,
        key: &K,
        version: Version,
        retry_num: usize,
        data: Option<V>,
    ) -> Result<WriteCellPtr<V>, Error> {
        // By construction there will only be a single writer, before the
        // write there will be no readers on the variable.
        // So it is safe to go ahead and write without any further check.

        let entry = self.get_entry(key, version)?;

        entry.write(data, retry_num);

        Ok(WriteCellPtr(Arc::as_ptr(&entry)))
    }

    /// Skips writing to `key` at `version`.
    /// Function will return an error if the write is not in the initial `possible_writes` list.
    /// `skip` should only be invoked when `key` at `version` hasn't been assigned.
    pub fn skip(&self, key: &K, version: Version) -> Result<(), Error> {
        let entry = self.get_entry(key, version)?;

        entry.skip();
        Ok(())
    }

    /// Get the value of `key` at `version`.
    /// Returns Ok(val) if such key is already assigned by previous transactions.
    /// Returns Err(None) if `version` is smaller than the write of all previous versions.
    /// Returns Err(Some(version)) if such key is dependent on the `version`-th transaction.
    pub fn read(
        &self,
        key: &K,
        version: Version,
    ) -> Result<(Option<V>, Version, usize), Option<(Version, usize)>> {
        let tree = self.data.get(key).ok_or(None)?;

        let mut iter = tree.range(0..version);

        while let Some((entry_key, entry_val)) = iter.next_back() {
            let flag = entry_val.flag.load(Ordering::SeqCst);
            let retry = entry_val.retry_num.load(Ordering::SeqCst);

            // Entry not yet computed, return the version that blocked this query.
            if flag == FLAG_UNASSIGNED || flag == FLAG_DIRTY {
                return Err(Some((*entry_key, retry)));
            }

            // Entry is skipped, go to previous version.
            if flag == FLAG_SKIP {
                continue;
            }

            // The entry is populated so return its contents
            if flag == FLAG_DONE {
                return Ok(((**entry_val.data.load()).clone(), *entry_key, retry));
            }

            unreachable!();
        }

        Err(None)
    }

    // returns (max depth, avg depth) of static hashmap
    pub fn get_depth(&self) -> (usize, usize) {
        if self.data.is_empty() {
            return (0, 0);
        }
        let mut max = 0;
        let mut sum = 0;
        for (_, map) in self.data.iter() {
            let size = map.len();
            sum += size;
            if size > max {
                max = size;
            }
        }
        return (max, sum / self.data.len());
    }
}

const PARALLEL_THRESHOLD: usize = 1000;

impl<K, V> StaticMVHashMap<K, V>
where
    K: PartialOrd + Send + Clone + Hash + Eq + Sync,
    V: Send + Sync,
{
    fn split_merge(
        num_cpus: usize,
        recursion_depth: usize,
        split: Vec<(K, Version, usize)>,
        op_shortcuts: &Vec<Vec<(K, ArcSwapOption<WriteCell<V>>)>>,
    ) -> (usize, HashMap<K, BTreeMap<Version, Arc<WriteCell<V>>>>) {
        if (1 << recursion_depth) > num_cpus || split.len() < PARALLEL_THRESHOLD {
            let mut data = HashMap::new();
            let mut max_len = 0;
            for (path, version, short_idx) in split.into_iter() {
                let place = data.entry(path).or_insert_with(BTreeMap::new);
                let cell = Arc::from(WriteCell::new());
                place.insert(version, Arc::clone(&cell));
                //assert!(op_shortcuts[version][short_idx].0 == path);
                op_shortcuts[version][short_idx].1.swap(Some(cell));
                max_len = max(max_len, place.len());
            }
            (max_len, data)
        } else {
            // Partition the possible writes by keys and work on each partition in parallel.
            let pivot_address = split[split.len() / 2].0.clone();
            let (left, right): (Vec<_>, Vec<_>) =
                split.into_iter().partition(|(p, _, _)| *p < pivot_address);
            let ((m0, mut left_map), (m1, right_map)) = rayon::join(
                || Self::split_merge(num_cpus, recursion_depth + 1, left, op_shortcuts),
                || Self::split_merge(num_cpus, recursion_depth + 1, right, op_shortcuts),
            );
            left_map.extend(right_map);
            (max(m0, m1), left_map)
        }
    }

    /// Create the MVHashMap structure from a list of possible writes in parallel.
    pub fn new_from_parallel(
        possible_writes: Vec<(K, usize, usize)>,
        op_shortcuts: &Vec<Vec<(K, ArcSwapOption<WriteCell<V>>)>>,
    ) -> (Self, usize) {
        let num_cpus = num_cpus::get();

        let (max_dependency_len, data) =
            Self::split_merge(num_cpus, 0, possible_writes, op_shortcuts);
        (StaticMVHashMap { data }, max_dependency_len)
    }
}

impl<K: Hash + Clone + Eq, V: Clone> DynamicMVHashMap<K, V> {
    pub fn new() -> DynamicMVHashMap<K, V> {
        DynamicMVHashMap {
            empty: AtomicUsize::new(1),
            data: Arc::new(DashMap::new()),
        }
    }

    pub fn write(
        &self,
        key: &K,
        version: Version,
        retry_num: usize,
        data: Option<V>,
    ) -> Result<WriteCellPtr<V>, ()> {
        if self.empty.load(Ordering::SeqCst) == 1 {
            self.empty.store(0, Ordering::SeqCst);
        }
        let ret = Arc::from(WriteCell::new_from(FLAG_DONE, retry_num, data));
        let ret_ptr = WriteCellPtr(Arc::as_ptr(&ret));
        let mut map = self.data.entry(key.clone()).or_insert(BTreeMap::new());
        map.insert(version, ret);

        Ok(ret_ptr)
    }

    pub fn skip(&self, key: &K, version: Version) {
        let mut map = self.data.entry(key.clone()).or_insert(BTreeMap::new());

        //TODO: update - logical skip. Later even with shortcut (and no need for wlock).
        // Make sure map traversal handles it.
        map.remove(&version);
    }

    // read when using Btree, may return Ok((Option<V>, Version, retry_num)), Err(Some(Version)) or Err(None)
    pub fn read(
        &self,
        key: &K,
        version: Version,
    ) -> Result<(Option<V>, Version, usize), Option<(Version, usize)>> {
        if self.empty.load(Ordering::SeqCst) == 1 {
            return Err(None);
        }
        match self.data.get(key) {
            Some(tree) => {
                // Find the dependency
                let mut iter = tree.range(0..version);
                while let Some((entry_key, entry_val)) = iter.next_back() {
                    // if *entry_key < version {
                    let flag = entry_val.flag.load(Ordering::SeqCst);

                    if flag == FLAG_DONE {
                        return Ok((
                            (**entry_val.data.load()).clone(),
                            *entry_key,
                            entry_val.retry_num.load(Ordering::SeqCst),
                        ));
                    }

                    if flag == FLAG_DIRTY {
                        return Err(Some((
                            *entry_key,
                            entry_val.retry_num.load(Ordering::SeqCst),
                        )));
                    }

                    // The entry is populated so return its contents

                    if flag == FLAG_SKIP {
                        continue;
                    }

                    unreachable!();
                    // }
                }
                return Err(None);
            }
            None => {
                return Err(None);
            }
        }
    }

    pub fn empty(&self) -> bool {
        self.empty.load(Ordering::SeqCst) == 1
    }

    // returns (max depth, avg depth) of dynamic hashmap
    pub fn get_depth(&self) -> (usize, usize) {
        if self.empty.load(Ordering::SeqCst) == 1 {
            return (0, 0);
        }
        let mut max = 0;
        let mut sum = 0;
        for item in self.data.iter() {
            let map = &*item;
            let size = map.len();
            sum += size;
            if size > max {
                max = size;
            }
        }
        return (max, sum / self.data.len());
    }
}
