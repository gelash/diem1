// Copyright (c) The Diem Core Contributors
// SPDX-License-Identifier: Apache-2.0

use std::{
    cmp::{max, PartialOrd},
    collections::{btree_map::BTreeMap, HashMap},
    hash::Hash,
    sync::atomic::{AtomicUsize, AtomicBool, Ordering},
    sync::Arc,
};
use dashmap::DashMap;
use arc_swap::ArcSwap;

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

#[cfg_attr(any(target_arch = "x86_64"), repr(align(128)))]
pub(crate) struct WriteVersionValue<V> {
    flag: AtomicUsize,
    retry_num: AtomicUsize,
    data: ArcSwap<Option<V>>,
}

impl<V> WriteVersionValue<V> {
    pub fn new() -> WriteVersionValue<V> {
        WriteVersionValue {
            flag: AtomicUsize::new(FLAG_UNASSIGNED),
            retry_num: AtomicUsize::new(0),
            data: ArcSwap::from(Arc::new(None)),
        }
    }
    pub fn new_from(flag: usize, retry_num: usize, data: Option<V>) -> WriteVersionValue<V> {
        WriteVersionValue {
            flag: AtomicUsize::new(flag),
            retry_num: AtomicUsize::new(retry_num),
            data: ArcSwap::from(Arc::new(data)),
        }
    }
}

pub struct StaticMVHashMap<K, V> {
    data: HashMap<K, BTreeMap<Version, WriteVersionValue<V>>>,
}

pub struct DynamicMVHashMap<K, V> {
    is_empty: AtomicBool,
    // data: Arc<DashMap<K, HashMap<Version, WriteVersionValue<V>>>>,
    data: Arc<DashMap<K, BTreeMap<Version, WriteVersionValue<V>>>>,
}

pub struct MultiVersionHashMap<K, V> {
    s_mvhashmap: StaticMVHashMap<K, V>,
    d_mvhashmap: DynamicMVHashMap<K, V>,
}

impl<K: PartialOrd + Send + Clone + Hash + Eq, V: Hash + Clone + Send + Sync> MultiVersionHashMap<K, V> {
    pub fn new() -> MultiVersionHashMap<K, V> {
        MultiVersionHashMap {
            s_mvhashmap: StaticMVHashMap::new(),
            d_mvhashmap: DynamicMVHashMap::new(),
        }
    }

    pub fn new_from_parallel(possible_writes: Vec<(K, Version)>) -> (usize, MultiVersionHashMap<K, V>) {
        let (max_dependency_length, s_mvhashmap) = StaticMVHashMap::new_from_parallel(possible_writes);
        let d_mvhashmap = DynamicMVHashMap::new();
        (max_dependency_length, MultiVersionHashMap{ s_mvhashmap, d_mvhashmap })
    }

    // For the entry of same verison/txn_id, it should only appear in only one mvhashmap since a write is either estimated or non-estimated
    // Return the higher version data ranked by version, if read data from both mvhashmaps
    // If read both dependency and data, return the higher version
    pub fn read(&self, key: &K, version: Version) -> Result<(Option<V>, Version, usize), Option<(Version, usize)>> {
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

    pub fn read_from_static(&self, key: &K, version: Version) -> Result<(Option<V>, Version, usize), Option<(Version, usize)>> {
        self.s_mvhashmap.read(key, version)
    }

    pub fn read_from_dynamic(&self, key: &K, version: Version) -> Result<(Option<V>, Version, usize), Option<(Version, usize)>> {
        self.d_mvhashmap.read(key, version)
    }

    pub fn write_to_static(&self, key: &K, version: Version, retry_num: usize, data: Option<V>) -> Result<(), ()> {
        self.s_mvhashmap.write(key, version, retry_num, data)
    }

    pub fn write_to_dynamic(&self, key: &K, version: Version, retry_num: usize, data: Option<V>) -> Result<(), ()> {
        self.d_mvhashmap.write(key, version, retry_num, data)
    }

    pub fn set_dirty_to_static(&self, key: &K, version: Version, retry_num: usize) -> Result<(), ()> {
        self.s_mvhashmap.set_dirty(key, version, retry_num)
    }

    pub fn set_dirty_to_dynamic(&self, key: &K, version: Version, retry_num: usize) -> Result<(), ()> {
        self.d_mvhashmap.set_dirty(key, version, retry_num)
    }

    pub fn skip(&self, key: &K, version: Version, retry_num: usize) -> Result<(), ()> {
        self.s_mvhashmap.skip(key, version, retry_num)
    }

    pub fn skip_if_not_set(&self, key: &K, version: Version, retry_num: usize) -> Result<(), ()> {
        self.s_mvhashmap.skip_if_not_set(key, version, retry_num)
    }

    // returns (max depth, avg depth) of dynamic hashmap
    pub fn get_depth(&self) -> ((usize, usize), (usize, usize)) {
        let s_depths = self.s_mvhashmap.get_depth();
        let d_depths = self.d_mvhashmap.get_depth();
        return (s_depths, d_depths);
    }
}

impl<K: Hash + Clone + Eq, V: Hash + Clone> DynamicMVHashMap<K, V> {
    pub fn new() -> DynamicMVHashMap<K, V> {
        DynamicMVHashMap {
            is_empty: AtomicBool::new(true),
            data: Arc::new(DashMap::new()),
        }
    }

    pub fn write(&self, key: &K, version: Version, retry_num: usize, data: Option<V>) -> Result<(), ()> {
        if self.is_empty.load(Ordering::SeqCst) {
            self.is_empty.store(false, Ordering::SeqCst);
        }
        let mut map = self.data.entry(key.clone()).or_insert(BTreeMap::new());
        map.insert(version, WriteVersionValue::new_from(FLAG_DONE, retry_num, data));

        Ok(())
    }

    pub fn set_dirty(&self, key: &K, version: Version, retry_num: usize) -> Result<(), ()> {
        if self.is_empty.load(Ordering::SeqCst) {
            self.is_empty.store(false, Ordering::SeqCst);
        }
        let mut map = self.data.entry(key.clone()).or_insert(BTreeMap::new());
        map.insert(version, WriteVersionValue::new_from(FLAG_DIRTY, retry_num, None));

        Ok(())
    }

    // read when using Btree, may return Ok((Option<V>, Version, retry_num)), Err(Some(Version)) or Err(None)
    pub fn read(&self, key: &K, version: Version) -> Result<(Option<V>, Version, usize), Option<(Version, usize)>> {
        if self.is_empty.load(Ordering::SeqCst) {
            return Err(None);
        }
        match self.data.get(key) {
            Some(tree) => {
                // Find the dependency
                let mut iter = tree.range(0..version);
                while let Some((entry_key, entry_val)) = iter.next_back() {
                    if *entry_key < version {
                        let flag = entry_val.flag.load(Ordering::SeqCst);

                        // If we are to skip this entry, pick the next one
                        if flag == FLAG_DIRTY {
                            continue;
                        }

                        // The entry is populated so return its contents
                        if flag == FLAG_DONE {
                            return Ok(((**entry_val.data.load()).clone(), *entry_key, entry_val.retry_num.load(Ordering::SeqCst)));
                        }

                        unreachable!();
                    }
                }
                return Err(None);
            },
            None => {
                return Err(None);
            }
        }
    }

    // returns (max depth, avg depth) of dynamic hashmap
    pub fn get_depth(&self) -> (usize, usize) {
        if self.is_empty.load(Ordering::SeqCst) {
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
        return (max, sum/self.data.len());
    }
}

impl<K: Hash + Clone + Eq, V: Hash + Clone> StaticMVHashMap<K, V> {
    pub fn new() -> StaticMVHashMap<K, V> {
        StaticMVHashMap {
            data: HashMap::new(),
        }
    }

    pub fn new_from(possible_writes: Vec<(K, Version)>) -> (usize, StaticMVHashMap<K, V>) {
        let mut map: HashMap<K, BTreeMap<Version, WriteVersionValue<V>>> = HashMap::new();
        for (key, version) in possible_writes.into_iter() {
            map.entry(key)
                .or_default()
                .insert(version, WriteVersionValue::new());
        }
        (
            map.values()
                .fold(0, |max_depth, map| max(max_depth, map.len())),
                StaticMVHashMap { data: map },
        )
    }

    pub fn get_change_set(&self) -> Vec<(K, Option<V>)> {
        let mut change_set = Vec::with_capacity(self.data.len());
        for (k, _) in self.data.iter() {
            let (val, _, _) = self.read(k, usize::MAX).unwrap();
            change_set.push((k.clone(), val.clone()));
        }
        change_set
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.data.contains_key(key)
    }

    pub fn write(&self, key: &K, version: Version, retry_num: usize, data: Option<V>) -> Result<(), ()> {
        let entry = self
            .data
            .get(key)
            .ok_or_else(|| ())?
            .get(&version)
            .ok_or_else(|| ())?;

        entry.data.store(Arc::new(data));
        entry.retry_num.store(retry_num, Ordering::SeqCst);
        entry.flag.store(FLAG_DONE, Ordering::SeqCst);
        Ok(())
    }

    pub fn skip_if_not_set(&self, key: &K, version: Version, retry_num: usize) -> Result<(), ()> {
        let entry = self
            .data
            .get(key)
            .ok_or_else(|| ())?
            .get(&version)
            .ok_or_else(|| ())?;

        let flag = entry.flag.load(Ordering::SeqCst);
        let retry = entry.retry_num.load(Ordering::SeqCst);
        if (retry < retry_num) || (retry == retry_num && flag != FLAG_DONE) {
            entry.flag.store(FLAG_SKIP, Ordering::SeqCst);
            entry.retry_num.store(retry_num, Ordering::SeqCst);
        }

        Ok(())
    }

    pub fn skip(&self, key: &K, version: Version, retry_num: usize) -> Result<(), ()> {
        let entry = self
            .data
            .get(key)
            .ok_or_else(|| ())?
            .get(&version)
            .ok_or_else(|| ())?;

        entry.flag.store(FLAG_SKIP, Ordering::SeqCst);
        entry.retry_num.store(retry_num, Ordering::SeqCst);
        Ok(())
    }

    pub fn set_dirty(&self, key: &K, version: Version, retry_num: usize) -> Result<(), ()> {
        let entry = self
            .data
            .get(key)
            .ok_or_else(|| ())?
            .get(&version)
            .ok_or_else(|| ())?;

        entry.flag.store(FLAG_DIRTY, Ordering::SeqCst);
        entry.retry_num.store(retry_num, Ordering::SeqCst);
        Ok(())
    }

    pub fn read(&self, key: &K, version: Version) -> Result<(Option<V>, Version, usize), Option<(Version, usize)>> {
        // Get the smaller key
        let tree = self.data.get(key).ok_or_else(|| None)?;

        let mut iter = tree.range(0..version);

        while let Some((entry_key, entry_val)) = iter.next_back() {
            if *entry_key < version {
                let flag = entry_val.flag.load(Ordering::SeqCst);
                let retry = entry_val.retry_num.load(Ordering::SeqCst);

                // Return this key, must wait.
                if flag == FLAG_UNASSIGNED {
                    return Err(Some((*entry_key, retry)));
                }

                // If we are to skip this entry, pick the next one
                if flag == FLAG_SKIP || flag == FLAG_DIRTY {
                    continue;
                }

                // The entry is populated so return its contents
                if flag == FLAG_DONE {
                    return Ok(((**entry_val.data.load()).clone(), *entry_key, retry));
                }

                unreachable!();
            }
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
        return (max, sum/self.data.len());
    }
}

impl<K, V> StaticMVHashMap<K, V>
where
    K: PartialOrd + Send + Clone + Hash + Eq,
    V: Send + Sync,
{
    fn split_merge(
        num_cpus: usize,
        num: usize,
        split: Vec<(K, Version)>,
    ) -> (usize, HashMap<K, BTreeMap<Version, WriteVersionValue<V>>>) {
        if ((2 << num) > num_cpus) || split.len() < 1000 {
            let mut data = HashMap::new();
            let mut max_len = 0;
            for (path, version) in split.into_iter() {
                let place = data.entry(path).or_insert(BTreeMap::new());
                place.insert(version, WriteVersionValue::new());
                max_len = max(max_len, place.len());
            }
            (max_len, data)
        } else {
            let pivot_address = split[split.len() / 2].0.clone();
            let (left, right): (Vec<_>, Vec<_>) =
                split.into_iter().partition(|(p, _)| *p < pivot_address);
            let ((m0, mut left_map), (m1, right_map)) = rayon::join(
                || Self::split_merge(num_cpus, num + 1, left),
                || Self::split_merge(num_cpus, num + 1, right),
            );
            left_map.extend(right_map);
            (max(m0, m1), left_map)
        }
    }

    pub fn new_from_parallel(possible_writes: Vec<(K, Version)>) -> (usize, StaticMVHashMap<K, V>) {
        let num_cpus = num_cpus::get();

        let (max_dependency_depth, data) = Self::split_merge(num_cpus, 0, possible_writes);
        (max_dependency_depth, StaticMVHashMap { data })
    }
}
#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn create_write_read_placeholder_struct() {
        let ap1 = b"/foo/b".to_vec();
        let ap2 = b"/foo/c".to_vec();

        let data = vec![(ap1.clone(), 10), (ap2.clone(), 10), (ap2.clone(), 20)];

        let (max_dep, mvtbl) = StaticMVHashMap::new_from(data);

        assert_eq!(2, max_dep);

        assert_eq!(2, mvtbl.len());

        // Reads that should go the the DB return Err(None)
        let r1 = mvtbl.read(&ap1, 5);
        assert_eq!(Err(None), r1);

        // Reads at a version return the previous versions, not this
        // version.
        let r1 = mvtbl.read(&ap1, 10);
        assert_eq!(Err(None), r1);

        // Check reads into non-ready structs return the Err(entry)

        // Reads at a higher version return the previous version
        let r1 = mvtbl.read(&ap1, 15);
        assert_eq!(Err(Some(10)), r1);

        // Writes populate the entry
        mvtbl.write(&ap1, 10, 1, Some(vec![0, 0, 0])).unwrap();

        // Subsequent higher reads read this entry
        let r1 = mvtbl.read(&ap1, 15);
        assert_eq!(Ok((Some(vec![0, 0, 0]), 10, 1)), r1);

        // Set skip works
        assert!(mvtbl.skip(&ap1, 20, 1).is_err());

        // Higher reads skip this entry
        let r1 = mvtbl.read(&ap1, 25);
        assert_eq!(Ok((Some(vec![0, 0, 0]), 10, 1)), r1);
    }

    #[test]
    fn create_write_read_placeholder_dyn_struct() {
        let ap1 = b"/foo/b".to_vec();
        let ap2 = b"/foo/c".to_vec();

        let data = vec![(ap1.clone(), 10), (ap2.clone(), 10), (ap2.clone(), 20), (ap2.clone(), 30)];

        let mvtbl = DynamicMVHashMap::new();

        // Reads that should go the the DB return Err(None)
        let r1 = mvtbl.read(&ap1, 10);
        assert_eq!(Err(None), r1);

        let r1 = mvtbl.read(&ap2, 20);
        assert_eq!(Err(None), r1);

        // Writes populate the entry
        mvtbl.write(&ap1, 10, 1, Some(vec![0, 0, 0])).unwrap();
        mvtbl.write(&ap2, 10, 1, Some(vec![0, 0, 1])).unwrap();
        mvtbl.write(&ap2, 20, 1, Some(vec![0, 0, 2])).unwrap();

        // Reads the same version should return None
        let r1 = mvtbl.read(&ap1, 10);
        assert_eq!(Err(None), r1);

        let r1 = mvtbl.read(&ap2, 10);
        assert_eq!(Err(None), r1);

        // Subsequent higher reads read this entry
        let r1 = mvtbl.read(&ap1, 15);
        assert_eq!(Ok((Some(vec![0, 0, 0]), 10, 1)), r1);

        let r1 = mvtbl.read(&ap2, 15);
        assert_eq!(Ok((Some(vec![0, 0, 1]), 10, 1)), r1);

        let r1 = mvtbl.read(&ap2, 25);
        assert_eq!(Ok((Some(vec![0, 0, 2]), 20, 1)), r1);

        // Set dirty
        assert!(!mvtbl.set_dirty(&ap2, 20, 1).is_err());

        // Higher reads mark this entry as dependency
        let r1 = mvtbl.read(&ap2, 25);
        assert_eq!(Err(Some(20)), r1);

        // Write a higher version and reads that
        mvtbl.write(&ap2, 30, 1, Some(vec![0, 0, 3])).unwrap();
        let r1 = mvtbl.read(&ap2, 35);
        assert_eq!(Ok((Some(vec![0, 0, 3]), 30, 1)), r1);

        let r1 = mvtbl.read(&ap2, 25);
        assert_eq!(Err(Some(20)), r1);

        let r1 = mvtbl.read(&ap2, 15);
        assert_eq!(Ok((Some(vec![0, 0, 1]), 10, 1)), r1);
    }
}
