// Copyright (c) The Diem Core Contributors
// SPDX-License-Identifier: Apache-2.0

use anyhow::{anyhow, Result};
use diem_state_view::StateView;
use diem_types::{
    access_path::AccessPath, transaction::TransactionOutput, vm_status::StatusCode,
    write_set::WriteOp,
};
use move_binary_format::errors::*;
use move_core_types::{
    account_address::AccountAddress,
    language_storage::{ModuleId, StructTag},
};
use move_vm_runtime::data_cache::MoveStorage;
use mvhashmap::MultiVersionHashMap;
use std::{borrow::Cow, collections::HashSet, convert::AsRef, thread, time::Duration};

pub struct VersionedDataCache(MultiVersionHashMap<AccessPath, Vec<u8>>);

pub struct VersionedStateView<'view> {
    version: usize,
    base_view: &'view dyn StateView,
    placeholder: &'view VersionedDataCache,
}

const ONE_MILLISEC: Duration = Duration::from_millis(10);

impl VersionedDataCache {
    pub fn new(write_sequence: Vec<(AccessPath, usize)>) -> (usize, Self) {
        let (max_dependency_length, mv_hashmap) = MultiVersionHashMap::new_from_parallel(write_sequence);
        (max_dependency_length, VersionedDataCache(mv_hashmap))
    }

    // returns (max depth, avg depth) of static and dynamic hashmap
    pub fn get_depth(&self) -> ((usize, usize), (usize, usize)) {
        self.as_ref().get_depth()
    }

    pub fn set_skip_all(&self, version: usize, retry_num: usize, estimated_writes: HashSet<AccessPath>) {
        // Put skip in all entires.
        for w in estimated_writes {
            // It should be safe to unwrap here since the MVMap was construted using
            // this estimated writes. If not it is a bug.
            self.as_ref().skip(&w, version, retry_num).unwrap();
        }
    }

    pub fn set_dirty_to_static(&self, version: usize, retry_num: usize, write_set: HashSet<AccessPath>) {
        for w in write_set {
            // It should be safe to unwrap here since the MVMap was construted using
            // this estimated writes. If not it is a bug.
            self.as_ref().set_dirty_to_static(&w, version, retry_num).unwrap();
        }
    }

    pub fn set_dirty_to_dynamic(&self, version: usize, retry_num: usize, write_set: HashSet<AccessPath>) {
        for w in write_set {
            // It should be safe to unwrap here since the MVMap was dynamic
            self.as_ref().set_dirty_to_dynamic(&w, version, retry_num).unwrap();
        }
    }

    // Apply the writes to both static and dynamic mvhashmap
    pub fn apply_output(
        &self,
        output: &TransactionOutput,
        version: usize,
        retry_num: usize,
        estimated_writes: impl Iterator<Item = AccessPath>,
    ) -> Result<(), ()> {
        // First get all non-estimated writes
        let estimated_writes: HashSet<AccessPath> = estimated_writes.collect();
        if !output.status().is_discarded() {
            for (k, v) in output.write_set() {
                let val = match v {
                    WriteOp::Deletion => None,
                    WriteOp::Value(data) => Some(data.clone()),
                };
                // Write estimated writes to static mvhashmap, and write non-estimated ones to dynamic mvhashmap
                if estimated_writes.contains(k) {
                    self.as_ref().write_to_static(k, version, retry_num, val).unwrap();
                } else {
                    self.as_ref().write_to_dynamic(k, version, retry_num, val).unwrap();
                }
            }

            // If any entries are not updated, write a 'skip' flag into them
            for w in estimated_writes {
                // It should be safe to unwrap here since the MVMap was construted using
                // this estimated writes. If not it is a bug.
                self.as_ref()
                    .skip_if_not_set(&w, version, retry_num)
                    .expect("Entry must exist.");
            }
        } else {
            self.set_skip_all(version, retry_num, estimated_writes);
        }
        Ok(())
    }
}

impl AsRef<MultiVersionHashMap<AccessPath, Vec<u8>>> for VersionedDataCache {
    fn as_ref(&self) -> &MultiVersionHashMap<AccessPath, Vec<u8>> {
        &self.0
    }
}

impl<'view> VersionedStateView<'view> {
    pub fn new(
        version: usize,
        base_view: &'view dyn StateView,
        placeholder: &'view VersionedDataCache,
    ) -> VersionedStateView<'view> {
        VersionedStateView {
            version,
            base_view,
            placeholder,
        }
    }

    // Return Some(version) when reading access_path is blocked by transaction of id=version, otherwise return None
    // Read from both static and dynamic mvhashmap
    pub fn will_read_from_both_block_return_version(&self, access_path: &AccessPath) -> Option<usize> {
        let read = self.placeholder.as_ref().read(access_path, self.version);
        if let Err(Some((version, _))) = read {
            return Some(version);
        }
        return None;
    }

    fn get_bytes_ref(&self, access_path: &AccessPath) -> PartialVMResult<Option<Cow<[u8]>>> {
        let mut loop_iterations = 0;
        loop {
            let read = self.placeholder.as_ref().read(access_path, self.version);

            // Go to the Database
            if let Err(None) = read {
                return self
                    .base_view
                    .get(access_path)
                    .map(|opt| opt.map(Cow::from))
                    .map_err(|_| PartialVMError::new(StatusCode::STORAGE_ERROR));
            }

            // Read is a success
            if let Ok((data, _, _)) = read {
                return Ok(data.map(Cow::from));
            }

            loop_iterations += 1;
            if loop_iterations < 500 {
                ::std::hint::spin_loop();
            } else {
                thread::sleep(ONE_MILLISEC);
            }
        }
    }

    fn get_bytes(&self, access_path: &AccessPath) -> PartialVMResult<Option<Vec<u8>>> {
        self.get_bytes_ref(access_path)
            .map(|opt| opt.map(Cow::into_owned))
    }

    // Return a (version, retry_num) pair, if the read returns a value or dependency
    // Otherwise return None
    // flag=true if no read-dependency
    pub fn get_single_read_version(&self, access_path: &AccessPath) -> (bool, Option<(usize, usize)>) {
        // reads may return Ok((Option<Vec<u8>>, version, retry_num) when there is value and no read dependency
        // or Err(Some<Version>) when there is read dependency
        // or Err(None) when there is no value and no read dependency
        match self.placeholder.as_ref().read(access_path, self.version) {
            Ok((_, version, retry_num)) => return (true, Some((version, retry_num))),
            Err(Some((version, retry_num))) => return (false, Some((version, retry_num))),
            Err(None) => return (true, None),
        }
    }

    // Return a vector of (version, retry_num) pair, for each read in the readset
    pub fn get_reads_version_vec(&self, reads: impl Iterator<Item = AccessPath>) -> Vec<(bool, Option<(usize, usize)>)> {
        let mut result_vec = Vec::new();
        for read in reads {
            result_vec.push(self.get_single_read_version(&read));
        }
        return result_vec;
    }
}

impl<'view> MoveStorage for VersionedStateView<'view> {
    fn get_module(&self, module_id: &ModuleId) -> VMResult<Option<Vec<u8>>> {
        // REVIEW: cache this?
        let ap = AccessPath::from(module_id);
        self.get_bytes(&ap)
            .map_err(|e| e.finish(Location::Undefined))
    }

    fn get_resource(
        &self,
        address: &AccountAddress,
        struct_tag: &StructTag,
    ) -> PartialVMResult<Option<Cow<[u8]>>> {
        let ap = AccessPath::new(
            *address,
            AccessPath::resource_access_vec(struct_tag.clone()),
        );
        self.get_bytes_ref(&ap)
    }
}

impl<'view> StateView for VersionedStateView<'view> {
    fn get(&self, access_path: &AccessPath) -> Result<Option<Vec<u8>>> {
        self.get_bytes(access_path)
            .map_err(|_| anyhow!("Failed to get data from VersionedStateView"))
    }

    fn is_genesis(&self) -> bool {
        self.base_view.is_genesis()
    }
}

//
// pub struct SingleThreadReadCache<'view> {
//     base_view : &'view dyn StateView,
//     cache : RefCell<HashMap<AccessPath, Option<Vec<u8>>>>,
// }
//
// impl<'view> SingleThreadReadCache<'view> {
//     pub fn new(base_view: &'view StateView) -> SingleThreadReadCache<'view> {
//         SingleThreadReadCache {
//             base_view,
//             cache : RefCell::new(HashMap::new()),
//         }
//     }
// }
//
// impl<'view> StateView for SingleThreadReadCache<'view> {
//     // Get some data either through the cache or the `StateView` on a cache miss.
//     fn get(&self, access_path: &AccessPath) -> anyhow::Result<Option<Vec<u8>>> {
//         if self.cache.borrow().contains_key(access_path) {
//             Ok(self.cache.borrow().get(access_path).unwrap().clone())
//         }
//         else {
//             let val = self.base_view.get(access_path)?;
//             self.cache.borrow_mut().insert(access_path.clone(), val.clone());
//             Ok(val)
//         }
//     }
//
//     fn multi_get(&self, _access_paths: &[AccessPath]) -> anyhow::Result<Vec<Option<Vec<u8>>>> {
//         unimplemented!()
//     }
//
//     fn is_genesis(&self) -> bool {
//         self.base_view.is_genesis()
//     }
// }
