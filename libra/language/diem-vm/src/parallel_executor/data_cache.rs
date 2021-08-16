// Copyright (c) The Diem Core Contributors
// SPDX-License-Identifier: Apache-2.0

use crate::parallel_executor::scheduler::ReadDescriptor;
use anyhow::{anyhow, Result};
use diem_state_view::StateView;
use diem_types::{
    access_path::AccessPath,
    transaction::TransactionOutput,
    vm_status::StatusCode,
    write_set::{WriteOp, WriteSet},
};
use move_binary_format::errors::*;
use move_core_types::{
    account_address::AccountAddress,
    language_storage::{ModuleId, StructTag},
};
use move_vm_runtime::data_cache::MoveStorage;
use mvhashmap::MultiVersionHashMap;
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    convert::AsRef,
    thread,
    time::Duration,
};

pub struct VersionedDataCache(MultiVersionHashMap<AccessPath, Vec<u8>>);

pub struct VersionedStateView<'view> {
    version: usize,
    base_view: &'view dyn StateView,
    placeholder: &'view VersionedDataCache,
    reads: HashMap<
        AccessPath,
        (
            Option<(usize, usize)>,
            PartialVMResult<Option<Cow<'view, [u8]>>>,
        ),
    >,
}

const ONE_MILLISEC: Duration = Duration::from_millis(10);

impl VersionedDataCache {
    pub fn new(write_sequence: Vec<(AccessPath, usize)>) -> (usize, Self) {
        let (max_dependency_length, mv_hashmap) =
            MultiVersionHashMap::new_from_parallel(write_sequence);
        (max_dependency_length, VersionedDataCache(mv_hashmap))
    }

    // returns (max depth, avg depth) of static and dynamic hashmap
    pub fn get_depth(&self) -> ((usize, usize), (usize, usize)) {
        self.as_ref().get_depth()
    }

    pub fn set_skip_all(
        &self,
        version: usize,
        retry_num: usize,
        estimated_writes: &HashSet<AccessPath>,
    ) {
        // Put skip in all entires.
        for w in estimated_writes {
            // It should be safe to unwrap here since the MVMap was construted using
            // this estimated writes. If not it is a bug.
            self.as_ref().skip(&w, version, retry_num).unwrap();
        }
    }

    fn set_dirty_static(&self, version: usize, retry_num: usize, writes: &HashSet<AccessPath>) {
        for w in writes {
            // It should be safe to unwrap here since the MVMap was construted using
            // this estimated writes. If not it is a bug.
            self.as_ref()
                .set_dirty_to_static(w, version, retry_num)
                .unwrap();
        }
    }

    fn set_dirty_dynamic(&self, version: usize, retry_num: usize, writes: &HashSet<AccessPath>) {
        for w in writes {
            // It should be safe to unwrap here since the MVMap was dynamic
            self.as_ref()
                .set_dirty_to_dynamic(w, version, retry_num)
                .unwrap();
        }
    }

    pub fn mark_dirty(
        &self,
        version: usize,
        retry_num: usize,
        estimated_writes: &HashSet<AccessPath>,
        actual_writes: &WriteSet,
    ) {
        let mut non_estimated_writes: HashSet<AccessPath> = HashSet::new();
        for (k, _) in actual_writes {
            if !estimated_writes.contains(k) {
                non_estimated_writes.insert(k.clone());
            }
        }
        self.set_dirty_static(version, retry_num, estimated_writes);
        self.set_dirty_dynamic(version, retry_num, &non_estimated_writes);
    }

    // Apply the writes to both static and dynamic mvhashmap
    pub fn apply_output(
        &self,
        output: &TransactionOutput,
        version: usize,
        retry_num: usize,
        estimated_writes: &HashSet<AccessPath>,
    ) -> Result<(), ()> {
        // First get all non-estimated writes
        if !output.status().is_discarded() {
            for (k, v) in output.write_set() {
                let val = match v {
                    WriteOp::Deletion => None,
                    WriteOp::Value(data) => Some(data.clone()),
                };
                // Write estimated writes to static mvhashmap, and write non-estimated ones to dynamic mvhashmap
                if estimated_writes.contains(k) {
                    self.as_ref()
                        .write_to_static(k, version, retry_num, val)
                        .unwrap();
                } else {
                    self.as_ref()
                        .write_to_dynamic(k, version, retry_num, val)
                        .unwrap();
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
            reads: HashMap::new(),
        }
    }

    // Drains the reads.
    pub fn drain_reads(&self) -> Vec<ReadDescriptor> {
        self.reads
            .drain()
            .map(|(path, (version_and_retry_num, _))| {
                ReadDescriptor::new(path, version_and_retry_num)
            })
            .collect()
    }

    // Return Some(version) when reading access_path is blocked by txn w. version, otherwise return None.
    // Read from both static and dynamic mvhashmap.
    pub fn read_blocking_dependency(
        &self,
        access_path: &AccessPath,
        read_version: usize,
    ) -> Option<usize> {
        let read = self.placeholder.as_ref().read(access_path, read_version);
        if let Err(Some((version, _))) = read {
            return Some(version);
        }
        return None;
    }

    // Return a (version, retry_num) if the read returns a value or dependency, otherwise return None.
    pub fn read_version_and_retry_num(&self, access_path: &AccessPath) -> Option<(usize, usize)> {
        // reads may return Ok((Option<Vec<u8>>, version, retry_num) when there is value and no read dependency
        // or Err(Some<Version>) when there is read dependency
        // or Err(None) when there is no value and no read dependency
        match self.placeholder.as_ref().read(access_path, self.version) {
            Ok((_, version, retry_num)) => Some((version, retry_num)),
            Err(Some((version, retry_num))) => Some((version, retry_num)),
            Err(None) => None,
        }
    }

    fn get_bytes_ref(&self, access_path: &AccessPath) -> PartialVMResult<Option<Cow<[u8]>>> {
        let mut loop_iterations = 0;
        loop {
            if let Some((_, ret)) = self.reads.get(access_path) {
                return ret.clone();
            }

            let read = self.placeholder.as_ref().read(access_path, self.version);

            // Go to the Database
            if let Err(None) = read {
                let ret = self
                    .base_view
                    .get(access_path)
                    .map(|opt| opt.map(Cow::from))
                    .map_err(|_| PartialVMError::new(StatusCode::STORAGE_ERROR));
                self.reads.insert(access_path.clone(), (None, ret.clone()));
                return ret.clone();
            }

            // Read is a success
            if let Ok((data, version, retry_num)) = read {
                let ret = Ok(data.map(Cow::from));
                self.reads.insert(
                    access_path.clone(),
                    (Some((version, retry_num)), ret.clone()),
                );
                return ret.clone();
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
