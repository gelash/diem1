// Copyright (c) The Libra Core Contributors
// SPDX-License-Identifier: Apache-2.0

use crate::account_config::constants::MONEY_ORDER_MODULE_NAME;
use anyhow::Result;
use move_core_types::{
    move_resource::MoveResource,
};
use serde::{Deserialize, Serialize};

/// Struct that represents a CanceledMoneyOrderEvent
#[derive(Debug, Serialize, Deserialize)]
pub struct CanceledMoneyOrderEvent {
    batch_index: u64,
    order_index: u64,
}

impl CanceledMoneyOrderEvent {
    /// Return the batch_index for the canceled order.
    pub fn batch_index(&self) -> u64 {
        self.batch_index
    }

    /// Return the index of the canceled order in the status BITBIN.
    pub fn order_index(&self) -> u64 {
        self.order_index
    }

    pub fn try_from_bytes(bytes: &[u8]) -> Result<Self> {
        lcs::from_bytes(bytes).map_err(Into::into)
    }
}

impl MoveResource for CanceledMoneyOrderEvent {
    const MODULE_NAME: &'static str = MONEY_ORDER_MODULE_NAME;
    const STRUCT_NAME: &'static str = "CanceledMoneyOrderEvent";
}
