// Copyright (c) The Diem Core Contributors
// SPDX-License-Identifier: Apache-2.0

use crate::{
    fat_type::{FatStructType, FatType},
    resolver::Resolver,
};
use anyhow::{anyhow, bail, Result};
use diem_state_view::StateView;
use diem_types::{
    access_path::AccessPath, account_address::AccountAddress, account_state::AccountState,
    contract_event::ContractEvent,
};
use move_binary_format::{
    errors::{Location, PartialVMError, PartialVMResult, VMResult},
    file_format::{Ability, AbilitySet},
};
use move_core_types::{
    identifier::Identifier,
    language_storage::{ModuleId, StructTag, TypeTag},
    value::{MoveStruct, MoveValue},
};
use move_vm_runtime::data_cache::MoveStorage;
use std::{
    borrow::Cow,
    collections::btree_map::BTreeMap,
    convert::TryInto,
    fmt::{Display, Formatter},
};

mod fat_type;
mod module_cache;
mod resolver;

#[derive(Debug)]
pub struct AnnotatedAccountStateBlob(BTreeMap<StructTag, AnnotatedMoveStruct>);

#[derive(Clone, Debug)]
pub struct AnnotatedMoveStruct {
    pub abilities: AbilitySet,
    pub type_: StructTag,
    pub value: Vec<(Identifier, AnnotatedMoveValue)>,
}

/// AnnotatedMoveValue is a fully expanded version of on chain Move data. This should only be used
/// for debugging/client purpose right now and just for a better visualization of on chain data. In
/// the long run, we would like to transform this struct to a Json value so that we can have a cross
/// platform interpretation of the on chain data.
#[derive(Clone, Debug)]
pub enum AnnotatedMoveValue {
    U8(u8),
    U64(u64),
    U128(u128),
    Bool(bool),
    Address(AccountAddress),
    Vector(TypeTag, Vec<AnnotatedMoveValue>),
    Bytes(Vec<u8>),
    Struct(AnnotatedMoveStruct),
}

impl AnnotatedMoveValue {
    pub fn get_type(&self) -> TypeTag {
        use AnnotatedMoveValue::*;
        match self {
            U8(_) => TypeTag::U8,
            U64(_) => TypeTag::U64,
            U128(_) => TypeTag::U128,
            Bool(_) => TypeTag::Bool,
            Address(_) => TypeTag::Address,
            Vector(t, _) => t.clone(),
            Bytes(_) => TypeTag::Vector(Box::new(TypeTag::U8)),
            Struct(s) => TypeTag::Struct(s.type_.clone()),
        }
    }
}

pub struct MoveValueAnnotator<'a> {
    cache: Resolver<'a>,
    _data_view: &'a dyn MoveStorage,
}

impl<'a> MoveValueAnnotator<'a> {
    pub fn new(view: &'a dyn MoveStorage) -> Self {
        Self {
            cache: Resolver::new(view, true),
            _data_view: view,
        }
    }

    pub fn new_no_stdlib(view: &'a dyn MoveStorage) -> Self {
        Self {
            cache: Resolver::new(view, false),
            _data_view: view,
        }
    }

    pub fn get_resource_bytes(&self, addr: &AccountAddress, tag: &StructTag) -> Option<Vec<u8>> {
        self.cache
            .state
            .get_resource(addr, tag)
            .map_err(|e: PartialVMError| e.finish(Location::Undefined).into_vm_status())
            .ok()?
            .map(|bytes| bytes.into_owned())
    }

    pub fn view_access_path(
        &self,
        access_path: AccessPath,
        blob: &[u8],
    ) -> Result<AnnotatedMoveStruct> {
        match access_path.get_struct_tag() {
            Some(tag) => self.view_resource(&tag, blob),
            None => bail!("Bad resource access path"),
        }
    }

    pub fn view_resource(&self, tag: &StructTag, blob: &[u8]) -> Result<AnnotatedMoveStruct> {
        let ty = self.cache.resolve_struct(tag)?;
        let struct_def = (&ty)
            .try_into()
            .map_err(|e: PartialVMError| e.finish(Location::Undefined).into_vm_status())?;
        let move_struct = MoveStruct::simple_deserialize(blob, &struct_def)?;
        self.annotate_struct(&move_struct, &ty)
    }

    pub fn view_contract_event(&self, event: &ContractEvent) -> Result<AnnotatedMoveValue> {
        let ty = self.cache.resolve_type(event.type_tag())?;
        let move_ty = (&ty)
            .try_into()
            .map_err(|e: PartialVMError| e.finish(Location::Undefined).into_vm_status())?;

        let move_value = MoveValue::simple_deserialize(event.event_data(), &move_ty)?;
        self.annotate_value(&move_value, &ty)
    }

    pub fn view_account_state(&self, state: &AccountState) -> Result<AnnotatedAccountStateBlob> {
        let mut output = BTreeMap::new();
        for (k, v) in state.iter() {
            let tag = match AccessPath::new(AccountAddress::random(), k.to_vec()).get_struct_tag() {
                Some(t) => t,
                None => {
                    println!("Uncached AccessPath: {:?}", k);
                    continue;
                }
            };
            let ty = self.cache.resolve_struct(&tag)?;
            let struct_def = (&ty)
                .try_into()
                .map_err(|e: PartialVMError| e.finish(Location::Undefined).into_vm_status())?;

            let move_struct = MoveStruct::simple_deserialize(v.as_slice(), &struct_def)?;
            output.insert(
                ty.struct_tag()
                    .map_err(|e| e.finish(Location::Undefined).into_vm_status())
                    .unwrap(),
                self.annotate_struct(&move_struct, &ty)?,
            );
        }
        Ok(AnnotatedAccountStateBlob(output))
    }

    fn annotate_struct(
        &self,
        move_struct: &MoveStruct,
        ty: &FatStructType,
    ) -> Result<AnnotatedMoveStruct> {
        let struct_tag = ty
            .struct_tag()
            .map_err(|e| e.finish(Location::Undefined).into_vm_status())?;
        let field_names = self.cache.get_field_names(ty)?;
        let mut annotated_fields = vec![];
        for (ty, v) in ty.layout.iter().zip(move_struct.fields().iter()) {
            annotated_fields.push(self.annotate_value(v, ty)?);
        }
        Ok(AnnotatedMoveStruct {
            abilities: ty.abilities.0,
            type_: struct_tag,
            value: field_names
                .into_iter()
                .zip(annotated_fields.into_iter())
                .collect(),
        })
    }

    fn annotate_value(&self, value: &MoveValue, ty: &FatType) -> Result<AnnotatedMoveValue> {
        Ok(match (value, ty) {
            (MoveValue::Bool(b), FatType::Bool) => AnnotatedMoveValue::Bool(*b),
            (MoveValue::U8(i), FatType::U8) => AnnotatedMoveValue::U8(*i),
            (MoveValue::U64(i), FatType::U64) => AnnotatedMoveValue::U64(*i),
            (MoveValue::U128(i), FatType::U128) => AnnotatedMoveValue::U128(*i),
            (MoveValue::Address(a), FatType::Address) => AnnotatedMoveValue::Address(*a),
            (MoveValue::Vector(a), FatType::Vector(ty)) => match ty.as_ref() {
                FatType::U8 => AnnotatedMoveValue::Bytes(
                    a.iter()
                        .map(|v| match v {
                            MoveValue::U8(i) => Ok(*i),
                            _ => Err(anyhow!("unexpected value type")),
                        })
                        .collect::<Result<_>>()?,
                ),
                _ => AnnotatedMoveValue::Vector(
                    ty.type_tag().unwrap(),
                    a.iter()
                        .map(|v| self.annotate_value(v, ty.as_ref()))
                        .collect::<Result<_>>()?,
                ),
            },
            (MoveValue::Struct(s), FatType::Struct(ty)) => {
                AnnotatedMoveValue::Struct(self.annotate_struct(s, ty.as_ref())?)
            }
            _ => {
                return Err(anyhow!(
                    "Cannot annotate value {:?} with type {:?}",
                    value,
                    ty
                ))
            }
        })
    }
}

fn write_indent(f: &mut Formatter, indent: u64) -> std::fmt::Result {
    for _i in 0..indent {
        write!(f, " ")?;
    }
    Ok(())
}

fn pretty_print_value(
    f: &mut Formatter,
    value: &AnnotatedMoveValue,
    indent: u64,
) -> std::fmt::Result {
    match value {
        AnnotatedMoveValue::Bool(b) => write!(f, "{}", b),
        AnnotatedMoveValue::U8(v) => write!(f, "{}u8", v),
        AnnotatedMoveValue::U64(v) => write!(f, "{}", v),
        AnnotatedMoveValue::U128(v) => write!(f, "{}u128", v),
        AnnotatedMoveValue::Address(a) => write!(f, "{}", a.short_str_lossless()),
        AnnotatedMoveValue::Vector(_, v) => {
            writeln!(f, "[")?;
            for value in v.iter() {
                write_indent(f, indent + 4)?;
                pretty_print_value(f, value, indent + 4)?;
                writeln!(f, ",")?;
            }
            write_indent(f, indent)?;
            write!(f, "]")
        }
        AnnotatedMoveValue::Bytes(v) => write!(f, "{}", hex::encode(&v)),
        AnnotatedMoveValue::Struct(s) => pretty_print_struct(f, s, indent),
    }
}

fn pretty_print_struct(
    f: &mut Formatter,
    value: &AnnotatedMoveStruct,
    indent: u64,
) -> std::fmt::Result {
    pretty_print_ability_modifiers(f, value.abilities)?;
    writeln!(f, "{} {{", value.type_)?;
    for (field_name, v) in value.value.iter() {
        write_indent(f, indent + 4)?;
        write!(f, "{}: ", field_name)?;
        pretty_print_value(f, v, indent + 4)?;
        writeln!(f)?;
    }
    write_indent(f, indent)?;
    write!(f, "}}")
}

fn pretty_print_ability_modifiers(f: &mut Formatter, abilities: AbilitySet) -> std::fmt::Result {
    for ability in abilities {
        match ability {
            Ability::Copy => write!(f, "copy ")?,
            Ability::Drop => write!(f, "drop ")?,
            Ability::Store => write!(f, "store ")?,
            Ability::Key => write!(f, "key ")?,
        }
    }
    Ok(())
}

impl Display for AnnotatedMoveValue {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        pretty_print_value(f, self, 0)
    }
}

impl Display for AnnotatedMoveStruct {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        pretty_print_struct(f, self, 0)
    }
}

impl Display for AnnotatedAccountStateBlob {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        writeln!(f, "{{")?;
        for v in self.0.values() {
            write!(f, "{}", v)?;
            writeln!(f, ",")?;
        }
        writeln!(f, "}}")
    }
}

#[derive(Default)]
pub struct NullStateView();

impl StateView for NullStateView {
    fn get(&self, _access_path: &AccessPath) -> Result<Option<Vec<u8>>> {
        Err(anyhow!("No data"))
    }

    fn is_genesis(&self) -> bool {
        false
    }
}

impl MoveStorage for NullStateView {
    fn get_module(&self, _module_id: &ModuleId) -> VMResult<Option<Vec<u8>>> {
        Ok(None)
    }

    fn get_resource(
        &self,
        _address: &AccountAddress,
        _tag: &StructTag,
    ) -> PartialVMResult<Option<Cow<[u8]>>> {
        Ok(None)
    }
}
