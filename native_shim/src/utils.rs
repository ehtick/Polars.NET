use std::ffi::{CStr, c_char};
use polars::frame::UniqueKeepStrategy;
use polars_arrow::ffi::ArrowArray;
use polars_arrow::ffi::{export_array_to_c,export_field_to_c};
use polars::prelude::{ArrowSchema, AsofStrategy, CsvEncoding, Expr, JoinCoalesce, JoinType, JoinValidation, MaintainOrderJoin, ParallelStrategy, SchemaRef};
use polars_arrow::datatypes::Field;
use polars_io::prelude::JsonFormat;
use polars_io::utils::sync_on_close::SyncOnCloseType;

use crate::types::{ExprContext,SchemaContext};

pub struct ArrowArrayContext {
    pub array: Box<dyn polars_arrow::array::Array>, 
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_arrow_array_free(ptr: *mut ArrowArrayContext) {
    if !ptr.is_null() {
        unsafe { let _ = Box::from_raw(ptr); }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_arrow_array_export(
    ptr: *mut ArrowArrayContext,
    out_c_array: *mut ArrowArray 
) {
    assert!(!ptr.is_null());
    assert!(!out_c_array.is_null());

    let ctx = unsafe { &*ptr };
    
    let array = ctx.array.clone(); 

    let rust_arrow_array = export_array_to_c(array);

    unsafe {
        std::ptr::write(out_c_array, rust_arrow_array);
    }
}
#[unsafe(no_mangle)]
pub extern "C" fn pl_arrow_schema_export(
    ptr: *mut ArrowArrayContext,
    out_c_schema: *mut ArrowSchema
) {
    assert!(!ptr.is_null());
    assert!(!out_c_schema.is_null());

    let ctx = unsafe { &*ptr };
    
    let dtype = ctx.array.dtype().clone();

    let field = Field::new("".into(), dtype, true);

    let rust_arrow_schema = export_field_to_c(&field);

    unsafe {
        std::ptr::write(out_c_schema as *mut _, rust_arrow_schema);
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_free_c_string(c_str: *mut std::os::raw::c_char) {
    if !c_str.is_null() {
        unsafe {
            let _ = std::ffi::CString::from_raw(c_str);
        }
    }
}

pub fn ptr_to_str<'a>(ptr: *const c_char) -> Result<&'a str, std::str::Utf8Error> {
    if ptr.is_null() { 
        panic!("Null pointer passed to ptr_to_str"); 
    }
    unsafe { CStr::from_ptr(ptr).to_str() }
}

pub(crate) unsafe fn consume_exprs_array(
    ptr: *const *mut ExprContext, 
    len: usize
) -> Vec<Expr> {
    let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
    slice.iter()
        .map(|&p| unsafe { Box::from_raw(p).inner })
        .collect()
}

pub(crate) unsafe fn ptr_to_schema_ref(ptr: *mut SchemaContext) -> Option<SchemaRef> {
    if ptr.is_null() {
        None
    } else {
        let ctx = unsafe { &*ptr };
        Some(ctx.schema.clone())
    }
}

pub(crate) fn map_jointype(code: u8) -> JoinType {
    match code {
        0 => JoinType::Inner,
        1 => JoinType::Left,
        2 => JoinType::Full, 
        3 => JoinType::Cross,
        4 => JoinType::Semi,
        5 => JoinType::Anti,
        6 => JoinType::IEJoin,
        _ => JoinType::Inner, // Default
    }
}

pub fn map_validation(code: u8) -> JoinValidation {
    match code {
        0 => JoinValidation::ManyToMany,
        1 => JoinValidation::ManyToOne,
        2 => JoinValidation::OneToMany,
        3 => JoinValidation::OneToOne,
        _ => JoinValidation::ManyToMany, // Default
    }
}

pub fn map_coalesce(code: u8) -> JoinCoalesce {
    match code {
        0 => JoinCoalesce::JoinSpecific,
        1 => JoinCoalesce::CoalesceColumns,
        2 => JoinCoalesce::KeepColumns,
        _ => JoinCoalesce::JoinSpecific, // Default
    }
}

pub fn map_maintain_order(code: u8) -> MaintainOrderJoin {
    match code {
        0 => MaintainOrderJoin::None,
        1 => MaintainOrderJoin::Left,
        2 => MaintainOrderJoin::Right,
        3 => MaintainOrderJoin::LeftRight,
        4 => MaintainOrderJoin::RightLeft,
        _ => MaintainOrderJoin::None, // Default
    }
}

pub(crate) fn map_asof_strategy(code: u8) -> AsofStrategy {
    match code {
        0 => AsofStrategy::Backward,
        1 => AsofStrategy::Forward,
        2 => AsofStrategy::Nearest,
        _ => AsofStrategy::Backward,
    }
}

// helper function: u8 -> UniqueKeepStrategy
// 0: First, 1: Last, 2: Any, 3: None
#[inline]
pub(crate) fn parse_keep_strategy(val: u8) -> UniqueKeepStrategy {
    match val {
        0 => UniqueKeepStrategy::First,
        1 => UniqueKeepStrategy::Last,
        2 => UniqueKeepStrategy::Any,
        3 => UniqueKeepStrategy::None,
        _ => UniqueKeepStrategy::First, // Default fallback
    }
}

pub(crate) fn map_parallel_strategy(code: u8) -> ParallelStrategy {
    match code {
        1 => ParallelStrategy::Columns,
        2 => ParallelStrategy::RowGroups,
        3 => ParallelStrategy::None,
        0 | _ => ParallelStrategy::Auto,
    }
}

#[inline]
pub(crate) fn map_csv_encoding(encoding: u8) -> CsvEncoding {
    match encoding {
        0 => CsvEncoding::Utf8,
        1 => CsvEncoding::LossyUtf8,
        _ => CsvEncoding::Utf8,
    }
}

#[inline]
pub(crate) fn map_json_format(code: u8) -> JsonFormat {
    match code {
        1 => JsonFormat::JsonLines, // .jsonl / .ndjson
        0 | _ => JsonFormat::Json,  // standard .json array
    }
}

#[inline]
pub fn map_sync_on_close(val: u8) -> SyncOnCloseType {
    match val {
        1 => SyncOnCloseType::Data,
        2 => SyncOnCloseType::All,
        _ => SyncOnCloseType::None,
    }
}