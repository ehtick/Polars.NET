use std::ffi::{CStr, c_char};

use polars_arrow::ffi::ArrowArray;
use polars_arrow::ffi::{export_array_to_c,export_field_to_c};
use polars::prelude::{ArrowSchema, Expr, JoinType};
use polars_arrow::datatypes::Field;

use crate::types::ExprContext;

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

pub(crate) fn map_jointype(code: i32) -> JoinType {
    match code {
        0 => JoinType::Inner,
        1 => JoinType::Left,
        2 => JoinType::Full, 
        3 => JoinType::Cross,
        4 => JoinType::Semi,
        5 => JoinType::Anti,
        _ => JoinType::Inner, // Default
    }
}