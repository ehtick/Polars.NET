use crate::{types::{DataTypeContext, LazyFrameContext, SchemaContext}, utils::ptr_to_str};
use std::{ffi::{CStr, CString}, os::raw::c_char};
use polars_core::prelude::*;

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_lazy_frame_get_schema(lf_ptr: *mut LazyFrameContext) -> *mut SchemaContext {
    ffi_try!({
        if lf_ptr.is_null() {
            return Ok(std::ptr::null_mut());
        }
        
        let ctx = unsafe { &mut *lf_ptr };
        
        let schema_ref = ctx.inner.collect_schema().map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        
        Ok(Box::into_raw(Box::new(SchemaContext { schema: schema_ref })))
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_schema_len(ptr: *mut SchemaContext) -> usize {
    if ptr.is_null() { return 0; }
    let ctx = unsafe {&*ptr};
    ctx.schema.len()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_schema_get_at_index(
    ptr: *mut SchemaContext,
    index: usize,
    name_out: *mut *mut c_char,
    dtype_out: *mut *mut DataType 
) {
    let ctx = unsafe{&*ptr};
    
    if let Some((name, dtype)) = ctx.schema.get_at_index(index) {
        unsafe {*name_out = CString::new(name.as_str()).unwrap().into_raw()};
        
        unsafe {*dtype_out = Box::into_raw(Box::new(dtype.clone()))};
    } else {
        unsafe {*name_out = std::ptr::null_mut()};
        unsafe {*dtype_out = std::ptr::null_mut()};
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_schema_new(
    names: *const *const c_char,
    dtypes: *const *mut DataTypeContext,
    len: usize,
) -> *mut SchemaContext {
    let mut schema = Schema::with_capacity(len);
    unsafe {
        for i in 0..len {
            let name_ptr = *names.add(i);
            let dtype_ptr = *dtypes.add(i);

            let name = CStr::from_ptr(name_ptr).to_string_lossy().into_owned();
            let dtype = &(*dtype_ptr).dtype;

            schema.insert(name.into(), dtype.clone());
        }
    }
    Box::into_raw(Box::new(SchemaContext { schema:schema.into() }))
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_schema_add_field(
    schema_ptr: *mut SchemaContext,
    name_ptr: *const c_char,
    dtype_ptr: *mut DataTypeContext
) {
    ffi_try_void!({
        if schema_ptr.is_null() {
            return Err(PolarsError::ComputeError("Schema handle is null".into()));
        }
        if name_ptr.is_null() {
            return Err(PolarsError::ComputeError("Name pointer is null".into()));
        }
        if dtype_ptr.is_null() {
            return Err(PolarsError::ComputeError("DataType handle is null".into()));
        }

        let name = ptr_to_str(name_ptr)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        let schema_ctx = unsafe { &mut *schema_ptr };

        let dtype_ctx = unsafe { &*dtype_ptr };
        let dtype = dtype_ctx.dtype.clone();

        let schema_map = std::sync::Arc::make_mut(&mut schema_ctx.schema);
        
        schema_map.insert(name.into(), dtype);

        Ok(())
    })
}


#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_schema_free(ptr: *mut SchemaContext) {
    if !ptr.is_null() {
        let _ = unsafe {Box::from_raw(ptr)};
    }
}