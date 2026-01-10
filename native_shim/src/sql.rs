use polars::prelude::*;
use polars::sql::SQLContext;
use std::os::raw::c_char;
use crate::{types::LazyFrameContext, utils::ptr_to_str};

// Define Context Container
pub struct SqlContextWrapper {
    pub inner: SQLContext,
}

// Create Context
#[unsafe(no_mangle)]
pub extern "C" fn pl_sql_context_new() -> *mut SqlContextWrapper {
    ffi_try!({
        let ctx = SQLContext::new();
        Ok(Box::into_raw(Box::new(SqlContextWrapper { inner: ctx })))
    })
}

// Release Context
#[unsafe(no_mangle)]
pub extern "C" fn pl_sql_context_free(ptr: *mut SqlContextWrapper) {
    ffi_try_void!({
        if !ptr.is_null() {
            unsafe { let _ = Box::from_raw(ptr); }
        }
        Ok(())
    })
}

// Register LazyFrame
#[unsafe(no_mangle)]
pub extern "C" fn pl_sql_context_register(
    ctx_ptr: *mut SqlContextWrapper,
    name_ptr: *const c_char,
    lf_ptr: *mut LazyFrameContext
) {
    ffi_try_void!({
        let ctx = unsafe { &mut *ctx_ptr };
        let name = ptr_to_str(name_ptr).unwrap();
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };

        ctx.inner.register(name, lf_ctx.inner);
        Ok(())
    })
}

// Execute) -> Return LazyFrame
#[unsafe(no_mangle)]
pub extern "C" fn pl_sql_context_execute(
    ctx_ptr: *mut SqlContextWrapper,
    query_ptr: *const c_char
) -> *mut LazyFrameContext {
    ffi_try!({
        let ctx = unsafe { &mut *ctx_ptr };
        let query = ptr_to_str(query_ptr).unwrap();

        let lf = ctx.inner.execute(query)?;
        
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: lf })))
    })
}