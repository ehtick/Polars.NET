// use std::ffi::c_char;

// use polars::prelude::*;

// use crate::{types::{DataFrameContext, LazyFrameContext, SchemaContext}, utils::ptr_to_str};

// #[unsafe(no_mangle)]
// pub extern "C" fn pl_read_delta(
//     path_ptr: *const c_char,
//     version: *const u64, // 支持 Time Travel: 传入版本号，传 null 则读最新
// ) -> *mut DataFrameContext {
//     ffi_try!({
//         let path = ptr_to_str(path_ptr)
//             .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

//         // 构造 Delta 扫描参数
//         let mut args = ScanArgsDelta::default();
        
//         // 处理 Time Travel (Version)
//         if !version.is_null() {
//             let v = unsafe { *version };
//             args.version = Some(v);
//         }

//         // Read Delta = Scan Delta + Collect
//         let df = LazyFrame::scan_delta(PlPath::new(path), args)?
//             .collect()?;

//         Ok(Box::into_raw(Box::new(DataFrameContext { df })))
//     })
// }

// #[unsafe(no_mangle)]
// pub extern "C" fn pl_scan_delta(
//     path_ptr: *const c_char,
//     version: *const u64 
// ) -> *mut LazyFrameContext {
//     ffi_try!({
//         let path = ptr_to_str(path_ptr)
//             .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

//         let mut args = ScanArgsDelta::default();
        
//         if !version.is_null() {
//             let v = unsafe { *version };
//             args.version = Some(v);
//         }

//         let lf = LazyFrame::scan_delta(PlPath::new(path), args)?;

//         Ok(Box::into_raw(Box::new(LazyFrameContext { inner: lf })))
//     })
// }
