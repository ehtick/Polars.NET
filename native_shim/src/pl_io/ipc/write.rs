use polars::prelude::*;
use polars_core::prelude::CompatLevel;
use std::os::raw::c_char;
use crate::pl_io::io_utils::build_unified_sink_args;
use crate::pl_io::ipc::ipc_utils::build_ipc_write_options;
use crate::types::{DataFrameContext, LazyFrameContext};
use crate::utils::ptr_to_str;
use polars_utils::compression::{ZstdLevel};

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_write_ipc(
    df_ptr: *mut DataFrameContext,
    path_ptr: *const c_char,
    // --- IpcWriterOptions params ---
    compression: u8,
    compat_level: i32,
    record_batch_size: usize,      
    record_batch_statistics: bool, 
    // --- UnifiedSinkArgs params  ---
    _maintain_order: bool,
    _sync_on_close: u8,
    _mkdir: bool,
    // --- Cloud Options ---
    _cloud_provider: u8,
    _cloud_retries: usize,
    _cloud_retry_timeout_ms: u64,
    _cloud_retry_init_backoff_ms: u64,
    _cloud_retry_max_backoff_ms: u64,
    _cloud_cache_ttl: u64,
    _cloud_keys: *const *const c_char,
    _cloud_values: *const *const c_char,
    _cloud_len: usize
) {
    ffi_try_void!({
        let ctx = unsafe { &mut *df_ptr };
        let path_str = ptr_to_str(path_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        let file = std::fs::File::create(path_str)
            .map_err(|e| PolarsError::ComputeError(format!("Failed to create file: {}", e).into()))?;

        let compression_opt = match compression {
            1 => Some(IpcCompression::LZ4),
            2 => {
                let level = ZstdLevel::try_new(3).unwrap();
                Some(IpcCompression::ZSTD(level))
            },
            _ => None,
        };

        let compat = if compat_level < 0 {
            CompatLevel::newest()
        } else {
            CompatLevel::with_level(compat_level as u16)
                .unwrap_or(CompatLevel::newest())
        };

        let batch_size_opt = if record_batch_size > 0 { Some(record_batch_size) } else { None };

        IpcWriter::new(file)
            .with_compression(compression_opt)
            .with_compat_level(compat)
            .with_record_batch_size(batch_size_opt)               
            .with_record_batch_statistics(record_batch_statistics) 
            .finish(&mut ctx.df)?;

        Ok(())
    })
}


#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_sink_ipc(
    lf_ptr: *mut LazyFrameContext,
    path_ptr: *const c_char,
    // --- IpcWriterOptions params ---
    compression: u8,      
    compat_level: i32,
    record_batch_size: usize,      
    record_batch_statistics: bool, 
    // --- UnifiedSinkArgs params ---
    maintain_order: bool,
    sync_on_close: u8,
    mkdir: bool,
    // --- Cloud Options ---
    cloud_provider: u8,
    cloud_retries: usize,
    cloud_retry_timeout_ms: u64,      
    cloud_retry_init_backoff_ms: u64, 
    cloud_retry_max_backoff_ms: u64,  
    cloud_cache_ttl: u64,
    cloud_keys: *const *const c_char,
    cloud_values: *const *const c_char,
    cloud_len: usize
) {
    ffi_try_void!({
        // 1. Consume LazyFrame & Path
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let path_str = ptr_to_str(path_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        // 2. Build UnifiedSinkArgs
        let unified_args = unsafe {
            build_unified_sink_args(
                mkdir,
                maintain_order,
                sync_on_close,
                cloud_provider,
                cloud_retries,
                cloud_retry_timeout_ms,
                cloud_retry_init_backoff_ms,
                cloud_retry_max_backoff_ms,
                cloud_cache_ttl,
                cloud_keys,
                cloud_values,
                cloud_len
            )
        };

        // 3. Build IpcWriterOptions
        let ipc_options = build_ipc_write_options(
            compression,
            compat_level,
            record_batch_size,
            record_batch_statistics
        )?;

        // 4. Construct FileWriteFormat
        let file_format = FileWriteFormat::Ipc(ipc_options);

        // 5. Construct SinkDestination
        let target = SinkTarget::Path(PlRefPath::from(path_str));
        let destination = SinkDestination::File { target };

        // 6. Execute via Unified Sink Pipeline
        let _ = lf_ctx.inner
            .sink(destination, file_format, unified_args)?
            .collect()?;
        
        Ok(())
    })
}
