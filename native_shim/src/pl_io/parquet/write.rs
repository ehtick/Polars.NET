use polars::prelude::*;
use polars_utils::compression::{BrotliLevel, GzipLevel, ZstdLevel};
use std::os::raw::c_char;
use crate::pl_io::io_utils::{build_partitioned_destination, build_unified_sink_args};
use crate::pl_io::parquet::parquet_utils::build_parquet_write_options;
use crate::types::{DataFrameContext, LazyFrameContext, SelectorContext};
use crate::utils::ptr_to_str;

// ==========================================
// Write&Sink Parquet
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_write_parquet(
    df_ptr: *mut DataFrameContext,
    path_ptr: *const c_char,
    // --- ParquetWriteOptions ---
    compression_code: u8,
    compression_level: i32, 
    statistics: bool,
    row_group_size: usize,
    data_page_size: usize,
    _compat_level: i32,     
    _maintain_order: bool, _sync_on_close: u8, _mkdir: bool,
    // --- Cloud Options (Eager 忽略) ---
    _cloud_provider: u8, _cloud_retries: usize, _cloud_retry_timeout_ms: u64,
    _cloud_retry_init_backoff_ms: u64, _cloud_retry_max_backoff_ms: u64,
    _cloud_cache_ttl: u64, _cloud_keys: *const *const c_char,
    _cloud_values: *const *const c_char, _cloud_len: usize
) {
    ffi_try_void!({
        let ctx = unsafe { &mut *df_ptr };
        let path_str = ptr_to_str(path_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        // Eager Mode
        let file = std::fs::File::create(path_str)
            .map_err(|e| PolarsError::ComputeError(format!("Failed to create file: {}", e).into()))?;

        // 2. 解析压缩格式和级别
        let compression = match compression_code {
            0 => ParquetCompression::Uncompressed,
            1 => ParquetCompression::Snappy,
            2 => { // Gzip
                // GzipLevel 需要 u8。C# 传 -1 或非法值时回退到 None (Default)
                let lvl = if compression_level >= 0 {
                    GzipLevel::try_new(compression_level as u8).ok()
                } else {
                    None
                };
                ParquetCompression::Gzip(lvl)
            },
            3 => { // Brotli
                // BrotliLevel 需要 u32
                let lvl = if compression_level >= 0 {
                    BrotliLevel::try_new(compression_level as u32).ok()
                } else {
                    None
                };
                ParquetCompression::Brotli(lvl)
            },
            4 => { 
                let lvl = if compression_level != -1 {
                    ZstdLevel::try_new(compression_level).ok()
                } else {
                    None
                };
                ParquetCompression::Zstd(lvl)
            },
            5 => ParquetCompression::Lz4Raw,
            _ => ParquetCompression::Snappy, // Fallback
        };

        // 3. 统计信息
        let stats_options = if statistics {
            StatisticsOptions::full()
        } else {
            StatisticsOptions::empty()
        };

        // 4. 构建 Writer
        // 注意：ParquetWriter 默认 parallel=true
        let mut writer = ParquetWriter::new(file)
            .with_compression(compression)
            .with_statistics(stats_options);

        // 5. 其他选项
        if row_group_size > 0 {
            writer = writer.with_row_group_size(Some(row_group_size));
        }
        
        if data_page_size > 0 {
            writer = writer.with_data_page_size(Some(data_page_size));
        }

        // 6. 写入
        writer.finish(&mut ctx.df)?;

        Ok(())
    })
}

fn fallback_eager_parquet(
    lf: LazyFrame,
    path: &str,
    options: &ParquetWriteOptions
) -> PolarsResult<()> {
    // 1. Collect
    let mut df = lf.collect()?;

    // 2. Create File
    let file = std::fs::File::create(path)
        .map_err(|e| PolarsError::ComputeError(format!("Fallback: failed to create file '{}': {}", path, e).into()))?;

    // 3. Configure Writer
    let mut writer = ParquetWriter::new(file)
        .with_compression(options.compression)
        .with_statistics(options.statistics.clone())
        .set_parallel(true);

    if let Some(s) = options.row_group_size { writer = writer.with_row_group_size(Some(s)); }
    if let Some(s) = options.data_page_size { writer = writer.with_data_page_size(Some(s)); }
    
    if let Some(_compat) = options.compat_level {
    }

    // 4. Write
    writer.finish(&mut df)?;

    Ok(())
}
#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_sink_parquet(
    lf_ptr: *mut LazyFrameContext,
    path_ptr: *const c_char,
    // --- Parquet Options ---
    compression: u8,
    compression_level: i32,
    statistics: bool,
    row_group_size: usize,
    data_page_size: usize,
    compat_level: i32,
    // --- Unified Options ---
    maintain_order: bool,
    sync_on_close: u8,
    mkdir: bool,
    // --- Cloud Params ---
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
        let ctx = unsafe { Box::from_raw(lf_ptr) };
        let lf = ctx.inner;
        let lf_clone = lf.clone();

        let path_str = ptr_to_str(path_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        let write_options_arc = build_parquet_write_options(
            compression,
            compression_level,
            statistics,
            row_group_size,
            data_page_size,
            compat_level
        )?;
        
        // 3. 构建 Unified Args (复用 Helper)
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

        let options_for_stream = write_options_arc.clone();
        
        let result = catch_unwind(AssertUnwindSafe(|| {
            let target = SinkTarget::Path(PlRefPath::from(path_str));
            let destination = SinkDestination::File { target };

            let file_format = FileWriteFormat::Parquet(options_for_stream);

            lf.sink(destination, file_format, unified_args)?
              .collect()
        }));

        match result {
            Ok(Ok(_)) => {
            },
            Ok(Err(e)) => {
                if cloud_provider != 0 {
                    return Err(e);
                }
                eprintln!("Streaming Sink failed: {}. Attempting fallback to Eager Write...", e);
                fallback_eager_parquet(lf_clone, path_str, &write_options_arc)?;
            },
            Err(_) => {
                if cloud_provider != 0 {
                    return Err(PolarsError::ComputeError("Streaming sink panicked on Cloud path (Eager fallback not supported for Cloud).".into()));
                }
                eprintln!("Streaming Sink panicked. Falling back to Eager Write...");
                fallback_eager_parquet(lf_clone, path_str, &write_options_arc)?;
            }
        }

        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_sink_parquet_partitioned(
    lf_ptr: *mut LazyFrameContext,
    base_path_ptr: *const c_char,
    
    // --- Partition Params ---
    partition_by_ptr: *mut SelectorContext,
    include_keys: bool,
    keys_pre_grouped: bool,
    max_rows_per_file: usize,
    approx_bytes_per_file: u64,

    // --- Parquet Options ---
    compression: u8,        
    compression_level: i32, 
    statistics: bool,       
    row_group_size: usize,  
    data_page_size: usize,
    compat_level: i32,
    
    // --- Unified Options ---
    maintain_order: bool,
    sync_on_close: u8,
    mkdir: bool,
    
    // --- Cloud Params ---
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
        let mut lf_ctx = unsafe { Box::from_raw(lf_ptr) };

        let schema = lf_ctx.inner.collect_schema()?;

        let destination = unsafe {
            build_partitioned_destination(
                base_path_ptr,
                ".parquet", 
                &schema,
                partition_by_ptr,
                include_keys,
                keys_pre_grouped,
                max_rows_per_file,
                approx_bytes_per_file
            )?
        };

        let write_options_arc = build_parquet_write_options(
            compression,
            compression_level,
            statistics,
            row_group_size,
            data_page_size,
            compat_level
        )?;
        let file_format = FileWriteFormat::Parquet(write_options_arc);

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

        let _ = lf_ctx.inner
            .sink(destination, file_format, unified_args)?
            .collect()?;

        Ok(())
    })
}
