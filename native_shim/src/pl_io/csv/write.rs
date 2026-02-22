use polars::prelude::*;
use std::os::raw::c_char;
use std::num::NonZeroUsize;
use crate::pl_io::io_utils::{build_partitioned_destination, build_unified_sink_args};
use crate::pl_io::csv::csv_utils::map_quote_style;
use crate::types::{DataFrameContext, LazyFrameContext, SelectorContext};
use crate::utils::{map_external_compression, ptr_to_opt_pl_str, ptr_to_pl_str, ptr_to_str};

unsafe fn build_serialize_options(
    date_format_ptr: *const c_char,
    time_format_ptr: *const c_char,
    datetime_format_ptr: *const c_char,
    float_scientific: i32, // -1: None, 0: False, 1: True
    float_precision: i32,  // -1: None, >=0: Precision
    decimal_comma: bool,
    separator: u8,
    quote_char: u8,
    null_value_ptr: *const c_char,
    line_terminator_ptr: *const c_char,
    quote_style_code: u8,
) -> SerializeOptions {
    
    // Float Scientific (Option<bool>)
    let float_scientific = match float_scientific {
        0 => Some(false),
        1 => Some(true),
        _ => None,
    };

    // Float Precision (Option<usize>)
    let float_precision = if float_precision >= 0 {
        Some(float_precision as usize)
    } else {
        None
    };

    unsafe {
    SerializeOptions {
        date_format: ptr_to_opt_pl_str(date_format_ptr),
        time_format: ptr_to_opt_pl_str(time_format_ptr),
        datetime_format: ptr_to_opt_pl_str(datetime_format_ptr),
        float_scientific,
        float_precision,
        decimal_comma,
        separator,
        quote_char,
        null: ptr_to_pl_str(null_value_ptr, ""), 
        line_terminator: ptr_to_pl_str(line_terminator_ptr, "\n"), 
        quote_style: map_quote_style(quote_style_code),
    }}
}

pub(crate) unsafe fn build_csv_writer_options(
    // --- CsvWriterOptions Params ---
    include_bom: bool,
    include_header: bool,
    batch_size: usize,
    check_extension: bool,
    // --- ExternalCompression Params ---
    compression_code: u8,
    compression_level: i32,
    // --- SerializeOptions Params ---
    date_format: *const c_char,
    time_format: *const c_char,
    datetime_format: *const c_char,
    float_scientific: i32,
    float_precision: i32,
    decimal_comma: bool,
    separator: u8,
    quote_char: u8,
    null_value: *const c_char,
    line_terminator: *const c_char,
    quote_style: u8,
) -> CsvWriterOptions {
    
    // SerializeOptions
    let serialize_options = unsafe { build_serialize_options(
        date_format, time_format, datetime_format,
        float_scientific, float_precision, decimal_comma,
        separator, quote_char, null_value, line_terminator, quote_style
    )};

    // Compression
    let compression = map_external_compression(compression_code, compression_level);

    // Batch Size
    let batch_size = NonZeroUsize::new(batch_size).unwrap_or(NonZeroUsize::new(1024).unwrap());

    CsvWriterOptions {
        include_bom,
        compression,
        check_extension, 
        include_header,
        batch_size,
        serialize_options: Arc::new(serialize_options), 
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_write_csv(
    df_ptr: *mut DataFrameContext,
    path_ptr: *const c_char,
    // --- CSV Params ---
    include_bom: bool,
    include_header: bool,
    batch_size: usize,
    _compression_code: u8, 
    _compression_level: i32,
    // SerializeOptions
    date_format_ptr: *const c_char,
    time_format_ptr: *const c_char,
    datetime_format_ptr: *const c_char,
    float_scientific: i32,
    float_precision: i32,
    decimal_comma: bool,
    separator: u8,
    quote_char: u8,
    null_value_ptr: *const c_char,
    line_terminator_ptr: *const c_char,
    quote_style: u8,
    // Ignored Unified/Cloud
    _maintain_order: bool, _sync_on_close: u8, _mkdir: bool,
    _cloud_provider: u8, _cloud_retries: usize, _cloud_retry_timeout_ms: u64,
    _cloud_retry_init_backoff_ms: u64, _cloud_retry_max_backoff_ms: u64,
    _cloud_cache_ttl: u64, _cloud_keys: *const *const c_char,
    _cloud_values: *const *const c_char, _cloud_len: usize
) {
    ffi_try_void!({
        let ctx = unsafe { &mut *df_ptr };
        let path_str = ptr_to_str(path_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        
        let file = std::fs::File::create(path_str)
            .map_err(|e| PolarsError::ComputeError(format!("Failed to create file: {}", e).into()))?;

        let serialize_options = unsafe {
             build_serialize_options(
                date_format_ptr, time_format_ptr, datetime_format_ptr,
                float_scientific, float_precision, decimal_comma,
                separator, quote_char, null_value_ptr, line_terminator_ptr, quote_style
            )
        };

        let mut writer = CsvWriter::new(file)
            .include_header(include_header)
            .include_bom(include_bom)
            .with_separator(serialize_options.separator)
            .with_quote_char(serialize_options.quote_char)
            .with_quote_style(serialize_options.quote_style)
            .with_null_value(serialize_options.null)
            .with_line_terminator(serialize_options.line_terminator)
            .with_decimal_comma(serialize_options.decimal_comma);
            
        if let Some(fmt) = serialize_options.date_format { writer = writer.with_date_format(Some(fmt)); }
        if let Some(fmt) = serialize_options.time_format { writer = writer.with_time_format(Some(fmt)); }
        if let Some(fmt) = serialize_options.datetime_format { writer = writer.with_datetime_format(Some(fmt)); }
        if let Some(p) = serialize_options.float_precision { writer = writer.with_float_precision(Some(p)); }
        if let Some(s) = serialize_options.float_scientific { writer = writer.with_float_scientific(Some(s)); }

        if batch_size > 0 {
             writer = writer.with_batch_size(std::num::NonZeroUsize::new(batch_size).unwrap());
        }

        writer.finish(&mut ctx.df)?;
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_sink_csv(
    lf_ptr: *mut LazyFrameContext,
    path_ptr: *const c_char,
    // --- CSV Specific Params ---
    include_bom: bool,
    include_header: bool,
    batch_size: usize,
    check_extension: bool,
    // Compression
    compression_code: u8,
    compression_level: i32,
    // Serialize Options
    date_format: *const c_char,
    time_format: *const c_char,
    datetime_format: *const c_char,
    float_scientific: i32,
    float_precision: i32,
    decimal_comma: bool,
    separator: u8,
    quote_char: u8,
    null_value: *const c_char,
    line_terminator: *const c_char,
    quote_style: u8,
    // --- UnifiedSinkArgs ---
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
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let path_str = ptr_to_str(path_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        // UnifiedSinkArgs
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

        // CsvWriterOptions
        let csv_options = unsafe {
            build_csv_writer_options(
                include_bom, include_header, batch_size, check_extension,
                compression_code, compression_level,
                date_format, time_format, datetime_format,
                float_scientific, float_precision, decimal_comma,
                separator, quote_char, null_value, line_terminator, quote_style
            )
        };

        // Format and Target
        let file_format = FileWriteFormat::Csv(csv_options);
        
        let target = SinkTarget::Path(PlRefPath::from(path_str));
        let destination = SinkDestination::File { target };

        // Sink
        let _ = lf_ctx.inner
            .sink(destination, file_format, unified_args)?
            .collect()?;

        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_sink_csv_partitioned(
    lf_ptr: *mut LazyFrameContext,
    base_path_ptr: *const c_char,
    
    // --- Partition Specific Params ---
    partition_by_ptr: *mut SelectorContext,
    include_keys: bool,
    keys_pre_grouped: bool,
    max_rows_per_file: usize,     
    approx_bytes_per_file: u64,

    // --- CSV Specific Params ---
    include_bom: bool,
    include_header: bool,
    batch_size: usize,
    check_extension: bool, 
    // Compression
    compression_code: u8,
    compression_level: i32,

    // Serialize Options 
    date_format: *const c_char,
    time_format: *const c_char,
    datetime_format: *const c_char,
    float_scientific: i32,
    float_precision: i32,
    decimal_comma: bool,
    separator: u8,
    quote_char: u8,
    null_value: *const c_char,
    line_terminator: *const c_char,
    quote_style: u8,

    // --- UnifiedSinkArgs ---
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
        
        // Get Schema
        let schema = lf_ctx.inner.collect_schema()?;

        //  Partitioned Destination
        let destination = unsafe {
            build_partitioned_destination(
                base_path_ptr,
                ".csv", 
                &schema,
                partition_by_ptr,
                include_keys,
                keys_pre_grouped,
                max_rows_per_file,
                approx_bytes_per_file
            )?
        };

        // Unified Args
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

        // CSV Options
        let csv_options = unsafe {
            build_csv_writer_options(
                include_bom, include_header, batch_size, check_extension,
                compression_code, compression_level,
                date_format, time_format, datetime_format,
                float_scientific, float_precision, decimal_comma,
                separator, quote_char, null_value, line_terminator, quote_style
            )
        };
        let file_format = FileWriteFormat::Csv(csv_options);

        let _ = lf_ctx.inner
            .sink(destination, file_format, unified_args)?
            .collect()?;

        Ok(())
    })
}