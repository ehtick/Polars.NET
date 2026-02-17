// ==========================================
// Delta Lake Support
// ==========================================

use deltalake::{DeltaTable, DeltaTableBuilder, Path};
use deltalake::kernel::engine::arrow_conversion::{TryIntoArrow, TryIntoKernel};
use deltalake::kernel::transaction::CommitBuilder;
use deltalake::kernel::scalars::ScalarExt;
use deltalake::parquet::data_type::ByteArray;
use deltalake::parquet::file::metadata::ParquetMetaData;
use deltalake::parquet::arrow::async_reader::{AsyncFileReader, ParquetObjectReader};
use deltalake::parquet::file::statistics::Statistics as ParquetStatistics;
use deltalake::arrow::datatypes::Schema as DeltaArrowSchema;
use deltalake::arrow::ffi::FFI_ArrowSchema as DeltaFFISchema;
use deltalake::kernel::{Action, Add, Remove, StructType, transaction};
use deltalake::protocol::{DeltaOperation, MergePredicate, SaveMode};
use polars::prelude::file_provider::HivePathProvider;
use polars::{error::PolarsError, prelude::*};
use polars_buffer::Buffer;
use polars_arrow::ffi::{import_field_from_c,ArrowSchema as PolarsFFISchema};
use tokio::runtime::Runtime;
use std::time::{SystemTime, UNIX_EPOCH};
use std::os::raw::c_char;
use std::sync::OnceLock;
use std::collections::{HashMap, HashSet};
use url::Url;
use uuid::Uuid;
use serde_json::{json, Value};
use futures::StreamExt;
use chrono::{DateTime, NaiveDate, TimeDelta, Utc};

use crate::io::{build_cloud_options, build_parquet_write_options, build_partitioned_destination, build_scan_args, build_unified_sink_args};
use crate::types::{ExprContext, LazyFrameContext, SchemaContext, SelectorContext};
use crate::utils::{ptr_to_str, ptr_to_vec_string};

static RUNTIME: OnceLock<Runtime> = OnceLock::new();
const MAX_FULL_JOB_RETRIES: usize = 5;
const MAX_PRUNING_CANDIDATES: usize = 100_000;
// -------------------------------------------------------------------------
// Helper Functions
// -------------------------------------------------------------------------

// Get global tokio runtime
fn get_runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Failed to create global Tokio runtime")
    })
}

/// Extract Parquet Stat then convert to Delta JSON format
/// Return: (num_records, stats_json_string)
fn extract_delta_stats(metadata: &ParquetMetaData) -> Result<(i64, String), PolarsError> {
    let file_metadata = metadata.file_metadata();
    let num_rows = file_metadata.num_rows();
    
    // Delta Stats Struct
    // {
    //   "numRecords": 100,
    //   "minValues": { "col1": 1, "col2": "a" },
    //   "maxValues": { "col1": 10, "col2": "z" },
    //   "nullCount": { "col1": 0, "col2": 5 }
    // }

    let mut min_values = serde_json::Map::new();
    let mut max_values = serde_json::Map::new();
    let mut null_count = serde_json::Map::new();

    let schema_descr = file_metadata.schema_descr();

    // Iter all columns
    for (col_idx, column_descr) in schema_descr.columns().iter().enumerate() {
        let col_name = column_descr.name();

        // Agg RowGroups Stats
        let mut global_min: Option<Value> = None;
        let mut global_max: Option<Value> = None;
        let mut global_nulls: i64 = 0;
        let mut has_stats = false;

        for rg in metadata.row_groups() {
            if let Some(col_chunk) = rg.column(col_idx).statistics() {
                has_stats = true;
                global_nulls += col_chunk.null_count_opt().unwrap_or(0) as i64;

                // Convert Min/Max
                let (min_val, max_val) = parquet_stats_to_json(col_chunk);
                
                // Aggregation: Min
                if let Some(v) = min_val {
                    match global_min {
                        None => global_min = Some(v),
                        Some(ref current) => {
                            if is_smaller(&v, current) {
                                global_min = Some(v);
                            }
                        }
                    }
                }

                // Aggregation: Max
                if let Some(v) = max_val {
                    match global_max {
                        None => global_max = Some(v),
                        Some(ref current) => {
                            if is_larger(&v, current) {
                                global_max = Some(v);
                            }
                        }
                    }
                }
            }
        }

        if has_stats {
            null_count.insert(col_name.to_string(), json!(global_nulls));
            if let Some(min) = global_min { min_values.insert(col_name.to_string(), min); }
            if let Some(max) = global_max { max_values.insert(col_name.to_string(), max); }
        }
    }

    let stats_json = json!({
        "numRecords": num_rows,
        "minValues": min_values,
        "maxValues": max_values,
        "nullCount": null_count
    });

    Ok((num_rows, stats_json.to_string()))
}

fn extract_stats_value<'a>(stats: &'a Option<serde_json::Value>, key: &str, col: &str) -> Option<&'a serde_json::Value> {
    stats.as_ref()?
        .get(key)? // "minValues" or "maxValues"
        .get(col)
}

// Convert JSON Value to i64
fn as_i64_safe(v: &serde_json::Value) -> Option<i64> {
    v.as_i64()
     .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
}

// Convert JSON Value to f64
fn as_f64_safe(v: &serde_json::Value) -> Option<f64> {
    v.as_f64()
     .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
}

// Overlap Check
fn check_file_overlap_optimized(
    stats: &Option<serde_json::Value>, 
    col: &str, 
    src_min: &serde_json::Value, 
    src_max: &serde_json::Value
) -> bool {
    let f_min_val = extract_stats_value(stats, "minValues", col);
    let f_max_val = extract_stats_value(stats, "maxValues", col);

    if f_min_val.is_none() || f_max_val.is_none() {
        return true;
    }
    let f_min = f_min_val.unwrap();
    let f_max = f_max_val.unwrap();

    match src_min {
        // A. Compare Number 
        serde_json::Value::Number(s_min_num) => {
            if let serde_json::Value::Number(s_max_num) = src_max {
                
                // [Step 1]: Integer (i64) 
                let s_min_i = s_min_num.as_i64();
                let s_max_i = s_max_num.as_i64();
                let f_min_i = as_i64_safe(f_min);
                let f_max_i = as_i64_safe(f_max);

                if let (Some(s_min), Some(s_max), Some(f_min), Some(f_max)) = (s_min_i, s_max_i, f_min_i, f_max_i) {
                     // Not Overlap
                     if f_max < s_min || f_min > s_max {
                        return false; 
                     }
                     return true; // Overlap
                }

                // [Step 2]: Fallback to f64
                let s_min_f = s_min_num.as_f64();
                let s_max_f = s_max_num.as_f64();
                let f_min_f = as_f64_safe(f_min);
                let f_max_f = as_f64_safe(f_max);

                if let (Some(s_min), Some(s_max), Some(f_min), Some(f_max)) = (s_min_f, s_max_f, f_min_f, f_max_f) {
                     if f_max < s_min || f_min > s_max {
                        return false;
                     }
                }
            }
        },
        
        // B. String/Date/Decimal (Lexicographical Compare)
        serde_json::Value::String(s_min_str) => {
             if let serde_json::Value::String(s_max_str) = src_max {
                 let f_min_s = f_min.as_str();
                 let f_max_s = f_max.as_str();
                 
                 if let (Some(f_min), Some(f_max)) = (f_min_s, f_max_s) {
                     if f_max < s_min_str.as_str() || f_min > s_max_str.as_str() {
                        return false;
                     }
                 }
             }
        },
        
        _ => return true,
    }

    true
}

// Compare JSON Value
fn is_smaller(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(n1), Value::Number(n2)) => {
             if let (Some(f1), Some(f2)) = (n1.as_f64(), n2.as_f64()) { f1 < f2 } 
             else if let (Some(i1), Some(i2)) = (n1.as_i64(), n2.as_i64()) { i1 < i2 }
             else { false }
        },
        (Value::String(s1), Value::String(s2)) => s1 < s2,
        _ => false 
    }
}

fn is_larger(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(n1), Value::Number(n2)) => {
             if let (Some(f1), Some(f2)) = (n1.as_f64(), n2.as_f64()) { f1 > f2 } 
             else if let (Some(i1), Some(i2)) = (n1.as_i64(), n2.as_i64()) { i1 > i2 }
             else { false }
        },
        (Value::String(s1), Value::String(s2)) => s1 > s2,
        _ => false 
    }
}

// Convert Parquet raw Stats to JSON Value
fn parquet_stats_to_json(stats: &ParquetStatistics) -> (Option<Value>, Option<Value>) {
    match stats {
        // Boolean
        ParquetStatistics::Boolean(s) => (
            s.min_opt().map(|&v| json!(v)), 
            s.max_opt().map(|&v| json!(v))
        ),
        // Int32
        ParquetStatistics::Int32(s) => (
            s.min_opt().map(|&v| json!(v)), 
            s.max_opt().map(|&v| json!(v))
        ),
        // Int64
        ParquetStatistics::Int64(s) => (
            s.min_opt().map(|&v| json!(v)), 
            s.max_opt().map(|&v| json!(v))
        ),
        // Float
        ParquetStatistics::Float(s) => (
            s.min_opt().map(|&v| json!(v)), 
            s.max_opt().map(|&v| json!(v))
        ),
        // Double
        ParquetStatistics::Double(s) => (
            s.min_opt().map(|&v| json!(v)), 
            s.max_opt().map(|&v| json!(v))
        ),
        // ByteArray (String / Binary)
        ParquetStatistics::ByteArray(s) => {
            let convert = |bytes: &ByteArray| -> Option<Value> {
                std::str::from_utf8(bytes.data()).ok().map(|s| json!(s))
            };
            (
                s.min_opt().and_then(convert),
                s.max_opt().and_then(convert)
            )
        },
        // Int96 (Legacy Timestamp)
        ParquetStatistics::Int96(_) => (None, None),
        
        // FixedLenByteArray (usually Decimal)
        ParquetStatistics::FixedLenByteArray(_) => (None, None),
    }
}

fn map_savemode(mode: u8) -> SaveMode {
    match mode {
        0 => SaveMode::Append,
        1 => SaveMode::Overwrite,
        2 => SaveMode::ErrorIfExists,
        3 => SaveMode::Ignore,
        _ => return SaveMode::Append
    }
}

fn get_polars_schema_from_delta(table: &DeltaTable) -> PolarsResult<Schema> {
    let snapshot = table.snapshot().map_err(|e| PolarsError::ComputeError(format!("Failed to get snapshot: {}", e).into()))?;
    let kernel_schema = snapshot.schema(); 
    
    // 1. Convert to Arrow Schema
    let arrow_schema: DeltaArrowSchema = TryIntoArrow::<DeltaArrowSchema>::try_into_arrow(kernel_schema.as_ref())
        .map_err(|e| PolarsError::ComputeError(format!("Schema conversion error: {}", e).into()))?;

    // 2. FFI Export -> Import to Polars Schema
    let delta_ffi = DeltaFFISchema::try_from(&arrow_schema)
        .map_err(|e| PolarsError::ComputeError(format!("FFI Export Error: {}", e).into()))?;

    let delta_ffi_ptr = &delta_ffi as *const DeltaFFISchema;
    let polars_ffi_ptr = delta_ffi_ptr as *const PolarsFFISchema;

    let imported_field = unsafe { import_field_from_c(&*polars_ffi_ptr) }
        .map_err(|e| PolarsError::ComputeError(format!("FFI Import error: {}", e).into()))?;

    match imported_field.dtype {
        ArrowDataType::Struct(fields) => {
            let polars_fields = fields.iter().map(|arrow_field| {
                Field::from(arrow_field)
            });
            Ok(Schema::from_iter(polars_fields))
        },
        _ => Err(PolarsError::ComputeError("Imported Delta Schema is not a Struct".into())),
    }
}

/// path: "date=2023-01-01/region=US/part-xxx.parquet"
/// cols: ["date", "region"]
fn parse_hive_partitions(path: &str, cols: &[String]) -> HashMap<String, Option<String>> {
    let mut map = HashMap::new();
    if cols.is_empty() { return map; }
    
    let parts: Vec<&str> = path.split('/').collect();
    for part in parts {
        if let Some((k, v)) = part.split_once('=') {
            if cols.contains(&k.to_string()) {
                let decoded_v = urlencoding::decode(v).map(|c| c.into_owned()).unwrap_or(v.to_string());
                map.insert(k.to_string(), Some(decoded_v));
            }
        }
    }
    for col in cols {
        map.entry(col.clone()).or_insert(None);
    }
    map
}

fn build_delta_storage_options_map(
    keys: *const *const c_char,
    values: *const *const c_char,
    len: usize
) -> HashMap<String, String> {
    let mut map = HashMap::new();
    if !keys.is_null() && !values.is_null() && len > 0 {
        let keys_slice = unsafe { std::slice::from_raw_parts(keys, len) };
        let vals_slice = unsafe { std::slice::from_raw_parts(values, len) };
        for i in 0..len {
            if !keys_slice[i].is_null() && !vals_slice[i].is_null() {
                let k = unsafe { std::ffi::CStr::from_ptr(keys_slice[i]).to_string_lossy().into_owned() };
                let v = unsafe { std::ffi::CStr::from_ptr(vals_slice[i]).to_string_lossy().into_owned() };
                map.insert(k, v);
            }
        }
    }
    map
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_scan_delta(
    path_ptr: *const c_char,
    // --- Time Travel Args ---
    version: *const i64,
    datetime_ptr: *const c_char,
    // --- Scan Args ---
    n_rows: *const usize,
    parallel_code: u8,
    low_memory: bool,
    use_statistics: bool,
    glob: bool,
    rechunk: bool, 
    cache: bool,   
    // --- Optional Names ---
    row_index_name_ptr: *const c_char,
    row_index_offset: u32,
    include_path_col_ptr: *const c_char,
    // --- Schema ---
    schema_ptr: *mut SchemaContext, 
    hive_schema_ptr: *mut SchemaContext,
    try_parse_hive_dates: bool,
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
) -> *mut LazyFrameContext {
    ffi_try!({
        let path_str = ptr_to_str(path_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        let version_val = if version.is_null() { None } else { unsafe { Some(*version) } };
        let datetime_str = if datetime_ptr.is_null() { None } else { Some(ptr_to_str(datetime_ptr).unwrap()) };

        let table_url = if let Ok(u) = Url::parse(path_str) { u } else {
            let abs = std::fs::canonicalize(path_str).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
            Url::from_directory_path(abs).map_err(|_| PolarsError::ComputeError("Invalid path".into()))?
        };

        let delta_opts = build_delta_storage_options_map(cloud_keys, cloud_values, cloud_len);
        // Load Delta Table & Extract Info (Async)
        let rt = get_runtime();
        let (file_uris, polars_schema) = rt.block_on(async {
            let mut builder = DeltaTableBuilder::from_url(table_url).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
            if let Some(v) = version_val { builder = builder.with_version(v); }
            else if let Some(dt) = datetime_str { builder = builder.with_datestring(dt).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?; }
            
            let table = builder.with_storage_options(delta_opts).load().await.map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

            let final_schema = get_polars_schema_from_delta(&table)?;

            let files = table.get_file_uris().map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
            let uris: Vec<String> = files.map(|s| s.to_string()).collect();
            Ok::<_, PolarsError>((uris, final_schema))
        })?;

        let allow_missing_columns = true;
        // 4. Build Basic Polars Scan Args
        let mut args = build_scan_args(
            n_rows, parallel_code, low_memory, use_statistics, 
            glob, allow_missing_columns, 
            row_index_name_ptr, row_index_offset, include_path_col_ptr,
            schema_ptr, 
            hive_schema_ptr, try_parse_hive_dates,
            rechunk, cache,
            cloud_provider, cloud_retries,    cloud_retry_timeout_ms,      
            cloud_retry_init_backoff_ms, 
            cloud_retry_max_backoff_ms,  cloud_cache_ttl, 
            cloud_keys, cloud_values, cloud_len
        );

        // 5. Inject Delta Schema 
        if args.schema.is_none() {
            args.schema = Some(Arc::new(polars_schema));
        }
        
        // 6. Scan
        let pl_paths: Vec<PlRefPath> = file_uris.iter().map(|s| PlRefPath::new(s)).collect();
        let buffer: Buffer<PlRefPath> = pl_paths.into();
        
        let lf = LazyFrame::scan_parquet_files(buffer, args)?;
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: lf })))
    })
}

pub(crate) fn parse_table_url(path_str: &str) -> PolarsResult<Url> {
    // Case A: Normal Url (如 s3://, abfss://, gs://)
    if let Ok(u) = Url::parse(path_str) {
        if u.has_host() || u.scheme() == "file" {
            return Ok(u);
        }
    }

    // Case B: Local Disk (./data/test or /tmp/test or C:\data\test)
    let abs_path = std::fs::canonicalize(path_str)
        .or_else(|_| {
            // If path is absent, try to join to current folder
            std::env::current_dir().map(|cwd| cwd.join(path_str))
        })
        .map_err(|e| PolarsError::ComputeError(format!("Invalid local path '{}': {}", path_str, e).into()))?;

    // Convert to file:// URL
    Url::from_directory_path(abs_path)
        .map_err(|_| PolarsError::ComputeError(format!("Could not convert path '{}' to file URL", path_str).into()))
}

pub(crate) fn convert_to_delta_schema(pl_schema: &Schema) -> PolarsResult<StructType> {
    // 1. Polars Schema -> Polars Arrow Schema
    let pl_arrow_schema = pl_schema.to_arrow(CompatLevel::newest());

    // 2. FFI Convert cycle: Polars Field -> Delta Field
    let delta_arrow_fields = pl_arrow_schema.iter_values().map(|pl_field| {
        // A. Export: Polars Field -> Polars FFI Struct
        let pl_ffi = polars_arrow::ffi::export_field_to_c(pl_field);
        
        // B. Cast: Polars FFI Ptr -> Delta FFI Ptr
        // Arrow C Data Interface is ABI compatiable
        let delta_ffi_ptr = &pl_ffi as *const _ as *const deltalake::arrow::ffi::FFI_ArrowSchema;

        // C. Import: Delta FFI -> Delta Arrow Field
        unsafe { deltalake::arrow::datatypes::Field::try_from(&*delta_ffi_ptr) }
            .map_err(|e| PolarsError::ComputeError(format!("FFI Import Error: {}", e).into()))
    }).collect::<Result<Vec<deltalake::arrow::datatypes::Field>, PolarsError>>()?;

    // 3. Build Delta (Standard) Arrow Schema
    let delta_arrow_schema = deltalake::arrow::datatypes::Schema::new(delta_arrow_fields);

    // 4. Convert to Delta Kernel Schema (StructType)
    let delta_schema: StructType = TryIntoKernel::<StructType>::try_into_kernel(&delta_arrow_schema)
         .map_err(|e| PolarsError::ComputeError(format!("Failed to convert to Delta Kernel Schema: {}", e).into()))?;

    Ok(delta_schema)
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_sink_delta(
    lf_ptr: *mut LazyFrameContext,
    base_path_ptr: *const c_char,
    mode: u8,
    can_evolve: bool,
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
        let base_path_str = ptr_to_str(base_path_ptr)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        // 1. Get Schema & Parse Partition Column
        let schema = lf_ctx.inner.collect_schema()
            .map_err(|e| PolarsError::ComputeError(format!("Failed to collect schema: {}", e).into()))?;

        let partition_cols = if !partition_by_ptr.is_null() {
            let selector_ctx = unsafe { &*partition_by_ptr };
            let ignored = PlHashSet::new();
            selector_ctx.inner.into_columns(&schema, &ignored)?
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<String>>()
        } else {
            Vec::new()
        };
        let rt = get_runtime();
        let save_mode = map_savemode(mode);
        let write_id = Uuid::new_v4(); 

        // 2. Build Staging Path
        // Example: s3://bucket/table/.tmp_write_<uuid>
        let staging_dir_name = format!(".tmp_write_{}", write_id);
        
        let staging_uri = if base_path_str.contains("://") {
            // Remote Path
            format!("{}/{}", base_path_str.trim_end_matches('/'), staging_dir_name)
        } else {
            // Local Path
            std::path::Path::new(base_path_str)
                .join(&staging_dir_name)
                .to_string_lossy()
                .to_string()
        };
        // 3. Build Cloud Options & Delta Table
        let delta_storage_options = build_delta_storage_options_map(cloud_keys, cloud_values, cloud_len);
        
        let (mut table, old_files_to_remove, should_skip) = rt.block_on(async {
            let table_url = parse_table_url(base_path_str)?;
            let dt = DeltaTable::try_from_url_with_storage_options(table_url.clone(), delta_storage_options.clone())
                .await.map_err(|e| PolarsError::ComputeError(format!("Delta init error: {}", e).into()))?;
            
            let mut old_files = Vec::new();
            let mut skip_write = false;

            if dt.version() >= Some(0) {
                match save_mode {
                    // Case A: ErrorIfExists -> Throw Exception
                    SaveMode::ErrorIfExists => {
                        return Err(PolarsError::ComputeError(format!("Table already exists at {}", table_url).into()));
                    },
                    // Case B: Ignore -> Set as skip
                    SaveMode::Ignore => {
                        skip_write = true;
                    },
                    // Case C: Overwrite -> Collect old files
                    SaveMode::Overwrite => {
                        let files = dt.get_files_by_partitions(&[]).await
                            .map_err(|e| PolarsError::ComputeError(format!("List files error: {}", e).into()))?;
                        for p in files {
                            old_files.push(p.to_string());
                        }
                    },
                    // Case D: Append -> Continue
                    _ => {} 
                }
            }
            Ok::<_, PolarsError>((dt, old_files, skip_write))
        })?;

        if should_skip {
            return Ok(());
        }

        // 4. Polars Sink -> Staging Folder
        let destination = unsafe {
            build_partitioned_destination(
                std::ffi::CString::new(staging_uri.clone()).unwrap().as_ptr(), 
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
            compression, compression_level, statistics, row_group_size, data_page_size, compat_level
        )?;
        let file_format = FileWriteFormat::Parquet(write_options_arc);
        
        let unified_args = unsafe {
            build_unified_sink_args(
                mkdir, maintain_order, sync_on_close,
                cloud_provider, cloud_retries, cloud_retry_timeout_ms,
                cloud_retry_init_backoff_ms, cloud_retry_max_backoff_ms, cloud_cache_ttl,
                cloud_keys, cloud_values, cloud_len
            )
        };

        // 执行写操作
        lf_ctx.inner
            .sink(destination, file_format, unified_args)?
            .collect_with_engine(Engine::Streaming)?;
            
        // 5. Staging -> Move -> Commit
        rt.block_on(async {
            // A. Auto Create Table
            if table.version() < Some(0) {
                 let delta_schema = convert_to_delta_schema(&schema)?;
                 table = table.create()
                    .with_columns(delta_schema.fields().cloned())
                    .with_partition_columns(partition_cols.clone()) 
                    .await
                    .map_err(|e| PolarsError::ComputeError(format!("Create table error: {}", e).into()))?;
            }

            let object_store = table.object_store();

            let mut actions = Vec::new();
            // ==========================================
            // Schema Evolution Check
            // ==========================================
            // 1. Get Newest Schema
            let current_snapshot = table.snapshot()
                .map_err(|e| PolarsError::ComputeError(format!("Failed to get snapshot: {}", e).into()))?;
            let current_delta_schema = current_snapshot.schema();
            
            // Convert Polars Schema to Delta Schema
            let new_delta_schema = convert_to_delta_schema(&schema)?;

            // Compare Schema 
            if current_delta_schema.as_ref() != &new_delta_schema {
                if !can_evolve {
                    return Err(PolarsError::ComputeError(
                        "Schema mismatch detected. If you want to evolve the schema, enable 'can_evolve'.".into()
                    ));
                }

                // Build Metadata Action 
                let current_metadata = current_snapshot.metadata();

                // Serlize JSON Value
                let mut meta_json = serde_json::to_value(current_metadata)
                    .map_err(|e| PolarsError::ComputeError(format!("Failed to serialize metadata: {}", e).into()))?;

                // Generate New Schema JSON String
                let new_schema_string = serde_json::to_string(&new_delta_schema)
                    .map_err(|e| PolarsError::ComputeError(format!("Failed to serialize new schema: {}", e).into()))?;

                // Modify JSON "schemaString" field
                if let Some(obj) = meta_json.as_object_mut() {
                    obj.insert("schemaString".to_string(), Value::String(new_schema_string));
                } else {
                    return Err(PolarsError::ComputeError("Metadata is not a JSON object".into()));
                }

                // Deserilize to Metadata struct
                let new_metadata_action: deltalake::kernel::Metadata = serde_json::from_value(meta_json)
                    .map_err(|e| PolarsError::ComputeError(format!("Failed to recreate metadata: {}", e).into()))?;

                // Inject Metadata Action
                actions.insert(0, Action::Metadata(new_metadata_action));
            }
            // ==========================================
            // Step B: Parallel Staging Processing
            // ==========================================
            // Fast Listing
            let add_actions = phase_process_staging(&table, &staging_dir_name, &partition_cols, write_id).await?;
            
            for res in add_actions {
                actions.push(res);
            }

            // Handle Overwrite
            if let SaveMode::Overwrite = save_mode {
                for old_path in old_files_to_remove {
                     let remove = Remove {
                         path: old_path,
                         deletion_timestamp: Some(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64),
                         data_change: true,
                         ..Default::default()
                     };
                     actions.push(Action::Remove(remove));
                }
            }

            if actions.is_empty() {
                return Ok::<(), PolarsError>(());
            }

            // Commit
            let operation = DeltaOperation::Write {
                mode: save_mode,
                partition_by: if !partition_cols.is_empty() { Some(partition_cols) } else { None },
                predicate: None,
            };

            let _ver = transaction::CommitBuilder::default()
                .with_actions(actions)
                .build(
                    table.state.as_ref().map(|s| s as &dyn transaction::TableReference), 
                    table.log_store().clone(), 
                    operation
                )
                .await
                .map_err(|e| PolarsError::ComputeError(format!("Commit failed: {}", e).into()))?;
            let _ = object_store.delete(&Path::from(staging_dir_name)).await;
            Ok(())
        })?;
        
        Ok(())
    })
}

// ==========================================
// Delete Implementation
// ==========================================
type PruningMap = HashMap<String, (serde_json::Value, serde_json::Value)>;

// Extract Expr (Col == Lit)
fn extract_bounds_from_expr(expr: &Expr, map: &mut PruningMap) {
    match expr {
        // (A == B) & (C == D) 
        Expr::BinaryExpr { left, op: Operator::And, right } => {
            extract_bounds_from_expr(left, map);
            extract_bounds_from_expr(right, map);
        },
        
        // Col("A") == Lit(10)
        Expr::BinaryExpr { left, op: Operator::Eq, right } => {
            if let (Expr::Column(name), Expr::Literal(lit_val)) = (left.as_ref(), right.as_ref()) {
                insert_bound(map, name.as_str(), lit_val);
            } 
            else if let (Expr::Literal(lit_val), Expr::Column(name)) = (left.as_ref(), right.as_ref()) {
                insert_bound(map, name.as_str(), lit_val);
            }
        },
        
        // Handle Alias
        Expr::Alias(inner, _) => {
            extract_bounds_from_expr(inner, map);
        },
        
        _ => {}
    }
}

fn insert_bound(map: &mut PruningMap, col_name: &str, lit: &LiteralValue) {
    if let Some(json_val) = polars_literal_to_json(lit) {
        map.insert(col_name.to_string(), (json_val.clone(), json_val));
    }
}

fn polars_literal_to_json(lit: &LiteralValue) -> Option<serde_json::Value> {
    match lit {
        // Unpack LiteralValue::Scalar
        LiteralValue::Scalar(scalar_struct) => {
            let any_value = &scalar_struct.value(); 

            match any_value {
                // Primitives
                AnyValue::Boolean(b) => Some(serde_json::json!(b)),
                AnyValue::Int8(v)    => Some(serde_json::json!(v)),
                AnyValue::Int16(v)   => Some(serde_json::json!(v)),
                AnyValue::Int32(v)   => Some(serde_json::json!(v)),
                AnyValue::Int64(v)   => Some(serde_json::json!(v)),
                AnyValue::UInt8(v)   => Some(serde_json::json!(v)),
                AnyValue::UInt16(v)  => Some(serde_json::json!(v)),
                AnyValue::UInt32(v)  => Some(serde_json::json!(v)),
                AnyValue::UInt64(v)  => Some(serde_json::json!(v)),
                AnyValue::Float32(v) => Some(serde_json::json!(v)),
                AnyValue::Float64(v) => Some(serde_json::json!(v)),

                // String / Utf8
                AnyValue::String(s)  => Some(serde_json::json!(s)),
                AnyValue::StringOwned(s) => Some(serde_json::json!(s)), 
                
                // Date
                AnyValue::Date(days) => {
                    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                    let date = TimeDelta::try_days(*days as i64)
                        .and_then(|delta| epoch.checked_add_signed(delta));
                    date.map(|d| serde_json::json!(d.format("%Y-%m-%d").to_string()))
                },

                // Timestamp
                // TimeZone is hard to convert
                AnyValue::Datetime(_val, _time_unit, _tz) => {
                    None 
                },

                // Others
                _ => None,
            }
        },
        
        _ => None
    }
}
#[unsafe(no_mangle)]
pub extern "C" fn pl_io_delta_delete(
    table_path_ptr: *const c_char,
    predicate_ptr: *mut ExprContext, 
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
        // 1. Arg Parsing
        let path_str = ptr_to_str(table_path_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        let table_url = parse_table_url(path_str)?;
        let delta_storage_options = build_delta_storage_options_map(cloud_keys, cloud_values, cloud_len);
        
        // Take ownership of Expr
        let predicate_ctx = unsafe { *Box::from_raw(predicate_ptr) };

        let root_expr = &predicate_ctx.inner;
        let mut key_bounds = HashMap::new();
        extract_bounds_from_expr(root_expr, &mut key_bounds);

        let rt = get_runtime();

        // 2. Load Table & Get Active Actions (Sync Block)
        let (table, polars_schema, partition_cols, active_add_actions) = rt.block_on(async {
            // 2.1 Load Table
            let t = DeltaTable::try_from_url_with_storage_options(table_url.clone(), delta_storage_options.clone())
                .await.map_err(|e| PolarsError::ComputeError(format!("Delta load error: {}", e).into()))?;
            let scan_t = t.clone();
            // 2.2 Get Schema & Partition Cols from Snapshot
            let snapshot = scan_t.snapshot().map_err(|e| PolarsError::ComputeError(format!("Failed to get snapshot: {}", e).into()))?;
            let schema = get_polars_schema_from_delta(&t)?;
            let metadata = snapshot.metadata();
            let part_cols = metadata.partition_columns().clone();

            // 2.3 Get Actions
            let mut stream = scan_t.get_active_add_actions_by_partitions(&[]);
            let mut actions = Vec::new();
            
            while let Some(item) = stream.next().await {
                // Unwrap DeltaResult<LogicalFileView>
                let view = item.map_err(|e| PolarsError::ComputeError(format!("Stream error: {}", e).into()))?;
                actions.push(view);
            }

            Ok::<_, PolarsError>((t, schema, part_cols, actions))
        })?;

        // // 3. Get actions

        let mut actions = Vec::new();
        let object_store = table.object_store(); 
        let table_root = table.table_url().to_string(); 

        for view in active_add_actions {
            let file_path_str = view.path().clone(); 
            
            let partition_struct_opt = view.partition_values();
            let (p_fields, p_values) = match &partition_struct_opt {
                Some(sd) => (sd.fields(), sd.values()),
                None => (&[][..], &[][..]), 
            };
            // =================================================================================
            // PHASE 1: Partition Pruning (The "Fast Drop" Check)
            // =================================================================================
            // Try build a single row DataFrame to predicate
            let mut can_skip_scan = false;
            let mut fast_drop = false;

            if !partition_cols.is_empty() {
                // Build Mini-DataFrame from partition values
                let mut columns = Vec::with_capacity(partition_cols.len());
                for target_col in &partition_cols {
                    // Find Partition value from actions
                    let val_str_opt = p_fields.iter()
                        .position(|f| f.name() == target_col) // Get index
                        .map(|idx| &p_values[idx])            // Get Scalar
                        .filter(|scalar| !scalar.is_null())   // Filter Null
                        .map(|scalar| {
                             scalar.serialize()
                        });

                    let dtype = polars_schema.get_field(target_col).map(|f| f.dtype.clone()).unwrap_or(DataType::String);
                    
                    // Build Series
                    let s = match val_str_opt {
                        Some(v) => Series::new(target_col.into(), &[v]).cast(&dtype)?,
                        None => Series::new_null(target_col.into(), 1).cast(&dtype)?
                    };
                    columns.push(s.into());
                }
                
                if let Ok(mini_df) = DataFrame::new(1,columns) {
                    // Try to evaluate predicate
                    // Clone expr because we might need it later if this fails
                    let eval_result = mini_df.lazy()
                        .select([predicate_ctx.inner.clone().alias("result")])
                        .collect_with_engine(Engine::Streaming);

                    match eval_result {
                        Ok(res) => {
                            // [Success]: Predicate only depends on Partition Columns!
                            // Check the boolean result
                            if let Ok(bool_s) = res.column("result") {
                                if let Ok(is_match) = bool_s.bool() {
                                    if is_match.get(0) == Some(true) {
                                        // Partition Matched Predicate -> Drop entire file
                                        fast_drop = true;
                                    } else {
                                        // Partition Did NOT Match -> Keep entire file (No-op)
                                        // Do nothing for this file loop
                                    }
                                    can_skip_scan = true;
                                }
                            }
                        },
                        Err(_e) => {
                            // [Failure]: Error likely means "ColumnNotFound".
                            // This confirms the Predicate involves Data Columns (e.g. "id").
                            // We MUST proceed to scan the file content.
                            // Ignore error and fall through to Phase 2.
                        }
                    }
                }
            }

            // --- Apply Pruning Result ---
            if can_skip_scan {
                if fast_drop {
                    // Fast Drop: Mark file for removal, skip IO
                    let remove = Remove {
                        path: file_path_str.to_string().clone(),
                        deletion_timestamp: Some(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64),
                        data_change: true,
                        ..Default::default()
                    };
                    actions.push(Action::Remove(remove));
                }
                // If fast_drop is false, we simply keep the file (do nothing), effectively skipping it.
                continue; 
            }

            // =========================================================
             // Phase 1.5: Stats Pruning (Auto-Derived)
             // =========================================================
            let stats_struct = view.stats()
                    .map(|s| serde_json::from_str(&s).unwrap_or(serde_json::Value::Null));
             if !key_bounds.is_empty() {

                 let mut file_overlaps = true;
                 for (key, (src_min, src_max)) in &key_bounds {
                     if !check_file_overlap_optimized(&stats_struct, key, src_min, src_max) {
                         file_overlaps = false;
                         break;
                     }
                 }
                 
                 if !file_overlaps {
                     continue;
                 }
            }

            // =================================================================================
            // PHASE 2: Scan & Filter (Stats Pushdown Optimized)
            // =================================================================================

            let total_rows_opt = stats_struct.as_ref()
                .and_then(|v| v.get("numRecords"))
                .and_then(|v| v.as_i64());
            
            let full_scan_path = format!("{}/{}", table_root.trim_end_matches('/'), file_path_str);
            let scan_cloud_options = unsafe {
                build_cloud_options(
                    cloud_provider, cloud_retries, cloud_retry_timeout_ms,
                    cloud_retry_init_backoff_ms, cloud_retry_max_backoff_ms, cloud_cache_ttl,
                    cloud_keys, cloud_values, cloud_len
                )
            };
            
            let mut lf = LazyFrame::scan_parquet(
                PlRefPath::new(full_scan_path.clone()), 
                ScanArgsParquet { cloud_options: scan_cloud_options, ..Default::default() }
            )?;

            // Inject Partitions
            for target_col in &partition_cols {
                // Find Value
                let val_str_opt = p_fields.iter()
                    .position(|f| f.name() == target_col)
                    .map(|idx| &p_values[idx])
                    .filter(|scalar| !scalar.is_null())
                    .map(|scalar| {
                         scalar.serialize()
                    });

                // Build Literal Expr
                let lit_expr = match val_str_opt {
                    Some(v) => lit(v),
                    None => lit(NULL)
                };

                // Cast and Inject
                let final_expr = if let Some(dtype) = polars_schema.get_field(target_col).map(|f| f.dtype.clone()) {
                    lit_expr.cast(dtype)
                } else {
                    lit_expr
                };
                
                lf = lf.with_column(final_expr.alias(target_col));
            }

            // [Optimization]: Use filter() first to allow Parquet Stats Pushdown
            // If Parquet Min/Max stats prove "No Match", filter returns empty LF instantly.
            let has_match_df = lf.clone()
                .filter(root_expr.clone())
                .limit(1)
                .collect_with_engine(Engine::Streaming)?;
            
            if has_match_df.height() == 0 {
                // Case 1: Keep (Predicate missed)
                continue;
            }
            
            let is_full_drop = if let Some(_total) = total_rows_opt {
                let keep_expr = root_expr.clone().not();
                let has_keep_df = lf.clone()
                    .filter(keep_expr)
                    .limit(1) 
                    .collect_with_engine(Engine::Streaming)?;
                
                has_keep_df.height() == 0
            } else {
                let counts_df = lf.clone()
                    .select([
                        len().alias("total"), 
                        root_expr.clone().cast(DataType::UInt32).sum().alias("matched") 
                    ])
                    .collect_with_engine(Engine::Streaming)?;

                let total = counts_df.column("total")?.u32()?.get(0).unwrap_or(0) as i64;
                let matched = counts_df.column("matched")?.u32()?.get(0).unwrap_or(0) as i64;

                // if matched == total => Drop
                matched == total
            };

            if is_full_drop {
                // Case 2: Drop (Full match)
                let remove = Remove {
                    path: file_path_str.to_string().clone(),
                    deletion_timestamp: Some(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64),
                    data_change: true,
                    ..Default::default()
                };
                actions.push(Action::Remove(remove));
            } else {
                // Case 3: Rewrite (Partial match)
                let remove = Remove {
                    path: file_path_str.to_string().clone(),
                    deletion_timestamp: Some(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64),
                    data_change: true,
                    ..Default::default()
                };
                actions.push(Action::Remove(remove));

                // Rewrite: Keep rows that do NOT match
                let keep_expr = predicate_ctx.inner.clone().not();
                
                // Construct Selector to drop partition cols
                let drop_names: Vec<PlSmallStr> = partition_cols.iter()
                    .map(|s| PlSmallStr::from_string(s.clone()))
                    .collect();
                let drop_selector = Selector::ByName { names: drop_names.into(), strict: true };

                let new_lf = lf.filter(keep_expr).drop(drop_selector);

                let new_name = format!("part-{}-{}.parquet", Utc::now().timestamp_millis(), Uuid::new_v4());
                let parent_dir = std::path::Path::new(file_path_str.as_ref()) 
                    .parent()
                    .map(|p| p.to_string_lossy().to_string())
                    .unwrap_or_else(|| "".to_string());
                let relative_new_path = if parent_dir.is_empty() { new_name.clone() } else { format!("{}/{}", parent_dir, new_name) };
                let full_write_path = format!("{}/{}", table_root.trim_end_matches('/'), relative_new_path);

                let write_opts = build_parquet_write_options(1, -1, true, 0, 0, -1)
                    .map_err(|e| PolarsError::ComputeError(format!("Options error: {}", e).into()))?;
                
                let target = SinkTarget::Path(PlRefPath::new(&full_write_path));
                let unified_args = unsafe {
                    build_unified_sink_args(
                        false, false, 0, cloud_provider, cloud_retries, cloud_retry_timeout_ms,
                        cloud_retry_init_backoff_ms, cloud_retry_max_backoff_ms, cloud_cache_ttl,
                        cloud_keys, cloud_values, cloud_len 
                    )
                };

                new_lf.sink(SinkDestination::File { target }, FileWriteFormat::Parquet(write_opts), unified_args)?
                    .collect_with_engine(Engine::Streaming)?;

                // Read Stats & Add Action
                let (file_size, stats_json) = rt.block_on(async {
                    let path = Path::from(relative_new_path.clone());
                    let meta = object_store.head(&path).await
                         .map_err(|e| PolarsError::ComputeError(format!("Head error: {}", e).into()))?;
                    let mut reader = ParquetObjectReader::new(object_store.clone(), path.clone())
                            .with_file_size(meta.size as u64);
                    let footer = reader.get_metadata(None).await
                            .map_err(|e| PolarsError::ComputeError(format!("Footer error: {}", e).into()))?;
                    let (_, json) = extract_delta_stats(&footer)?;
                    Ok::<_, PolarsError>((meta.size as i64, json))
                })?;

                let new_partition_values = parse_hive_partitions(&relative_new_path, &partition_cols);
                let add = Add {
                    path: relative_new_path,
                    size: file_size,
                    partition_values: new_partition_values,
                    modification_time: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64,
                    data_change: true,
                    stats: Some(stats_json),
                    ..Default::default()
                };
                actions.push(Action::Add(add));
            }
        }
        // 4. Commit
        if !actions.is_empty() {
            rt.block_on(async {
                let operation = DeltaOperation::Delete {
                    predicate: None, 
                };
                
                let _ver = CommitBuilder::default()
                    .with_actions(actions)
                    .build(
                        table.snapshot().ok().map(|s| s as &dyn transaction::TableReference),
                        table.log_store().clone(),
                        operation
                    ).await.map_err(|e| PolarsError::ComputeError(format!("Commit failed: {}", e).into()))?;
                
                Ok::<(), PolarsError>(())
            })?;
        }
        Ok(())
    })
}

// -------------------------------------------------------------------------
// Merge / Update Helpers
// -------------------------------------------------------------------------
#[derive(Debug, Clone, PartialEq, PartialOrd)]
enum PruningBound {
    Integer(i128),
    Float(f64),
    Lexical(String), 
}
fn bound_to_json(bound: &PruningBound) -> serde_json::Value {
    match bound {
        // A. Integer (i128)
        PruningBound::Integer(i) => {
            if let Ok(val) = i64::try_from(*i) {
                serde_json::json!(val)
            } else {
                serde_json::json!(*i as f64)
            }
        },
        
        // B. Float (f64) -> JSON Number
        PruningBound::Float(f) => serde_json::json!(f),
        
        // C. Lexical (String) -> JSON String
        // 包含 Date, String 等
        PruningBound::Lexical(s) => serde_json::json!(s),
    }
}
/// Parse Source LazyFrame to extract partition values
fn get_source_partition_values(
    source_lf: &LazyFrame,
    partition_cols: &[String]
) -> Option<HashSet<Vec<String>>> {
    if partition_cols.is_empty() {
        return None;
    }
    let cols: Vec<Expr> = partition_cols.iter().map(|c| col(c)).collect();
    let distinct_df = source_lf.clone().select(cols).unique(None, UniqueKeepStrategy::Any).collect().ok()?;

    let height = distinct_df.height();
    if height == 0 { return Some(HashSet::new()); }

    let mut affected_partitions = HashSet::with_capacity(height);
    for row_idx in 0..height {
        let mut row_values = Vec::with_capacity(partition_cols.len());
        for col_name in partition_cols {
            let s = distinct_df.column(col_name).ok()?;
            let val = s.get(row_idx).ok()?;
            row_values.push(any_value_to_string(&val));
        }
        affected_partitions.insert(row_values);
    }
    Some(affected_partitions)
}
fn any_value_to_string(val: &AnyValue) -> String {
    match val {
        AnyValue::Null => "__HIVE_DEFAULT_PARTITION__".to_string(),
        AnyValue::String(s) => s.to_string(),
        AnyValue::StringOwned(s) => s.to_string(),
        AnyValue::Binary(b) => String::from_utf8_lossy(b).to_string(),
        AnyValue::BinaryOwned(b) => String::from_utf8_lossy(&b).to_string(),
        v => v.to_string(),
    }
}

fn any_value_to_bound(val: &AnyValue, _dtype: &DataType) -> Option<PruningBound> {
    match val {
        AnyValue::Int8(v) => Some(PruningBound::Integer(*v as i128)),
        AnyValue::Int16(v) => Some(PruningBound::Integer(*v as i128)),
        AnyValue::Int32(v) => Some(PruningBound::Integer(*v as i128)),
        AnyValue::Int64(v) => Some(PruningBound::Integer(*v as i128)),
        AnyValue::UInt8(v) => Some(PruningBound::Integer(*v as i128)),
        AnyValue::UInt16(v) => Some(PruningBound::Integer(*v as i128)),
        AnyValue::UInt32(v) => Some(PruningBound::Integer(*v as i128)),
        AnyValue::UInt64(v) => Some(PruningBound::Integer(*v as i128)),
        
        AnyValue::Float32(v) => Some(PruningBound::Float(*v as f64)),
        AnyValue::Float64(v) => Some(PruningBound::Float(*v)),
        
        AnyValue::String(s) => Some(PruningBound::Lexical(s.to_string())),
        AnyValue::StringOwned(s) => Some(PruningBound::Lexical(s.to_string())),
        AnyValue::Date(days) => {
            let date = NaiveDate::from_num_days_from_ce_opt(*days + 719163)?;
            Some(PruningBound::Lexical(date.format("%Y-%m-%d").to_string()))
        },
        AnyValue::Datetime(v, unit, _) => {
            let (seconds, nanos) = match unit {
                TimeUnit::Nanoseconds => (v.div_euclid(1_000_000_000), (v.rem_euclid(1_000_000_000)) as u32),
                TimeUnit::Microseconds => (v.div_euclid(1_000_000), (v.rem_euclid(1_000_000) * 1_000) as u32),
                TimeUnit::Milliseconds => (v.div_euclid(1_000), (v.rem_euclid(1_000) * 1_000_000) as u32),
            };
            let dt_utc = DateTime::from_timestamp(seconds, nanos)?;
            Some(PruningBound::Lexical(dt_utc.to_rfc3339()))
        },
        _ => None 
    }
}

// Get Source Min/Max
fn get_source_key_bounds(lf: &LazyFrame, key: &str, dtype: &DataType) -> Option<(PruningBound, PruningBound)> {
    if !dtype.is_numeric() && !dtype.is_temporal() && !matches!(dtype, DataType::String) {
        return None;
    }
    let df = lf.clone()
        .select([col(key).min().alias("min_v"), col(key).max().alias("max_v")])
        .collect().ok()?;

    let min_s = df.column("min_v").ok()?;
    let max_s = df.column("max_v").ok()?;
    
    let min_val = any_value_to_bound(&min_s.get(0).ok()?, dtype)?;
    let max_val = any_value_to_bound(&max_s.get(0).ok()?, dtype)?;

    Some((min_val, max_val))
}

#[derive(Debug, Clone)]
enum PruningCandidates {
    Integers(Vec<i64>), 
    Strings(Vec<String>), 
}

struct MergeContext {
    pub merge_keys: Vec<String>,
    pub table_url: Url,
    pub storage_options: HashMap<String, String>,
    pub can_evolve: bool,
    // Conditions (Owned Exprs)
    pub cond_update: Option<Expr>,     // Default: true
    pub cond_delete: Option<Expr>,     // Default: false
    pub cond_insert: Option<Expr>,     // Default: true
    pub cond_src_delete: Option<Expr>, // Default: false
}

#[derive(Clone, Copy)]
pub struct RawCloudArgs {
    pub provider: u8,
    pub retries: usize,
    pub retry_timeout_ms: u64,      
    pub retry_init_backoff_ms: u64, 
    pub retry_max_backoff_ms: u64, 
    pub cache_ttl: u64,
    pub keys: *const *const c_char,
    pub values: *const *const c_char,
    pub len: usize,
}

/// Phase 1: Pruning
/// Partition Pruning, Z-Order Bounds Pruning, Discrete Candidates Pruning
async fn phase_1_load_table(
    ctx: &MergeContext,
) -> PolarsResult<(DeltaTable, Vec<String>, SchemaRef)> {
    let table = DeltaTable::try_from_url_with_storage_options(
        ctx.table_url.clone(),
        ctx.storage_options.clone()
    ).await.map_err(|e| PolarsError::ComputeError(format!("Delta load error: {}", e).into()))?;

    let polars_schema = get_polars_schema_from_delta(&table)?;
    let snapshot = table.snapshot().map_err(|e| PolarsError::ComputeError(format!("Snapshot: {}", e).into()))?;
    let part_cols = snapshot.metadata().partition_columns().clone();

    Ok((table, part_cols, polars_schema.into()))
}
// =========================================================
// Step 2: Analyze Source (Sync - Pure CPU/Polars)
// =========================================================
fn phase_1_analyze_source(
    ctx: &MergeContext,
    source_lf: &LazyFrame,
    polars_schema: &Schema,
    part_cols: &[String],
) -> PolarsResult<(Option<HashSet<Vec<String>>>, HashMap<String, (serde_json::Value, serde_json::Value)>, Option<PruningCandidates>)> {
    
    // A. Partition Values
    let source_partitions = get_source_partition_values(source_lf, part_cols);

    // B. Key Bounds
    let mut key_bounds_json = HashMap::new(); 
    for key in &ctx.merge_keys {
        let dtype = polars_schema.get_field(key).map(|f| f.dtype.clone()).unwrap_or(DataType::Null);
        if let Some((min_lit, max_lit)) = get_source_key_bounds(source_lf, key, &dtype) {
            let min_json = bound_to_json(&min_lit);
            let max_json = bound_to_json(&max_lit);
            key_bounds_json.insert(key.clone(), (min_json, max_json));
        }
    }

    // C. Discrete Candidates
    let mut pruning_candidates: Option<PruningCandidates> = None;
    if ctx.merge_keys.len() == 1 {
        let key_col = &ctx.merge_keys[0];
        let distinct_lf = source_lf.clone()
            .select([col(key_col)])
            .unique(None, UniqueKeepStrategy::Any)
            .limit((MAX_PRUNING_CANDIDATES + 1) as u32);

        if let Ok(df) = distinct_lf.collect_with_engine(Engine::Streaming) {
            if df.height() <= MAX_PRUNING_CANDIDATES {
                let s = df.column(key_col).unwrap();
                let dtype = s.dtype();
                if dtype.is_integer() {
                    if let Ok(s_cast) = s.cast(&DataType::Int64) {
                        if let Ok(ca) = s_cast.i64() {
                            let mut keys: Vec<i64> = ca.into_iter().flatten().collect();
                            keys.sort_unstable();
                            if !keys.is_empty() { pruning_candidates = Some(PruningCandidates::Integers(keys)); }
                        }
                    }
                } else if dtype == &DataType::String {
                     if let Ok(ca) = s.str() {
                         let mut keys: Vec<String> = ca.into_iter().flatten().map(|v| v.to_string()).collect();
                         keys.sort_unstable();
                         if !keys.is_empty() { pruning_candidates = Some(PruningCandidates::Strings(keys)); }
                     }
                }
            }
        }
    }

    Ok((source_partitions, key_bounds_json, pruning_candidates))
}
async fn phase_1_scan_and_prune(
    table: &DeltaTable,
    part_cols: &[String],
    source_partitions: Option<HashSet<Vec<String>>>,
    key_bounds_json: HashMap<String, (serde_json::Value, serde_json::Value)>,
    pruning_candidates: Option<PruningCandidates>,
    merge_keys_0: Option<&String>,
) -> PolarsResult<Vec<Remove>> {
    
    let mut remove_actions = Vec::new();
    let t_scan = table.clone(); 
    let mut stream = t_scan.get_active_add_actions_by_partitions(&[]);

    while let Some(action_res) = stream.next().await {
        let view = action_res.map_err(|e| PolarsError::ComputeError(format!("Delta stream error: {}", e).into()))?;
        
        // --- Filter 1: Partition Pruning ---
        if let Some(affected_set) = &source_partitions {
            let maybe_key = view.partition_values().map(|struct_data| {
                 let fields = struct_data.fields();
                 let values = struct_data.values();
                 let mut key = Vec::with_capacity(part_cols.len());
                 for target_col_name in part_cols {
                    let val_str = fields.iter().zip(values.iter())
                        .find(|(f, _)| f.name() == target_col_name)
                        .map(|(_, v)| if v.is_null() { "__HIVE_DEFAULT_PARTITION__".to_string() } else { v.serialize() })
                        .unwrap_or_else(|| "__HIVE_DEFAULT_PARTITION__".to_string());
                    key.push(val_str);
                 }
                 key
            });

            let file_key = match maybe_key {
                Some(k) => k,
                None => {
                    let path_str = view.path();
                    let file_parts_map = parse_hive_partitions(path_str.as_ref(), part_cols);
                    part_cols.iter().map(|c| file_parts_map.get(c).cloned().flatten().unwrap_or_else(|| "__HIVE_DEFAULT_PARTITION__".to_string())).collect()
                }
            };
            
            if !affected_set.contains(&file_key) { continue; }
        }

        // --- Filter 2: Stats Pruning ---
        let mut file_overlaps = true;
        let stats_json_str = view.stats();
        let stats_struct: Option<serde_json::Value> = stats_json_str
            .map(|s| serde_json::from_str(&s).unwrap_or(serde_json::Value::Null));

        if !key_bounds_json.is_empty() {
            for (key, (src_min, src_max)) in &key_bounds_json {
                if !check_file_overlap_optimized(&stats_struct, key, src_min, src_max) {
                     file_overlaps = false; break;
                }
            }
        }

        // --- Filter 3: Discrete Candidates ---
        if file_overlaps && pruning_candidates.is_some() {
            if let Some(key_col) = merge_keys_0 {
                let candidates = pruning_candidates.as_ref().unwrap();
                let f_min_opt = extract_stats_value(&stats_struct, "minValues", key_col);
                let f_max_opt = extract_stats_value(&stats_struct, "maxValues", key_col);

                if let (Some(f_min_val), Some(f_max_val)) = (f_min_opt, f_max_opt) {
                    let hit = match candidates {
                        PruningCandidates::Integers(sorted_keys) => {
                            let f_min_i = as_i64_safe(f_min_val);
                            let f_max_i = as_i64_safe(f_max_val);
                            if let (Some(mn), Some(mx)) = (f_min_i, f_max_i) {
                                let idx = sorted_keys.partition_point(|&x| x < mn);
                                idx < sorted_keys.len() && sorted_keys[idx] <= mx
                            } else { true }
                        },
                        PruningCandidates::Strings(sorted_keys) => {
                             let f_min_s = f_min_val.as_str();
                             let f_max_s = f_max_val.as_str();
                             if let (Some(mn), Some(mx)) = (f_min_s, f_max_s) {
                                 let idx = sorted_keys.partition_point(|x| x.as_str() < mn);
                                 idx < sorted_keys.len() && sorted_keys[idx].as_str() <= mx
                             } else { true }
                        }
                    };
                    if !hit { file_overlaps = false; }
                }
            }
        }

        if file_overlaps {
            remove_actions.push(view.remove_action(true));
        }
    }
    Ok(remove_actions)
}

fn phase_validation(
    ctx: &MergeContext,
    source_lf: &mut LazyFrame,
    target_schema: &Schema,
) -> PolarsResult<()> {
    
    // =========================================================
    // Schema Compatibility Check (Metadata Level)
    // =========================================================
    let src_schema = source_lf.collect_schema()
        .map_err(|e| PolarsError::ComputeError(format!("Failed to get source schema: {}", e).into()))?;

    // A. Merge Key Checks
    for key in &ctx.merge_keys {
        // 1. Check Source Existence
        let src_field = src_schema.get(key)
            .ok_or_else(|| PolarsError::ComputeError(format!(
                "Merge Key '{}' not found in SOURCE table schema.", key
            ).into()))?;

        // 2. Check Target Existence
        let tgt_field = target_schema.get_field(key)
            .ok_or_else(|| PolarsError::ComputeError(format!(
                "Merge Key '{}' not found in TARGET table schema.", key
            ).into()))?;

        // 3. Strict Type Check
        if src_field != &tgt_field.dtype {
             return Err(PolarsError::SchemaMismatch(format!(
                "Merge Key Type Mismatch for column '{}'! \n\
                 Source: {:?} \n\
                 Target: {:?} \n\
                 Merge Keys must have identical types.", 
                key, src_field, tgt_field.dtype
            ).into()));
        }
    }

    // Extra Column Check (Schema Evolution Guard)
    if !ctx.can_evolve {
        for name in src_schema.iter_names() {
            if target_schema.get_field(name).is_none() {
                return Err(PolarsError::SchemaMismatch(
                    format!("Schema mismatch: Source column '{}' is not present in target. Pass 'can_evolve=true' to allow.", name).into()
                ));
            }
        }
    }

    // =========================================================
    // 2. Data Quality Check (Execution Level)
    // =========================================================
    let partition_exprs: Vec<Expr> = ctx.merge_keys.iter().map(|k| col(k)).collect();
    
    let has_null_expr = polars::prelude::any_horizontal(
         ctx.merge_keys.iter().map(|k| col(k).is_null()).collect::<Vec<_>>()
    )?.alias("has_null_key");

    let check_lf = source_lf.clone()
        .select([
            len().over(partition_exprs.clone()).alias("group_count"),
            has_null_expr
        ])
        .filter(
            col("group_count").gt(lit(1)) // Duplicated
            .or(col("has_null_key"))      // Null Key Found
        )
        .limit(1); 

    let check_df = check_lf.collect_with_engine(Engine::Streaming)?;

    // =========================================================
    // Error Reporting
    // =========================================================
    if check_df.height() > 0 {
        // Null or Duplicate
        let is_null_error = check_df.column("has_null_key")?.bool()?.get(0).unwrap_or(false);

        if is_null_error {
             let msg = format!(
                "CRITICAL ERROR: Null values detected in Merge Keys! \n\
                 Merge Keys: {:?}", 
                ctx.merge_keys
            );
            return Err(PolarsError::ComputeError(msg.into()));
        } else {
            let example_dupes = source_lf.clone()
                .group_by(partition_exprs) 
                .agg([len().alias("duplicate_count")]) 
                .filter(col("duplicate_count").gt(lit(1))) 
                .sort(["duplicate_count"], SortMultipleOptions { 
                    descending: vec![true], 
                    ..Default::default() 
                }) 
                .limit(5) 
                .collect()?; 

            let msg = format!(
                "CRITICAL ERROR: Duplicate keys detected in SOURCE table!\n\
                 Merge expects unique source keys per checking round.\n\
                 [Merge Keys]: {:?}\n\
                 --- Duplicate Key Examples (Top 5) ---\n\
                 {}", 
                ctx.merge_keys, 
                example_dupes
            );
            
            return Err(PolarsError::Duplicate(msg.into()));
        }
    }

    Ok(())
}

fn construct_target_lf(
    ctx: &MergeContext,
    remove_actions: &[Remove],
    target_schema: &Schema,
    cloud_args: &RawCloudArgs,
) -> PolarsResult<LazyFrame> {
    
    if remove_actions.is_empty() {
        return Ok(DataFrame::empty_with_schema(target_schema).lazy());
    }

    // Build FileName list
    // table_url e.g., "s3://bucket/table/" -> trim -> "s3://bucket/table"
    let root_trimmed = ctx.table_url.as_str().trim_end_matches('/');
    
    let full_paths: Vec<PlRefPath> = remove_actions.iter()
        .map(|r| {
            let full = format!("{}/{}", root_trimmed, r.path);
            PlRefPath::new(full)
        })
        .collect();
    
    let paths_buffer = Buffer::from(full_paths);

    // Build ScanArgs
    let mut scan_args = ScanArgsParquet::default();
    
    // Build HiveOptions
    scan_args.hive_options = HiveOptions {
        enabled: Some(true),
        hive_start_idx: 0,
        schema: None,
        try_parse_dates: true,
    };

    // Build Cloud Options 
    scan_args.cloud_options = unsafe {
        build_cloud_options(
            cloud_args.provider,
            cloud_args.retries,
            cloud_args.retry_timeout_ms,
            cloud_args.retry_init_backoff_ms,
            cloud_args.retry_max_backoff_ms,
            cloud_args.cache_ttl,
            cloud_args.keys,
            cloud_args.values,
            cloud_args.len,
        )
    };

    // return LazyFrame
    LazyFrame::scan_parquet_files(paths_buffer, scan_args)
}

/// Phase 3: Planning
fn phase_planning(
    ctx: &MergeContext,
    target_lf: LazyFrame,
    mut source_lf: LazyFrame,
    target_schema: &Schema, 
) -> PolarsResult<LazyFrame> {

    // =========================================================
    // 1. Rename Source Columns (Avoid Collision)
    // =========================================================
    let src_schema = source_lf.collect_schema()
        .map_err(|e| PolarsError::ComputeError(format!("Source schema error: {}", e).into()))?;
    
    let mut rename_old = Vec::new();
    let mut rename_new = Vec::new();
    let mut source_cols_set = HashSet::new();

    for name in src_schema.iter_names() {
        rename_old.push(name.as_str());
        rename_new.push(format!("{}_src_tmp", name)); 
        source_cols_set.insert(name.to_string());
    }

    let source_renamed = source_lf.rename(rename_old, rename_new, true);

    // =========================================================
    // Execute Full Outer Join
    // =========================================================
    let left_on: Vec<Expr> = ctx.merge_keys.iter().map(|k| col(k)).collect();
    let right_on: Vec<Expr> = ctx.merge_keys.iter().map(|k| col(&format!("{}_src_tmp", k))).collect();

    let join_args = JoinArgs {
        how: JoinType::Full,
        validation: JoinValidation::ManyToMany, 
        coalesce: JoinCoalesce::KeepColumns,    
        maintain_order: MaintainOrderJoin::None,
        ..Default::default()
    };

    let joined_lf = target_lf.join(source_renamed, left_on, right_on, join_args);

    // =========================================================
    // 3. Define Logic Constants & State
    // =========================================================
    // Action Constants
    let act_keep   = lit(0); // Keep Target
    let act_insert = lit(1); // Insert new value in Source 
    let act_update = lit(2); // Update Target with Source 
    let act_delete = lit(3); // Marked as delete
    let act_ignore = lit(4); // Ignore

    // Row State 
    let first_key = &ctx.merge_keys[0];
    let src_key_col = format!("{}_src_tmp", first_key);
    
    let src_exists = col(&src_key_col).is_not_null();
    let tgt_exists = col(first_key).is_not_null();

    // Three Status
    let is_matched     = src_exists.clone().and(tgt_exists.clone());
    let is_source_only = src_exists.clone().and(tgt_exists.clone().not());
    let is_target_only = src_exists.not().and(tgt_exists);
    let expr_update = ctx.cond_update.clone().unwrap_or(lit(true));
    let expr_delete = ctx.cond_delete.clone().unwrap_or(lit(false));
    let expr_insert = ctx.cond_insert.clone().unwrap_or(lit(true));
    let expr_src_del = ctx.cond_src_delete.clone().unwrap_or(lit(false));
    // =========================================================
    // Calculate Action Column
    // =========================================================
    // Delete > Update > Insert > Ignore/Keep
    let action_expr = when(is_matched.clone().and(expr_delete))
        .then(act_delete.clone())
        .when(is_matched.and(expr_update))
        .then(act_update.clone())
        .when(is_source_only.clone().and(expr_insert))
        .then(act_insert.clone())
        .when(is_source_only)
        .then(act_ignore.clone()) 
        .when(is_target_only.and(expr_src_del))
        .then(act_delete.clone()) 
        .otherwise(act_keep)      
        .alias("_merge_action");

    // =========================================================
    // 5. Generate Column Expressions (Projection & Evolution)
    // =========================================================
    let mut final_exprs = Vec::new();

    // Target Schema + New Source Columns
    let mut output_cols = Vec::new();
    let mut seen = HashSet::new();

    // Add Target Columns
    for name in target_schema.iter_names() {
        output_cols.push(name.to_string());
        seen.insert(name.to_string());
    }
    // Add Source New Column (Schema Evolution)
    for name in src_schema.iter_names() {
        if !seen.contains(name.as_str()) {
            output_cols.push(name.to_string());
        }
    }

    for col_name in output_cols {
        let src_col_alias = format!("{}_src_tmp", col_name);
        let has_source = source_cols_set.contains(&col_name);
        let has_target = target_schema.get_field(&col_name).is_some();

        let src_val = if has_source { col(&src_col_alias) } else { lit(NULL) };
        let tgt_val = if has_target { col(&col_name) } else { lit(NULL) };

        let col_expr = if has_source {
            // Case 1: Source has this column (Full Update / New Data)
            // If Insert or Update, use Source；or Target
            when(col("_merge_action").eq(act_insert.clone())
                .or(col("_merge_action").eq(act_update.clone())))
            .then(src_val)
            .otherwise(tgt_val)
        } else {
            // Case 2: Source does not have this column (Partial Update / Evolution Old Rows)
            // Logic: 
            // - Insert (new row): Null
            // - Update (old row): Keep target
            // - Keep: Target
            when(col("_merge_action").eq(act_insert.clone()))
            .then(lit(NULL))
            .otherwise(tgt_val)
        };

        final_exprs.push(col_expr.alias(&col_name));
    }

    // =========================================================
    // 6. Final Assemble
    // =========================================================
    let processed_lf = joined_lf
        .with_column(action_expr)
        .filter(
            col("_merge_action").neq(act_delete.clone()) 
            .and(col("_merge_action").neq(act_ignore))   
        )
        .select(final_exprs);

    Ok(processed_lf)
}

/// Phase 4: Execution 
fn phase_execution(
    ctx: &MergeContext,
    processed_lf: LazyFrame,
    partition_cols: &[String],
    cloud_args: &RawCloudArgs,
) -> PolarsResult<(String, Uuid)> {
    
    // 1. Generate Identity
    let write_id = Uuid::new_v4();
    let staging_dir_name = format!(".merge_staging_{}", write_id);
    
    // table_url: s3://bucket/table -> trim -> s3://bucket/table
    let root_trimmed = ctx.table_url.as_str().trim_end_matches('/');
    let staging_uri = format!("{}/{}", root_trimmed, staging_dir_name);

    // 2. Configure Partition Strategy
    let partition_strategy = if partition_cols.is_empty() {
        PartitionStrategy::FileSize
    } else {
        let keys: Vec<Expr> = partition_cols.iter().map(|n| col(n)).collect();
        PartitionStrategy::Keyed {
            keys,
            include_keys: false,
            keys_pre_grouped: false,
        }
    };

    // 3. Configure Sink Destination (Hive Style)
    let hive_provider = HivePathProvider {
        extension: PlSmallStr::from_str(".parquet"),
    };
    
    let destination = SinkDestination::Partitioned {
        base_path: PlRefPath::new(&staging_uri), 
        file_path_provider: Some(file_provider::FileProviderType::Hive(hive_provider)),
        partition_strategy,
        max_rows_per_file: u32::MAX, 
        approximate_bytes_per_file: usize::MAX as u64,
    };

    // 4. Configure Parquet Options
    let write_opts = build_parquet_write_options(
        1,  // compression: Snappy
        -1, // compression level: Default
        true, // statistics: true 
        0,  // row_group_size: Default
        0,  // data_page_size: Default
        -1  // compat_level: Default
    ).map_err(|e| PolarsError::ComputeError(format!("Options error: {}", e).into()))?;

    // 5. Re-build Cloud Args (Unsafe FFI)
    let unified_args = unsafe {
        build_unified_sink_args(
            false, // mkdir
            false, // maintain_order: false
            0,     // sync_on_close: Default
            cloud_args.provider,
            cloud_args.retries,
            cloud_args.retry_timeout_ms,
            cloud_args.retry_init_backoff_ms,
            cloud_args.retry_max_backoff_ms,
            cloud_args.cache_ttl,
            cloud_args.keys,
            cloud_args.values,
            cloud_args.len,
        )
    };

    // 6. Execute
    processed_lf
        .sink(destination, FileWriteFormat::Parquet(write_opts), unified_args)?
        .collect_with_engine(Engine::Streaming)?;

    Ok((staging_dir_name, write_id))
}

/// Phase 5-A: Process Staging Files
/// Scan Staging, read Stats, move file, generate Add Actions
pub async fn phase_process_staging(
    table: &DeltaTable,
    staging_dir_name: &str,
    partition_cols: &[String],
    write_id: Uuid,
) -> PolarsResult<Vec<Action>> {
    
    let object_store = table.object_store();
    let staging_path = Path::from(staging_dir_name);

    // 1. List Files
    let file_metas: Vec<_> = object_store.list(Some(&staging_path))
        .filter_map(|res| async { res.ok() }) 
        .filter(|meta| futures::future::ready(meta.location.to_string().ends_with(".parquet")))
        .collect()
        .await;

    if file_metas.is_empty() {
        return Ok(Vec::new());
    }

    // 2. Parallel Processing (Read Stats + Rename)
    const CONCURRENCY: usize = 64; 

    let stream = futures::stream::iter(file_metas)
        .map(|meta| {
            let object_store = object_store.clone();
            let staging_dir_name = staging_dir_name.to_string();
            let partition_cols = partition_cols.to_vec();
            let write_id = write_id;

            async move {
                let src_path_str = meta.location.to_string();
                let file_size = meta.size as i64;

                // ---------------------------------------------------------
                // A. Read Parquet Footer (IO - Small Read)
                // ---------------------------------------------------------
                let mut reader = ParquetObjectReader::new(object_store.clone(), meta.location.clone())
                    .with_file_size(meta.size as u64);
                
                let footer = reader.get_metadata(None).await
                    .map_err(|e| PolarsError::ComputeError(format!("Read parquet footer error: {}", e).into()))?;
                
                // Extract Stats 
                let (_, stats_json) = extract_delta_stats(&footer)?;

                // ---------------------------------------------------------
                // B. Rename & Promote (IO - Metadata Operation)
                // ---------------------------------------------------------
                // Old Path: .merge_staging_xxx/region=A/part-001.parquet
                // Target Path: region=A/part-001-<uuid>.parquet
                
                // 1. Remove Staging Prefix
                let rel_path = src_path_str.trim_start_matches(&format!("{}/", staging_dir_name));
                // 2. Inject Write ID
                let dest_path_str = rel_path.replace(".parquet", &format!("-{}.parquet", write_id));
                
                let src_path = Path::from(src_path_str);
                let dest_path = Path::from(dest_path_str.clone());

                // Rename (S3: Copy + Delete，Azure/HDFS: Rename)
                object_store.rename(&src_path, &dest_path).await
                    .map_err(|e| PolarsError::ComputeError(format!("Rename failed: {}", e).into()))?;

                // ---------------------------------------------------------
                // C. Build Add Action (CPU)
                // ---------------------------------------------------------
                // Parse hive partitions
                let partition_values = parse_hive_partitions(&dest_path_str, &partition_cols);

                Ok::<Action, PolarsError>(Action::Add(Add {
                    path: dest_path_str,
                    size: file_size,
                    partition_values,
                    modification_time: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64,
                    data_change: true, 
                    stats: Some(stats_json),
                    tags: None,
                    deletion_vector: None,
                    base_row_id: None,
                    default_row_commit_version: None,
                    clustering_provider: None,
                }))
            }
        })
        .buffer_unordered(CONCURRENCY); 

    // 3. Collect Results
    let results: Vec<Result<Action, PolarsError>> = stream.collect().await;

    // Unwrap results
    let mut add_actions = Vec::with_capacity(results.len());
    for res in results {
        add_actions.push(res?);
    }

    Ok(add_actions)
}

/// Phase 5-B: Commit Transaction
async fn phase_commit(
    ctx: &MergeContext,
    table: &mut DeltaTable,
    mut actions: Vec<Action>, // Including Add(New) and Remove(Old)
    staging_dir_name: &str,
    source_schema: &Schema,   // Polars Source Schema
    target_schema: &Schema,   // Polars Target Schema
) -> PolarsResult<()> {

    // =========================================================
    // 1. Construct Logic Metadata (Audit Logs)
    // =========================================================
    let mut matched_preds = Vec::new();
    let mut not_matched_preds = Vec::new();           
    let mut not_matched_by_source_preds = Vec::new(); 

    // A. Matched (Update/Delete)
    if ctx.cond_delete.is_some() {
        matched_preds.push(MergePredicate {
            predicate: Some("matched_delete_condition".into()),
            action_type: "DELETE".into(),
        });
    }

    // Update 
    if ctx.cond_update.is_some() {
        // Update Cond -> Predicate
        matched_preds.push(MergePredicate { 
            predicate: Some("custom_update_condition".into()), 
            action_type: "UPDATE".into() 
        });
    } else {
        //  Upsert -> No Predicate
        matched_preds.push(MergePredicate { 
            predicate: None, 
            action_type: "UPDATE".into() 
        });
    }

    // B. Not Matched (Insert)
    if ctx.cond_insert.is_some() {
        not_matched_preds.push(MergePredicate {
            predicate: Some("not_matched_insert_condition".into()),
            action_type: "INSERT".into(),
        });
    } else {
        not_matched_preds.push(MergePredicate {
            predicate: None,
            action_type: "INSERT".into(),
        });
    }

    // C. Not Matched By Source (Target Delete)
    if ctx.cond_src_delete.is_some() {
        not_matched_by_source_preds.push(MergePredicate {
            predicate: Some("not_matched_by_source_delete_condition".into()),
            action_type: "DELETE".into(),
        });
    }

    // =========================================================
    // 2. Schema Evolution (Metadata Action)
    // =========================================================
    
    // Build New Polars Schema (Target + Source New Cols)
    let mut new_pl_schema = target_schema.clone();
    let mut has_schema_change = false;
    
    for (name, dtype) in source_schema.iter() {
        if new_pl_schema.get_field(name).is_none() {
            new_pl_schema.with_column(name.to_string().into(), dtype.clone());
            has_schema_change = true;
        }
    }

    if has_schema_change {
        // Convert to Delta Schema
        let new_delta_schema = convert_to_delta_schema(&new_pl_schema)?;
        
        let current_snapshot = table.snapshot().map_err(|e| PolarsError::ComputeError(format!("Snapshot error: {}", e).into()))?;
        let current_delta_schema = current_snapshot.schema();

        if current_delta_schema.as_ref() != &new_delta_schema {
            if !ctx.can_evolve {
                return Err(PolarsError::SchemaMismatch("Schema mismatch detected. Pass 'can_evolve=true'.".into()));
            }
            
            // Current Metadata => JSON Value
            let current_metadata = current_snapshot.metadata();
            let mut meta_json = serde_json::to_value(current_metadata)
                .map_err(|e| PolarsError::ComputeError(format!("Failed to serialize metadata: {}", e).into()))?;

            // New Schema => String
            let new_schema_string = serde_json::to_string(&new_delta_schema)
                .map_err(|e| PolarsError::ComputeError(format!("Failed to serialize new schema: {}", e).into()))?;

            // Replace "schemaString" field in JSON
            if let Some(obj) = meta_json.as_object_mut() {
                obj.insert("schemaString".to_string(), serde_json::Value::String(new_schema_string));
            } else {
                return Err(PolarsError::ComputeError("Metadata is not a JSON object".into()));
            }

            // Convert to Metadata Action
            let new_metadata_action: deltalake::kernel::Metadata = serde_json::from_value(meta_json)
                .map_err(|e| PolarsError::ComputeError(format!("Failed to deserialize modified metadata: {}", e).into()))?;

            // Inject Actions List
            actions.insert(0, Action::Metadata(new_metadata_action));
        }
    }

    // =========================================================
    // 3. Commit Transaction
    // =========================================================
    
    // Build SQL Style Predicate (e.g. "source.id = target.id")
    let join_predicate = ctx.merge_keys.iter()
        .map(|k| format!("source.{} = target.{}", k, k))
        .collect::<Vec<_>>()
        .join(" AND ");

    let operation = DeltaOperation::Merge {
        predicate: Some(join_predicate.clone()),
        merge_predicate: Some(join_predicate), 
        matched_predicates: matched_preds,
        not_matched_predicates: not_matched_preds,
        not_matched_by_source_predicates: not_matched_by_source_preds,
    };

    // =========================================================
    // 3. Strict Commit (No Internal Retry)
    // =========================================================
    
    let commit_res = CommitBuilder::default()
        .with_actions(actions)
        .with_max_retries(0) // NO internal retry
        .build(
            Some(table.snapshot().map_err(|e| PolarsError::ComputeError(format!("{}", e).into()))?), 
            table.log_store().clone(),
            operation
        )
        .await;

    commit_res.map_err(|e| PolarsError::ComputeError(format!("Commit failed (Strict Mode): {}", e).into()))?;

    // =========================================================
    // 4. Cleanup
    // =========================================================
    let object_store = table.object_store();
    let staging_path = Path::from(staging_dir_name);
    let _ = object_store.delete(&staging_path).await;
    
    Ok(())
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_io_delta_merge(
    source_lf_ptr: *mut LazyFrameContext, 
    target_path_ptr: *const c_char,
    merge_keys_ptr: *const *const c_char,
    merge_keys_len: usize,
    
    // --- Merge Conditions (Expr) ---
    matched_update_cond: *mut ExprContext,        // WHEN MATCHED AND cond THEN UPDATE
    matched_delete_cond: *mut ExprContext,        // WHEN MATCHED AND cond THEN DELETE
    not_matched_insert_cond: *mut ExprContext,    // WHEN NOT MATCHED AND cond THEN INSERT
    not_matched_by_source_delete_cond: *mut ExprContext, // WHEN NOT MATCHED BY SOURCE AND cond THEN DELETE

    // --- Schema Evolution Switch ---
    can_evolve: bool, 
    // --- Cloud Args ---
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
        // -------------------------------------------------------------------------
        // Step 0: Unpack Arguments & Initialize Context
        // -------------------------------------------------------------------------
        
        // 1. Parse Basic Args
        let path_str = ptr_to_str(target_path_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        let merge_keys = unsafe { ptr_to_vec_string(merge_keys_ptr, merge_keys_len) };
        let table_url = parse_table_url(path_str)?;
        if merge_keys.is_empty() {
             return Err(PolarsError::ComputeError("Merge keys cannot be empty".into()));
        }

        // 2. Consume Source LazyFrame
        let source_lf_ctx = unsafe { Box::from_raw(source_lf_ptr) };
        let mut source_lf = source_lf_ctx.inner; 

        // 3. Consume Conditions (Handle Defaults for Upsert Logic)
        let cond_update = if !matched_update_cond.is_null() { 
            Some(unsafe { *Box::from_raw(matched_update_cond) }.inner)
        } else { 
            None 
        };

        let cond_delete = if !matched_delete_cond.is_null() { 
            Some(unsafe { *Box::from_raw(matched_delete_cond) }.inner)
        } else { 
            None 
        };

        let cond_insert = if !not_matched_insert_cond.is_null() { 
            Some(unsafe { *Box::from_raw(not_matched_insert_cond) }.inner)
        } else { 
            None
        };

        let cond_src_delete = if !not_matched_by_source_delete_cond.is_null() { 
            Some(unsafe { *Box::from_raw(not_matched_by_source_delete_cond) }.inner)
        } else { 
            None
        };

        // 4. Build Cloud Options Map (For Delta-RS)
        let delta_storage_options = build_delta_storage_options_map(cloud_keys, cloud_values, cloud_len);

        // 5. Pack Context
        let ctx = MergeContext {
            table_url,
            merge_keys,
            storage_options: delta_storage_options,
            can_evolve,
            cond_update,
            cond_delete,
            cond_insert,
            cond_src_delete,
        };

        // 6. Pack Raw Cloud Args (For Polars Scan/Sink later)
        let cloud_args = RawCloudArgs {
            provider: cloud_provider,
            retries: cloud_retries,
            retry_timeout_ms: cloud_retry_timeout_ms,
            retry_init_backoff_ms: cloud_retry_init_backoff_ms,
            retry_max_backoff_ms: cloud_retry_max_backoff_ms,
            cache_ttl: cloud_cache_ttl,
            keys: cloud_keys,
            values: cloud_values,
            len: cloud_len,
        };

        let rt = get_runtime();

        // =========================================================
        // Phase 1: Pruning (Async Context 1)
        // =========================================================
        let (mut table, partition_cols,mut target_schema) = rt.block_on(
            phase_1_load_table(&ctx)
        )?;
        
        let (source_partitions, key_bounds, candidates) = phase_1_analyze_source(
            &ctx, 
            &source_lf, 
            &target_schema, 
            &partition_cols
        )?;
        // =========================================================================
        // THE GRAND RETRY LOOP
        // =========================================================================
        let mut attempt = 0;
        
        loop {
            attempt += 1;
            
            // Reload table status if attempted
                if attempt > 1 {
                println!("[Delta-RS] Conflict detected. Starting Full Retry attempt {}/{}...", attempt, MAX_FULL_JOB_RETRIES);
                
                rt.block_on(async {
                    table.update_state().await
                        .map_err(|e| PolarsError::ComputeError(format!("Reload table failed: {}", e).into()))
                })?;
                
                // Update Target Schema
                let _snapshot = table.snapshot().map_err(|e| PolarsError::ComputeError(format!("{}", e).into()))?;
                let polars_schema = get_polars_schema_from_delta(&table)?;
                target_schema = polars_schema.into();
            }

            // ---------------------------------------------------------------------
            // Phase 1-C: Scan & Prune
            // ---------------------------------------------------------------------
            let remove_actions = rt.block_on(
                phase_1_scan_and_prune(
                    &table, 
                    &partition_cols, 
                    source_partitions.clone(), 
                    key_bounds.clone(), 
                    candidates.clone(),
                    ctx.merge_keys.get(0)
                )
            )?;

            // ---------------------------------------------------------------------
            // Phase 2, 3: Validation & Planning (Polars)
            // ---------------------------------------------------------------------
            // Clone source_lf_plan
            let mut current_source_lf = source_lf.clone(); 
            
            phase_validation(&ctx, &mut current_source_lf, &target_schema)?;

            let target_lf = construct_target_lf(&ctx, &remove_actions, &target_schema, &cloud_args)?;
            
            let processed_lf = phase_planning(&ctx, target_lf, current_source_lf, &target_schema)?;

            // ---------------------------------------------------------------------
            // Phase 4: Execution (IO - Write NEW Staging Files)
            // ---------------------------------------------------------------------
            let (staging_dir, write_id) = phase_execution(&ctx, processed_lf, &partition_cols, &cloud_args)?;

            let staging_dir_for_commit = staging_dir.clone();
            // ---------------------------------------------------------------------
            // Phase 5: Commit (Try to finalize)
            // ---------------------------------------------------------------------
            let commit_result = rt.block_on(async {
                let add_actions = phase_process_staging(&table, &staging_dir, &partition_cols, write_id).await?;

                let mut final_actions = Vec::with_capacity(remove_actions.len() + add_actions.len());
                for r in remove_actions { final_actions.push(Action::Remove(r)); }
                final_actions.extend(add_actions);

                if !final_actions.is_empty() {
                    let src_schema = source_lf.collect_schema().unwrap();
                    
                    phase_commit(
                        &ctx, 
                        &mut table, 
                        final_actions, 
                        &staging_dir, 
                        &src_schema,
                        &target_schema
                    ).await?;
                } else {
                     let object_store = table.object_store();
                     let _ = object_store.delete(&Path::from(staging_dir)).await;
                }
                Ok::<(), PolarsError>(())
            });

            // ---------------------------------------------------------------------
            // Error Handling & Retry Decision
            // ---------------------------------------------------------------------
            match commit_result {
                Ok(_) => {
                    break;
                },
                Err(e) => {
                    let err_msg = format!("{:?}", e);
                    
                    let is_conflict = err_msg.contains("Transaction") || 
                                      err_msg.contains("Conflict") || 
                                      err_msg.contains("VersionMismatch"); 

                    if is_conflict && attempt < MAX_FULL_JOB_RETRIES {
                        rt.block_on(async {
                            let object_store = table.object_store();
                            let _ = object_store.delete(&Path::from(staging_dir_for_commit)).await;
                        });
                        
                        let base_backoff = 100 * 2_u64.pow(attempt as u32);

                        // Insert 0~50% Jitter
                        let jitter = rand::Rng::random_range(&mut rand::rng(), 0..base_backoff/2); 
                        let final_backoff = base_backoff + jitter;

                        std::thread::sleep(std::time::Duration::from_millis(final_backoff));
                        
                        continue; 
                    } else {
                        return Err(e);
                    }
                }
            }
        } // End Loop

        Ok(())
    })
}