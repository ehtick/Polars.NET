// ==========================================
// Delta Lake Support
// ==========================================

use chrono::{DateTime, NaiveDate, TimeDelta, Utc};
use deltalake::kernel::engine::arrow_conversion::{TryIntoArrow, TryIntoKernel};
use deltalake::kernel::transaction::{CommitBuilder};
use deltalake::kernel::scalars::ScalarExt;
use deltalake::parquet::data_type::ByteArray;
use deltalake::parquet::file::metadata::ParquetMetaData;
use deltalake::{DeltaTable, DeltaTableBuilder, Path};
use deltalake::protocol::{DeltaOperation, MergePredicate, SaveMode};
use deltalake::parquet::arrow::async_reader::{AsyncFileReader, ParquetObjectReader};
use deltalake::parquet::file::statistics::Statistics as ParquetStatistics;
use futures::StreamExt;
use polars::prelude::file_provider::HivePathProvider;
use serde_json::{json, Value};
use polars::{error::PolarsError, prelude::*};
use polars_buffer::Buffer;
use tokio::runtime::Runtime;
use crate::io::{build_cloud_options, build_parquet_write_options, build_partitioned_destination, build_scan_args, build_unified_sink_args};
use crate::types::{ExprContext, LazyFrameContext, SchemaContext, SelectorContext};
use crate::utils::{ptr_to_str, ptr_to_vec_string};
use std::time::{SystemTime, UNIX_EPOCH};
use std::os::raw::c_char;
use std::sync::OnceLock;
use url::Url;
use uuid::Uuid;
use std::collections::{HashMap, HashSet};
use deltalake::arrow::ffi::FFI_ArrowSchema as DeltaFFISchema;
use polars_arrow::ffi::{ArrowSchema as PolarsFFISchema};
use polars_arrow::ffi::import_field_from_c;
use deltalake::arrow::datatypes::{Schema as DeltaArrowSchema};
use deltalake::kernel::{Action, Add, Remove, StructType, transaction};

static RUNTIME: OnceLock<Runtime> = OnceLock::new();

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

/// 提取 Parquet 统计信息并转换为 Delta JSON 格式
/// 返回: (num_records, stats_json_string)
fn extract_delta_stats(metadata: &ParquetMetaData) -> Result<(i64, String), PolarsError> {
    let file_metadata = metadata.file_metadata();
    let num_rows = file_metadata.num_rows();
    
    // Delta Stats 结构
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

    // 遍历所有列
    for (col_idx, column_descr) in schema_descr.columns().iter().enumerate() {
        let col_name = column_descr.name();
        // let col_path = column_descr.path().string(); // 处理嵌套列名 a.b.c

        // 聚合该列在所有 RowGroups 中的统计信息
        let mut global_min: Option<Value> = None;
        let mut global_max: Option<Value> = None;
        let mut global_nulls: i64 = 0;
        let mut has_stats = false;

        for rg in metadata.row_groups() {
            if let Some(col_chunk) = rg.column(col_idx).statistics() {
                has_stats = true;
                global_nulls += col_chunk.null_count_opt().unwrap_or(0) as i64;

                // 转换 Min/Max
                let (min_val, max_val) = parquet_stats_to_json(col_chunk);
                
                // Aggregation: Min
                if let Some(v) = min_val {
                    match global_min {
                        None => global_min = Some(v),
                        Some(ref current) => {
                            // 简单的比较逻辑，这里假设类型一致。
                            // 实际上 JSON Value 比较可能不够精确，但在 Delta 场景下通常够用
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

// 辅助 helper：尝试把 JSON Value 转为 i64
// 兼容 JSON Number 和 JSON String (因为 Delta Log 有时把大整数存成字符串)
fn as_i64_safe(v: &serde_json::Value) -> Option<i64> {
    v.as_i64()
     .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
}

// 辅助 helper：尝试把 JSON Value 转为 f64
fn as_f64_safe(v: &serde_json::Value) -> Option<f64> {
    v.as_f64()
     .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
}

// 优化版的 Overlap Check (接收 JSON Value 而不是 String)
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
        // A. 数字比较 (优先 i64，兜底 f64)
        serde_json::Value::Number(s_min_num) => {
            if let serde_json::Value::Number(s_max_num) = src_max {
                
                // [Step 1]: 尝试 Integer (i64) 路径 -> 保证 ID/Timestamp 精度
                // 只有当 source 和 target 全都能转成 i64 时才走这条路
                let s_min_i = s_min_num.as_i64();
                let s_max_i = s_max_num.as_i64();
                let f_min_i = as_i64_safe(f_min);
                let f_max_i = as_i64_safe(f_max);

                if let (Some(s_min), Some(s_max), Some(f_min), Some(f_max)) = (s_min_i, s_max_i, f_min_i, f_max_i) {
                     // 整数比较：不重叠条件
                     if f_max < s_min || f_min > s_max {
                        return false; 
                     }
                     return true; // 整数判定重叠，直接返回
                }

                // [Step 2]: 如果 i64 解析失败 (说明可能是浮点数)，Fallback 到 f64
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
        
        // B. 字符串/日期/Decimal (Lexicographical Compare)
        // 注意：Decimal 如果通过 Scalar::to_json 变成了 String，会走这里
        // 字符串比较对于 ISO Date 是安全的，但对于 "10.0" vs "2.0" 是不安全的 (Decimal 需注意)
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

// 辅助：比较 JSON Value 大小 (简化版)
fn is_smaller(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(n1), Value::Number(n2)) => {
             if let (Some(f1), Some(f2)) = (n1.as_f64(), n2.as_f64()) { f1 < f2 } 
             else if let (Some(i1), Some(i2)) = (n1.as_i64(), n2.as_i64()) { i1 < i2 }
             else { false }
        },
        (Value::String(s1), Value::String(s2)) => s1 < s2,
        _ => false // 其他类型暂时忽略比较
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

// 辅助：将 Parquet 原始 Stats 转为 JSON Value
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
        // Delta Lake 要求 String 列的 Stats 必须是 UTF-8 字符串
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
        // 很难直接转为 JSON 可读格式，且已被废弃，暂时忽略
        ParquetStatistics::Int96(_) => (None, None),
        
        // FixedLenByteArray (通常是 Decimal)
        // 需要 Schema 中的 Precision/Scale 才能正确反序列化，
        // 这里没有 Schema 上下文，盲目转换会导致数值错误，安全起见忽略。
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
    // allow_missing_columns: bool, // <--- 这一点至关重要，必须为 true
    rechunk: bool, 
    cache: bool,   
    // --- Optional Names ---
    row_index_name_ptr: *const c_char,
    row_index_offset: u32,
    include_path_col_ptr: *const c_char,
    // --- Schema ---
    schema_ptr: *mut SchemaContext, // 用户手动传的 Schema (优先级最高)
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
        // 这里我们先用 build_scan_args 构建基础参数
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

        // 5. Inject Delta Schema (如果是自动 Schema 模式)
        // 如果用户没有显式传入 Schema (args.schema.is_none())，
        // 我们就把从 Delta Log 里拿到的 Schema 塞进去。
        if args.schema.is_none() {
            // polars_schema 已经是 Schema 类型了，直接装箱
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
    // Case A: 这是一个标准的 URL (如 s3://, abfss://, gs://)
    if let Ok(u) = Url::parse(path_str) {
        // 这里可以加一个额外的检查：如果它是单盘符路径 (如 "C:/tmp")，Url::parse 可能会报错或者解析出意料之外的结果
        // 但通常带 scheme 的 (s3://) 都会在这里成功直接返回
        if u.has_host() || u.scheme() == "file" {
            return Ok(u);
        }
    }

    // Case B: 这是一个本地文件路径 (如 ./data/test 或 /tmp/test 或 C:\data\test)
    // 必须转换为绝对路径，才能生成合法的 file:// URL
    
    // 尝试标准化路径 (解析 . 和 ..)
    // 注意：canonicalize 要求路径必须存在。如果是新建表，路径可能尚不存在。
    let abs_path = std::fs::canonicalize(path_str)
        .or_else(|_| {
            // 如果路径不存在 (比如新建表)，尝试手动拼接当前目录
            std::env::current_dir().map(|cwd| cwd.join(path_str))
        })
        .map_err(|e| PolarsError::ComputeError(format!("Invalid local path '{}': {}", path_str, e).into()))?;

    // 转换为 file:// URL
    // 注意：from_directory_path 要求必须是绝对路径
    Url::from_directory_path(abs_path)
        .map_err(|_| PolarsError::ComputeError(format!("Could not convert path '{}' to file URL", path_str).into()))
}

pub(crate) fn convert_to_delta_schema(pl_schema: &Schema) -> PolarsResult<StructType> {
    // 1. Polars Schema -> Polars Arrow Schema
    // CompatLevel::newest() 确保使用最新的 Arrow 规范
    let pl_arrow_schema = pl_schema.to_arrow(CompatLevel::newest());

    // 2. FFI 转换循环: Polars Field -> Delta Field
    let delta_arrow_fields = pl_arrow_schema.iter_values().map(|pl_field| {
        // A. Export: Polars Field -> Polars FFI Struct
        // export_field_to_c 是 Polars 提供的将 Field 导出为 C 结构体的方法
        let pl_ffi = polars_arrow::ffi::export_field_to_c(pl_field);
        
        // B. Cast: Polars FFI Ptr -> Delta FFI Ptr
        //这是最关键的一步：利用 C Data Interface 的二进制兼容性进行指针强转。
        // 我们假设 Delta (deltalake::arrow) 的 FFI_ArrowSchema 内存布局与 Polars 的一致。
        let delta_ffi_ptr = &pl_ffi as *const _ as *const deltalake::arrow::ffi::FFI_ArrowSchema;

        // C. Import: Delta FFI -> Delta Arrow Field
        // 使用 deltalake 依赖的 arrow crate 从 FFI 指针重建 Field
        unsafe { deltalake::arrow::datatypes::Field::try_from(&*delta_ffi_ptr) }
            .map_err(|e| PolarsError::ComputeError(format!("FFI Import Error: {}", e).into()))
    }).collect::<Result<Vec<deltalake::arrow::datatypes::Field>, PolarsError>>()?;

    // 3. 构建 Delta (Standard) Arrow Schema
    let delta_arrow_schema = deltalake::arrow::datatypes::Schema::new(delta_arrow_fields);

    // 4. 转换为 Delta Kernel Schema (StructType)
    // 这一步如果不提出来，就会报你刚才那个 E0277 错误，因为 Polars Schema 没有实现 TryIntoKernel
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
     // Delta SaveMode (Append/Overwrite/...)
    
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

        // 1. 获取 Schema & 解析分区列
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
        let write_id = Uuid::new_v4(); // 本次写入的唯一 ID

        // 2. 构造暂存目录路径 (Staging Path)
        // 例如: s3://bucket/table/.tmp_write_<uuid>
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
        // 3. 构建 Cloud Options & Delta Table
        let delta_storage_options = build_delta_storage_options_map(cloud_keys, cloud_values, cloud_len);
        
        let (mut table, old_files_to_remove, should_skip) = rt.block_on(async {
            let table_url = parse_table_url(base_path_str)?;
            let dt = DeltaTable::try_from_url_with_storage_options(table_url.clone(), delta_storage_options.clone())
                .await.map_err(|e| PolarsError::ComputeError(format!("Delta init error: {}", e).into()))?;
            
            let mut old_files = Vec::new();
            let mut skip_write = false;

            if dt.version() >= Some(0) {
                match save_mode {
                    // Case A: ErrorIfExists -> 直接报错
                    SaveMode::ErrorIfExists => {
                        return Err(PolarsError::ComputeError(format!("Table already exists at {}", table_url).into()));
                    },
                    // Case B: Ignore -> 标记跳过
                    SaveMode::Ignore => {
                        skip_write = true;
                    },
                    // Case C: Overwrite -> 收集旧文件
                    SaveMode::Overwrite => {
                        let files = dt.get_files_by_partitions(&[]).await
                            .map_err(|e| PolarsError::ComputeError(format!("List files error: {}", e).into()))?;
                        for p in files {
                            old_files.push(p.to_string());
                        }
                    },
                    // Case D: Append -> 正常继续
                    _ => {} 
                }
            }
            Ok::<_, PolarsError>((dt, old_files, skip_write))
        })?;

        if should_skip {
            return Ok(());
        }

        // 4. 执行 Polars Sink -> 写入到 Staging 目录
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
            
        // ==========================================

        // 5. 扫描 Staging -> Move -> Commit
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
            let staging_path = Path::from(staging_dir_name.clone());

            let mut actions = Vec::new();
            // ==========================================
            // OPTIMIZATION: Schema Evolution Check
            // ==========================================
            // 1. 获取当前表的 Schema (从最新 Snapshot)
            let current_snapshot = table.snapshot()
                .map_err(|e| PolarsError::ComputeError(format!("Failed to get snapshot: {}", e).into()))?;
            let current_delta_schema = current_snapshot.schema();
            
            // 2. 将当前的 Polars Schema 转为 Delta Schema
            let new_delta_schema = convert_to_delta_schema(&schema)?;

            // 3. 对比 Schema 是否发生变化
            if current_delta_schema.as_ref() != &new_delta_schema {
                if !can_evolve {
                    return Err(PolarsError::ComputeError(
                        "Schema mismatch detected. If you want to evolve the schema, enable 'can_evolve'.".into()
                    ));
                }

                // 4. 构造 Metadata Action 进行演变
                // 我们保留原有的 configuration 和 created_time，只更新 schema
                let current_metadata = current_snapshot.metadata();

                // 2. 序列化为 JSON Value (保留 ID, Config, Partitions 等所有旧状态)
                let mut meta_json = serde_json::to_value(current_metadata)
                    .map_err(|e| PolarsError::ComputeError(format!("Failed to serialize metadata: {}", e).into()))?;

                // 3. 生成新的 Schema JSON String
                let new_schema_string = serde_json::to_string(&new_delta_schema)
                    .map_err(|e| PolarsError::ComputeError(format!("Failed to serialize new schema: {}", e).into()))?;

                // 4. 修改 JSON 对象中的 "schemaString" 字段
                // 注意：Delta Protocol 定义的 JSON Key 是 "schemaString" (CamelCase)，不是 Rust 字段名 schema_string
                if let Some(obj) = meta_json.as_object_mut() {
                    obj.insert("schemaString".to_string(), Value::String(new_schema_string));
                } else {
                    return Err(PolarsError::ComputeError("Metadata is not a JSON object".into()));
                }

                // 5. 反序列化回 Metadata 结构体
                let new_metadata_action: deltalake::kernel::Metadata = serde_json::from_value(meta_json)
                    .map_err(|e| PolarsError::ComputeError(format!("Failed to recreate metadata: {}", e).into()))?;

                // 6. 注入 Metadata Action (放在最前面)
                actions.insert(0, Action::Metadata(new_metadata_action));
            }
            // ==========================================
            // Step B: Parallel Staging Processing 🚀
            // ==========================================
            // 1. 获取所有文件列表 (Fast Listing)
            use futures::StreamExt;
            let file_metas: Vec<_> = object_store.list(Some(&staging_path))
                .filter_map(|res| async { res.ok() }) // 过滤 List 错误
                .filter(|meta| futures::future::ready(meta.location.to_string().ends_with(".parquet")))
                .collect()
                .await;

            // 2. 设定并发度 (例如 64)
            const CONCURRENCY: usize = 64;

            // 3. 构建并发流
            let stream = futures::stream::iter(file_metas)
                .map(|meta| {
                    // Clone 变量以移入 async block
                    let object_store = object_store.clone();
                    let staging_dir_name = staging_dir_name.clone();
                    let partition_cols = partition_cols.clone();
                    let write_id = write_id.clone();
                    
                    async move {
                        let src_path_str = meta.location.to_string();
                        let file_size = meta.size as i64;

                        // C. Read Stats (Network IO)
                        let mut reader = ParquetObjectReader::new(object_store.clone(), meta.location.clone())
                                .with_file_size(meta.size as u64);
                        // 注意：这里最好给 get_metadata 传 None 或者默认 options
                        let footer = reader.get_metadata(None).await
                                .map_err(|e| PolarsError::ComputeError(format!("Read footer error: {}", e).into()))?;
                        let (_, stats_json) = extract_delta_stats(&footer)?;

                        // D. Rename (Heavy Network IO)
                        let rel_path = src_path_str.trim_start_matches(&format!("{}/", staging_dir_name));
                        // 清理路径开头可能的 '/'
                        let rel_path_clean = rel_path.trim_start_matches('/');
                        let dest_path_str = rel_path_clean.replace(".parquet", &format!("-{}.parquet", write_id));
                        
                        let src_path = Path::from(src_path_str);
                        let dest_path = Path::from(dest_path_str.clone());
                        
                        // 执行 Rename
                        object_store.rename(&src_path, &dest_path).await
                             .map_err(|e| PolarsError::ComputeError(format!("Rename failed: {}", e).into()))?;

                        // E. Build Add Action (CPU)
                        let partition_values = parse_hive_partitions(&dest_path_str, &partition_cols);

                        Ok::<Action, PolarsError>(Action::Add(Add {
                            path: dest_path_str,
                            size: file_size,
                            partition_values,
                            modification_time: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64,
                            data_change: true,
                            stats: Some(stats_json),
                            ..Default::default()
                        }))
                    }
                })
                .buffer_unordered(CONCURRENCY); // 🚀 乱序并发执行

            // 4. 收集并发结果
            let processed_files: Vec<Result<Action, PolarsError>> = stream.collect().await;
            
            // 解包并将 Add Action 加入 final_actions
            for res in processed_files {
                actions.push(res?);
            }

            // G. 处理 Overwrite (删除旧文件)
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

            // H. 提交事务
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
// 定义我们内部的 Bound Map
type PruningMap = HashMap<String, (serde_json::Value, serde_json::Value)>;

// 递归提取 Expr 中的等于条件 (Col == Lit)
fn extract_bounds_from_expr(expr: &Expr, map: &mut PruningMap) {
    match expr {
        // 处理 (A == B) & (C == D) 的情况：递归左右
        Expr::BinaryExpr { left, op: Operator::And, right } => {
            extract_bounds_from_expr(left, map);
            extract_bounds_from_expr(right, map);
        },
        
        // 处理 Col("A") == Lit(10)
        Expr::BinaryExpr { left, op: Operator::Eq, right } => {
            // 检查 left 是否是 Col, right 是否是 Lit
            if let (Expr::Column(name), Expr::Literal(lit_val)) = (left.as_ref(), right.as_ref()) {
                insert_bound(map, name.as_str(), lit_val);
            } 
            // 反过来也行：Lit(10) == Col("A")
            else if let (Expr::Literal(lit_val), Expr::Column(name)) = (left.as_ref(), right.as_ref()) {
                insert_bound(map, name.as_str(), lit_val);
            }
        },
        
        // 处理 Alias (有些 Expr 会被 alias 包裹)
        Expr::Alias(inner, _) => {
            extract_bounds_from_expr(inner, map);
        },
        
        // 其他复杂 Expr (Gt, Lt, Or, Function) 暂时忽略，不做剪枝
        _ => {}
    }
}

fn insert_bound(map: &mut PruningMap, col_name: &str, lit: &LiteralValue) {
    // 调用下面的转换函数
    if let Some(json_val) = polars_literal_to_json(lit) {
        // 对于 (Col == Lit) 的情况，剪枝范围就是 [Lit, Lit]
        // 所以我们把同一个值 clone 两份，分别作为 min 和 max
        map.insert(col_name.to_string(), (json_val.clone(), json_val));
    }
}

fn polars_literal_to_json(lit: &LiteralValue) -> Option<serde_json::Value> {
    match lit {
        // 第一层洋葱：解包 LiteralValue::Scalar
        LiteralValue::Scalar(scalar_struct) => {
            // 第二层洋葱：直接拿 scalar_struct.value (它是 AnyValue)
            // 注意：根据 Polars 版本，可能是 .value 字段或者 .value() 方法
            // 假设你的 struct 定义是 pub value，直接访问即可
            let any_value = &scalar_struct.value(); 

            // 第三层洋葱：匹配 AnyValue 的具体变体
            match any_value {
                // 1. 基础数值 (直接转 JSON Number)
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

                // 2. 字符串 (String / Utf8)
                // Polars 的 String 有时是 Utf8，有时是 String，视版本而定
                AnyValue::String(s)  => Some(serde_json::json!(s)),
                AnyValue::StringOwned(s) => Some(serde_json::json!(s)), 
                
                // 3. 日期 (重点！)
                // Delta Stats 里的 Date 是 "YYYY-MM-DD" 字符串
                // Polars AnyValue::Date(i32) 是 days since epoch
                AnyValue::Date(days) => {
                    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                    let date = TimeDelta::try_days(*days as i64)
                        .and_then(|delta| epoch.checked_add_signed(delta));
                    date.map(|d| serde_json::json!(d.format("%Y-%m-%d").to_string()))
                },

                // 4. 时间戳 (Timestamp)
                // Delta Stats 通常是 ISO String。
                // Polars AnyValue::Datetime(val, unit, tz)
                // 这里的 val 是 long (时间戳数值)，unit 是精度 (ms/us/ns)
                AnyValue::Datetime(_val, _time_unit, _tz) => {
                    // 为了简化，这里先不支持带时区的复杂转换，防止剪枝错误
                    // 如果你的 Delta 表里存的是 UTC 时间戳，可以尝试转换
                    // 否则最安全的做法是：遇到 Timestamp 就不做 Stats Pruning，只做 Partition Pruning
                    None 
                },

                // 5. 其他 (Null, Binary, List, Struct...) -> 不支持剪枝
                _ => None,
            }
        },
        
        // LiteralValue::Range, LiteralValue::Series 等不支持静态分析
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
        // [Auto-Derive]: 从 Expr 提取 Key Bounds
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

            // 2.3 Get Actions (Fix: Use table method + Collect Stream)
            // 这是一个 Stream，我们需要用 futures::StreamExt 把它收集起来
            use futures::StreamExt; 
            
            let mut stream = scan_t.get_active_add_actions_by_partitions(&[]);
            let mut actions = Vec::new();
            
            while let Some(item) = stream.next().await {
                // 解包 DeltaResult<LogicalFileView>
                let view = item.map_err(|e| PolarsError::ComputeError(format!("Stream error: {}", e).into()))?;
                // LogicalFileView 通常包含 add action 的信息
                // 如果你的版本 view 不能直接用，可能需要 view.into() 或者 view.action()
                // 这里假设 view 可以直接用，或者它本身就是我们需要的结构
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
            // 尝试仅基于分区值构建一个 1 行的 DataFrame 并运行 Predicate
            let mut can_skip_scan = false;
            let mut fast_drop = false;

            if !partition_cols.is_empty() {
                // Build Mini-DataFrame from partition values
                let mut columns = Vec::with_capacity(partition_cols.len());
                for target_col in &partition_cols {
                    // 1. 在 StructData 里查找对应的列索引
                    let val_str_opt = p_fields.iter()
                        .position(|f| f.name() == target_col) // 找到 index
                        .map(|idx| &p_values[idx])            // 拿到 Scalar
                        .filter(|scalar| !scalar.is_null())   // 过滤 Null
                        .map(|scalar| {
                             // 使用 serialize() 拿到纯净字符串 (如 "2024-01-01")
                             scalar.serialize()
                        });

                    let dtype = polars_schema.get_field(target_col).map(|f| f.dtype.clone()).unwrap_or(DataType::String);
                    
                    // 2. 构建 Series
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
             // Phase 1.5: Stats Pruning (Auto-Derived) 🚀
             // =========================================================
            let stats_struct = view.stats()
                    .map(|s| serde_json::from_str(&s).unwrap_or(serde_json::Value::Null));
             if !key_bounds.is_empty() {

                 let mut file_overlaps = true;
                 for (key, (src_min, src_max)) in &key_bounds {
                     // 这里的 src_min 已经是我们辛苦转换好的 JSON Value 了
                     // 可以直接传给那个 check_file_overlap_optimized
                     if !check_file_overlap_optimized(&stats_struct, key, src_min, src_max) {
                         file_overlaps = false;
                         break;
                     }
                 }
                 
                 if !file_overlaps {
                     continue; // Skip file (Zero-IO)
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
                // 1. 查找值 (逻辑同上)
                let val_str_opt = p_fields.iter()
                    .position(|f| f.name() == target_col)
                    .map(|idx| &p_values[idx])
                    .filter(|scalar| !scalar.is_null())
                    .map(|scalar| {
                         scalar.serialize()
                    });

                // 2. 构建 Literal Expr
                let lit_expr = match val_str_opt {
                    Some(v) => lit(v),
                    None => lit(NULL)
                };

                // 3. Cast 并注入
                let final_expr = if let Some(dtype) = polars_schema.get_field(target_col).map(|f| f.dtype.clone()) {
                    lit_expr.cast(dtype)
                } else {
                    lit_expr
                };
                
                lf = lf.with_column(final_expr.alias(target_col));
            }

            // [Optimization]: Use filter() first to allow Parquet Stats Pushdown
            // If Parquet Min/Max stats prove "No Match", filter returns empty LF instantly.
            // We select len() to count matches.
            let has_match_df = lf.clone()
                .filter(root_expr.clone())
                .limit(1) // 只要找到 1 行就停止扫描
                .collect_with_engine(Engine::Streaming)?;
            
            // let matched_rows = stats_df.column("matched_count")?.u32()?.get(0).unwrap_or(0);

            if has_match_df.height() == 0 {
                // Case 1: Keep (Predicate 没命中任何行)
                continue;
            }
            
            let is_full_drop = if let Some(_total) = total_rows_opt {
                // 路径 A: 如果我们知道总行数，且刚才 limit(1) 没法告诉我们是否全匹配
                // 我们需要反向检查：有没有“保留”的行？
                let keep_expr = root_expr.clone().not();
                let has_keep_df = lf.clone()
                    .filter(keep_expr)
                    .limit(1) // 🚀 只要找到 1 行不需要删的，就是 Rewrite
                    .collect_with_engine(Engine::Streaming)?;
                
                has_keep_df.height() == 0
            } else {
                // 路径 B: 极端情况，Delta Log 里没写 numRecords
                // [Optimization]: 一次扫描同时计算 Total 和 Matched
                // pred 是 boolean 列，cast(UInt32) 后，sum() 就是匹配行数
                let counts_df = lf.clone()
                    .select([
                        len().alias("total"), // 总行数
                        root_expr.clone().cast(DataType::UInt32).sum().alias("matched") // 匹配行数
                    ])
                    .collect_with_engine(Engine::Streaming)?;

                let total = counts_df.column("total")?.u32()?.get(0).unwrap_or(0) as i64;
                let matched = counts_df.column("matched")?.u32()?.get(0).unwrap_or(0) as i64;

                // 如果匹配行数 == 总行数，说明全是垃圾，直接 Drop
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
                // [Fix]: 使用 DeltaOperation::Delete 以获得正确的事务语义
                let operation = DeltaOperation::Delete {
                    predicate: None, // 目前我们只有 Expr 对象，无法轻易还原为 SQL 字符串，故填 None
                };
                
                let _ver = CommitBuilder::default()
                    .with_actions(actions)
                    .build(
                        table.snapshot().ok().map(|s| s as &dyn transaction::TableReference),
                        table.log_store().clone(),
                        operation
                    ).await.map_err(|e| PolarsError::ComputeError(format!("Commit failed: {}", e).into()))?;
                
                // Optional: Print version for debug
                // println!("[Debug] Delete Commit Success. Version: {}", _ver.version);
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
        // serde_json::Number 默认不支持 i128，我们需要降级处理
        // 绝大多数 ID 或 Timestamp 都能塞进 i64
        PruningBound::Integer(i) => {
            if let Ok(val) = i64::try_from(*i) {
                serde_json::json!(val)
            } else {
                // 如果超出了 i64 (极大整数)，转成 f64 比较
                // 虽然精度有损，但用于 Pruning (剪枝) 是安全的 (区间会变宽，不会误删)
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
/// 分析 Source LazyFrame，提取出所有涉及的分区值组合。
/// 返回类型: Option<HashSet<Vec<String>>>
/// - None: 无法剪枝 (无分区列，或执行出错)
/// - Some(Set): 包含所有出现的 (col1_val, col2_val, ...) 组合
fn get_source_partition_values(
    source_lf: &LazyFrame,
    partition_cols: &[String]
) -> Option<HashSet<Vec<String>>> {
    if partition_cols.is_empty() {
        return None;
    }
    let cols: Vec<Expr> = partition_cols.iter().map(|c| col(c)).collect();
    // 使用 UniqueKeepStrategy::Any 只要唯一的组合
    let distinct_df = source_lf.clone().select(cols).unique(None, UniqueKeepStrategy::Any).collect().ok()?;

    let height = distinct_df.height();
    if height == 0 { return Some(HashSet::new()); }

    let mut affected_partitions = HashSet::with_capacity(height);
    for row_idx in 0..height {
        let mut row_values = Vec::with_capacity(partition_cols.len());
        for col_name in partition_cols {
            let s = distinct_df.column(col_name).ok()?;
            let val = s.get(row_idx).ok()?;
            // [关键]: 使用稳健转换
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
        // 对于数值和时间，直接转
        v => v.to_string(),
    }
}

fn any_value_to_bound(val: &AnyValue, _dtype: &DataType) -> Option<PruningBound> {
    match val {
        // --- 整数类型：全部提升至 i128 确保 u64 不丢失精度 ---
        AnyValue::Int8(v) => Some(PruningBound::Integer(*v as i128)),
        AnyValue::Int16(v) => Some(PruningBound::Integer(*v as i128)),
        AnyValue::Int32(v) => Some(PruningBound::Integer(*v as i128)),
        AnyValue::Int64(v) => Some(PruningBound::Integer(*v as i128)),
        AnyValue::UInt8(v) => Some(PruningBound::Integer(*v as i128)),
        AnyValue::UInt16(v) => Some(PruningBound::Integer(*v as i128)),
        AnyValue::UInt32(v) => Some(PruningBound::Integer(*v as i128)),
        AnyValue::UInt64(v) => Some(PruningBound::Integer(*v as i128)),
        
        // --- 浮点类型 ---
        AnyValue::Float32(v) => Some(PruningBound::Float(*v as f64)),
        AnyValue::Float64(v) => Some(PruningBound::Float(*v)),
        
        // --- 字符串与时间 (保持 ISO 8601 逻辑) ---
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

// 获取 Source 的 Min/Max
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

#[derive(Debug)]
enum PruningCandidates {
    Integers(Vec<i64>), // 针对 Int, BigInt, TinyInt
    Strings(Vec<String>), // 针对 String, Date (ISO格式), Timestamp
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
        // 1. Unpack Basic Args
        let path_str = ptr_to_str(target_path_ptr).map_err(|e| PolarsError::InvalidOperation(e.to_string().into()))?;
        let merge_keys = unsafe { ptr_to_vec_string(merge_keys_ptr, merge_keys_len) };
        if merge_keys.is_empty() {
             return Err(PolarsError::InvalidOperation("Merge keys cannot be empty".into()));
        }
        let source_lf_ctx = unsafe { Box::from_raw(source_lf_ptr) };
        let mut source_lf = source_lf_ctx.inner; 

        let table_url = parse_table_url(path_str)?;
        let delta_storage_options = build_delta_storage_options_map(cloud_keys, cloud_values, cloud_len);

        let cond_update = if !matched_update_cond.is_null() { 
            unsafe { *Box::from_raw(matched_update_cond) }.inner 
        } else { 
            lit(true) 
        };

        let cond_match_delete = if !matched_delete_cond.is_null() { 
            unsafe { *Box::from_raw(matched_delete_cond) }.inner 
        } else { 
            lit(false) 
        };

        let cond_insert = if !not_matched_insert_cond.is_null() { 
            unsafe { *Box::from_raw(not_matched_insert_cond) }.inner 
        } else { 
            lit(true) 
        };

        let cond_source_delete = if !not_matched_by_source_delete_cond.is_null() { 
            unsafe { *Box::from_raw(not_matched_by_source_delete_cond) }.inner 
        } else { 
            lit(false) 
        };

        let rt = get_runtime();

        // 2. Load Target Delta Table & Prune Files (Multi-Key + Partition Aware)
        let (table, partition_cols, pruned_remove_actions, polars_schema) = rt.block_on(async {
            // A. Load Table
            let t = DeltaTable::try_from_url_with_storage_options(table_url.clone(), delta_storage_options.clone())
                .await.map_err(|e| PolarsError::ComputeError(format!("Delta load error: {}", e).into()))?;
            
            // B. Get Polars Schema
            let schema = get_polars_schema_from_delta(&t)?;

            // C. Get Partition Cols metadata
            let snapshot = t.snapshot().map_err(|e| PolarsError::ComputeError(format!("Snapshot error: {}", e).into()))?;
            let part_cols = snapshot.metadata().partition_columns().clone();

            // =========================================================
            // OPTIMIZATION 1: Analyze Source Partitions
            // =========================================================
            // Get Source Partition Value to prune
            let source_partitions = get_source_partition_values(&source_lf, &part_cols);

            // =========================================================
            // OPTIMIZATION 2: Analyze Source Key Bounds (Multi-Key)
            // =========================================================
            // For Z-Order / Data Skipping
            let mut key_bounds = HashMap::new();
            for key in &merge_keys {
                let dtype = schema.get_field(key).map(|f| f.dtype.clone()).unwrap_or(DataType::Null);
                if let Some(bounds) = get_source_key_bounds(&source_lf, key, &dtype) {
                    key_bounds.insert(key.clone(), bounds);
                }
            }

            // =========================================================
            // D. Iterate & Prune Active Files
            // =========================================================
            let mut remove_actions = Vec::new();
            // ==========================================
            // 0. Pre-computation: Discrete Pruning Candidates
            // ==========================================
            let mut pruning_candidates: Option<PruningCandidates> = None;

            // 仅针对单列主键进行优化 (复合主键逻辑太复杂，暂跳过)
            if merge_keys.len() == 1 {
                let key_col_name = &merge_keys[0];
                // 设定阈值：如果 Source Key 太多，构建 Vec 的开销 > 收益，就降级
                let max_candidates = 100_000; 

                // 1. 提取去重后的 Keys
                // 使用 LazyFrame 的优化查询
                if let Ok(df) = source_lf.clone()
                    .select(&[col(key_col_name)])
                    .unique(None, UniqueKeepStrategy::Any)
                    .limit(max_candidates as u32 + 1) // 多取一个用于判断是否超限
                    .collect() 
                {
                    if df.height() <= max_candidates {
                        let s = df.column(key_col_name).unwrap();
                        let dtype = s.dtype();

                        if dtype.is_integer() {
                            // A. 整数路径 (转 i64 -> 排序)
                            if let Ok(s_cast) = s.cast(&DataType::Int64) {
                                let mut keys: Vec<i64> = s_cast.i64().unwrap()
                                    .into_iter().flatten().collect();
                                keys.sort_unstable(); // 必须排序才能二分查找
                                pruning_candidates = Some(PruningCandidates::Integers(keys));
                            }
                        } else {
                            // B. 字符串/日期路径 (序列化为 String -> 排序)
                            // 对于 Date/Timestamp，Delta Stats 存的是 String，所以这里也要转 String
                            // Polars 的 cast(String) 对于 Date 会生成 ISO 格式，正好匹配
                            if let Ok(s_cast) = s.cast(&DataType::String) {
                                let mut keys: Vec<String> = s_cast.str().unwrap()
                                    .into_iter().flatten().map(|v| v.to_string()).collect();
                                keys.sort_unstable();
                                pruning_candidates = Some(PruningCandidates::Strings(keys));
                            }
                        }
                    }
                }
            }
            // Get Active File Stream
            let t_scan = t.clone(); 
            let mut stream = t_scan.get_active_add_actions_by_partitions(&[]);

            while let Some(action_res) = futures::StreamExt::next(&mut stream).await {
                let view = action_res.map_err(|e| PolarsError::ComputeError(format!("Delta stream error: {}", e).into()))?;
                    
                // ==========================================
                // 1. Partition Pruning (Optimized)
                // ==========================================
                if let Some(affected_set) = &source_partitions {
                    
                    // A. Parse Partition Value from Delta Log (StructData) 
                    let maybe_key = view.partition_values().map(|struct_data| {
                        let fields = struct_data.fields();
                        let values = struct_data.values();
                        
                        let mut key = Vec::with_capacity(part_cols.len());
                        
                        for target_col_name in &part_cols {
                            let val_str = fields.iter()
                                .position(|f| f.name() == target_col_name)
                                .map(|idx| &values[idx])
                                .map(|scalar| {
                                    if scalar.is_null() {
                                        "__HIVE_DEFAULT_PARTITION__".to_string()
                                    } else {
                                        scalar.serialize()
                                    }
                                })
                                .unwrap_or_else(|| {
                                    "__HIVE_DEFAULT_PARTITION__".to_string()
                                });
                            
                            key.push(val_str);
                        }
                        key
                    });

                    // Build file_key 
                    let file_key = match maybe_key {
                        Some(k) => k,
                        None => {
                            let path_cow = view.path();
                            let path_str = path_cow.as_ref();
                            let file_parts_map = parse_hive_partitions(path_str, &part_cols);
                            
                            let mut k = Vec::with_capacity(part_cols.len());
                            for col_name in &part_cols {
                                let v = file_parts_map.get(col_name).cloned().flatten()
                                    .unwrap_or_else(|| "__HIVE_DEFAULT_PARTITION__".to_string());
                                k.push(v);
                            }
                            k
                        }
                    };
                    
                    // Check Pruning
                    if !affected_set.contains(&file_key) {
                        continue; // Skip this file
                    }
                }

                // ==========================================
                // 2. Stats Pruning (Global + Discrete)
                // ==========================================
                let mut file_overlaps = true;
                
                if !key_bounds.is_empty() {
                    let stats_json = view.stats();
                    // 提前解析 Stats JSON，供两步使用
                    let stats_struct: Option<serde_json::Value> = stats_json
                        .map(|s| serde_json::from_str(&s).unwrap_or(serde_json::Value::Null));

                    // ---------------------------------------------------
                    // Step A: Global Range Check (粗筛)
                    // ---------------------------------------------------
                    for (key, (src_min, src_max)) in &key_bounds {
                         // 使用之前定义的 check_file_overlap_optimized
                        let src_min_json = bound_to_json(src_min);
                        let src_max_json = bound_to_json(src_max);

                         if !check_file_overlap_optimized(&stats_struct, key, &src_min_json, &src_max_json) {
                            file_overlaps = false;
                            break;
                         }
                    }

                    // ---------------------------------------------------
                    // Step B: Discrete Candidates Check (细筛 - DFP)
                    // 只有当粗筛通过，且有候选集时才执行
                    // ---------------------------------------------------
                    if file_overlaps {
                        if let Some(candidates) = &pruning_candidates {
                             let key_col = &merge_keys[0];
                             
                             // 尝试提取文件级的 Min/Max
                             let f_min_opt = extract_stats_value(&stats_struct, "minValues", key_col);
                             let f_max_opt = extract_stats_value(&stats_struct, "maxValues", key_col);

                             if let (Some(f_min_val), Some(f_max_val)) = (f_min_opt, f_max_opt) {
                                 let hit = match candidates {
                                     // 整数二分查找
                                     PruningCandidates::Integers(sorted_keys) => {
                                         // 尝试解析 Stats 为 i64
                                         let f_min_i = as_i64_safe(f_min_val);
                                         let f_max_i = as_i64_safe(f_max_val);
                                         
                                         if let (Some(min_i), Some(max_i)) = (f_min_i, f_max_i) {
                                             // 1. 找到第一个 >= FileMin 的位置
                                             let idx = sorted_keys.partition_point(|&x| x < min_i);
                                             // 2. 检查该位置的值是否 <= FileMax
                                             // 如果 idx 越界，或者 key > max_i，说明文件范围内没有 key
                                             if idx < sorted_keys.len() && sorted_keys[idx] <= max_i {
                                                 true // 命中！
                                             } else {
                                                 false // 没命中，可以剪枝
                                             }
                                         } else {
                                             true // 解析失败，保守保留
                                         }
                                     },
                                     // 字符串二分查找
                                     PruningCandidates::Strings(sorted_keys) => {
                                         let f_min_s = f_min_val.as_str();
                                         let f_max_s = f_max_val.as_str();
                                         
                                         if let (Some(min_s), Some(max_s)) = (f_min_s, f_max_s) {
                                             let idx = sorted_keys.partition_point(|x| x.as_str() < min_s);
                                             if idx < sorted_keys.len() && sorted_keys[idx].as_str() <= max_s {
                                                 true
                                             } else {
                                                 false
                                             }
                                         } else {
                                             true
                                         }
                                     }
                                 };

                                 if !hit {
                                     file_overlaps = false; // 🔪 剪枝成功！
                                 }
                             }
                        }
                    }
                }

                // 3. Keep File
                if file_overlaps {
                    remove_actions.push(view.remove_action(true));
                }
            }
            
            Ok::<_, PolarsError>((t, part_cols, remove_actions, schema))
        })?;

        let table_root = table.table_url().to_string(); // e.g. "s3://bucket/table"
        let root_trimmed = table_root.trim_end_matches('/');
        
        // Pure Insert or Table Empty
        let target_lf = if pruned_remove_actions.is_empty() {
            // Use Polars Schema from Delta Snapshot to build DataFrame
            DataFrame::empty_with_schema(&polars_schema)
                .lazy()
        } else {
            let full_paths: Vec<PlRefPath> = pruned_remove_actions.iter()
                .map(|r| {
                    let full = format!("{}/{}", root_trimmed, r.path);
                    PlRefPath::new(full)
                })
                .collect();
            
            let paths_buffer = Buffer::from(full_paths);
            
            // Build ScanArgs
            let mut scan_args = ScanArgsParquet::default();
            scan_args.hive_options = HiveOptions {
                enabled: Some(true),
                hive_start_idx: 0,
                schema: None,
                try_parse_dates: true,
            };
            scan_args.cloud_options = unsafe {
                build_cloud_options(
                    cloud_provider, cloud_retries, cloud_retry_timeout_ms,
                    cloud_retry_init_backoff_ms, cloud_retry_max_backoff_ms, cloud_cache_ttl,
                    cloud_keys, cloud_values, cloud_len
                )
            };
            
            LazyFrame::scan_parquet_files(paths_buffer, scan_args)?
        };

        // =================================================================================
        // 4. The Grand Join (Full Merge Logic)
        // =================================================================================

        // A. Rename Source (Add Suffix)
        let source_schema = source_lf.collect_schema()?;
        let mut source_cols_renamed = Vec::new();
        let mut old_names = Vec::new();
        let mut new_names = Vec::new();

        // [New]: Schema Validation Logic with can_evolve
        for src_col_name in source_schema.iter_names() {
            if polars_schema.get_field(src_col_name).is_none() {
                if !can_evolve {
                    return Err(PolarsError::SchemaMismatch(
                        format!("Schema mismatch: Source column '{}' is not present in target. Pass 'can_evolve=true' to allow.", src_col_name).into()
                    ));
                }
            }
        }
        let mut source_keys_renamed_exprs = Vec::with_capacity(merge_keys.len());
        let mut target_keys_exprs = Vec::with_capacity(merge_keys.len());

        for key in &merge_keys {
            let renamed = format!("{}_src_tmp", key);
            source_keys_renamed_exprs.push(col(&renamed));
            target_keys_exprs.push(col(key));
        }
        
        for name in source_schema.iter_names() {
            let new_name = format!("{}_src_tmp", name);
            source_cols_renamed.push(new_name.clone());
            old_names.push(name.as_str());
            new_names.push(new_name);
        }
        // -------------------------------------------------------------------------
        // 0. Schema Validation (Existence & Type Compatibility)
        // -------------------------------------------------------------------------
        
        // 获取 Source 的 Schema (LazyFrame 获取 Schema 是瞬时的)
        let src_schema = source_lf.collect_schema()
            .map_err(|e| PolarsError::ComputeError(format!("Failed to get source schema: {}", e).into()))?;
        
        // 这里的 polars_schema 是 Target Table (Delta) 的 Schema
        for key in &merge_keys {
            // A. 检查 Source 是否存在 Key
            let src_field = src_schema.get(key)
                .ok_or_else(|| PolarsError::ComputeError(format!(
                    "Merge Key '{}' not found in SOURCE table schema.", key
                ).into()))?;

            // B. 检查 Target 是否存在 Key
            let tgt_field = polars_schema.get_field(key)
                .ok_or_else(|| PolarsError::ComputeError(format!(
                    "Merge Key '{}' not found in TARGET table schema.", key
                ).into()))?;

            // C. 严格类型检查 (Strict Type Match)
            // 防止 Int32 vs Int64，或者 String vs LargeString 导致的 Join/Pruning 失败
            if src_field != &tgt_field.dtype {
                 return Err(PolarsError::SchemaMismatch(format!(
                    "Merge Key Type Mismatch for column '{}'! \n\
                     Source: {:?} \n\
                     Target: {:?} \n\
                     Merge Keys must have identical types to ensure pruning and join correctness. \
                     Please cast your source column.", 
                    key, src_field, tgt_field.dtype
                ).into()));
            }
        }
        // -------------------------------------------------------------------------
        // 1. Strict Data Quality Check (Duplicates & Nulls)
        // -------------------------------------------------------------------------
        // 我们不希望静默去重，而是希望在上游数据有问题时直接炸掉，
        // 迫使开发者去修复数据源，而不是掩盖问题。
        
        // [Fix]: 1. 将 String 转换为 Expr (col("key"))
        // over() 需要的是 Vec<Expr>，而不是 Selector
        let partition_exprs: Vec<Expr> = merge_keys.iter()
            .map(|k| col(k)) 
            .collect();

        let has_null_expr = polars::prelude::any_horizontal(
             merge_keys.iter().map(|k| col(k).is_null()).collect::<Vec<_>>()
        )?.alias("has_null_key"); // <--- 注意这里的 ?

        let quality_check_lf = source_lf.clone()
            .select([
                // 检查 A: 重复 (Window Function)
                len().over(partition_exprs.clone()).alias("group_count"),
                // 检查 B: Nulls (只要任意一个 Key 列是 Null)
                // 我们可以生成一个 boolean 列 "has_null_key"
            has_null_expr
            ])
            .filter(
                col("group_count").gt(lit(1)) // 有重复
                .or(col("has_null_key"))      // 或者 有 Null
            )
            .limit(1); // 同样，只要发现一个坏蛋就立刻报警

        let check_df = quality_check_lf.collect_with_engine(Engine::Streaming)?;

        if check_df.height() > 0 {
            // 🚨 3. 发现重复！抓取“犯罪现场”证据 (Error Reporting Path)
            // 既然已经出错了，这里多花一点点时间做个 GroupBy 是值得的
            // 我们提取出 Top 5 组重复的主键及其重复次数，打印给用户看
            let null_row = check_df.column("has_null_key")?.bool()?.get(0).unwrap_or(false);
            if null_row {
                 let msg = format!(
                    "CRITICAL ERROR: Null values detected in Merge Keys! \n\
                     Merge Keys cannot contain NULLs as they are used for identification. \n\
                     Merge Keys: {:?}", 
                    merge_keys
                );
                return Err(PolarsError::ComputeError(msg.into()));
            } else {
                let example_dupes = source_lf.clone()
                    .group_by(partition_exprs.clone()) // 按主键分组
                    .agg([len().alias("duplicate_count")]) // 统计每组数量
                    .filter(col("duplicate_count").gt(lit(1))) // 只看重复的
                    .sort(["duplicate_count"], SortMultipleOptions { 
                        descending: vec![true], 
                        ..Default::default() 
                    }) // 甚至可以贴心地按重复次数倒序，把重复最严重的放前面
                    .limit(5) // 只取前 5 个例子，防止报错信息炸屏
                    .collect_with_engine(Engine::Streaming)?;

                // 4. 构造包含表格的精美报错信息
                let msg = format!(
                    "CRITICAL ERROR: Duplicate keys detected in SOURCE table!\n\
                    Merge expects unique source keys per checking round.\n\
                    Please dedup your source data before merging.\n\
                    \n\
                    [Merge Keys]: {:?}\n\
                    \n\
                    --- Duplicate Key Examples (Top 5) ---\n\
                    {}", 
                    merge_keys, 
                    example_dupes // Polars DataFrame 实现了 Display，直接打印就是一张漂亮的表
                );
                
                // 使用 PolarsError::Duplicate (如果你的 Polars 版本有这个变体)，或者 ComputeError
                return Err(PolarsError::Duplicate(msg.into()));
            }
        }

        let source_renamed = source_lf.clone()
            // .unique(Some(selector), UniqueKeepStrategy::Last) 
            .rename(old_names, new_names, true);

        let join_args = JoinArgs {
            how: JoinType::Full,
            validation: JoinValidation::ManyToMany, 
            suffix: None, 
            slice: None,
            nulls_equal: false, 
            coalesce: JoinCoalesce::KeepColumns, 
            maintain_order: MaintainOrderJoin::None, 
            build_side: Default::default(), 
        };

        // Execute Full Join
        let joined_lf = target_lf.join(
            source_renamed,
            target_keys_exprs,         // Left On: [Key1, Key2]
            source_keys_renamed_exprs, // Right On: [Key1_tmp, Key2_tmp]
            join_args 
        );
        // =================================================================================
        // C. Define State & Action Constants
        // =================================================================================
        // 0=KEEP (Target Only / Matched but condition failed)
        // 1=INSERT
        // 2=UPDATE
        // 3=DELETE (Matched Delete / NotMatchedBySource Delete)
        // 4=IGNORE (Source Only but Insert condition failed)
        let act_keep = lit(0);
        let act_insert = lit(1);
        let act_update = lit(2);
        let act_delete = lit(3);
        let act_ignore = lit(4);

        // Define Row State (准确定义三种状态)
        // 假设我们 Join 时把 source columns 重命名为了 {col}_src_tmp
        // 通过检查 Source 的主键是否为 Null 来判断 Source 是否存在
        let first_src_key = format!("{}_src_tmp", merge_keys[0]);
        let first_tgt_key = &merge_keys[0];

        let src_exists = col(&first_src_key).is_not_null();
        let tgt_exists = col(first_tgt_key).is_not_null();

        let is_matched = src_exists.clone().and(tgt_exists.clone());
        let is_source_only = src_exists.clone().and(tgt_exists.clone().not()); // 新增行
        let is_target_only = src_exists.not().and(tgt_exists.clone());         // 存量行

        // =================================================================================
        // D. Calculate Action Column (The Logic Core 🧠)
        // =================================================================================
        // 这里集中处理所有优先级逻辑，只生成一个表达式，效率极高
        let action_expr = polars::prelude::when(is_matched.clone().and(cond_match_delete.clone()))
            .then(act_delete.clone()) // 优先级最高：匹配且满足删除条件
            .when(is_matched.clone().and(cond_update.clone()))
            .then(act_update.clone()) // 其次：匹配且满足更新条件
            .when(is_source_only.clone().and(cond_insert.clone()))
            .then(act_insert.clone()) // 新增：满足插入条件
            .when(is_source_only.clone())
            .then(act_ignore.clone()) // 新增：但不满足插入条件 -> 丢弃
            .when(is_target_only.clone().and(cond_source_delete.clone()))
            .then(act_delete.clone()) // 目标独有：满足 NotMatchedBySource Delete
            .otherwise(act_keep) // 其他情况（如更新条件失败、目标独有且不删） -> 保持原样
            .alias("_merge_action");

        // =================================================================================
        // E. Generate Column Expressions (Support Partial Update)
        // =================================================================================
        
        // 1. 收集所有列名 (Union)
        let mut output_columns_order = Vec::new();
        let mut seen_columns = PlHashSet::new();
        
        // Target Cols
        for name in polars_schema.iter_names() {
            output_columns_order.push(name.to_string());
            seen_columns.insert(name.to_string());
        }
        // Source Cols (Schema Evolution)
        for name in source_schema.iter_names() {
            if !seen_columns.contains(name.as_str()) {
                output_columns_order.push(name.to_string());
                seen_columns.insert(name.to_string());
            }
        }

        let mut final_exprs = Vec::new();

        for col_name in output_columns_order {
            let src_col_alias = format!("{}_src_tmp", col_name);
            let has_source_col = source_cols_renamed.contains(&src_col_alias);
            let has_target_col = polars_schema.get_field(&col_name).is_some();

            // 定义基础值
            let src_val = if has_source_col { col(&src_col_alias) } else { lit(NULL) };
            let tgt_val = if has_target_col { col(&col_name) } else { lit(NULL) };

            // 🌟 核心修复逻辑 🌟
            let final_col_expr = if has_source_col {
                // 场景 A: Source 有这个列 (Full Update / New Data)
                // 逻辑: 只要是 Insert 或 Update，都由 Source 说了算
                polars::prelude::when(
                    col("_merge_action").eq(act_insert.clone())
                    .or(col("_merge_action").eq(act_update.clone()))
                )
                .then(src_val)      // 用 Source 覆盖
                .otherwise(tgt_val) // Keep 用 Target
            } else {
                // 场景 B: Source 没有这个列 (Partial Update / Missing Data)
                // 逻辑: 
                // - 如果是 Insert (新行): 没办法，只能是 Null
                // - 如果是 Update (旧行): Source 没给值，所以**必须保留 Target 原值** (Partial Update)
                // - 如果是 Keep: 也是 Target 原值
                // 结论: 只有 Insert 才是 Null，其他情况都是 Target
                polars::prelude::when(col("_merge_action").eq(act_insert.clone()))
                    .then(lit(NULL)) 
                    .otherwise(tgt_val) // <--- 这里救了 ColB！Update 时会走到这里
            };
            
            final_exprs.push(final_col_expr.alias(&col_name));
        }

        // =================================================================================
        // F. Execute: Inject Action -> Filter -> Project
        // =================================================================================
        let processed_lf = joined_lf
            .with_column(action_expr) // 1. 先打标
            .filter(
                col("_merge_action").neq(act_delete)
                .and(col("_merge_action").neq(act_ignore))
            ) // 2. 根据标记过滤 (Delete & Ignore)
            .select(final_exprs); // 3. 投影最终列 (内部会自动应用取值逻辑)

        // =================================================================================
        // Build Final Schema with new cols before Sink
        // =================================================================================
        
        // 1. Get Target Schema 
        let mut final_schema = polars_schema.clone();
        
        // 2. Add Source new schema
        for (name, dtype) in source_schema.iter() {
            if final_schema.get_field(name).is_none() {
                final_schema.with_column(name.to_string().into(), dtype.clone());
            }
        }

        // =================================================================================
        // 5. Sink to Staging (Write New Files)
        // =================================================================================
        
        let write_id = Uuid::new_v4();
        let staging_dir_name = format!(".merge_staging_{}", write_id);
        
        // Staging URI: s3://bucket/table/.merge_staging_xxx
        let staging_uri = format!("{}/{}", root_trimmed, staging_dir_name);
        
        // Partition Strategy
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

        // Build SinkDestination
        let hive_provider = HivePathProvider {
            extension: PlSmallStr::from_str(".parquet"),
        };
        let file_path_provider = Some(file_provider::FileProviderType::Hive(hive_provider));
        
        let destination = SinkDestination::Partitioned {
            base_path: PlRefPath::new(&staging_uri), 
            file_path_provider,
            partition_strategy,
            max_rows_per_file: u32::MAX, 
            approximate_bytes_per_file: usize::MAX as u64,
        };

        // Build Write Options
        let write_opts = build_parquet_write_options(
            1, // compression (Snappy)
            -1, // level
            true, // stats
            0, // row_group_size
            0, // data_page_size
            -1 // compat_level
        ).map_err(|e| PolarsError::ComputeError(format!("Options error: {}", e).into()))?;
        
        // Build Unified Cloud Args
        let unified_args = unsafe {
            build_unified_sink_args(
                false, // mkdir
                false, // maintain_order
                0,     // sync_on_close
                cloud_provider, cloud_retries, cloud_retry_timeout_ms,
                cloud_retry_init_backoff_ms, cloud_retry_max_backoff_ms, cloud_cache_ttl,
                cloud_keys, cloud_values, cloud_len
            )
        };

        // Execute Write
        processed_lf.sink(destination, FileWriteFormat::Parquet(write_opts), unified_args)?
            .collect_with_engine(Engine::Streaming)?;

        // =================================================================================
        // 6. Scan Staging, Rename & Build Actions
        // =================================================================================
        
        let object_store = table.object_store();
        let mut final_actions = Vec::new();

        // A. Remove Old Files
        for remove_action in pruned_remove_actions {
            final_actions.push(Action::Remove(remove_action));
        }

        // B. Add New Files (From Staging) - 🚀 Parallel Optimized
        let add_actions = rt.block_on(async {
            let staging_path = Path::from(staging_dir_name.clone());
            
            // 1. 获取文件列表 (Listing is fast)
            // 这里的 list 还是线性的，没问题
            let file_metas: Vec<_> = object_store.list(Some(&staging_path))
                .filter_map(|res| async { res.ok() }) // 过滤错误
                .filter(|meta| futures::future::ready(meta.location.to_string().ends_with(".parquet")))
                .collect()
                .await;

            // 2. 并发处理 (Read Stats + Rename) 
            // 设定并发度，比如 50 或 100，避免把 S3 客户端打挂
            const CONCURRENCY: usize = 64; 

            let stream = futures::stream::iter(file_metas)
                .map(|meta| {
                    // Clone 需要的变量以移入 async block
                    let object_store = object_store.clone();
                    let staging_dir_name = staging_dir_name.clone();
                    let partition_cols = partition_cols.clone();
                    let write_id = write_id.clone();

                    async move {
                        let src_path_str = meta.location.to_string();
                        let file_size = meta.size as i64;

                        // A. Read Stats (IO)
                        let mut reader = ParquetObjectReader::new(object_store.clone(), meta.location.clone())
                            .with_file_size(meta.size as u64);
                        let footer = reader.get_metadata(None).await
                            .map_err(|e| PolarsError::ComputeError(format!("Read footer error: {}", e).into()))?;
                        let (_, stats_json) = extract_delta_stats(&footer)?;

                        // B. Rename (Heavy IO)
                        let rel_path = src_path_str.trim_start_matches(&format!("{}/", staging_dir_name));
                        // 注意处理路径分隔符，防止 trim 后剩下 /
                        let rel_path_clean = rel_path.trim_start_matches('/');
                        let dest_path_str = rel_path_clean.replace(".parquet", &format!("-{}.parquet", write_id));
                        
                        let src_path = Path::from(src_path_str.clone());
                        let dest_path = Path::from(dest_path_str.clone());

                        object_store.rename(&src_path, &dest_path).await
                            .map_err(|e| PolarsError::ComputeError(format!("Rename failed: {}", e).into()))?;

                        // C. Build Action (CPU)
                        // 记得这里要 URL Decode!
                        let partition_values = parse_hive_partitions(&dest_path_str, &partition_cols);

                        Ok::<Action, PolarsError>(Action::Add(Add {
                            path: dest_path_str.into(),
                            size: file_size,
                            partition_values,
                            modification_time: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64,
                            data_change: true,
                            stats: Some(stats_json),
                            ..Default::default()
                        }))
                    }
                })
                .buffer_unordered(CONCURRENCY); // 🚀 关键：乱序并发执行

            // 3. 收集结果
            let results: Vec<Result<Action, PolarsError>> = stream.collect().await;
            
            // 解包 Result，如果有任何一个失败，整体失败
            let mut final_actions = Vec::with_capacity(results.len());
            for res in results {
                final_actions.push(res?);
            }

            Ok::<Vec<Action>, PolarsError>(final_actions)
        })?;

        final_actions.extend(add_actions);
        // =================================================================================
        // 7: Commit Transaction
        // =================================================================================

        if !final_actions.is_empty() {
             rt.block_on(async {
                // -------------------------------------------------------------------------
                // Construct Logic Metadata (Audit Logs)
                // -------------------------------------------------------------------------
                
                let mut matched_preds = Vec::new();
                let mut not_matched_preds = Vec::new();           // INSERT
                let mut not_matched_by_source_preds = Vec::new(); // DELETE (Target Only)

                // 1. Matched Logic
                if !matched_update_cond.is_null() {
                    matched_preds.push(MergePredicate { 
                        predicate: Some("custom_update_condition".into()), 
                        action_type: "UPDATE".into() 
                    });
                } else {
                    // => Upsert = UPDATE
                    matched_preds.push(MergePredicate { 
                        predicate: None, 
                        action_type: "UPDATE".into() 
                    });
                }
                
                if !matched_delete_cond.is_null() {
                    matched_preds.push(MergePredicate {
                        predicate: Some("matched_delete_condition".to_string()),
                        action_type: "DELETE".to_string(),
                    });
                }

                // 2. Not Matched (Source Only) -> INSERT
                if !not_matched_insert_cond.is_null() {
                    not_matched_preds.push(MergePredicate {
                        predicate: Some("not_matched_insert_condition".to_string()),
                        action_type: "INSERT".to_string(),
                    });
                } else {
                     not_matched_preds.push(MergePredicate {
                        predicate: None,
                        action_type: "INSERT".to_string(),
                    });
                }

                // 3. Not Matched By Source (Target Only) -> DELETE
                if !not_matched_by_source_delete_cond.is_null() {
                    not_matched_by_source_preds.push(MergePredicate {
                        predicate: Some("not_matched_by_source_delete_condition".to_string()),
                        action_type: "DELETE".to_string(),
                    });
                }

                // --- Schema Evolution Logic (Corrected Variable Scope) ---
                
                let current_snapshot = table.snapshot().map_err(|e| PolarsError::ComputeError(format!("Snapshot: {}", e).into()))?;
                let current_delta_schema = current_snapshot.schema();

                let mut new_pl_schema = polars_schema.clone();
                let mut has_schema_change = false;
                for (name, dtype) in source_schema.iter() {
                    if new_pl_schema.get_field(name).is_none() {
                        new_pl_schema.with_column(name.to_string().into(), dtype.clone());
                        has_schema_change = true;
                    }
                }

                if has_schema_change {
                    let new_delta_schema = convert_to_delta_schema(&new_pl_schema)?;

                    if current_delta_schema.as_ref() != &new_delta_schema {
                        if !can_evolve {
                             return Err(PolarsError::ComputeError("Schema mismatch detected. Pass 'can_evolve=true'.".into()));
                        }

                        let current_metadata = current_snapshot.metadata();
                        let mut meta_json = serde_json::to_value(current_metadata).map_err(|e| PolarsError::ComputeError(format!("{}", e).into()))?;
                        let new_schema_string = serde_json::to_string(&new_delta_schema).map_err(|e| PolarsError::ComputeError(format!("{}", e).into()))?;

                        if let Some(obj) = meta_json.as_object_mut() {
                            obj.insert("schemaString".to_string(), serde_json::Value::String(new_schema_string));
                        }
                        let new_metadata_action: deltalake::kernel::Metadata = serde_json::from_value(meta_json).map_err(|e| PolarsError::ComputeError(format!("{}", e).into()))?;
                        final_actions.insert(0, Action::Metadata(new_metadata_action));
                    }
                }

                // -------------------------------------------------------------------------
                // Build & Commit
                // -------------------------------------------------------------------------

                let join_predicate = merge_keys.iter()
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

                let _ver = CommitBuilder::default()
                    .with_actions(final_actions) // Remove(Pruned) + Add(Staging) + Metadata(Evolution)
                    .build(
                        table.snapshot().ok().map(|s| s as &dyn transaction::TableReference),
                        table.log_store().clone(),
                        operation
                    )
                    .await
                    .map_err(|e| PolarsError::ComputeError(format!("Commit failed: {}", e).into()))?;
                
                // -------------------------------------------------------------------------
                // D. Cleanup
                // -------------------------------------------------------------------------
                let _ = object_store.delete(&Path::from(staging_dir_name)).await;
                
                Ok::<(), PolarsError>(())
            })?;
        }
       
        Ok(())
    })
}