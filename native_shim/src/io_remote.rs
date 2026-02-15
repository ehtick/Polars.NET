// ==========================================
// Delta Lake Support
// ==========================================

use chrono::{DateTime, NaiveDate, Utc};
use deltalake::kernel::engine::arrow_conversion::{TryIntoArrow, TryIntoKernel};
use deltalake::kernel::transaction::{CommitBuilder};
use deltalake::parquet::arrow::arrow_reader::ArrowReaderOptions;
use deltalake::parquet::data_type::ByteArray;
use deltalake::parquet::file::metadata::ParquetMetaData;
use deltalake::{DeltaTable, DeltaTableBuilder, Path};
use deltalake::protocol::{DeltaOperation, MergePredicate, SaveMode};
use deltalake::parquet::arrow::async_reader::{AsyncFileReader, ParquetObjectReader};
use deltalake::parquet::file::statistics::Statistics as ParquetStatistics;
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

fn build_add_action(
    path: String, 
    size: i64, 
    stats_json: Option<String>
) -> Add {
    // 1. 获取当前时间戳 (毫秒)
    let modification_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    // 2. 构建结构体
    Add {
        // --- 必填字段 ---
        
        // 数据文件的路径 (URL encoded URI)
        // 注意：如果是 S3，通常是相对路径；如果是本地文件，也是相对表根目录的路径
        path, 
        
        // 文件大小
        size, 
        
        // 分区值映射
        // 如果表没有分区，这里是一个空的 HashMap。
        // 如果有分区 (e.g. date=2023-01-01)，这里需要填 {"date": Some("2023-01-01".into())}
        partition_values: HashMap::new(), 

        // 修改时间
        modification_time, 

        // 是否修改了数据 (对于 Append 操作通常是 true)
        data_change: true, 

        // --- 选填字段 (核心) ---

        // 统计信息 (Min/Max/NullCount)
        // 格式是 JSON String。如果为 None，Delta 读表时无法做 Data Skipping，但数据仍可读。
        stats: stats_json, 

        // --- 选填字段 (高级功能，默认 None) ---

        // 用户自定义标签
        tags: None, 

        // 删除向量 (用于 Merge/Delete 优化，Append 不需要)
        deletion_vector: None, 

        // 行 ID (用于行级追踪，默认 None)
        base_row_id: None, 

        // 默认提交版本 (用于流式去重，默认 None)
        default_row_commit_version: None, 

        // 聚类提供者 (用于 Liquid Clustering，默认 None)
        clustering_provider: None,
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


/// 简单的 Hive 路径解析器
/// path: "date=2023-01-01/region=US/part-xxx.parquet"
/// cols: ["date", "region"]
fn parse_hive_partitions(path: &str, cols: &[String]) -> HashMap<String, Option<String>> {
    let mut map = HashMap::new();
    if cols.is_empty() { return map; }
    
    let parts: Vec<&str> = path.split('/').collect();
    for part in parts {
        if let Some((k, v)) = part.split_once('=') {
            if cols.contains(&k.to_string()) {
                // 这里建议做 url decode，防止特殊字符问题
                let decoded_v = urlencoding::decode(v).map(|c| c.into_owned()).unwrap_or(v.to_string());
                map.insert(k.to_string(), Some(decoded_v));
            }
        }
    }
    // Delta 规范要求所有分区列必须存在
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
    table_path_ptr: *const c_char,
    mode: u8,
    // Parquet Options (复用 Polars 的配置)
    compression: u8,
    compression_level: i32,
    statistics: bool,
    row_group_size: usize,
    data_page_size: usize,
    compat_level: i32,
    // Sink Options
    maintain_order: bool,
    sync_on_close: u8,
    mkdir: bool,
    // Cloud Options
    cloud_provider: u8, // Polars sink_parquet 可能会用到，这里暂时透传
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
        let path_str = ptr_to_str(table_path_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        let table_url = parse_table_url(path_str)?;

        let delta_storage_options = build_delta_storage_options_map(cloud_keys, cloud_values, cloud_len);

        let save_mode = map_savemode(mode);

        let rt = get_runtime();

        let table_exists = rt.block_on(async {
            let table = DeltaTable::try_from_url_with_storage_options(
                table_url.clone(), 
                delta_storage_options.clone()
            ).await;
            
            match table {
                Ok(t) => t.version() >= Some (0), // 表存在且版本 >= 0
                Err(_) => false
            }
        });

        match save_mode {
            SaveMode::ErrorIfExists if table_exists => {
                return Err(PolarsError::ComputeError(format!("Table already exists at {}", table_url).into()));
            },
            SaveMode::Ignore if table_exists => {
                println!("[Debug] Table exists and mode is Ignore. Skipping write.");
                return Ok(());
            },
            _ => {} 
        }
        // ==========================================
        // Step A: Get Schema
        // ==========================================
        
        // Convert polars schema to delta schema
        let pl_schema = lf_ctx.inner.collect_schema()
            .map_err(|e| PolarsError::ComputeError(format!("Failed to resolve schema: {}", e).into()))?;
        
        let delta_schema = convert_to_delta_schema(&pl_schema)?;
        // Generate File Name
        let file_name = format!("part-{}-{}.parquet", Utc::now().timestamp_millis(), Uuid::new_v4());
        
        // Build File Path
        let full_write_path = if path_str.ends_with('/') {
            format!("{}{}", path_str, file_name)
        } else {
            format!("{}/{}", path_str, file_name)
        };

        // Build Polars Parquet Write Options
        let parquet_options = build_parquet_write_options(
            compression,
            compression_level,
            statistics,
            row_group_size,
            data_page_size,
            compat_level, 
        ).map_err(|e| PolarsError::ComputeError(format!("Failed to build parquet options: {}", e).into()))?;
        
        let file_format = FileWriteFormat::Parquet(parquet_options);
        
        // Polars Sink
        let target = SinkTarget::Path(PlRefPath::new(&full_write_path));
        let destination = SinkDestination::File { target };
        
        // Build UnifiedSinkArgs
        let unified_args = unsafe {
            build_unified_sink_args(
                mkdir, maintain_order, sync_on_close,
                cloud_provider, cloud_retries, cloud_retry_timeout_ms,
                cloud_retry_init_backoff_ms, cloud_retry_max_backoff_ms, cloud_cache_ttl,
                cloud_keys, cloud_values, cloud_len
            )
        };

        lf_ctx.inner
            .sink(destination, file_format, unified_args)?
            .collect_with_engine(Engine::Streaming)?;
        // ==========================================
        // Step C: 提交事务 (包含 Create Table 逻辑)
        // ==========================================
        rt.block_on(async {
            // 1. 初始化 Table
            let mut table = DeltaTable::try_from_url_with_storage_options(
                table_url.clone(), 
                delta_storage_options.clone()
            )
            .await
            .map_err(|e| PolarsError::ComputeError(format!("Failed to init DeltaTable: {}", e).into()))?;

            // 2. 检查表是否存在，不存在则创建 (Create if not exists)
            if let Err(_) = table.load().await {
                // 如果表不存在，无论什么模式(Append/Overwrite)都需要 Create
                table = table.create()
                    .with_columns(delta_schema.fields().cloned())
                    .await
                    .map_err(|e| PolarsError::ComputeError(format!("Failed to create Delta Table: {}", e).into()))?;
            }

            // 3. 获取文件元数据 (Size)
            let object_store = table.object_store();
            let path = deltalake::Path::from(file_name.clone());

            let file_meta = object_store.head(&path).await
                .map_err(|e| PolarsError::ComputeError(format!("Failed to get file metadata: {}", e).into()))?;
            
            let file_size = file_meta.size as i64;

            let mut reader = ParquetObjectReader::new(object_store.clone(), path.clone())
                .with_file_size(file_size as u64);

            let options = ArrowReaderOptions::default();
            let metadata = reader.get_metadata(Some(&options)).await
                .map_err(|e| PolarsError::ComputeError(format!("Failed to read Parquet Footer: {}", e).into()))?;
           
            let (_num_records, stats_json) = extract_delta_stats(&metadata)?;
            
            // println!("[Debug] Parquet Analysis -> Rows: {}, Size: {} bytes, Stats Length: {}", 
            //     num_records, file_size, stats_json.len());

            // 4. 构造 Add Action
            let add_action = build_add_action(
                file_name, 
                file_size,     // 物理大小
                Some(stats_json) // 包含 numRecords 的 JSON 字符串
            );

            // B. 准备 Actions 列表
            let mut actions = Vec::new();

            actions.push(Action::Add(add_action));
            // 2. 如果是 Overwrite 模式，必须把老文件全部 Remove！
            if let SaveMode::Overwrite = save_mode {
                 println!("[Debug] Mode is Overwrite. Generating Remove actions...");
                 
                 let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
                 
                 // 核心修正：
                 // 1. 使用 get_files_by_partitions 
                 // 2. 传入空切片 &[] 作为过滤器，表示匹配所有分区（即全表）
                 // 3. 得到的 path 是 deltalake::Path (object_store::Path)，本质就是相对路径
                 match table.get_files_by_partitions(&[]).await {
                     Ok(paths) => {
                         for path in paths {
                             // path.to_string() 返回的就是相对路径 (例如 "part-0000.parquet")
                             // 正好是 Remove Action 需要的格式
                             let remove = Remove {
                                 path: path.to_string(),
                                 deletion_timestamp: Some(timestamp),
                                 data_change: true,
                                 extended_file_metadata: Some(false), 
                                 partition_values: None, 
                                 size: None, 
                                 tags: None,
                                 base_row_id: None,
                                 default_row_commit_version: None,
                                 deletion_vector: None 
                             };
                             actions.push(Action::Remove(remove));
                         }
                     },
                     Err(e) => {
                         // 只有当表确实有数据但读取失败时才报错；如果是空表，get_files_by_partitions 通常返回空 Vec 而不是 Error
                         // 但为了保险，打印个日志
                         println!("[Warn] Failed to get files for overwrite (maybe table is empty?): {}", e);
                     }
                 }
            }

            // ==========================================
            // 提交
            // ==========================================
            let operation = DeltaOperation::Write {
                mode: save_mode, // 传入 Append 或 Overwrite
                partition_by: None,
                predicate: None, // Overwrite 全表时 predicate 为 None
            };
            // B. 获取构建所需的组件
            // LogStore: 负责实际的原子写入
            let log_store = table.log_store(); 
            // Snapshot: 当前表的版本状态 (用于并发冲突检测)
            // 如果是刚 Create 的表，snapshot 应该已经有了；如果是 Append，从 table 获取
            let snapshot = table.snapshot().ok(); 

            // C. 构建并提交
            // 这里的 snapshot 需要转为 Option<&dyn TableReference>
            // 你的源码里显示 impl TableReference for DeltaTableState
            // 并且 IntoFuture 已经实现好了，直接 await 即可
            let _commit_result = CommitBuilder::default()
                .with_actions(actions)
                .build(
                    snapshot.map(|s| s as &dyn transaction::TableReference), // 传入当前快照
                    log_store.clone(), // 传入存储接口
                    operation
                )
                .await // 触发 PreCommit -> Prepared -> PostCommit 流程
                .map_err(|e| PolarsError::ComputeError(format!("CommitBuilder failed: {}", e).into()))?;


            Ok::<(), PolarsError>(())
        })?;

        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_sink_delta_partitioned(
    lf_ptr: *mut LazyFrameContext,
    base_path_ptr: *const c_char,
    mode: u8,
    
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
            
            // B. 遍历 Staging 目录
            use futures::StreamExt;
            let mut files_stream = object_store.list(Some(&staging_path));
            
            while let Some(item) = files_stream.next().await {
                let meta = item.map_err(|e| PolarsError::ComputeError(format!("List staging error: {}", e).into()))?;
                let src_path_str = meta.location.to_string(); 

                if src_path_str.ends_with(".parquet") {
                    // C. 关键修复：先在 Staging 原地读取 Stats，确保数据一致性
                    let file_size = meta.size as i64;
                    let mut reader = ParquetObjectReader::new(object_store.clone(), meta.location.clone())
                            .with_file_size(meta.size as u64);
                    let options = ArrowReaderOptions::default();
                    let metadata = reader.get_metadata(Some(&options)).await
                            .map_err(|e| PolarsError::ComputeError(format!("Read footer error: {}", e).into()))?;
                    
                    let (_num_records, stats_json) = extract_delta_stats(&metadata)?;

                    // D. 计算目标路径 (Rename)
                    // 去掉 staging 前缀 -> Year=2024/part-0.parquet
                    let rel_path = src_path_str.trim_start_matches(&format!("{}/", staging_dir_name));
                    
                    // 注入 UUID 防止覆盖 -> Year=2024/part-0-<uuid>.parquet
                    let dest_path_str = rel_path.replace(".parquet", &format!("-{}.parquet", write_id));
                    
                    let src_path = Path::from(src_path_str.clone());
                    let dest_path = Path::from(dest_path_str.clone());
                    let partition_values = parse_hive_partitions(&dest_path_str, &partition_cols);

                    // E. 执行 Rename
                    object_store.rename(&src_path, &dest_path).await
                         .map_err(|e| PolarsError::ComputeError(format!("Failed to rename file: {}", e).into()))?;
                    
                    // F. 构造 Add Action (使用 dest_path，但 stats 来自 src)
                    let add = Add {
                        path: dest_path_str,
                        size: file_size,
                        partition_values,
                        modification_time: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64,
                        data_change: true,
                        stats: Some(stats_json),
                        ..Default::default()
                    };
                    actions.push(Action::Add(add));
                }
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

        let rt = get_runtime();

        // 2. Load Table & Meta (Sync Block)
        let (table, polars_schema, partition_cols) = rt.block_on(async {
            let t = DeltaTable::try_from_url_with_storage_options(table_url.clone(), delta_storage_options.clone())
                .await.map_err(|e| PolarsError::ComputeError(format!("Delta load error: {}", e).into()))?;
            
            let schema = get_polars_schema_from_delta(&t)?;
            let snapshot = t.snapshot().map_err(|e| PolarsError::ComputeError(format!("Failed to get snapshot: {}", e).into()))?;
            let metadata = snapshot.metadata();
            let part_cols = metadata.partition_columns().clone();

            Ok::<_, PolarsError>((t, schema, part_cols))
        })?;

        // 3. Get Files
        let files = rt.block_on(async {
             table.get_files_by_partitions(&[]).await
                .map_err(|e| PolarsError::ComputeError(format!("List files error: {}", e).into()))
        })?;

        let mut actions = Vec::new();
        let object_store = table.object_store(); 
        let table_root = table.table_url().to_string(); 

        for file_path in files {
            let file_path_str = file_path.to_string(); 
            let partition_map = parse_hive_partitions(&file_path_str, &partition_cols);

            // =================================================================================
            // PHASE 1: Partition Pruning (The "Fast Drop" Check)
            // =================================================================================
            // 尝试仅基于分区值构建一个 1 行的 DataFrame 并运行 Predicate
            let mut can_skip_scan = false;
            let mut fast_drop = false;

            if !partition_cols.is_empty() {
                // Build Mini-DataFrame from partition values
                let mut columns = Vec::with_capacity(partition_cols.len());
                for col_name in &partition_cols {
                    let val_opt = partition_map.get(col_name).cloned().flatten();
                    let dtype = polars_schema.get_field(col_name).map(|f| f.dtype.clone()).unwrap_or(DataType::String);
                    
                    // Create Series (len=1)
                    let s = match val_opt {
                        Some(v) => Series::new(col_name.into(), &[v]).cast(&dtype)?,
                        None => Series::new_null(col_name.into(), 1).cast(&dtype)?
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
                        path: file_path_str.clone(),
                        deletion_timestamp: Some(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64),
                        data_change: true,
                        ..Default::default()
                    };
                    actions.push(Action::Remove(remove));
                }
                // If fast_drop is false, we simply keep the file (do nothing), effectively skipping it.
                continue; 
            }

            // =================================================================================
            // PHASE 2: Scan & Filter (Stats Pushdown Optimized)
            // =================================================================================
            
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
            for (col_name, val_opt) in &partition_map {
                let lit_expr = match val_opt {
                    Some(v) => lit(v.clone()),
                    None => lit(NULL)
                };
                let final_expr = if let Some(dtype) = polars_schema.get_field(col_name).map(|f| f.dtype.clone()) {
                    lit_expr.cast(dtype)
                } else {
                    lit_expr
                };
                lf = lf.with_column(final_expr.alias(col_name));
            }

            // [Optimization]: Use filter() first to allow Parquet Stats Pushdown
            // If Parquet Min/Max stats prove "No Match", filter returns empty LF instantly.
            // We select len() to count matches.
            let stats_df = lf.clone()
                .filter(predicate_ctx.inner.clone()) // Pushdown candidate
                .select([len().alias("matched_count")])
                .collect_with_engine(Engine::Streaming)?;
            
            let matched_rows = stats_df.column("matched_count")?.u32()?.get(0).unwrap_or(0);

            if matched_rows == 0 {
                // Case 1: Keep (No rows matched predicate)
                continue;
            } 
            
            // If we have matches, we need to check if it's a Full Drop or Rewrite.
            // But we don't know Total Rows yet (unless we scan again or trust stats).
            // Optimization: Since we already filtered, we can check if we *filtered everything*.
            // Wait, we need total count to know if it's a Full Drop.
            // Let's get total count from metadata/stats if possible, or simple select(len()) on raw LF.
            
            // However, Scan overhead is paid. Let's just do a quick check on the original LF for Total Count.
            // NOTE: This might trigger another metadata read, but usually cached or cheap.
            let total_rows_df = lf.clone().select([len().alias("total")]).collect_with_engine(Engine::Streaming)?;
            let total_rows = total_rows_df.column("total")?.u32()?.get(0).unwrap_or(0);

            if matched_rows == total_rows {
                // Case 2: Drop (Full match)
                let remove = Remove {
                    path: file_path_str.clone(),
                    deletion_timestamp: Some(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64),
                    data_change: true,
                    ..Default::default()
                };
                actions.push(Action::Remove(remove));
            } else {
                // Case 3: Rewrite (Partial match)
                let remove = Remove {
                    path: file_path_str.clone(),
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

                // ... (Writing logic matches previous implementation) ...
                let new_name = format!("part-{}-{}.parquet", Utc::now().timestamp_millis(), Uuid::new_v4());
                let parent_dir = std::path::Path::new(&file_path_str).parent()
                    .map(|p| p.to_string_lossy().to_string()).unwrap_or_else(|| "".to_string());
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

// 检查文件 Stats 重叠
fn check_file_overlap(stats_json: Option<&str>, col_name: &str, src_min: PruningBound, src_max: PruningBound) -> bool {
    let json_str = match stats_json {
        Some(s) => s,
        None => return true, 
    };
    let json_val: Value = match serde_json::from_str(json_str) {
        Ok(v) => v,
        Err(_) => return true, 
    };

    let extract_bound = |field: &str| -> Option<PruningBound> {
        let val = json_val.get(field)?.get(col_name)?;
        match (&src_min, val) {
            // 如果 Source 是整数，尝试从 JSON 解析为 i128
            (PruningBound::Integer(_), Value::Number(n)) => {
                n.as_i64().map(|v| PruningBound::Integer(v as i128))
                 .or_else(|| n.as_u64().map(|v| PruningBound::Integer(v as i128)))
            },
            // 如果 Source 是浮点，从 JSON 析出 f64
            (PruningBound::Float(_), Value::Number(n)) => {
                n.as_f64().map(PruningBound::Float)
            },
            // 如果 Source 是词法（String/Date），从 JSON 析出 String
            (PruningBound::Lexical(_), Value::String(s)) => {
                Some(PruningBound::Lexical(s.clone()))
            },
            _ => None 
        }
    };

    let file_min = match extract_bound("minValues") {
        Some(v) => v,
        None => return true, 
    };
    let file_max = match extract_bound("maxValues") {
        Some(v) => v,
        None => return true, 
    };

    !(src_max < file_min || src_min > file_max)
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_io_delta_merge(
    source_lf_ptr: *mut LazyFrameContext, 
    target_path_ptr: *const c_char,
    merge_keys_ptr: *const *const c_char,
    merge_keys_len: usize,
    
    // --- Merge Conditions (Expr) ---
    // 允许为空指针 (null_ptr)，为空则使用默认行为 (true/false)
    matched_update_cond: *mut ExprContext,        // WHEN MATCHED AND cond THEN UPDATE
    matched_delete_cond: *mut ExprContext,        // WHEN MATCHED AND cond THEN DELETE
    not_matched_insert_cond: *mut ExprContext,    // WHEN NOT MATCHED AND cond THEN INSERT
    not_matched_by_source_delete_cond: *mut ExprContext, // WHEN NOT MATCHED BY SOURCE AND cond THEN DELETE

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
        let path_str = ptr_to_str(target_path_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        let merge_keys = unsafe { ptr_to_vec_string(merge_keys_ptr, merge_keys_len) };
        if merge_keys.is_empty() {
             return Err(PolarsError::ComputeError("Merge keys cannot be empty".into()));
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
            // 获取 Source 数据涉及的分区值集合 (HashSet<Vec<String>>)
            // 如果表没有分区，这里返回 None 或 空集合
            let source_partitions = get_source_partition_values(&source_lf, &part_cols);

            // =========================================================
            // OPTIMIZATION 2: Analyze Source Key Bounds (Multi-Key)
            // =========================================================
            // 计算 Source 数据中每个 Merge Key 的 (Min, Max) 范围
            // 用于 Z-Order / Data Skipping
            let mut key_bounds = HashMap::new();
            for key in &merge_keys {
                let dtype = schema.get_field(key).map(|f| f.dtype.clone()).unwrap_or(DataType::Null);
                // 假设 get_source_key_bounds 返回 Option<(LiteralValue, LiteralValue)>
                if let Some(bounds) = get_source_key_bounds(&source_lf, key, &dtype) {
                    key_bounds.insert(key.clone(), bounds);
                }
            }

            // =========================================================
            // D. Iterate & Prune Active Files
            // =========================================================
            let mut remove_actions = Vec::new();
            
            // 获取活跃文件流
            let t_scan = t.clone(); 
            let mut stream = t_scan.get_active_add_actions_by_partitions(&[]);
            // let mut stream = t.get_active_add_actions_by_partitions(&[]);

            while let Some(action_res) = futures::StreamExt::next(&mut stream).await {
                let view = action_res.map_err(|e| PolarsError::ComputeError(format!("Delta stream error: {}", e).into()))?;
                
                // 1. Partition Pruning (分区剪枝)
                // 如果 Source 提供了分区信息，且当前文件不在涉及的分区内，直接跳过
                if let Some(affected_set) = &source_partitions {
                    let path_cow = view.path();
                    let path_str = path_cow.as_ref();
                    
                    // 解析文件路径中的分区值 (更快，避免 view.partition_values 的开销)
                    let file_parts_map = parse_hive_partitions(path_str, &part_cols);
                    
                    // 构建分区 Key 向量，顺序必须与 part_cols 一致
                    let mut file_key = Vec::with_capacity(part_cols.len());
                    for col_name in &part_cols {
                        let v = file_parts_map.get(col_name).cloned().flatten()
                                .unwrap_or_else(|| "__HIVE_DEFAULT_PARTITION__".to_string());
                        file_key.push(v);
                    }
                    
                    // 核心检查：如果文件所在分区不在 Source 涉及的分区集合中 -> Skip
                    if !affected_set.contains(&file_key) {
                        continue; 
                    }
                }

                // 2. Stats Pruning (统计信息剪枝 - Multi-Key Intersection)
                // 如果文件通过了分区检查，再看它的 Min/Max 是否与 Source 重叠
                // 逻辑：所有 Key 必须同时重叠 (Intersection)，文件才可能包含需要 Merge 的数据
                let mut file_overlaps = true;
                if !key_bounds.is_empty() {
                    let stats_json = view.stats();
                    
                    for (key, (src_min, src_max)) in &key_bounds {
                        // 只要有任意一个 Key 的范围不重叠，这个文件就不需要读取
                        if !check_file_overlap(stats_json.as_deref(), key, src_min.clone(), src_max.clone()) {
                            file_overlaps = false;
                            break; // 只要一个 Key 没命中，整个文件就可以判死刑
                        }
                    }
                }

                // 3. Keep File
                // 只有通过了所有剪枝策略的文件，才会被加入读取列表
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
            // 使用从 Delta Snapshot 获取的 Polars Schema 构建空 DataFrame
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
            // 必须开启 Hive 解析，否则 Polars 读不到分区列数据
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
        // [New]: Schema Validation
        // 确保 Source 中的每一列都存在于 Target (polars_schema) 中
        // 除非未来支持 Schema Evolution，否则这里必须报错
        for src_col_name in source_schema.iter_names() {
            if polars_schema.get_field(src_col_name).is_none() {
                return Err(PolarsError::ComputeError(
                    format!(
                        "Schema mismatch: Source column '{}' is not present in target table. \
                        Schema evolution is not currently supported.", 
                        src_col_name
                    ).into()
                ));
            }
        }
        // 关键：Source Key 也要改名，用于判断 "Source Exists"
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
        let source_renamed = source_lf.rename(old_names, new_names,true);

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
        // B. Execute Full Join
        let joined_lf = target_lf.join(
            source_renamed,
            target_keys_exprs,         // Left On: [Key1, Key2]
            source_keys_renamed_exprs, // Right On: [Key1_tmp, Key2_tmp]
            join_args 
        );

        // C. Define Status Expressions
        // Source Exists if ANY key is not null? 
        // In a proper Full Join on keys, if the row exists in Source, ALL key columns should be non-null.
        // We can check the first key for performance.
        let first_src_key = format!("{}_src_tmp", merge_keys[0]);
        let first_tgt_key = &merge_keys[0];

        let is_source_exists = col(&first_src_key).is_not_null();
        let is_target_exists = col(first_tgt_key).is_not_null();

        let is_matched = is_source_exists.clone().and(is_target_exists.clone());
        let is_not_matched = is_source_exists.clone().and(is_target_exists.clone().not());     // Insert
        let is_not_matched_by_source = is_source_exists.not().and(is_target_exists.clone()); // Target Only

        // D. Calculate Column Values (Vectorized When-Then)
        let mut final_exprs = Vec::new();

        for (col_name_smart, _) in polars_schema.iter() {
            let col_name = col_name_smart.as_str();
            let src_col_alias = format!("{}_src_tmp", col_name);
            let has_source_col = source_cols_renamed.contains(&src_col_alias);

            // Logic:
            // 1. Matched & UpdateCond -> Source Value
            // 2. NotMatched & InsertCond -> Source Value
            // 3. Otherwise -> Target Value
            
            // 注意：如果 Source 没给这个列，Matched/Insert 时只能 fallback 到 Target (或者 Null)
            // 这里为了简单，如果 Source 没给，就保持 Target 值 (Partial Update)
            
            let source_val_expr = if has_source_col { col(&src_col_alias) } else { col(col_name) };

            final_exprs.push(
                when(
                    // Case 1: Matched Update
                    is_matched.clone().and(cond_update.clone())
                )
                .then(source_val_expr.clone())
                .when(
                    // Case 2: Not Matched Insert
                    is_not_matched.clone().and(cond_insert.clone())
                )
                .then(source_val_expr)
                .otherwise(col(col_name)) // Case 3: Keep Original (or Matched-NoUpdate)
                .alias(col_name)
            );
        }

        // E. Filter Rows (Delete Logic)
        // We want to KEEP the row if:
        // 1. It was Updated/Inserted (and not Deleted)
        // 2. It was Kept (Target Only) (and not Deleted)
        
        // Reverse Logic: Drop if...
        // 1. Matched AND DeleteCond
        // 2. NotMatchedBySource AND DeleteSourceCond
        // 3. NotMatched AND !InsertCond (Ignore new row)
        
        let should_delete = 
            (is_matched.clone().and(cond_match_delete.clone())) // Matched Delete
            .or(is_not_matched_by_source.clone().and(cond_source_delete.clone())) // Source Delete
            .or(is_not_matched.clone().and(cond_insert.not())); // Insert Condition Failed

        let processed_lf = joined_lf
            .with_columns(final_exprs) // 1. Calc Values
            .filter(should_delete.not()) // 2. Remove Deleted Rows
            .select(polars_schema.iter_names().map(|n| col(n.as_str())).collect::<Vec<_>>()); // 3. Clean Columns

            // =================================================================================
            // 5. Sink to Staging (Write New Files)
            // =================================================================================
            
            let write_id = Uuid::new_v4();
            let staging_dir_name = format!(".merge_staging_{}", write_id);
            
            // Staging URI: s3://bucket/table/.merge_staging_xxx
            let staging_uri = format!("{}/{}", root_trimmed, staging_dir_name);
            
            // Partition Strategy
            // 必须与原表保持一致
            let partition_strategy = if partition_cols.is_empty() {
                PartitionStrategy::FileSize
            } else {
                let keys: Vec<Expr> = partition_cols.iter().map(|n| col(n)).collect();
                PartitionStrategy::Keyed {
                    keys,
                    include_keys: false, // Delta 默认不将分区键写入 Parquet 数据列中
                    keys_pre_grouped: false,
                }
            };

            // Sink Destination (Partitioned)
            let hive_provider = HivePathProvider {
                extension: PlSmallStr::from_str(".parquet"),
            };
            let file_path_provider = Some(file_provider::FileProviderType::Hive(hive_provider));
            
            let destination = SinkDestination::Partitioned {
                base_path: PlRefPath::new(staging_uri), 
                file_path_provider,
                partition_strategy,
                max_rows_per_file: u32::MAX, // 让 Polars 自己决定或使用默认
                approximate_bytes_per_file: usize::MAX as u64,
            };

            let write_opts = build_parquet_write_options(1, -1, true, 0, 0, -1)
                .map_err(|e| PolarsError::ComputeError(format!("Options error: {}", e).into()))?;
            
            // Unified Cloud Args
            let unified_args = unsafe {
                build_unified_sink_args(
                    false, false, 0,
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

            // B. Add New Files (From Staging)
            let add_actions = rt.block_on(async {
                let mut adds = Vec::new();
                let staging_path = Path::from(staging_dir_name.clone());
                
                use futures::StreamExt;
                let mut files_stream = object_store.list(Some(&staging_path));

                while let Some(item) = files_stream.next().await {
                    let meta = item.map_err(|e| PolarsError::ComputeError(format!("List staging error: {}", e).into()))?;
                    let src_path_str = meta.location.to_string();

                    if src_path_str.ends_with(".parquet") {
                        // 1. Read Stats
                        let file_size = meta.size as i64;
                        let mut reader = ParquetObjectReader::new(object_store.clone(), meta.location.clone())
                                .with_file_size(meta.size as u64);
                        let footer = reader.get_metadata(None).await
                                .map_err(|e| PolarsError::ComputeError(format!("Read footer error: {}", e).into()))?;
                        let (_, stats_json) = extract_delta_stats(&footer)?;

                        // 2. Rename (Move out of staging)
                        // src: table/.merge_staging_xxx/year=2024/part-0.parquet
                        // dest: table/year=2024/part-0-<uuid>.parquet
                        
                        let rel_path = src_path_str.trim_start_matches(&format!("{}/", staging_dir_name));
                        let dest_path_str = rel_path.replace(".parquet", &format!("-{}.parquet", write_id));
                        
                        let src_path = Path::from(src_path_str);
                        let dest_path = Path::from(dest_path_str.clone());
                        
                        object_store.rename(&src_path, &dest_path).await
                            .map_err(|e| PolarsError::ComputeError(format!("Rename failed: {}", e).into()))?;
                        
                        // 3. Add Action
                        // 重新解析分区值 (从目标路径)
                        let partition_values = parse_hive_partitions(&dest_path_str, &partition_cols);
                        
                        adds.push(Action::Add(Add {
                            path: dest_path_str,
                            size: file_size,
                            partition_values,
                            modification_time: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64,
                            data_change: true,
                            stats: Some(stats_json),
                            ..Default::default()
                        }));
                    }
                }
                Ok::<Vec<Action>, PolarsError>(adds)
            })?;

            final_actions.extend(add_actions);
        // =================================================================================
        // PHASE 7: Commit Transaction
        // =================================================================================

        if !final_actions.is_empty() {
             rt.block_on(async {
                // Construct Logic Metadata (告诉 Delta Log 我们做了什么)
                // 我们根据用户是否传入了对应的 Condition 指针，来推断执行了哪些操作类型。

                let mut matched_preds = Vec::new();
                let mut not_matched_preds = Vec::new();           // 通常对应 INSERT
                let mut not_matched_by_source_preds = Vec::new(); // 通常对应 DELETE (Target Only)

                // 1. Matched Update Logic
                if !matched_update_cond.is_null() {
                    // 用户指定了条件 (Conditional Update)
                    matched_preds.push(MergePredicate { 
                        predicate: Some("custom_update_condition".into()), 
                        action_type: "UPDATE".into() 
                    });
                } else {
                    // [重要] 指针为空 = 默认 Upsert = 无条件 Update
                    matched_preds.push(MergePredicate { 
                        predicate: None, 
                        action_type: "UPDATE".into() 
                    });
                }
                // 如果传入了 Delete 条件，则记录 DELETE 操作
                if !matched_delete_cond.is_null() {
                    matched_preds.push(MergePredicate {
                        predicate: Some("matched_delete_condition".to_string()),
                        action_type: "DELETE".to_string(),
                    });
                }

                // B. Not Matched (Source Only) -> INSERT
                if !not_matched_insert_cond.is_null() {
                    not_matched_preds.push(MergePredicate {
                        predicate: Some("not_matched_insert_condition".to_string()),
                        action_type: "INSERT".to_string(),
                    });
                } else {
                    // 如果没传条件，默认为无条件插入 (Unconditional Insert)
                    // 这是 Upsert 的标准行为
                     not_matched_preds.push(MergePredicate {
                        predicate: None,
                        action_type: "INSERT".to_string(),
                    });
                }

                // C. Not Matched By Source (Target Only) -> DELETE
                if !not_matched_by_source_delete_cond.is_null() {
                    not_matched_by_source_preds.push(MergePredicate {
                        predicate: Some("not_matched_by_source_delete_condition".to_string()),
                        action_type: "DELETE".to_string(),
                    });
                }

                // 2. Construct the Delta Operation
                // join_predicate 仅用于日志审计，描述关联键
                let join_predicate = merge_keys.iter()
                    .map(|k| format!("source.{} = target.{}", k, k))
                    .collect::<Vec<_>>()
                    .join(" AND ");

                let operation = DeltaOperation::Merge {
                    predicate: Some(join_predicate.clone()),
                    merge_predicate: Some(join_predicate), // 某些 Delta 版本可能需要这个字段
                    matched_predicates: matched_preds,
                    not_matched_predicates: not_matched_preds,
                    not_matched_by_source_predicates: not_matched_by_source_preds,
                };

                // 3. Execute Commit
                // 这一步将 Actions (Add/Remove) 和 Operation 写入 _delta_log
                let _ver = CommitBuilder::default()
                    .with_actions(final_actions)
                    .build(
                        table.snapshot().ok().map(|s| s as &dyn transaction::TableReference),
                        table.log_store().clone(),
                        operation
                    )
                    .await
                    .map_err(|e| PolarsError::ComputeError(format!("Commit failed: {}", e).into()))?;
                
                // 4. Cleanup Staging (Best Effort)
                // 尝试删除空的 staging 目录。忽略错误，因为事务已提交，且对象存储上目录通常是虚拟的。
                // 这在本地文件系统测试时很有用，能保持目录整洁。
                let _ = object_store.delete(&Path::from(staging_dir_name)).await;
                
                Ok::<(), PolarsError>(())
            })?;
        }
       
        Ok(())
    })
}