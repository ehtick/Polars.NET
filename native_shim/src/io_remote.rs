// ==========================================
// Delta Lake Support
// ==========================================

use chrono::Utc;
use deltalake::kernel::engine::arrow_conversion::{TryIntoArrow, TryIntoKernel};
use deltalake::kernel::transaction::{CommitBuilder};
use deltalake::parquet::arrow::arrow_reader::ArrowReaderOptions;
use deltalake::parquet::data_type::ByteArray;
use deltalake::parquet::file::metadata::ParquetMetaData;
use deltalake::{DeltaTable, DeltaTableBuilder, Path};
use deltalake::protocol::{DeltaOperation, SaveMode};
use deltalake::parquet::arrow::async_reader::{AsyncFileReader, ParquetObjectReader};
use deltalake::parquet::file::statistics::Statistics as ParquetStatistics;
use serde_json::{json, Value};
use polars::{error::PolarsError, prelude::*};
use polars_buffer::Buffer;
use tokio::runtime::Runtime;
use crate::io::{build_partitioned_destination, build_scan_args, build_unified_sink_args,build_parquet_write_options};
use crate::types::{SchemaContext, SelectorContext,LazyFrameContext};
use crate::utils::ptr_to_str;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{ffi::CStr, os::raw::c_char};
use std::sync::OnceLock;
use url::Url;
use uuid::Uuid;
use std::collections::{HashMap};
use deltalake::arrow::ffi::FFI_ArrowSchema as DeltaFFISchema;
use polars_arrow::ffi::{ArrowSchema as PolarsFFISchema};
use polars_arrow::ffi::import_field_from_c;
use deltalake::arrow::datatypes::{Schema as DeltaArrowSchema};
use deltalake::kernel::{Action, Add, Remove, StructType, transaction};


static RUNTIME: OnceLock<Runtime> = OnceLock::new();

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

        // 2. Cloud Options for Delta Crate
        let mut delta_opts = HashMap::new();
        if !cloud_keys.is_null() && !cloud_values.is_null() && cloud_len > 0 {
             let k_slice = unsafe { std::slice::from_raw_parts(cloud_keys, cloud_len) };
             let v_slice = unsafe { std::slice::from_raw_parts(cloud_values, cloud_len) };
             for i in 0..cloud_len {
                 let k = unsafe { CStr::from_ptr(k_slice[i]).to_string_lossy().into_owned() };
                 let v = unsafe { CStr::from_ptr(v_slice[i]).to_string_lossy().into_owned() };
                 delta_opts.insert(k, v);
             }
        }

        // 3. Load Delta Table & Extract Info (Async)
        let rt = get_runtime();
        let (file_uris, polars_schema) = rt.block_on(async {
            let mut builder = DeltaTableBuilder::from_url(table_url).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
            if let Some(v) = version_val { builder = builder.with_version(v); }
            else if let Some(dt) = datetime_str { builder = builder.with_datestring(dt).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?; }
            
            let table = builder.with_storage_options(delta_opts).load().await.map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
            
            // --- 关键点：获取 Delta Log 中的权威 Schema ---
            // 我们克隆一份出来传给外面
            // 1. 获取快照 (Snapshot/State)
            let snapshot = table.snapshot().map_err(|e| PolarsError::ComputeError(format!("Failed to get snapshot: {}", e).into()))?;
            
            // 1. 获取 Delta Kernel Schema (这是你发现的 API)
            // schema() 返回的是 KernelSchemaRef (通常是 Arc<StructType>) 或者是 &StructType
            let kernel_schema = snapshot.schema(); 
            
            // 2. 手动转换为 Arrow Schema
            // deltalake 实现了 TryInto<ArrowSchema> for &KernelSchema (或类似 trait)
            // 注意：这里需要 deltalake 的 arrow feature 开启 (默认通常是开启的)
            let arrow_schema: DeltaArrowSchema = TryIntoArrow::<DeltaArrowSchema>::try_into_arrow(kernel_schema.as_ref())
                        .map_err(|e| PolarsError::ComputeError(format!("Schema conversion error: {}", e).into()))?;
            // -------------------------------------------------------------------------
            // 核心修复：通过 FFI Interface "偷渡" Schema
            // -------------------------------------------------------------------------
            
            // Step A: 将 Delta Schema 导出为 FFI 格式
            // 这是一个 Rust Wrapper，里面包着 C 指针
            let delta_ffi = DeltaFFISchema::try_from(&arrow_schema)
                .map_err(|e| PolarsError::ComputeError(format!("FFI Export Error: {}", e).into()))?;

            // Step B: 指针强转 (Pointer Cast)
            // 原理：FFI_ArrowSchema 在两个库里都是对 `struct ArrowSchema` 的封装。
            // 我们不 Transmute 整个结构体（那样涉及所有权），而是借用指针读取。
            // Schema 的导入通常是 Copy 语义（读取 C Struct 里的数据创建新 Schema），而不是 Move 语义，
            // 所以我们只需要让 Polars "看一眼" 这个 C 指针即可。
            let delta_ffi_ptr = &delta_ffi as *const DeltaFFISchema;
            let polars_ffi_ptr = delta_ffi_ptr as *const PolarsFFISchema;

            let imported_field = unsafe { import_field_from_c(&*polars_ffi_ptr) }
                .map_err(|e| PolarsError::ComputeError(format!("FFI Import error: {}", e).into()))?;
            // Step C: Polars 从 C Struct 导入 Schema
            // 注意：这里 try_from 会读取 C 结构体并创建一个全新的 Polars Arrow Schema
            // delta_ffi 在这里仍然存活，负责持有 C 内存，直到 block 结束被 drop，非常安全。
            let final_schema = match imported_field.dtype {
                ArrowDataType::Struct(fields) => {
                    // fields 类型是 Vec<ArrowField>
                    
                    // 1. 遍历 Arrow 字段，逐个转换为 Polars 字段
                    // Field::from(&ArrowField) 是 polars-core 实现的标准转换
                    let polars_fields = fields.iter().map(|arrow_field| {
                        Field::from(arrow_field)
                    });

                    // 2. 从 Polars 字段迭代器构建 Schema
                    // Schema 实现了 FromIterator<Field>
                    Schema::from_iter(polars_fields)
                },
                _ => return Err(PolarsError::ComputeError("Imported Delta Schema is not a Struct".into())),
            };

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
            .collect()?;
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
           
            let (num_records, stats_json) = extract_delta_stats(&metadata)?;
            
            println!("[Debug] Parquet Analysis -> Rows: {}, Size: {} bytes, Stats Length: {}", 
                num_records, file_size, stats_json.len());

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

            println!("[Debug] Streaming Commit Success! Version: {}", _commit_result.version);

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
            .collect()?;
            
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
            Ok(())
        })?;
        
        Ok(())
    })
}

// -------------------------------------------------------------------------
// Helper Functions (如果尚未定义，需补充)
// -------------------------------------------------------------------------

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