// ==========================================
// Delta Lake Support
// ==========================================

use chrono::Utc;
use deltalake::kernel::engine::arrow_conversion::TryIntoArrow;
use deltalake::kernel::transaction::{CommitBuilder};
use deltalake::parquet::arrow::arrow_reader::ArrowReaderOptions;
use deltalake::parquet::data_type::ByteArray;
use deltalake::parquet::file::metadata::ParquetMetaData;
use deltalake::{ArrayType, DeltaTable, DeltaTableBuilder};
use deltalake::protocol::{DeltaOperation, SaveMode};
use deltalake::parquet::arrow::async_reader::{AsyncFileReader, ParquetObjectReader};
use deltalake::parquet::file::statistics::Statistics as ParquetStatistics;
use serde_json::{json, Value};
use polars::{error::PolarsError, prelude::*};
use polars_arrow::buffer::Buffer;
use tokio::runtime::Runtime;
use crate::io::build_scan_args;
use crate::types::SchemaContext;
use crate::{io::{build_cloud_options,build_parquet_write_options}, types::LazyFrameContext, utils::ptr_to_str};
use std::time::{SystemTime, UNIX_EPOCH};
use std::{ffi::CStr, os::raw::c_char};
use std::sync::OnceLock;
use url::Url;
use uuid::Uuid;
use std::collections::HashMap;
use polars::prelude::ArrowDataType as PlArrowDataType;
use deltalake::arrow::ffi::FFI_ArrowSchema as DeltaFFISchema;
use polars_arrow::ffi::ArrowSchema as PolarsFFISchema;
use polars_arrow::ffi::import_field_from_c;
// use deltalake::arrow::array::{StructArray}; 
// use deltalake::arrow::record_batch::RecordBatch; 
use deltalake::arrow::datatypes::Schema as DeltaArrowSchema;
use deltalake::kernel::{Action, Add, DataType as DeltaDataType, MapType, Remove, StructField, StructType, transaction};
use deltalake::PrimitiveType;
// use deltalake::arrow::datatypes::{DataType as ArrowDataType};

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

// fn arrow_to_delta_type(arrow_type: &ArrowDataType) -> Result<DeltaDataType, PolarsError> {
//     match arrow_type {
//         // Primitive Types
//         ArrowDataType::Boolean => Ok(DeltaDataType::Primitive(PrimitiveType::Boolean)),
//         ArrowDataType::Int8 => Ok(DeltaDataType::Primitive(PrimitiveType::Byte)),
//         ArrowDataType::Int16 => Ok(DeltaDataType::Primitive(PrimitiveType::Short)),
//         ArrowDataType::Int32 => Ok(DeltaDataType::Primitive(PrimitiveType::Integer)),
//         ArrowDataType::Int64 => Ok(DeltaDataType::Primitive(PrimitiveType::Long)),
//         ArrowDataType::UInt8 => Ok(DeltaDataType::Primitive(PrimitiveType::Byte)),  
//         ArrowDataType::UInt16 => Ok(DeltaDataType::Primitive(PrimitiveType::Short)),
//         ArrowDataType::UInt32 => Ok(DeltaDataType::Primitive(PrimitiveType::Integer)),
//         ArrowDataType::UInt64 => Ok(DeltaDataType::Primitive(PrimitiveType::Long)),
//         ArrowDataType::Float16 => Ok(DeltaDataType::Primitive(PrimitiveType::Float)),
//         ArrowDataType::Float32 => Ok(DeltaDataType::Primitive(PrimitiveType::Float)),
//         ArrowDataType::Float64 => Ok(DeltaDataType::Primitive(PrimitiveType::Double)),
//         ArrowDataType::Utf8 | ArrowDataType::Utf8View| ArrowDataType::LargeUtf8 => Ok(DeltaDataType::Primitive(PrimitiveType::String)),
//         ArrowDataType::Binary | ArrowDataType::LargeBinary => Ok(DeltaDataType::Primitive(PrimitiveType::Binary)),
//         ArrowDataType::Date32 => Ok(DeltaDataType::Primitive(PrimitiveType::Date)),
//         ArrowDataType::Date64 => Ok(DeltaDataType::Primitive(PrimitiveType::Date)),
//         ArrowDataType::Timestamp(_, _) => Ok(DeltaDataType::Primitive(PrimitiveType::Timestamp)), 
//         ArrowDataType::Decimal128(p, s) | ArrowDataType::Decimal256(p, s) => {
//              DeltaDataType::decimal(*p as u8, *s as u8)
//                  .map_err(|e| PolarsError::ComputeError(format!("Invalid Decimal: {}", e).into()))
//         },
        
//         // Struct
//         ArrowDataType::Struct(fields) => {
//             let delta_fields = fields.iter().map(|f| {
//                 let dtype = arrow_to_delta_type(f.data_type())?;
//                 Ok(StructField::new(f.name().clone(), dtype, f.is_nullable()))
//             }).collect::<Result<Vec<StructField>, PolarsError>>()?;
            

//             Ok(DeltaDataType::Struct(Box::new(StructType::try_new(delta_fields).map_err(|e| PolarsError::ComputeError(format!("Struct error: {}", e).into()))?)))
//         },

//         // 复杂类型：List
//         ArrowDataType::List(field) | ArrowDataType::LargeList(field)| 
//         ArrowDataType::FixedSizeList(field, _) => {
//              let inner_type = arrow_to_delta_type(field.data_type())?;
//              // Delta ArrayType 包含 element_type 和 contains_null
//              Ok(DeltaDataType::Array(Box::new(deltalake::kernel::ArrayType::new(inner_type, field.is_nullable()))))
//         },

//         // [Map]
//         ArrowDataType::Map(field, _sorted) => {
//             if let ArrowDataType::Struct(struct_fields) = field.data_type() {
//                 if struct_fields.len() != 2 {
//                      return Err(PolarsError::ComputeError("Arrow Map type must have exactly 2 fields".into()));
//                 }
//                 let key_type = arrow_to_delta_type(struct_fields[0].data_type())?;
//                 let value_type = arrow_to_delta_type(struct_fields[1].data_type())?;
//                 let value_nullable = struct_fields[1].is_nullable();
                
//                 Ok(DeltaDataType::Map(Box::new(MapType::new(key_type, value_type, value_nullable))))
//             } else {
//                  return Err(PolarsError::ComputeError("Arrow Map field must be a Struct".into()));
//             }
//         },

//         _ => Err(PolarsError::ComputeError(format!("Unsupported Arrow data type for Delta Lake: {:?}", arrow_type).into())),
//     }
// }

fn polars_arrow_to_delta_type(arrow_type: &PlArrowDataType) -> Result<DeltaDataType, PolarsError> {
    match arrow_type {
        // 基础类型
        PlArrowDataType::Boolean => Ok(DeltaDataType::Primitive(PrimitiveType::Boolean)),
        PlArrowDataType::Int8 => Ok(DeltaDataType::Primitive(PrimitiveType::Byte)),
        PlArrowDataType::Int16 => Ok(DeltaDataType::Primitive(PrimitiveType::Short)),
        PlArrowDataType::Int32 => Ok(DeltaDataType::Primitive(PrimitiveType::Integer)),
        PlArrowDataType::Int64 => Ok(DeltaDataType::Primitive(PrimitiveType::Long)),
        PlArrowDataType::UInt8 => Ok(DeltaDataType::Primitive(PrimitiveType::Byte)),
        PlArrowDataType::UInt16 => Ok(DeltaDataType::Primitive(PrimitiveType::Short)),
        PlArrowDataType::UInt32 => Ok(DeltaDataType::Primitive(PrimitiveType::Integer)),
        PlArrowDataType::UInt64 => Ok(DeltaDataType::Primitive(PrimitiveType::Long)),
        PlArrowDataType::Float16 => Ok(DeltaDataType::Primitive(PrimitiveType::Float)),
        PlArrowDataType::Float32 => Ok(DeltaDataType::Primitive(PrimitiveType::Float)),
        PlArrowDataType::Float64 => Ok(DeltaDataType::Primitive(PrimitiveType::Double)),
        PlArrowDataType::Utf8 | PlArrowDataType::LargeUtf8 | PlArrowDataType::Utf8View => Ok(DeltaDataType::Primitive(PrimitiveType::String)),
        PlArrowDataType::Binary | PlArrowDataType::LargeBinary => Ok(DeltaDataType::Primitive(PrimitiveType::Binary)),
        PlArrowDataType::Date32 => Ok(DeltaDataType::Primitive(PrimitiveType::Date)),
        PlArrowDataType::Date64 => Ok(DeltaDataType::Primitive(PrimitiveType::Date)),
        PlArrowDataType::Timestamp(_, _) => Ok(DeltaDataType::Primitive(PrimitiveType::Timestamp)),
        
        // Decimal
        PlArrowDataType::Decimal(p, s) | PlArrowDataType::Decimal256(p, s) => {
             DeltaDataType::decimal(*p as u8, *s as u8)
                 .map_err(|e| PolarsError::ComputeError(format!("Invalid Decimal: {}", e).into()))
        },

        // Struct
        PlArrowDataType::Struct(fields) => {
            let delta_fields = fields.iter().map(|f| {
                // <--- 递归调用 polars_arrow_to_delta_type
                let dtype = polars_arrow_to_delta_type(f.dtype())?; 
                Ok(StructField::new(f.name.clone(), dtype, f.is_nullable))
            }).collect::<Result<Vec<StructField>, PolarsError>>()?;
            
            DeltaDataType::try_struct_type(delta_fields)
                .map_err(|e| PolarsError::ComputeError(format!("Struct error: {}", e).into()))
        },

        // List
        PlArrowDataType::List(field) | 
        PlArrowDataType::LargeList(field) | 
        PlArrowDataType::FixedSizeList(field, _) => {
             let element_type = polars_arrow_to_delta_type(field.dtype())?;
             Ok(DeltaDataType::Array(Box::new(ArrayType::new(element_type, field.is_nullable))))
        },

        // Map
        PlArrowDataType::Map(field, _sorted) => {
            if let PlArrowDataType::Struct(struct_fields) = field.dtype() {
                if struct_fields.len() != 2 {
                     return Err(PolarsError::ComputeError("Arrow Map type must have exactly 2 fields".into()));
                }
                let key_type = polars_arrow_to_delta_type(struct_fields[0].dtype())?;
                let value_type = polars_arrow_to_delta_type(struct_fields[1].dtype())?;
                
                Ok(DeltaDataType::Map(Box::new(MapType::new(key_type, value_type, struct_fields[1].is_nullable))))
            } else {
                 return Err(PolarsError::ComputeError("Arrow Map field must be a Struct".into()));
            }
        },

        _ => Err(PolarsError::ComputeError(format!("Unsupported Polars Arrow data type: {:?}", arrow_type).into())),
    }
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


// #[unsafe(no_mangle)]
// pub extern "C" fn pl_scan_delta(
//     path_ptr: *const c_char,
//     // --- Time Travel Args ---
//     version: *const i64,          // null for None
//     datetime_ptr: *const c_char,  // null for None
//     // --- Cloud Options ---
//     cloud_provider: u8,
//     cloud_retries: usize,
//     cloud_cache_ttl: u64,
//     cloud_keys: *const *const c_char,
//     cloud_values: *const *const c_char,
//     cloud_len: usize
// ) -> *mut LazyFrameContext {
//     ffi_try!({
//         let path_str = ptr_to_str(path_ptr)
//             .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

//         let version_val = if version.is_null() { None } else { unsafe { Some(*version) } };
//         let datetime_str = if datetime_ptr.is_null() {
//             None
//         } else {
//             Some(
//                 ptr_to_str(datetime_ptr)
//                     .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?
//             )
//         };

//         // URL Parsing
//         let table_url = if let Ok(u) = Url::parse(path_str) {
//             u
//         } else {
//             let abs_path = std::fs::canonicalize(path_str)
//                 .map_err(|e| PolarsError::ComputeError(format!("Invalid local path '{}': {}", path_str, e).into()))?;
//             Url::from_directory_path(abs_path)
//                 .map_err(|_| PolarsError::ComputeError(format!("Could not convert path '{}' to URL", path_str).into()))?
//         };

//         // =================================================================
//         // Prepare Delta Lake Storage Options
//         // =================================================================
//         let mut delta_storage_options = HashMap::new();
        
//         if !cloud_keys.is_null() && !cloud_values.is_null() && cloud_len > 0 {
//             let keys_slice = unsafe { std::slice::from_raw_parts(cloud_keys, cloud_len) };
//             let vals_slice = unsafe { std::slice::from_raw_parts(cloud_values, cloud_len) };

//             for i in 0..cloud_len {
//                 let k_ptr = keys_slice[i];
//                 let v_ptr = vals_slice[i];
//                 if !k_ptr.is_null() && !v_ptr.is_null() {
//                     let k = unsafe { CStr::from_ptr(k_ptr).to_string_lossy().into_owned() };
//                     let v = unsafe { CStr::from_ptr(v_ptr).to_string_lossy().into_owned() };
//                     delta_storage_options.insert(k, v);
//                 }
//             }
//         }

//         // =================================================================
//         // Parse Delta Log
//         // =================================================================
//         let rt = get_runtime();

//         let file_uris = rt.block_on(async {
//             // Create Builder: from_url
//             let mut builder = DeltaTableBuilder::from_url(table_url)
//                 .map_err(|e| PolarsError::ComputeError(format!("Failed to create DeltaTableBuilder: {}", e).into()))?;
            
//             // Time Travel
//             if let Some(v) = version_val {
//                 builder = builder.with_version(v);
//             } else if let Some(dt) = datetime_str {
//                 // with_datestring accept ISO-8601 string
//                 builder = builder.with_datestring(dt)
//                     .map_err(|e| PolarsError::ComputeError(format!("Invalid datetime format: {}", e).into()))?;
//             }

//             // Set Options and Load
//             let table = builder
//                 .with_storage_options(delta_storage_options) 
//                 .load()
//                 .await
//                 .map_err(|e| PolarsError::ComputeError(format!("Failed to load Delta table: {}", e).into()))?;
            
//             // Get File List
//             let files_iter = table.get_file_uris()
//                 .map_err(|e| PolarsError::ComputeError(format!("Failed to get file URIs: {}", e).into()))?;

//             let uris: Vec<String> = files_iter.map(|s| s.to_string()).collect();
//             Ok::<Vec<String>, PolarsError>(uris)
//         })?;

//         // =================================================================
//         // Prepare Polars Scan Args
//         // =================================================================
        
//         // Build Buffer<PlPath>
//         let pl_paths: Vec<PlPath> = file_uris.iter()
//             .map(|s| PlPath::new(s)) 
//             .collect();
//         let buffer: Buffer<PlPath> = pl_paths.into();

//         // Build ScanArgsParquet and CloudOptions
//         let mut args = ScanArgsParquet::default();
        
//         args.cloud_options = unsafe {
//             build_cloud_options(
//                 cloud_provider,
//                 cloud_retries,
//                 cloud_cache_ttl,
//                 cloud_keys,
//                 cloud_values,
//                 cloud_len
//             )
//         };

//         // =================================================================
//         // Scan
//         // =================================================================
//         let lf = LazyFrame::scan_parquet_files(buffer, args)?;

//         Ok(Box::into_raw(Box::new(LazyFrameContext { inner: lf })))
//     })
// }

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
    allow_missing_columns: bool, // <--- 这一点至关重要，必须为 true
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

        // 4. Build Basic Polars Scan Args
        // 这里我们先用 build_scan_args 构建基础参数
        let mut args = build_scan_args(
            n_rows, parallel_code, low_memory, use_statistics, 
            glob, allow_missing_columns, 
            row_index_name_ptr, row_index_offset, include_path_col_ptr,
            schema_ptr, // 如果用户传了 schema_ptr，args.schema 就会被设置
            hive_schema_ptr, try_parse_hive_dates,
            rechunk, cache,
            cloud_provider, cloud_retries, cloud_cache_ttl, 
            cloud_keys, cloud_values, cloud_len
        );

        // 5. Inject Delta Schema (如果是自动 Schema 模式)
        // 如果用户没有显式传入 Schema (args.schema.is_none())，
        // 我们就把从 Delta Log 里拿到的 Schema 塞进去。
        if args.schema.is_none() {
            // polars_schema 已经是 Schema 类型了，直接装箱
            args.schema = Some(Arc::new(polars_schema));
        }
        
        // 强制开启 allow_missing_columns 以支持 Schema 漂变
        // 哪怕用户传了 false，为了 Delta 的语义正确性，通常建议为 true，
        // 但为了尊重 API，我们这里保留用户的选择。
        // 不过，要在 C# 层建议用户设为 true。
        // if args.schema.is_some() {
        //    args.allow_missing_columns = true; 
        // }

        // 6. Scan
        let pl_paths: Vec<PlPath> = file_uris.iter().map(|s| PlPath::new(s)).collect();
        let buffer: Buffer<PlPath> = pl_paths.into();
        
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

// #[unsafe(no_mangle)]
// pub extern "C" fn pl_sink_delta(
//     lf_ptr: *mut LazyFrameContext,
//     path_ptr: *const c_char,
//     mode:u8,
//     // --- Cloud Options ---
//     _cloud_provider: u8,
//     _cloud_retries: usize,
//     _cloud_cache_ttl: u64,
//     cloud_keys: *const *const c_char,
//     cloud_values: *const *const c_char,
//     cloud_len: usize
// ) {
//     ffi_try_void!({
//         let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
//         let path_str = ptr_to_str(path_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

//         // Parse URL and Cloud options
//         let table_url = if let Ok(u) = Url::parse(path_str) {
//             u
//         } else {
//             let abs_path = std::fs::canonicalize(path_str)
//                 .map_err(|e| PolarsError::ComputeError(format!("Invalid local path '{}': {}", path_str, e).into()))?;
//             Url::from_directory_path(abs_path)
//                 .map_err(|_| PolarsError::ComputeError(format!("Could not convert path '{}' to URL", path_str).into()))?
//         };

//         let mut delta_storage_options = HashMap::new();
//         if !cloud_keys.is_null() && !cloud_values.is_null() && cloud_len > 0 {
//             let keys_slice = unsafe { std::slice::from_raw_parts(cloud_keys, cloud_len) };
//             let vals_slice = unsafe { std::slice::from_raw_parts(cloud_values, cloud_len) };
//             for i in 0..cloud_len {
//                 let k_ptr = keys_slice[i];
//                 let v_ptr = vals_slice[i];
//                 if !k_ptr.is_null() && !v_ptr.is_null() {
//                     let k = unsafe { CStr::from_ptr(k_ptr).to_string_lossy().into_owned() };
//                     let v = unsafe { CStr::from_ptr(v_ptr).to_string_lossy().into_owned() };
//                     delta_storage_options.insert(k, v);
//                 }
//             }
//         }

//         // =================================================================
//         // Polars DF -> (FFI) -> Standard RecordBatch
//         // =================================================================
        
//         // Collect LazyFrame -> DataFrame
//         let df = lf_ctx.inner.collect()?;
//         // println!("[Debug] Polars DF Height: {}", df.height());

//         // Convert DataFrame to Polars Arrow StructArray
//         let columns = df.get_columns()
//              .iter()
//              .map(|s| s.clone().rechunk_to_arrow(CompatLevel::newest()))
//              .collect::<Vec<_>>();

//         let pl_arrow_schema = df.schema().to_arrow(CompatLevel::newest());
//         // Convert Polars Field to Polars Arrow Field
//         let fields: Vec<polars_arrow::datatypes::Field> = pl_arrow_schema.iter_values().cloned().collect();

//         let pl_struct_array = polars_arrow::array::StructArray::new(
//             polars_arrow::datatypes::ArrowDataType::Struct(fields.clone()),
//             df.height(),
//             columns,
//             None
//         );

//         // Export FFI (Polars)
//         // Export StructArray and its Schema
//         let pl_root_field = polars_arrow::datatypes::Field::new(
//             "root".into(), 
//             polars_arrow::datatypes::ArrowDataType::Struct(fields), 
//             false
//         );
        
//         // Export: Polars FFI Struct
//         // export_array_to_c return FFI_ArrowArray
//         let pl_ffi_array = p_ffi::export_array_to_c(Box::new(pl_struct_array));
//         // export_field_to_c return FFI_ArrowSchema
//         let pl_ffi_schema = p_ffi::export_field_to_c(&pl_root_field);

//         // Transmute value type
//         // Convert polars_arrow::ffi::FFI_ArrowArray into arrow::ffi::FFI_ArrowArray
//         let arrow_ffi_array: a_ffi::FFI_ArrowArray = unsafe { std::mem::transmute(pl_ffi_array) };
//         let arrow_ffi_schema: a_ffi::FFI_ArrowSchema = unsafe { std::mem::transmute(pl_ffi_schema)};

//         // Import as DeltaLake Arrow Array
//         // from_ffi : array (transfer ownership), schema (transfer reference)
//         let import_data = unsafe {
//             a_ffi::from_ffi(arrow_ffi_array, &arrow_ffi_schema)
//                 .map_err(|e| PolarsError::ComputeError(format!("FFI Import failed: {}", e).into()))?
//         };

//         let std_struct_array = StructArray::from(import_data);

//         // Convert to RecordBatch
//         let record_batch = RecordBatch::from(&std_struct_array);
//         // println!("[Debug] Converted Arrow RecordBatch Rows: {}", record_batch.num_rows());
//         // =================================================================
//         // Write to Delta Table
//         // =================================================================

//         // Parse save mode
//         let save_mode = map_savemode(mode);

//         // Create Tokio runtime
//         let rt = get_runtime();
//         rt.block_on(async {
//             // Initiate DeltaTable instance
//             let mut table = DeltaTable::try_from_url_with_storage_options(table_url.clone(), delta_storage_options)
//                 .await
//                 .map_err(|e| PolarsError::ComputeError(format!("Failed to init DeltaTable: {}", e).into()))?;

//             // B. 尝试加载 Metadata
//             if let Err(_) = table.load().await {
//                 // 表不存在 -> 需要执行 Create 操作
                
//                 // 1. 获取 Arrow Schema
//                 let arrow_schema = record_batch.schema();
                
//                 // 2. 手动转换为 Delta Fields (Vec<StructField>)
//                 // 既然没有直接的 TryFrom，我们手动 map
//                 let delta_fields = arrow_schema.fields().iter()
//                     .map(|f| {
//                         // 调用我们手写的转换函数
//                         let dtype = arrow_to_delta_type(f.data_type())?;
//                         Ok(StructField::new(f.name().clone(), dtype, f.is_nullable()))
//                     })
//                     .collect::<Result<Vec<StructField>, PolarsError>>()?;

//                 // 3. 构建 Delta Schema (StructType)
//                 // 使用你提供的 try_new API
//                 let delta_schema = StructType::try_new(delta_fields)
//                      .map_err(|e| PolarsError::ComputeError(format!("Failed to create Delta schema: {}", e).into()))?;

//                 // 3. 执行 Create，并传入列定义
//                 // 关键修正：将创建后的新表实例赋值给 table 变量
//                 table = table.create()
//                     .with_columns(delta_schema.fields().cloned()) // 传入列定义
//                     .await
//                     .map_err(|e| PolarsError::ComputeError(format!("Failed to create Delta Table: {}", e).into()))?;
//             }
//             // println!("[Debug] Writing to table: {}", table_url);
//             // println!("[Debug] Storage Options: {:?}", delta_storage_options.clone());
//             // Table.write
//             table.write(vec![record_batch])
//                 .with_save_mode(save_mode)
//                 .await
//                 .map_err(|e| PolarsError::ComputeError(format!("Delta write failed: {}", e).into()))?;
//             // println!("[Debug] Write operation finished.");
//             Ok::<(), PolarsError>(())
//         })?;
//         Ok(())
//     })
// }

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
    // Sink Options
    maintain_order: bool,
    // Cloud Options
    cloud_provider: u8, // Polars sink_parquet 可能会用到，这里暂时透传
    cloud_retries: usize,
    cloud_cache_ttl: u64,
    cloud_keys: *const *const c_char,
    cloud_values: *const *const c_char,
    cloud_len: usize
) {
    ffi_try_void!({
        let mut lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let path_str = ptr_to_str(table_path_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        
        // Parse URL
        let table_url: Url = if let Ok(u) = Url::parse(path_str) {
            // Case A: 这是一个标准的 URL (如 s3://, abfss://, gs://)
            u
        } else {
            // Case B: 这是一个本地文件路径 (如 ./data/test 或 /tmp/test)
            // 必须转换为绝对路径，才能生成合法的 file:// URL
            
            // 尝试标准化路径 (解析 . 和 ..)
            // 注意：canonicalize 要求路径必须存在。如果是新建表，
            // 通常由外层 (C#) 保证 mkdir，或者我们这里容忍它不存在并手动拼接 CurrentDir。
            let abs_path = std::fs::canonicalize(path_str)
                .or_else(|_| {
                    // 如果路径不存在 (比如新建表)，尝试手动拼接当前目录
                    std::env::current_dir().map(|cwd| cwd.join(path_str))
                })
                .map_err(|e| PolarsError::ComputeError(format!("Invalid local path '{}': {}", path_str, e).into()))?;

            // 转换为 file:// URL
            // 注意：from_directory_path 要求必须是绝对路径
            Url::from_directory_path(abs_path)
                .map_err(|_| PolarsError::ComputeError(format!("Could not convert path '{}' to file URL", path_str).into()))?
        };
        // 1. 准备 Cloud Options (供 Polars 使用)
        // Polars 的 cloud options 构建逻辑 (复用你之前的 build_cloud_options)
        let cloud_options = unsafe {
            build_cloud_options(
                cloud_provider,
                cloud_retries,
                cloud_cache_ttl,
                cloud_keys,
                cloud_values,
                cloud_len
            )
        };

        // 同样也需要一个 HashMap 给 Delta Lake 用
        let mut delta_storage_options = HashMap::new();
        if !cloud_keys.is_null() && !cloud_values.is_null() && cloud_len > 0 {
            let keys_slice = unsafe { std::slice::from_raw_parts(cloud_keys, cloud_len) };
            let vals_slice = unsafe { std::slice::from_raw_parts(cloud_values, cloud_len) };
            for i in 0..cloud_len {
                let k_ptr = keys_slice[i];
                let v_ptr = vals_slice[i];
                if !k_ptr.is_null() && !v_ptr.is_null() {
                    let k = unsafe { CStr::from_ptr(k_ptr).to_string_lossy().into_owned() };
                    let v = unsafe { CStr::from_ptr(v_ptr).to_string_lossy().into_owned() };
                    delta_storage_options.insert(k, v);
                }
            }
        }

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
            _ => {} // Continue to write
        }
        // ==========================================
        // Step A: 提取 Schema (关键新增步骤)
        // ==========================================
        // 我们必须在 sink_parquet 消耗掉 LazyFrame 之前，先拿到 Schema。
        // 1. 获取 Polars Schema
        let pl_schema = lf_ctx.inner.collect_schema()
            .map_err(|e| PolarsError::ComputeError(format!("Failed to resolve schema: {}", e).into()))?;
        
        // 2. 转为 Arrow Schema
        let arrow_schema = pl_schema.to_arrow(CompatLevel::newest());
        
        // 3. 转为 Delta Schema (使用之前的 arrow_to_delta_type 辅助函数)
        let delta_fields = arrow_schema.iter_values().map(|f| {
            let dtype = polars_arrow_to_delta_type(f.dtype())?;
            Ok(StructField::new(f.name.clone(), dtype, f.is_nullable))
        }).collect::<Result<Vec<StructField>, PolarsError>>()?;

        // 这里的 delta_schema 会被 move 到 async 闭包里使用
        let delta_schema = StructType::try_new(delta_fields)
             .map_err(|e| PolarsError::ComputeError(format!("Failed to build Delta schema: {}", e).into()))?;

        // 2. 生成文件名
        // Delta 习惯命名: part-<part-idx>-<uuid>-<cluster-id>.c000.snappy.parquet
        // 这里我们简化: part-<timestamp>-<uuid>.parquet
        let file_name = format!("part-{}-{}.parquet", Utc::now().timestamp_millis(), Uuid::new_v4());
        
        // 构造完整写入路径
        // 注意：如果是 S3 路径，Polars 和 Delta 都需要正确处理 '/'
        let full_write_path = if path_str.ends_with('/') {
            format!("{}{}", path_str, file_name)
        } else {
            format!("{}/{}", path_str, file_name)
        };

        // 3. 构建 Polars Parquet Write Options
        let write_options = build_parquet_write_options(
            compression,
            compression_level,
            statistics,
            row_group_size,
            data_page_size
        ).map_err(|e| PolarsError::ComputeError(format!("Failed to build parquet options: {}", e).into()))?;
        
        // 4. 执行 Polars Sink (Streaming!)
        // 这一步是真正的重活，Polars 会流式地把数据通过网络/文件系统写出去

        // 这里的 sink_parquet 调用可能会阻塞，直到写完
        // 注意：lf_ctx.inner 此时被消耗掉
        let target = SinkTarget::Path(PlPath::new(&full_write_path));
        
        // 注意：SinkOptions 结构体构建可能随 Polars 版本不同，请参考原有的实现
        let sink_options = SinkOptions {
             maintain_order,
             ..Default::default()
        };

        lf_ctx.inner
            .sink_parquet(
                target, 
                write_options, 
                cloud_options, 
                sink_options
            )?
            // 关键：开启 Streaming
            .with_new_streaming(true) 
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
            // let file_size = reader.len().await as i64;

            // let file_meta = object_store.head(&path).await
            //     .map_err(|e| PolarsError::ComputeError(format!("Failed to get file metadata: {}", e).into()))?;
            let options = ArrowReaderOptions::default();
            let metadata = reader.get_metadata(Some(&options)).await
                .map_err(|e| PolarsError::ComputeError(format!("Failed to read Parquet Footer: {}", e).into()))?;
           
            let (num_records, stats_json) = extract_delta_stats(&metadata)?;
            
            println!("[Debug] Parquet Analysis -> Rows: {}, Size: {} bytes, Stats Length: {}", 
                num_records, file_size, stats_json.len());

            // 4. 构造 Add Action
            // let add_action = build_add_action(
            //     file_name, 
            //     file_meta.size as i64, 
            // );
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