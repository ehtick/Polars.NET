// ==========================================
// Delta Lake Support
// ==========================================

use deltalake::{DeltaTable, DeltaTableBuilder};
use deltalake::protocol::SaveMode;
use polars::{error::PolarsError, prelude::*};
use polars_arrow::buffer::Buffer;
use crate::{io::build_cloud_options, types::LazyFrameContext, utils::ptr_to_str};
use std::{ffi::CStr, os::raw::c_char};
use url::Url;
use std::collections::HashMap;
use polars_arrow::ffi as p_ffi; // Polars Arrow FFI
use deltalake::arrow::ffi as a_ffi; // DeltaLake Arrow FFI
use deltalake::arrow::array::{StructArray}; 
use deltalake::arrow::record_batch::RecordBatch; 
use deltalake::kernel::{StructType, StructField,DataType as DeltaDataType,ArrayType, MapType, DecimalType};
use deltalake::PrimitiveType;
use deltalake::arrow::datatypes::{DataType as ArrowDataType};

fn arrow_to_delta_type(arrow_type: &ArrowDataType) -> Result<DeltaDataType, PolarsError> {
    match arrow_type {
        // Primitive Types
        ArrowDataType::Boolean => Ok(DeltaDataType::Primitive(PrimitiveType::Boolean)),
        ArrowDataType::Int8 => Ok(DeltaDataType::Primitive(PrimitiveType::Byte)),
        ArrowDataType::Int16 => Ok(DeltaDataType::Primitive(PrimitiveType::Short)),
        ArrowDataType::Int32 => Ok(DeltaDataType::Primitive(PrimitiveType::Integer)),
        ArrowDataType::Int64 => Ok(DeltaDataType::Primitive(PrimitiveType::Long)),
        ArrowDataType::UInt8 => Ok(DeltaDataType::Primitive(PrimitiveType::Byte)),  
        ArrowDataType::UInt16 => Ok(DeltaDataType::Primitive(PrimitiveType::Short)),
        ArrowDataType::UInt32 => Ok(DeltaDataType::Primitive(PrimitiveType::Integer)),
        ArrowDataType::UInt64 => Ok(DeltaDataType::Primitive(PrimitiveType::Long)),
        ArrowDataType::Float16 => Ok(DeltaDataType::Primitive(PrimitiveType::Float)),
        ArrowDataType::Float32 => Ok(DeltaDataType::Primitive(PrimitiveType::Float)),
        ArrowDataType::Float64 => Ok(DeltaDataType::Primitive(PrimitiveType::Double)),
        ArrowDataType::Utf8 | ArrowDataType::Utf8View| ArrowDataType::LargeUtf8 => Ok(DeltaDataType::Primitive(PrimitiveType::String)),
        ArrowDataType::Binary | ArrowDataType::LargeBinary => Ok(DeltaDataType::Primitive(PrimitiveType::Binary)),
        ArrowDataType::Date32 => Ok(DeltaDataType::Primitive(PrimitiveType::Date)),
        ArrowDataType::Date64 => Ok(DeltaDataType::Primitive(PrimitiveType::Date)),
        ArrowDataType::Timestamp(_, _) => Ok(DeltaDataType::Primitive(PrimitiveType::Timestamp)), 
        ArrowDataType::Decimal128(p, s) | ArrowDataType::Decimal256(p, s) => {
             DeltaDataType::decimal(*p as u8, *s as u8)
                 .map_err(|e| PolarsError::ComputeError(format!("Invalid Decimal: {}", e).into()))
        },
        
        // 复杂类型：Struct (递归处理)
        ArrowDataType::Struct(fields) => {
            let delta_fields = fields.iter().map(|f| {
                let dtype = arrow_to_delta_type(f.data_type())?;
                Ok(StructField::new(f.name().clone(), dtype, f.is_nullable()))
            }).collect::<Result<Vec<StructField>, PolarsError>>()?;
            

            Ok(DeltaDataType::Struct(Box::new(StructType::try_new(delta_fields).map_err(|e| PolarsError::ComputeError(format!("Struct error: {}", e).into()))?)))
        },

        // 复杂类型：List
        ArrowDataType::List(field) | ArrowDataType::LargeList(field) => {
             let inner_type = arrow_to_delta_type(field.data_type())?;
             // Delta ArrayType 包含 element_type 和 contains_null
             Ok(DeltaDataType::Array(Box::new(deltalake::kernel::ArrayType::new(inner_type, field.is_nullable()))))
        },

        _ => Err(PolarsError::ComputeError(format!("Unsupported Arrow data type for Delta Lake: {:?}", arrow_type).into())),
    }
}



#[unsafe(no_mangle)]
pub extern "C" fn pl_scan_delta(
    path_ptr: *const c_char,
    // --- Time Travel Args ---
    version: *const i64,          // null for None
    datetime_ptr: *const c_char,  // null for None
    // --- Cloud Options ---
    cloud_provider: u8,
    cloud_retries: usize,
    cloud_cache_ttl: u64,
    cloud_keys: *const *const c_char,
    cloud_values: *const *const c_char,
    cloud_len: usize
) -> *mut LazyFrameContext {
    ffi_try!({
        let path_str = ptr_to_str(path_ptr)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        let version_val = if version.is_null() { None } else { unsafe { Some(*version) } };
        let datetime_str = if datetime_ptr.is_null() {
            None
        } else {
            Some(
                ptr_to_str(datetime_ptr)
                    .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?
            )
        };

        // URL Parsing
        let table_url = if let Ok(u) = Url::parse(path_str) {
            u
        } else {
            let abs_path = std::fs::canonicalize(path_str)
                .map_err(|e| PolarsError::ComputeError(format!("Invalid local path '{}': {}", path_str, e).into()))?;
            Url::from_directory_path(abs_path)
                .map_err(|_| PolarsError::ComputeError(format!("Could not convert path '{}' to URL", path_str).into()))?
        };

        // =================================================================
        // Prepare Delta Lake Storage Options
        // =================================================================
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

        // =================================================================
        // Parse Delta Log
        // =================================================================
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| PolarsError::ComputeError(format!("Failed to create Tokio runtime: {}", e).into()))?;

        let file_uris = rt.block_on(async {
            // Create Builder: from_url
            let mut builder = DeltaTableBuilder::from_url(table_url)
                .map_err(|e| PolarsError::ComputeError(format!("Failed to create DeltaTableBuilder: {}", e).into()))?;
            
            // Time Travel
            if let Some(v) = version_val {
                builder = builder.with_version(v);
            } else if let Some(dt) = datetime_str {
                // with_datestring accept ISO-8601 string
                builder = builder.with_datestring(dt)
                    .map_err(|e| PolarsError::ComputeError(format!("Invalid datetime format: {}", e).into()))?;
            }

            // Set Options and Load
            let table = builder
                .with_storage_options(delta_storage_options) 
                .load()
                .await
                .map_err(|e| PolarsError::ComputeError(format!("Failed to load Delta table: {}", e).into()))?;
            
            // Get File List
            let files_iter = table.get_file_uris()
                .map_err(|e| PolarsError::ComputeError(format!("Failed to get file URIs: {}", e).into()))?;

            let uris: Vec<String> = files_iter.map(|s| s.to_string()).collect();
            Ok::<Vec<String>, PolarsError>(uris)
        })?;

        // if file_uris.is_empty() {
        //      return Err(PolarsError::ComputeError("Delta table contains no files.".into()));
        // }

        // =================================================================
        // Prepare Polars Scan Args
        // =================================================================
        
        // Build Buffer<PlPath>
        let pl_paths: Vec<PlPath> = file_uris.iter()
            .map(|s| PlPath::new(s)) 
            .collect();
        let buffer: Buffer<PlPath> = pl_paths.into();

        // Build ScanArgsParquet and CloudOptions
        let mut args = ScanArgsParquet::default();
        
        args.cloud_options = unsafe {
            build_cloud_options(
                cloud_provider,
                cloud_retries,
                cloud_cache_ttl,
                cloud_keys,
                cloud_values,
                cloud_len
            )
        };

        // =================================================================
        // Scan
        // =================================================================
        let lf = LazyFrame::scan_parquet_files(buffer, args)?;

        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: lf })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_sink_delta(
    lf_ptr: *mut LazyFrameContext,
    path_ptr: *const c_char,
    mode:u8,
    // --- Cloud Options ---
    _cloud_provider: u8,
    _cloud_retries: usize,
    _cloud_cache_ttl: u64,
    cloud_keys: *const *const c_char,
    cloud_values: *const *const c_char,
    cloud_len: usize
) {
    ffi_try_void!({
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let path_str = ptr_to_str(path_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        // Parse URL and Cloud options
        let table_url = if let Ok(u) = Url::parse(path_str) {
            u
        } else {
            let abs_path = std::fs::canonicalize(path_str)
                .map_err(|e| PolarsError::ComputeError(format!("Invalid local path '{}': {}", path_str, e).into()))?;
            Url::from_directory_path(abs_path)
                .map_err(|_| PolarsError::ComputeError(format!("Could not convert path '{}' to URL", path_str).into()))?
        };

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

        // =================================================================
        // Polars DF -> (FFI) -> Standard RecordBatch
        // =================================================================
        
        // Collect LazyFrame -> DataFrame
        let df = lf_ctx.inner.collect()?;
        // println!("[Debug] Polars DF Height: {}", df.height());

        // Convert DataFrame to Polars Arrow StructArray
        let columns = df.get_columns()
             .iter()
             .map(|s| s.clone().rechunk_to_arrow(CompatLevel::newest()))
             .collect::<Vec<_>>();

        let pl_arrow_schema = df.schema().to_arrow(CompatLevel::newest());
        // Convert Polars Field to Polars Arrow Field
        let fields: Vec<polars_arrow::datatypes::Field> = pl_arrow_schema.iter_values().cloned().collect();

        let pl_struct_array = polars_arrow::array::StructArray::new(
            polars_arrow::datatypes::ArrowDataType::Struct(fields.clone()),
            df.height(),
            columns,
            None
        );

        // Export FFI (Polars)
        // Export StructArray and its Schema
        let pl_root_field = polars_arrow::datatypes::Field::new(
            "root".into(), 
            polars_arrow::datatypes::ArrowDataType::Struct(fields), 
            false
        );
        
        // Export: Polars FFI Struct
        // export_array_to_c return FFI_ArrowArray
        let pl_ffi_array = p_ffi::export_array_to_c(Box::new(pl_struct_array));
        // export_field_to_c return FFI_ArrowSchema
        let pl_ffi_schema = p_ffi::export_field_to_c(&pl_root_field);

        // Transmute value type
        // Convert polars_arrow::ffi::FFI_ArrowArray into arrow::ffi::FFI_ArrowArray
        let arrow_ffi_array: a_ffi::FFI_ArrowArray = unsafe { std::mem::transmute(pl_ffi_array) };
        let arrow_ffi_schema: a_ffi::FFI_ArrowSchema = unsafe { std::mem::transmute(pl_ffi_schema)};

        // Import as DeltaLake Arrow Array
        // from_ffi : array (transfer ownership), schema (transfer reference)
        let import_data = unsafe {
            a_ffi::from_ffi(arrow_ffi_array, &arrow_ffi_schema)
                .map_err(|e| PolarsError::ComputeError(format!("FFI Import failed: {}", e).into()))?
        };

        let std_struct_array = StructArray::from(import_data);

        // Convert to RecordBatch
        let record_batch = RecordBatch::from(&std_struct_array);
        // println!("[Debug] Converted Arrow RecordBatch Rows: {}", record_batch.num_rows());
        // =================================================================
        // Write to Delta Table
        // =================================================================

        // Parse save mode
        let save_mode = match mode {
            0 => SaveMode::Append,
            1 => SaveMode::Overwrite,
            2 => SaveMode::ErrorIfExists,
            3 => SaveMode::Ignore,
            _ => return Err(PolarsError::ComputeError(format!("Unknown save mode code: {}", mode).into())),
        };

        // Create Tokio runtime
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| PolarsError::ComputeError(format!("Failed to create Tokio runtime: {}", e).into()))?;

        rt.block_on(async {
            // Initiate DeltaTable instance
            let mut table = DeltaTable::try_from_url_with_storage_options(table_url.clone(), delta_storage_options)
                .await
                .map_err(|e| PolarsError::ComputeError(format!("Failed to init DeltaTable: {}", e).into()))?;

            // B. 尝试加载 Metadata
            if let Err(_) = table.load().await {
                // 表不存在 -> 需要执行 Create 操作
                
                // 1. 获取 Arrow Schema
                let arrow_schema = record_batch.schema();
                
                // 2. 手动转换为 Delta Fields (Vec<StructField>)
                // 既然没有直接的 TryFrom，我们手动 map
                let delta_fields = arrow_schema.fields().iter()
                    .map(|f| {
                        // 调用我们手写的转换函数
                        let dtype = arrow_to_delta_type(f.data_type())?;
                        Ok(StructField::new(f.name().clone(), dtype, f.is_nullable()))
                    })
                    .collect::<Result<Vec<StructField>, PolarsError>>()?;

                // 3. 构建 Delta Schema (StructType)
                // 使用你提供的 try_new API
                let delta_schema = StructType::try_new(delta_fields)
                     .map_err(|e| PolarsError::ComputeError(format!("Failed to create Delta schema: {}", e).into()))?;

                // 3. 执行 Create，并传入列定义
                // 关键修正：将创建后的新表实例赋值给 table 变量
                table = table.create()
                    .with_columns(delta_schema.fields().cloned()) // 传入列定义
                    .await
                    .map_err(|e| PolarsError::ComputeError(format!("Failed to create Delta Table: {}", e).into()))?;
            }
            // println!("[Debug] Writing to table: {}", table_url);
            // println!("[Debug] Storage Options: {:?}", delta_storage_options.clone());
            // Table.write
            table.write(vec![record_batch])
                .with_save_mode(save_mode)
                .await
                .map_err(|e| PolarsError::ComputeError(format!("Delta write failed: {}", e).into()))?;
            // println!("[Debug] Write operation finished.");
            Ok::<(), PolarsError>(())
        })?;
        Ok(())
    })
}