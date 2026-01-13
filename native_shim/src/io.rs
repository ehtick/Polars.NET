use polars::prelude::*;
use polars_arrow::ffi::{self, ArrowArray, ArrowSchema, export_array_to_c, export_field_to_c};
use polars_arrow::array::StructArray;
use polars_arrow::datatypes::{ArrowDataType, Field};
use polars_core::prelude::CompatLevel;
use std::ffi::{CStr, c_void};
use std::io::BufReader;
use std::os::raw::c_char;
use std::fs::File;
use crate::types::{DataFrameContext, LazyFrameContext, SchemaContext};
use crate::utils::ptr_to_str;

// ==========================================
// Read csv
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_read_csv(
    path: *const c_char,
    schema_ptr: *mut SchemaContext,
    has_header: bool,
    separator: u8,
    skip_rows: usize,
    try_parse_dates: bool
) -> *mut DataFrameContext {
    ffi_try!({
        let p = unsafe { CStr::from_ptr(path).to_string_lossy() };
        
        // Build ParseOptions
        let parse_options = CsvParseOptions::default()
            .with_separator(separator)
            .with_try_parse_dates(try_parse_dates);
        
        // Schema Overrides
        let schema = if schema_ptr.is_null() {
            None
        } else {
            Some(unsafe { &*schema_ptr }.schema.clone())
        };

        // Build ReadOptions
        let options = CsvReadOptions::default()
            .with_has_header(has_header)
            .with_skip_rows(skip_rows)
            .with_parse_options(parse_options)
            .with_schema_overwrite(schema);

        // Execute
        // p.into_owned().into() -> String -> PathBuf
        let df = options
            .try_into_reader_with_file_path(Some(p.into_owned().into()))?
            .finish()?;

        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn pl_scan_csv(
    path: *const c_char,
    schema_ptr: *mut SchemaContext,
    has_header: bool,
    separator: u8,
    skip_rows: usize,
    try_parse_dates: bool
) -> *mut LazyFrameContext {
    ffi_try!({
        let p = unsafe { CStr::from_ptr(path).to_string_lossy() };

        // Schema Overrides
        let schema = if schema_ptr.is_null() {
            None
        } else {
            Some(unsafe { &*schema_ptr }.schema.clone())
        };
        
        let reader = LazyCsvReader::new(PlPath::new(&p))
            .with_has_header(has_header)
            .with_separator(separator)
            .with_skip_rows(skip_rows)
            .with_try_parse_dates(try_parse_dates)
            .with_dtype_overwrite(schema); 

        let inner = reader.finish()?;
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner })))
    })
}
// ==========================================
// Read Parquet
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_read_parquet(path_ptr: *const c_char) -> *mut DataFrameContext {
    ffi_try!({
        let path = ptr_to_str(path_ptr)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        let file = File::open(path)
            .map_err(|e| PolarsError::ComputeError(format!("File not found: {}", e).into()))?;

        let df = ParquetReader::new(file)
            .finish()?;

        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_scan_parquet(path_ptr: *const c_char) -> *mut LazyFrameContext {
    ffi_try!({
        let path = ptr_to_str(path_ptr)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        
        let args = ScanArgsParquet::default();
        let lf = LazyFrame::scan_parquet(PlPath::new(path), args)?;

        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: lf })))
    })
}

// ==========================================
// JSON
// ==========================================
// Read JSON (Eager)
#[unsafe(no_mangle)]
pub extern "C" fn pl_read_json(path_ptr: *const c_char) -> *mut DataFrameContext {
    ffi_try!({
        let path = ptr_to_str(path_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        let file = File::open(path).map_err(|e| PolarsError::ComputeError(format!("File not found: {}", e).into()))?;
        
        // JsonReader 需要 BufReader
        let reader = BufReader::new(file);
        let df = JsonReader::new(reader).finish()?;

        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}

// Scan NDJSON (Lazy)
#[unsafe(no_mangle)]
pub extern "C" fn pl_scan_ndjson(path_ptr: *const c_char) -> *mut LazyFrameContext {
    ffi_try!({
        let path = ptr_to_str(path_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        
        // LazyJsonLineReader 接受路径
        let lf = LazyJsonLineReader::new(PlPath::new(path)).finish()?;

        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: lf })))
    })
}
// ==========================================
// IPC
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_read_ipc(path_ptr: *const c_char) -> *mut DataFrameContext {
    ffi_try!({
        let path = ptr_to_str(path_ptr).unwrap();
        let file = File::open(path).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        
        let df = IpcReader::new(file).finish()?;
        
        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn pl_scan_ipc(path_ptr: *const c_char) -> *mut LazyFrameContext {
    ffi_try!({
        let path = ptr_to_str(path_ptr).unwrap();
        // [Polars 0.52 Change]
        // 1. ScanArgsIpc -> IpcScanOptions 
        let ipc_options = IpcScanOptions::default();
        
        // 2.  UnifiedScanArgs (Cloud, Schema, RowCount,etc)
        let unified_args = UnifiedScanArgs::default();

        // 3. call scan_ipc(path, options, unified_args)
        let lf = LazyFrame::scan_ipc(PlPath::new(path), ipc_options, unified_args)?;
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: lf })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_sink_ipc(
    lf_ptr: *mut LazyFrameContext,
    path_ptr: *const c_char
) {
    ffi_try_void!({
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let path = ptr_to_str(path_ptr).unwrap();

        // Prepare Option
        let writer_options = IpcWriterOptions::default();
        let sink_options = SinkOptions::default();

        // Build Target
        let target = SinkTarget::Path(PlPath::new(path));

        // Call sink_ipc
        // target, options, cloud_options, sink_options
        let sink_lf = lf_ctx.inner.sink_ipc(
            target, 
            writer_options, 
            None, // CloudOptions
            sink_options
        )?;

        
        let _ = sink_lf
        .with_new_streaming(true)
        .collect()?;
        
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_from_arrow_record_batch(
    c_array_ptr: *mut ffi::ArrowArray, 
    c_schema_ptr: *mut ffi::ArrowSchema
) -> *mut DataFrameContext {
    ffi_try!({
        if c_array_ptr.is_null() || c_schema_ptr.is_null() {
            return Err(PolarsError::ComputeError("Null pointer passed to pl_from_arrow".into()));
        }

        // Import Arrow Schema
        let field = unsafe { ffi::import_field_from_c(&*c_schema_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))? };
        
        // Import Array
        let arrow_array_struct = unsafe { std::ptr::read(c_array_ptr) };
        let array = unsafe { 
            ffi::import_array_from_c(arrow_array_struct, field.dtype.clone())
                .map_err(|e| PolarsError::ComputeError(e.to_string().into()))? 
        };
        
        let df = match array.as_any().downcast_ref::<StructArray>() {
            Some(struct_arr) => {
                let columns: Vec<Column> = struct_arr
                    .values()
                    .iter()
                    .zip(struct_arr.fields())
                    .map(|(arr, field)| {
                        let name = PlSmallStr::from_str(&field.name);
                        
                        Series::from_arrow(name, arr.clone())
                            .map(|s| Column::from(s)) 
                    })
                    .collect::<PolarsResult<Vec<_>>>()?;
                
                DataFrame::new(columns)?
            },
            None => {
                let name = PlSmallStr::from_str(&field.name);
                let series = Series::from_arrow(name, array)?;
                
                DataFrame::new(vec![Column::from(series)])?
            }
        };

        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}
// ==========================================
// Write Ops
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_write_csv(df_ptr: *mut DataFrameContext, path_ptr: *const c_char) {
    ffi_try_void!({
        let ctx = unsafe { &mut *df_ptr };
        let path = ptr_to_str(path_ptr)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        let mut file = File::create(path)
            .map_err(|e| PolarsError::ComputeError(format!("Could not create file: {}", e).into()))?;

        CsvWriter::new(&mut file)
            .finish(&mut ctx.df)?;
        
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_write_parquet(df_ptr: *mut DataFrameContext, path_ptr: *const c_char) {
    ffi_try_void!({
        let ctx = unsafe { &mut *df_ptr };
        let path = ptr_to_str(path_ptr)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        let file = File::create(path)
            .map_err(|e| PolarsError::ComputeError(format!("Could not create file: {}", e).into()))?;

        ParquetWriter::new(file)
            .finish(&mut ctx.df)?;
            
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_write_ipc(df_ptr: *mut DataFrameContext, path: *const c_char) {
    ffi_try_void!({
        let ctx = unsafe { &mut *df_ptr };
        let p = unsafe { CStr::from_ptr(path).to_string_lossy() };
        
        let file = File::create(p.as_ref()).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        
        IpcWriter::new(file)
            .finish(&mut ctx.df)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_write_json(df_ptr: *mut DataFrameContext, path: *const c_char) {
    ffi_try_void!({
        let ctx = unsafe { &mut *df_ptr };
        let p = unsafe { CStr::from_ptr(path).to_string_lossy() };
        
        let file = File::create(p.as_ref()).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        
        JsonWriter::new(file)
        .with_json_format(JsonFormat::Json)
        .finish(&mut ctx.df)
    })
}


// ==========================================
// Memory and Convert Ops
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_free_dataframe(ptr: *mut DataFrameContext) {
    ffi_try_void!({
        if !ptr.is_null() {
        unsafe { let _ = Box::from_raw(ptr); }
        }
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_to_arrow(
    ctx_ptr: *mut DataFrameContext, 
    out_chunk: *mut ArrowArray, 
    out_schema: *mut ArrowSchema
) {
    ffi_try_void!({
        if ctx_ptr.is_null() {
             return Err(PolarsError::ComputeError("Null pointer passed to pl_to_arrow".into()));
        }
        
        let ctx = unsafe { &mut *ctx_ptr };
        let df = &mut ctx.df;

        let columns = df.get_columns()
            .iter()
            .map(|s| s.clone().rechunk_to_arrow(CompatLevel::newest()))
            .collect::<Vec<_>>();

        let arrow_schema = df.schema().to_arrow(CompatLevel::newest());
        let fields: Vec<Field> = arrow_schema.iter_values().cloned().collect();

        let struct_array = StructArray::new(
            ArrowDataType::Struct(fields.clone()), 
            df.height(),
            columns,
            None
        );

        unsafe {
            *out_chunk = export_array_to_c(Box::new(struct_array));
            let root_field = Field::new("".into(), ArrowDataType::Struct(fields), false);
            *out_schema = export_field_to_c(&root_field);
        }
        
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_sink_parquet(
    lf_ptr: *mut LazyFrameContext,
    path_ptr: *const c_char
) {
    ffi_try_void!({
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let path_str = ptr_to_str(path_ptr).unwrap();

        let pl_path = PlPath::new(path_str);
        let target = SinkTarget::Path(pl_path.into());

        let write_options = ParquetWriteOptions::default();
        let sink_options = SinkOptions::default();

        let sink_lf = lf_ctx.inner.sink_parquet(
            target, 
            write_options, 
            None, // cloud_options
            sink_options
        )?;

        let _ = sink_lf
        .with_new_streaming(true)
        .collect()?;

        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_sink_json(
    lf_ptr: *mut LazyFrameContext,
    path_ptr: *const c_char
) {
    ffi_try_void!({
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let path_str = ptr_to_str(path_ptr).unwrap();
        let pl_path = PlPath::new(path_str);
        
        let target = SinkTarget::Path(pl_path.into());
        let writer_options = JsonWriterOptions::default();
        let sink_options = SinkOptions::default();

        let sink_lf = lf_ctx.inner.sink_json(
            target, 
            writer_options, 
            None, 
            sink_options
        )?;
        
        let _ = sink_lf
        .with_new_streaming(true)
        .collect()?;

        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_sink_csv(
    lf_ptr: *mut LazyFrameContext,
    path_ptr: *const c_char
) {
    ffi_try_void!({
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let path_str = ptr_to_str(path_ptr).unwrap();
        let pl_path = PlPath::new(path_str);
        
        let target = SinkTarget::Path(pl_path.into());
        let writer_options = CsvWriterOptions::default();
        let sink_options = SinkOptions::default();

        let sink_lf = lf_ctx.inner.sink_csv(
            target, 
            writer_options, 
            None, 
            sink_options
        )?;
        
        let _ = sink_lf
        .with_new_streaming(true)
        .collect()?;

        Ok(())
    })
}
// ==========================================
// Streaming Sink to DataBase
// ==========================================
// 1. Define Callback
type SinkCallback = extern "C" fn(
    *mut ffi::ArrowArray, 
    *mut ffi::ArrowSchema,
    *mut std::os::raw::c_char
) -> i32;

type CleanupCallback = extern "C" fn(*mut c_void);

// 2. Define Struct 
#[derive(Clone)]
struct CSharpSinkUdf {
    callback: SinkCallback,
    cleanup: CleanupCallback,
    user_data: *mut c_void, // GCHandle
}

// Send + Sync
unsafe impl Send for CSharpSinkUdf {}
unsafe impl Sync for CSharpSinkUdf {}

impl Drop for CSharpSinkUdf {
    fn drop(&mut self) {
        (self.cleanup)(self.user_data);
    }
}

impl CSharpSinkUdf {
    fn call(&self, df: DataFrame) -> PolarsResult<DataFrame> {
        // DataFrame -> StructArray (RecordBatch)
        let struct_s = df.into_struct("batch".into()).into_series();
        
        // Get Chunks (Box<dyn Array>)
        let chunks = struct_s.chunks();

        // Error Message Buffer (1KB)
        let mut error_msg_buf = [0u8; 1024]; 
        let error_ptr = error_msg_buf.as_mut_ptr() as *mut std::os::raw::c_char;

        for array_ref in chunks {
            let field = ArrowField::new("".into(), array_ref.dtype().clone(), true);
            let c_array = ffi::export_array_to_c(array_ref.clone());
            let c_schema = ffi::export_field_to_c(&field);

            // Alloc array to heap
            let ptr_array = Box::into_raw(Box::new(c_array));
            let ptr_schema = Box::into_raw(Box::new(c_schema));

            // Call C# Callback
            let status = (self.callback)(ptr_array, ptr_schema, error_ptr);

            if status != 0 {
                let msg = unsafe { CStr::from_ptr(error_ptr).to_string_lossy().into_owned() };
                return Err(PolarsError::ComputeError(format!("C# Sink Failed: {}", msg).into()));
            }
        }

        // Return empty DataFrame
        Ok(DataFrame::empty())
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_map_batches(
    lf_ptr: *mut LazyFrameContext,
    callback: SinkCallback,
    cleanup: CleanupCallback,
    user_data: *mut c_void
) -> *mut LazyFrameContext {
    ffi_try!({
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        
        // Build UDF Object
        let udf = Arc::new(CSharpSinkUdf { 
            callback, 
            cleanup, 
            user_data 
        });

        // fn map<F>(self, function: F, optimizations: AllowedOptimizations, schema: Option<Arc<dyn UdfSchema>>, name: Option<&'static str>)
        
        let new_lf = lf_ctx.inner.map(
            // 1. function
            move |df| udf.call(df),
            
            // 2. optimizations
            AllowedOptimizations::default(), 

            // 3. schema
            None,

            // 4. name
            Some("csharp_sink"), 
        );

        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_export_batches(
    df_ptr: *mut DataFrameContext,
    callback: SinkCallback,
    cleanup: CleanupCallback,
    user_data: *mut c_void
) {
    ffi_try_void!({
        let df_ctx = unsafe { &*df_ptr }; 
        let df = &df_ctx.df;

        let udf = CSharpSinkUdf { 
            callback, 
            cleanup, 
            user_data 
        };

        let struct_s = df.clone().into_struct("batch".into()).into_series();
        let chunks = struct_s.chunks();

        let mut error_msg_buf = [0u8; 1024]; 
        let error_ptr = error_msg_buf.as_mut_ptr() as *mut std::os::raw::c_char;

        for array_ref in chunks {
            let field = ArrowField::new("".into(), array_ref.dtype().clone(), true);
            let c_array = ffi::export_array_to_c(array_ref.clone());
            let c_schema = ffi::export_field_to_c(&field);

            let ptr_array = Box::into_raw(Box::new(c_array));
            let ptr_schema = Box::into_raw(Box::new(c_schema));

            let status = (udf.callback)(ptr_array, ptr_schema, error_ptr);

            if status != 0 {
                return Ok(()); 
            }
        }
        
        Ok(())
    })
}