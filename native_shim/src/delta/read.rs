use deltalake::DeltaTableBuilder;
use futures::StreamExt;
use polars::{error::{PolarsError, PolarsResult}, frame::DataFrame, prelude::{LazyFrame, PlRefPath}};
use polars_buffer::Buffer;
use polars_io::HiveOptions;
use url::Url;
use std::{ffi::c_char, sync::Arc};

use crate::delta::utils::{build_delta_storage_options_map, get_polars_schema_from_delta, get_runtime};
use crate::pl_io::parquet::parquet_utils::build_scan_args;
use crate::types::{LazyFrameContext, SchemaContext};
use crate::utils::ptr_to_str;
use crate::delta::deletion_vector::*;

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
        let (table, polars_schema, partition_cols,clean_paths, dirty_infos) = rt.block_on(async {
            let mut builder = DeltaTableBuilder::from_url(table_url).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
            if let Some(v) = version_val { builder = builder.with_version(v); }
            else if let Some(dt) = datetime_str { builder = builder.with_datestring(dt).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?; }
            
            let table = builder.with_storage_options(delta_opts).load().await.map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

            let final_schema = get_polars_schema_from_delta(&table)?;

            let snapshot = table.snapshot().map_err(|e| PolarsError::ComputeError(format!("Snapshot error: {}", e).into()))?;
            let partition_cols = snapshot.metadata().partition_columns().clone();

            // Prepare Vectors
            let mut clean = Vec::new();
            let mut dirty = Vec::new(); 

            // Get iterator
            let binding = table.clone();
            let mut stream = binding.get_active_add_actions_by_partitions(&[]);
            let binding_table_url = table.table_url().to_string();
            let table_root = binding_table_url.trim_end_matches('/'); // e.g. "s3://bucket/table"

            while let Some(item) = stream.next().await {
                // Item is DeltaResult<LogicalFileView>
                let view = item.map_err(|e| PolarsError::ComputeError(format!("Action stream error: {}", e).into()))?;
                
                //  Build full path
                let full_path = format!("{}/{}", table_root, view.path());

                // Check Deletion Vector
                if let Some(dv_descriptor) = view.deletion_vector_descriptor() {
                    // Has DV -> Dirty 
                    dirty.push((full_path, dv_descriptor));
                } else {
                    // No DV -> Clean
                    clean.push(full_path);
                }
            }
            Ok::<_, PolarsError>((table, final_schema, partition_cols,clean, dirty))
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
            args.schema = Some(Arc::new(polars_schema.clone()));
        }

        // 6. Inject Hive Partition Schema 
        if hive_schema_ptr.is_null() && !partition_cols.is_empty() {
            let mut part_schema = polars::prelude::Schema::with_capacity(partition_cols.len());
            
            // Iter Delta log partition column
            for col_name in &partition_cols {
                // Check cols dtype from Schema 
                if let Some(dtype) = polars_schema.get(col_name) {
                    part_schema.insert(col_name.into(), dtype.clone());
                }
            }
            
            // Get partition schema
            if !part_schema.is_empty() {
                args.hive_options = HiveOptions {
                    enabled: Some(true),
                    hive_start_idx: 0,
                    schema: Some(Arc::new(part_schema)), 
                    try_parse_dates: try_parse_hive_dates, 
                };
            }
        }

        // =========================================================
        // Phase 3: Build LazyFrames
        // =========================================================
        let mut lfs = Vec::new();

        // 3.1 Handle Clean Files
        if !clean_paths.is_empty() {
            let pl_paths: Vec<PlRefPath> = clean_paths.iter().map(|s| PlRefPath::new(s)).collect();
            let buffer: Buffer<PlRefPath> = pl_paths.into();
            
            let lf_clean = LazyFrame::scan_parquet_files(buffer, args.clone())?;
            lfs.push(lf_clean);
        }

        // 3.2 Handle Dirty Files
        if !dirty_infos.is_empty() {
            let object_store = table.object_store(); 
            let table_root = deltalake::Path::from(table.table_url().to_string());

            let dirty_lfs = rt.block_on(async {
                let mut processed = Vec::with_capacity(dirty_infos.len());
                
                // Iter (path, dv) tuple
                for (full_path, dv_descriptor) in dirty_infos {
                    
                    // A. Scan single file
                    let mut lf = LazyFrame::scan_parquet(
                        PlRefPath::new(&full_path), 
                        args.clone()
                    )?;

                    // B. Read and apply DV
                    let bitmap = read_deletion_vector(
                        object_store.clone(), 
                        &dv_descriptor, 
                        &table_root
                    ).await?;

                    lf = apply_deletion_vector(lf, bitmap)?;
                    
                    processed.push(lf);
                }
                Ok::<_, PolarsError>(processed)
            })?;
            
            lfs.extend(dirty_lfs);
        }

        // =========================================================
        // Phase 4: Union / Concat
        // =========================================================
        if lfs.is_empty() {
            let lf = polars::prelude::IntoLazy::lazy(DataFrame::empty_with_schema(&polars_schema));
            return Ok(Box::into_raw(Box::new(LazyFrameContext { inner: lf })));
        }

        // Concat
        if lfs.len() == 1 {
            let lf = lfs.pop().unwrap(); 
            return Ok(Box::into_raw(Box::new(LazyFrameContext { inner: lf })));
        }

        // Build UnionArgs
        let args = polars::prelude::UnionArgs {
            parallel: true,           
            rechunk: false,           
            to_supertypes: true,      
            maintain_order: false,    
            strict: false,            
            diagonal: true,           
            from_partitioned_ds: false,
        };

        let final_lf = polars::prelude::concat(lfs, args)
            .map_err(|e| PolarsError::ComputeError(format!("Concat failed: {}", e).into()))?;
        
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: final_lf })))
    })
}