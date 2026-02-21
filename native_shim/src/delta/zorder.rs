use polars::prelude::*;
use polars_arrow::array::{BinaryArray, PrimitiveArray};
use polars_arrow::array::Array;
use polars_arrow::offset::OffsetsBuffer;
use polars_buffer::Buffer;

/// Z-Order Interleave calculator
/// Input：UInt32 Series (Rank Value)
/// Output Binary Series (Z-Address)
pub fn interleave_columns(columns: &[Series]) -> PolarsResult<Series> {
    if columns.is_empty() {
        return Err(PolarsError::ComputeError("Z-Order requires at least one column".into()));
    }

    let row_count = columns[0].len();
    let num_cols = columns.len();
    
    // Convert Series to Arrow Slice
    // Step A: Transfer Ownership
    let mut _guards: Vec<Box<dyn Array>> = Vec::with_capacity(num_cols);
    
    for s in columns {
        if s.null_count() > 0 {
             return Err(PolarsError::ComputeError(
                format!("Z-Order column '{}' contains nulls. Please fill nulls before interleaving.", s.name()).into()
            ));
        }

        if s.dtype() != &DataType::UInt32 {
            return Err(PolarsError::ComputeError(
                format!("Z-Order input must be UInt32, got {:?}", s.dtype()).into()
            ));
        }
        
        // Rechunk to ensure continous memory
        let s_contiguous = s.rechunk();
        
        // Convert to Arrow Array (Box<dyn Array>)
        let arr = s_contiguous.to_arrow(0, CompatLevel::newest());
        
        // Push into guards
        _guards.push(arr);
    }

    // Step B: Get ref (Borrower)
    let mut column_slices: Vec<&[u32]> = Vec::with_capacity(num_cols);

    for arr in &_guards {
        let primitive_arr = arr.as_any()
            .downcast_ref::<PrimitiveArray<u32>>()
            .ok_or_else(|| PolarsError::ComputeError("Failed to downcast to UInt32 array".into()))?;

        column_slices.push(primitive_arr.values());
    }

    // Calculate output size
    // (num_cols * 32) / 8 = num_cols * 4。
    let bytes_per_row = num_cols * 4;
    let total_bytes = row_count * bytes_per_row;

    let mut value_buffer: Vec<u8> = vec![0u8; total_bytes]; 
    
    // Calculate Offsets
    let mut offsets: Vec<i64> = Vec::with_capacity(row_count + 1);
    let mut current_offset = 0;
    for _ in 0..=row_count {
        offsets.push(current_offset);
        current_offset += bytes_per_row as i64;
    }

    // Bitwise Calculation
    const LANES: usize = 8;
    
    let mut i = 0;
    let dst_ptr_base = value_buffer.as_mut_ptr();

    // -- Main Loop --
    while i + LANES <= row_count {
        // Set 8 accumlator
        let mut acc = [0u8; LANES];
        let mut bits_filled = 0;

        // Calc 8 lanes write start ptr
        let mut dst_ptrs = [std::ptr::null_mut::<u8>(); LANES];
        for k in 0..LANES {
            // Safety: Pre malloc total_bytes and i+k < row_count
            unsafe {
                dst_ptrs[k] = dst_ptr_base.add((i + k) * bytes_per_row);
            }
        }

        // Iter Bit (31..0)
        for bit_idx in (0..32).rev() {
            // Iter Col
            for col_idx in 0..num_cols {
                let col_data = column_slices[col_idx];
                
                // Unroll: get data of 8 lanes
                let vals = [
                    col_data[i], col_data[i+1], col_data[i+2], col_data[i+3],
                    col_data[i+4], col_data[i+5], col_data[i+6], col_data[i+7]
                ];

                // Get bit and accumlate
                for k in 0..LANES {
                    let bit = (vals[k] >> bit_idx) & 1;
                    acc[k] = (acc[k] << 1) | (bit as u8);
                }
                
                bits_filled += 1;

                // Filled 8 bits then write into memory
                if bits_filled == 8 {
                    for k in 0..LANES {
                        unsafe {
                            *dst_ptrs[k] = acc[k]; // write byte
                            dst_ptrs[k] = dst_ptrs[k].add(1); // shift ptr
                            acc[k] = 0; // reset accumlator
                        }
                    }
                    bits_filled = 0;
                }
            }
        }
        i += LANES;
    }

    // -- Residue Loops --
    while i < row_count {
        let mut byte_accumulator: u8 = 0;
        let mut bits_filled = 0;
        
        // Calc destination ptr for current lane
        let mut current_dst_ptr = unsafe { dst_ptr_base.add(i * bytes_per_row) };

        for bit_idx in (0..32).rev() {
            for col_idx in 0..num_cols {
                let val = column_slices[col_idx][i];
                let bit = (val >> bit_idx) & 1;
                byte_accumulator = (byte_accumulator << 1) | (bit as u8);
                bits_filled += 1;

                if bits_filled == 8 {
                    unsafe {
                        *current_dst_ptr = byte_accumulator;
                        current_dst_ptr = current_dst_ptr.add(1);
                    }
                    byte_accumulator = 0;
                    bits_filled = 0;
                }
            }
        }
        i += 1;
    }

    // Build Polars Binary Series
    let arrow_array = unsafe {
        // Step A: Vec<u8> -> Buffer<u8>
        let values_buf = Buffer::from(value_buffer);

        // Step B: Vec<i64> -> Buffer<i64> -> OffsetsBuffer<i64>
        let offsets_buf = Buffer::from(offsets);
        let offsets_arrow = OffsetsBuffer::new_unchecked(offsets_buf);

        // Step C: Build BinaryArray
        BinaryArray::<i64>::new_unchecked(
            DataType::Binary.to_arrow(CompatLevel::newest()), 
            offsets_arrow, 
            values_buf,    
            None,          // No nulls
        )
    };

    // Series::from_arrow
    let series = Series::from_arrow("z_address".into(), Box::new(arrow_array))?;
    Ok(series)
}

// ------------------------------------------------------------------
// Helper: Apply Z-Order to LazyFrame
// ------------------------------------------------------------------

/// Apply Z-Order to LazyFrame 
/// columns: The column names need Z-Order 
pub fn apply_z_order(lf: LazyFrame, columns: &[String]) -> PolarsResult<LazyFrame> {
    if columns.is_empty() {
        return Ok(lf);
    }

    let cols_owned = columns.to_vec();
    
    let z_order_map_func = move |c: Column| {
        // Column -> Series
        let s = c.as_materialized_series();

        // Unpack Struct
        let ca = s.struct_()?;
        
        // Get Field (Vec<Series>)
        let fields = ca.fields_as_series(); 
        
        // Calculate interleave
        let result_series = interleave_columns(&fields)?;
        
        // Series -> Column
        Ok(Column::from(result_series))
    };

    // 2. Output Type Function: (&Schema, &Field) -> PolarsResult<Field>
    let z_order_output_type_func = move |_schema: &Schema, input_field: &Field| {
        Ok(Field::new(input_field.name().clone(), DataType::Binary))
    };

    let z_col_name = "__z_order_addr";
    let z_col_pl_str = PlSmallStr::from_static(z_col_name);
    
    // Build Rank Expr
    let rank_exprs: Vec<Expr> = cols_owned.iter()
        .map(|name| {
            col(name)
                .rank(RankOptions {
                    method: RankMethod::Dense,
                    descending: false,
                }, None)
                .fill_null(0)
                .cast(DataType::UInt32)
                .alias(name)
        })
        .collect();

    // Build e2e procedure
    let lf_sorted = lf
        .with_column(
            as_struct(rank_exprs)
                .map(
                    z_order_map_func, 
                    z_order_output_type_func 
                )
                .alias(z_col_name)
        )
        .sort(
            [z_col_pl_str.clone()], 
            SortMultipleOptions::default()
        )
        .drop(Selector::ByName { 
            names: Arc::from([PlSmallStr::from_static(z_col_name)]), 
            strict: true 
        });

    Ok(lf_sorted)
}