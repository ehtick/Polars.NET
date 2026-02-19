use polars::prelude::*;
use std::sync::Arc;
use deltalake::kernel::{DeletionVectorDescriptor, StorageType};
use deltalake::{ObjectStore, Path};
use polars::error::{PolarsError, PolarsResult};
use roaring::RoaringBitmap;
use uuid::Uuid;

/// 读取并解析 Deletion Vector
/// 
/// 适配 Delta Lake Protocol 的三种存储模式：
/// - Inline (i): Z85 编码存储在 Log 中
/// - UuidRelativePath (u): 基于 UUID 派生的文件名，存储在 Table Root 下
/// - AbsolutePath (p): 绝对路径 (极少见)
pub async fn read_deletion_vector(
    object_store: Arc<dyn ObjectStore>,
    descriptor: &DeletionVectorDescriptor,
    table_root: &Path,
) -> PolarsResult<RoaringBitmap> {
    
    match descriptor.storage_type {
        // =========================================================
        // Case 1: Inline (内联模式) 'i'
        // <base85 encoded bytes>
        // =========================================================
        StorageType::Inline => {
            // 1. Z85 Decode (注意：不是 Base64)
            let raw_bytes = z85::decode(&descriptor.path_or_inline_dv)
                .map_err(|e| PolarsError::ComputeError(format!("Failed to Z85 decode inline DV: {}", e).into()))?;

            // 2. Deserialize
            let bitmap = RoaringBitmap::deserialize_from(&raw_bytes[..])
                .map_err(|e| PolarsError::ComputeError(format!("Failed to deserialize inline DV: {}", e).into()))?;

            Ok(bitmap)
        },

        // =========================================================
        // Case 2: UUID Relative Path 'u'
        // <random prefix><base85 encoded uuid>
        // =========================================================
        StorageType::UuidRelativePath => {
            // 1. 重建文件名 (Reconstruct Filename)
            // 逻辑：取字符串的最后 20 个字符 -> Z85 解码 -> 16 字节 -> UUID
            // 文件名格式标准：deletion_vector_{uuid}.bin
            
            let encoded_str = &descriptor.path_or_inline_dv;
            if encoded_str.len() < 20 {
                 return Err(PolarsError::ComputeError(
                    format!("Invalid DV format 'u': string length too short '{}'", encoded_str).into()
                ));
            }

            // 取最后 20 个字符 (Base85 UUID 部分)
            let uuid_part = &encoded_str[encoded_str.len() - 20..];
            
            // 解码为 UUID Bytes
            let uuid_bytes = z85::decode(uuid_part)
                .map_err(|e| PolarsError::ComputeError(format!("Failed to Z85 decode DV UUID: {}", e).into()))?;
            
            // 构建 UUID 对象
            let uuid = Uuid::from_slice(&uuid_bytes)
                .map_err(|e| PolarsError::ComputeError(format!("Invalid DV UUID bytes: {}", e).into()))?;

            // 拼接文件名
            let file_name = format!("deletion_vector_{}.bin", uuid);
            
            // 拼接完整路径: table_root + file_name
            let file_path = table_root.child(file_name);

            // 2. 计算读取范围
            let offset = descriptor.offset.unwrap_or(0) as u64;
            let size = descriptor.size_in_bytes as u64;
            let range = offset..(offset + size);

            // 3. 异步读取
            let raw_bytes = object_store.get_range(&file_path, range).await
                .map_err(|e| PolarsError::ComputeError(format!("Failed to read DV file '{}': {}", file_path, e).into()))?;

            // 4. Deserialize
            let bitmap = RoaringBitmap::deserialize_from(&raw_bytes[..])
                .map_err(|e| PolarsError::ComputeError(format!("Failed to deserialize file DV: {}", e).into()))?;

            Ok(bitmap)
        },

        // =========================================================
        // Case 3: Absolute Path 'p'
        // <absolute path>
        // =========================================================
        StorageType::AbsolutePath => {
            // 直接使用路径
            let file_path = Path::from(descriptor.path_or_inline_dv.as_str());
            
            let offset = descriptor.offset.unwrap_or(0) as u64;
            let size = descriptor.size_in_bytes as u64;
            let range = offset..(offset + size);

            let raw_bytes = object_store.get_range(&file_path, range).await
                .map_err(|e| PolarsError::ComputeError(format!("Failed to read absolute DV file: {}", e).into()))?;

            let bitmap = RoaringBitmap::deserialize_from(&raw_bytes[..])
                .map_err(|e| PolarsError::ComputeError(format!("Failed to deserialize absolute DV: {}", e).into()))?;

            Ok(bitmap)
        }
    }
}

/// 将 Deletion Vector 应用到 LazyFrame
///
/// 逻辑：
/// 1. 将 RoaringBitmap 转为 Polars Series (UInt32)。
/// 2. 给 LazyFrame 加上临时行号列。
/// 3. 过滤掉行号存在于 Bitmap 中的行。
/// 4. 删除临时行号列。
pub fn apply_deletion_vector(
    lf: LazyFrame,
    bitmap: RoaringBitmap
) -> PolarsResult<LazyFrame> {
    
    // 0. 快速路径：如果没有行被删除，直接返回原表
    // 这是一个非常高频的场景（大部分文件没有 DV，或者 DV 是空的）
    if bitmap.is_empty() {
        return Ok(lf);
    }

    // 1. 将 RoaringBitmap 转换为 Vec<u32>
    // RoaringBitmap 的迭代器产生 u32，直接 collect 即可
    let deleted_indices: Vec<u32> = bitmap.iter().collect();

    // 2. 构建 Polars Series
    // 我们将其包装为 Literal Expr 用于过滤
    let del_series = Series::new("deleted_idx".into(), deleted_indices);

    // 3. 构建计算图
    // Delta Lake 的行号是从 0 开始的 UInt32
    let lf_filtered = lf
        .with_row_index("row_nr", None) 
        .filter(
            // 逻辑：保留那些 [行号] 不在 [删除列表] 里的行
            col("row_nr").is_in(lit(del_series).implode(),false).not()
        )
        // 4. 清理临时列
        .drop(Selector::ByName {
            // from_static 用于处理 "row_nr" 这种硬编码字符串，效率更高
            names: Arc::from([PlSmallStr::from_static("row_nr")]),
            strict: true // 必须为 true，因为这列是我们刚生成的，必须存在
        });

    Ok(lf_filtered)
}