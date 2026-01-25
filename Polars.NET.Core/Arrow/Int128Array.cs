// using System.Runtime.CompilerServices;
// using Apache.Arrow;
// using Apache.Arrow.Memory;
// using Apache.Arrow.Types;

// namespace Polars.NET.Core.Arrow;

// public static partial class ArrowExtensions
// {
//     /// <summary>
//     /// Custom Int128Array implementation aligned with Polars/Rust Int128.
//     /// Physical Layout: 16-byte contiguous buffer (Little Endian).
//     /// </summary>
//     public class Int128Array : PrimitiveArray<Int128>
//     {
//         /// <summary>
//         /// 构造函数
//         /// </summary>
//         /// <param name="data">底层的 ArrayData（包含 ValidityBitmap 和 ValueBuffer）</param>
//         public Int128Array(ArrayData data) 
//             : base(data)
//         {
//             if (data.Buffers[1].Length % Unsafe.SizeOf<Int128>() != 0)
//             {
//                 throw new ArgumentException($"Value buffer length must be a multiple of {Unsafe.SizeOf<Int128>()} for Int128Array.");
//             }
//         }

//         public override void Accept(IArrowArrayVisitor visitor)
//         {
//             throw new NotSupportedException(
//                 "Int128Array is a custom extension and does not support standard Arrow visitors. " +
//                 "Please access .Values property directly.");
//         }
//         public class Builder : IArrowArrayBuilder<Int128, Int128Array, Builder>
//         {
//             // 1. 核心存储：值缓冲区 + 有效性位图
//             // 使用 ArrowBuffer.Builder<T> 来管理内存扩容
//             private readonly ArrowBuffer.Builder<Int128> _valueBuilder;
//             private readonly ArrowBuffer.BitmapBuilder _validityBuilder;

//             public int Length => _valueBuilder.Length;
//             public int Capacity => _valueBuilder.Capacity;
            
//             // 构造函数
//             public Builder()
//             {
//                 _valueBuilder = new ArrowBuffer.Builder<Int128>();
//                 _validityBuilder = new ArrowBuffer.BitmapBuilder();
//             }

//             // -----------------------------------------------------------------
//             // IArrowArrayBuilder 接口实现
//             // -----------------------------------------------------------------

//             /// <summary>
//             // 预分配容量，减少扩容开销
//             /// </summary>
//             public Builder Reserve(int capacity)
//             {
//                 _valueBuilder.Reserve(capacity);
//                 _validityBuilder.Reserve(capacity);
//                 return this;
//             }

//             /// <summary>
//             /// 调整大小（通常用于截断或预设）
//             /// </summary>
//             public Builder Resize(int length)
//             {
//                 _valueBuilder.Resize(length);
//                 _validityBuilder.Resize(length);
//                 return this;
//             }

//             /// <summary>
//             /// 清空 Builder
//             /// </summary>
//             public Builder Clear()
//             {
//                 _valueBuilder.Clear();
//                 _validityBuilder.Clear();
//                 return this;
//             }

//             /// <summary>
//             /// 追加一个空值 (Null)
//             /// </summary>
//             public Builder AppendNull()
//             {
//                 _valueBuilder.Append(Int128.Zero); // 填充 0
//                 _validityBuilder.Append(false); // 标记为无效
//                 return this;
//             }

//             /// <summary>
//             /// 追加一个具体值 (Int128)
//             /// </summary>
//             public Builder Append(Int128 value)
//             {
//                 _valueBuilder.Append(value);
//                 _validityBuilder.Append(true);
//                 return this;
//             }

//             /// <summary>
//             /// 批量追加 (IEnumerable) - 接口要求
//             /// </summary>
//             public Builder AppendRange(IEnumerable<Int128> values)
//             {
//                 foreach (var v in values)
//                 {
//                     Append(v);
//                 }
//                 return this;
//             }
            
//             // -----------------------------------------------------------------
//             // ★ 高性能扩展 API (Zero-Copy / SIMD Integration)
//             // -----------------------------------------------------------------

//             /// <summary>
//             /// [Turbo] 批量追加 Span (内存拷贝)
//             /// 既然我们自己实现了 Builder，就一定要加这个方法，
//             /// 它可以直接把 Int128[] 拷贝到底层 Buffer，比循环 Append 快得多。
//             /// </summary>
//             public Builder AppendSpan(ReadOnlySpan<Int128> values)
//             {
//                 int len = values.Length;
                
//                 // 1. 批量设置 Validity 为 true
//                 _validityBuilder.AppendRange(true, len);
                
//                 // 2. 批量拷贝 Values
//                 _valueBuilder.Append(values);
                
//                 return this;
//             }

//             /// <summary>
//             /// [Turbo] 直接追加 Decimal (集成 DecimalPacker)
//             /// 这里连接了你之前的 DecimalPacker 逻辑！
//             /// </summary>
//             public unsafe Builder AppendDecimalRange(decimal[] values)
//             {

//                 int len = values.Length;
                
//                 // 1. 预分配 Bitmap (Validity)
//                 _validityBuilder.AppendRange(true, len);
                
//                 _valueBuilder.Reserve(_valueBuilder.Length + len);

//                 // 直接循环转换，这是最安全且语义正确的方式
//                 for (int i = 0; i < len; i++)
//                 {
//                     _valueBuilder.Append((Int128)values[i]);
//                 }

//                 return this;
//             }

//             // -----------------------------------------------------------------
//             // Build: 产出成品 Array
//             // -----------------------------------------------------------------

//             /// <summary>
//             /// 构建 Int128Array
//             /// </summary>
//             public Int128Array Build(MemoryAllocator allocator = null!)
//             {
//                 // 1. 拿出构建好的 Buffer
//                 ArrowBuffer valueBuffer = _valueBuilder.Build(allocator);
//                 ArrowBuffer validityBuffer = _validityBuilder.Build(allocator);

//                 int length = valueBuffer.Length / Unsafe.SizeOf<Int128>();
//                 int nullCount = length - _validityBuilder.SetBitCount;

//                 // 2. 构造 ArrayData
//                 // 关键点：Type 怎么填？
//                 // Polars 默认把 Int128 视为 Decimal128(38, 0) 或者 FixedSizeBinary(16)
//                 // 这里我们使用 Decimal128Type(38, 0) 以获得最好的兼容性
//                 var type = new Decimal128Type(38, 0);

//                 var data = new ArrayData(
//                     type,
//                     length,
//                     nullCount,
//                     offset: 0,
//                     buffers: new[] { validityBuffer, valueBuffer }
//                 );

//                 return new Int128Array(data);
//             }

//             public Builder Append(ReadOnlySpan<Int128> span)
//             {
//                 int len = span.Length;
//                 _validityBuilder.AppendRange(true, len);
//                 _valueBuilder.Append(span);
//                 return this;
//             }

//             public Builder Swap(int i, int j)
//             {
//                 // 1. 交换值
//                 Span<Int128> valSpan = _valueBuilder.Span;
//                 (valSpan[i], valSpan[j]) = (valSpan[j], valSpan[i]);

//                 // 2. 交换有效性位 (Validity)
//                 Span<byte> validSpan = _validityBuilder.Span;
                
//                 // 读取旧状态
//                 bool bitI = BitUtility.GetBit(validSpan, i);
//                 bool bitJ = BitUtility.GetBit(validSpan, j);
                
//                 // 写入新状态 (交换)
//                 BitUtility.SetBit(validSpan, i, bitJ);
//                 BitUtility.SetBit(validSpan, j, bitI);

//                 return this;
//             }

//             public Builder Set(int index, Int128 value)
//             {
//                 // 1. 修改值：直接操作底层的 Span
//                 _valueBuilder.Span[index] = value;
                
//                 // 2. 修改有效性：标记为 true (非空)
//                 // Arrow 的 BitUtility 可以帮我们在 Span 上设置具体的位
//                 BitUtility.SetBit(_validityBuilder.Span, index, true);
                
//                 return this;
//             }
//         } 
//     }
//     /// <summary>
//     /// UInt128Array: 基于 PrimitiveArray 的 UInt128 实现
//     /// </summary>
//     public class UInt128Array : PrimitiveArray<UInt128>
//     {
//         public UInt128Array(ArrayData data) 
//             : base(data)
//         {
//             if (data.Buffers[1].Length % Unsafe.SizeOf<UInt128>() != 0)
//             {
//                 throw new ArgumentException($"Value buffer length must be a multiple of {Unsafe.SizeOf<UInt128>()} for UInt128Array.");
//             }
//         }

//         public override void Accept(IArrowArrayVisitor visitor)
//         {
//             throw new NotSupportedException(
//                     "UInt128Array is a custom extension and does not support standard Arrow visitors. " +
//                     "Please access .Values property directly.");
//         }
//         public class Builder : IArrowArrayBuilder<UInt128, UInt128Array, Builder>
//         {
//             // 1. 核心存储：值缓冲区 + 有效性位图
//             // 使用 ArrowBuffer.Builder<T> 来管理内存扩容
//             private readonly ArrowBuffer.Builder<UInt128> _valueBuilder;
//             private readonly ArrowBuffer.BitmapBuilder _validityBuilder;

//             public int Length => _valueBuilder.Length;
//             public int Capacity => _valueBuilder.Capacity;
            
//             // 构造函数
//             public Builder()
//             {
//                 _valueBuilder = new ArrowBuffer.Builder<UInt128>();
//                 _validityBuilder = new ArrowBuffer.BitmapBuilder();
//             }

//             // -----------------------------------------------------------------
//             // IArrowArrayBuilder 接口实现
//             // -----------------------------------------------------------------

//             /// <summary>
//             // 预分配容量，减少扩容开销
//             /// </summary>
//             public Builder Reserve(int capacity)
//             {
//                 _valueBuilder.Reserve(capacity);
//                 _validityBuilder.Reserve(capacity);
//                 return this;
//             }

//             /// <summary>
//             /// 调整大小（通常用于截断或预设）
//             /// </summary>
//             public Builder Resize(int length)
//             {
//                 _valueBuilder.Resize(length);
//                 _validityBuilder.Resize(length);
//                 return this;
//             }

//             /// <summary>
//             /// 清空 Builder
//             /// </summary>
//             public Builder Clear()
//             {
//                 _valueBuilder.Clear();
//                 _validityBuilder.Clear();
//                 return this;
//             }

//             /// <summary>
//             /// 追加一个空值 (Null)
//             /// </summary>
//             public Builder AppendNull()
//             {
//                 _valueBuilder.Append(UInt128.Zero); // 填充 0
//                 _validityBuilder.Append(false); // 标记为无效
//                 return this;
//             }

//             /// <summary>
//             /// 追加一个具体值 (Int128)
//             /// </summary>
//             public Builder Append(UInt128 value)
//             {
//                 _valueBuilder.Append(value);
//                 _validityBuilder.Append(true);
//                 return this;
//             }

//             /// <summary>
//             /// 批量追加 (IEnumerable) - 接口要求
//             /// </summary>
//             public Builder AppendRange(IEnumerable<UInt128> values)
//             {
//                 foreach (var v in values)
//                 {
//                     Append(v);
//                 }
//                 return this;
//             }
            
//             // -----------------------------------------------------------------
//             // ★ 高性能扩展 API (Zero-Copy / SIMD Integration)
//             // -----------------------------------------------------------------

//             /// <summary>
//             /// [Turbo] 批量追加 Span (内存拷贝)
//             /// 既然我们自己实现了 Builder，就一定要加这个方法，
//             /// 它可以直接把 Int128[] 拷贝到底层 Buffer，比循环 Append 快得多。
//             /// </summary>
//             public Builder AppendSpan(ReadOnlySpan<UInt128> values)
//             {
//                 int len = values.Length;
                
//                 // 1. 批量设置 Validity 为 true
//                 _validityBuilder.AppendRange(true, len);
                
//                 // 2. 批量拷贝 Values
//                 _valueBuilder.Append(values);
                
//                 return this;
//             }

//             /// <summary>
//             /// [Turbo] 直接追加 Decimal (集成 DecimalPacker)
//             /// 这里连接了你之前的 DecimalPacker 逻辑！
//             /// </summary>
//             public unsafe Builder AppendDecimalRange(decimal[] values)
//             {

//                 int len = values.Length;
                
//                 // 1. 预分配 Bitmap (Validity)
//                 _validityBuilder.AppendRange(true, len);
                
//                 _valueBuilder.Reserve(_valueBuilder.Length + len);

//                 // 直接循环转换，这是最安全且语义正确的方式
//                 for (int i = 0; i < len; i++)
//                 {
//                     _valueBuilder.Append((UInt128)values[i]);
//                 }

//                 return this;
//             }

//             // -----------------------------------------------------------------
//             // Build: 产出成品 Array
//             // -----------------------------------------------------------------

//             /// <summary>
//             /// 构建 IUnt128Array
//             /// </summary>
//             public UInt128Array Build(MemoryAllocator allocator = null!)
//             {
//                 // 1. 拿出构建好的 Buffer
//                 ArrowBuffer valueBuffer = _valueBuilder.Build(allocator);
//                 ArrowBuffer validityBuffer = _validityBuilder.Build(allocator);

//                 int length = valueBuffer.Length / Unsafe.SizeOf<UInt128>();
//                 int nullCount = length - _validityBuilder.SetBitCount;

//                 // 2. 构造 ArrayData
//                 // 关键点：Type 怎么填？
//                 // Polars 默认把 Int128 视为 Decimal128(38, 0) 或者 FixedSizeBinary(16)
//                 // 这里我们使用 Decimal128Type(38, 0) 以获得最好的兼容性
//                 var type = new Decimal128Type(38, 0);

//                 var data = new ArrayData(
//                     type,
//                     length,
//                     nullCount,
//                     offset: 0,
//                     buffers: new[] { validityBuffer, valueBuffer }
//                 );

//                 return new UInt128Array(data);
//             }

//             public Builder Append(ReadOnlySpan<UInt128> span)
//             {
//                 int len = span.Length;
//                 _validityBuilder.AppendRange(true, len);
//                 _valueBuilder.Append(span);
//                 return this;
//             }

//             public Builder Swap(int i, int j)
//             {
//                 // 1. 交换值
//                 Span<UInt128> valSpan = _valueBuilder.Span;
//                 (valSpan[i], valSpan[j]) = (valSpan[j], valSpan[i]);

//                 // 2. 交换有效性位 (Validity)
//                 Span<byte> validSpan = _validityBuilder.Span;
                
//                 // 读取旧状态
//                 bool bitI = BitUtility.GetBit(validSpan, i);
//                 bool bitJ = BitUtility.GetBit(validSpan, j);
                
//                 // 写入新状态 (交换)
//                 BitUtility.SetBit(validSpan, i, bitJ);
//                 BitUtility.SetBit(validSpan, j, bitI);

//                 return this;
//             }

//             public Builder Set(int index, UInt128 value)
//             {
//                 // 1. 修改值：直接操作底层的 Span
//                 _valueBuilder.Span[index] = value;
                
//                 // 2. 修改有效性：标记为 true (非空)
//                 // Arrow 的 BitUtility 可以帮我们在 Span 上设置具体的位
//                 BitUtility.SetBit(_validityBuilder.Span, index, true);
                
//                 return this;
//             }
//         }
//     }
// }