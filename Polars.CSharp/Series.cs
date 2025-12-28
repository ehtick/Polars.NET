using Polars.NET.Core;
using Apache.Arrow;
using Polars.NET.Core.Arrow;

namespace Polars.CSharp;

/// <summary>
/// Represents a Polars Series.
/// </summary>
public class Series : IDisposable
{
    internal SeriesHandle Handle { get; }

    internal Series(SeriesHandle handle)
    {
        Handle = handle;
    }

    internal Series(string name, SeriesHandle handle)
    {
        PolarsWrapper.SeriesRename(handle, name);
        Handle = handle;
    }

    private Series ApplyExpr(Expr expr)
    {
        // 1. 临时包装成 DataFrame
        // DataFrame.New 会增加 Series 的引用计数，所以是安全的
        using var df = new DataFrame(this);

        // 2. 执行 Select
        // 这会生成一个新的 DataFrame
        using var dfRes = df.Select(expr);

        // 3. 提取结果列
        // 索引器 dfRes[0] 会返回一个新的 Series 对象
        return dfRes[0];
    }
    // ==========================================
    // Metadata
    // ==========================================

    /// <summary>
    /// Get the string representation of the Series data type (e.g. "i64", "str", "datetime(μs)").
    /// </summary>
    public string DataTypeName => PolarsWrapper.GetSeriesDtypeString(Handle);
    /// <summary>
    /// Gets the DataType of the Series.
    /// </summary>
    /// <remarks>
    /// This property creates a new DataType instance every time it is accessed.
    /// Since DataType wraps a native handle, consider caching it locally if accessed frequently in a loop.
    /// </remarks>
    public DataType DataType
    {
        get
        {
            // [重构] 不再进行字符串解析
            // 1. 直接从底层获取 DataTypeHandle
            var handle = PolarsWrapper.GetSeriesDataType(Handle);
            
            // 2. 使用 Handle 构造 C# 对象 (Kind 会自动从 Rust 获取)
            return new DataType(handle);
        }
    }
    
    // ==========================================
    // Scalar Accessors (Native Speed ⚡)
    // ==========================================

    /// <summary>
    /// Get an item at the specified index.
    /// Supports: int, long, double, bool, string, decimal, DateTime, TimeSpan, DateOnly, TimeOnly.
    /// </summary>
    public T? GetValue<T>(long index)
    {
        var type = typeof(T);
        var underlying = Nullable.GetUnderlyingType(type) ?? type;

        if (index < 0 || index >= Length)
            throw new IndexOutOfRangeException($"Index {index} is out of bounds for Series length {Length}.");

        // 1. Numeric
        if (underlying == typeof(int)) 
            return (T?)(object?)(int?)PolarsWrapper.SeriesGetInt(Handle, index); // Long -> Int (Narrowing)
            
        if (underlying == typeof(long)) 
            return (T?)(object?)PolarsWrapper.SeriesGetInt(Handle, index);

        if (underlying == typeof(double)) 
            return (T?)(object?)PolarsWrapper.SeriesGetDouble(Handle, index);

        if (underlying == typeof(float)) 
            return (T?)(object?)(float?)PolarsWrapper.SeriesGetDouble(Handle, index);

        // 2. Boolean
        if (underlying == typeof(bool)) 
            return (T?)(object?)PolarsWrapper.SeriesGetBool(Handle, index);

        // 3. String
        if (underlying == typeof(string)) 
        {
            // 1. 先检查 Validity Bitmap (位图)
            if (PolarsWrapper.SeriesIsNullAt(Handle, index))
            {
                // 这里返回 default! 是为了压制 "可能返回 null" 的警告
                // 对于 string?，default 是 null；对于 string，default 也是 null (但在非空上下文中需要 !)
                return default!; 
            }

            // 2. 获取实际字符串
            var strVal = PolarsWrapper.SeriesGetString(Handle, index);
            
            // 3. 压制警告并返回
            // strVal! 告诉编译器：根据前面的 IsNullAt 检查，我确信这里 strVal 不会是 null
            return (T)(object)strVal!;
        }

        // 4. Decimal
        if (underlying == typeof(decimal))
            return (T?)(object?)PolarsWrapper.SeriesGetDecimal(Handle, index);

        // // 5. Temporal (Time)
        if (underlying == typeof(DateOnly))
            return (T?)(object?)PolarsWrapper.SeriesGetDate(Handle, index);
            
        if (underlying == typeof(TimeOnly))
            return (T?)(object?)PolarsWrapper.SeriesGetTime(Handle, index);
            
        if (underlying == typeof(TimeSpan))
            return (T?)(object?)PolarsWrapper.SeriesGetDuration(Handle, index);

        // ==============================================================
        // 🐢 慢车道 (Universal Path) - 使用 Arrow Infrastructure
        // 针对 Struct, List, F# Option, DateTimeOffset 等复杂类型
        // ==============================================================
        
        // 1. 切片：只取这一行
        using var slice = Slice(index, 1);
        
        // 2. 导出为 Arrow Array
        // 因为 ArrowReader 需要 IArrowArray，我们暂时没有 Series.ToArrow 直接绑定
        // 所以我们把它包在 DataFrame 里导出，然后取第一列
        using var df = new DataFrame(slice);
        using var batch = df.ToArrow(); // 调用 Core 层的 ExportDataFrame
        var column = batch.Column(0);

        // 3. 使用强大的 ArrowReader 解析
        // 这里会自动处理 Struct 递归、F# Option 解包、DateTimeOffset 时区归一化
        return ArrowReader.ReadItem<T>(column, 0);

        // throw new NotSupportedException($"Type {type.Name} is not supported for Series.GetValue.");
    }
    
    /// <summary>
    /// Get an item at the specified index as object (boxed).
    /// </summary>
    /// <summary>
    /// 语法糖 1: s[index]
    /// 让你可以写: var val = df["Name"][0];
    /// </summary>
    /// <param name="index"></param>
    /// <returns></returns>
    /// <exception cref="NotSupportedException"></exception>
    public object? this[int index]
    {
        get
        {
            // 根据 DataType 分发到具体的泛型实现
            // 这就是 object 拆箱的代价，为了语法糖是值得的
            return DataType.Kind switch
            {
                    // 整数家族
                    DataTypeKind.Int8 => GetValue<sbyte?>(index),
                    DataTypeKind.Int16 => GetValue<short?>(index),
                    DataTypeKind.Int32 => GetValue<int?>(index),
                    DataTypeKind.Int64 => GetValue<long?>(index),
                    DataTypeKind.UInt8 => GetValue<byte?>(index),
                    DataTypeKind.UInt16 => GetValue<ushort?>(index),
                    DataTypeKind.UInt32 => GetValue<uint?>(index),
                    DataTypeKind.UInt64 => GetValue<ulong?>(index),
                    DataTypeKind.Decimal => GetValue<decimal?>(index),

                    // 浮点数
                    DataTypeKind.Float32 => GetValue<float?>(index),
                    DataTypeKind.Float64 => GetValue<double?>(index),

                    // 布尔
                    DataTypeKind.Boolean => GetValue<bool?>(index),

                    // 字符串
                    DataTypeKind.String => GetValue<string>(index),

                    // Duration
                    DataTypeKind.Duration => GetValue<TimeSpan?>(index),

                    // [补全] 时间点 (Time) -> TimeOnly (如果 .NET 6+) 或 TimeSpan
                    DataTypeKind.Time => GetValue<TimeOnly?>(index),

                    // [补全] 日期
                    DataTypeKind.Date => GetValue<DateOnly?>(index), 
                    DataTypeKind.Datetime => string.IsNullOrEmpty(this.DataType.TimeZone) 
                        ? GetValue<DateTime?>(index)      // 无时区：返回 DateTime
                        : (object?)GetValue<DateTimeOffset?>(index),

                    // [补全] 二进制
                    DataTypeKind.Binary => GetValue<byte[]>(index),

                    // 复杂类型 (返回 List 或 Struct 的 object 形式)
                    // 注意：Series.GetValue<object> 需要在内部处理好 List/Struct 的装箱
                    DataTypeKind.List => GetValue<object>(index), 
                    DataTypeKind.Struct => GetValue<object>(index),
                
                _ => throw new NotSupportedException($"Indexer not supported for type {DataType.Kind}")
            };
        }
    }
    // ==========================================
    // Arithmetic Operators (算术运算符)
    // ==========================================
    /// <summary>
    /// Add Series
    /// </summary>
    /// <param name="left"></param>
    /// <param name="right"></param>
    /// <returns></returns>
    public static Series operator +(Series left, Series right)
    {
        return new Series(PolarsWrapper.SeriesAdd(left.Handle, right.Handle));
    }
    /// <summary>
    /// Minus Series
    /// </summary>
    /// <param name="left"></param>
    /// <param name="right"></param>
    /// <returns></returns>
    public static Series operator -(Series left, Series right)
    {
        return new Series(PolarsWrapper.SeriesSub(left.Handle, right.Handle));
    }
    /// <summary>
    /// Multiple Series
    /// </summary>
    /// <param name="left"></param>
    /// <param name="right"></param>
    /// <returns></returns>
    public static Series operator *(Series left, Series right)
    {
        return new Series(PolarsWrapper.SeriesMul(left.Handle, right.Handle));
    }
    /// <summary>
    /// Divide Series
    /// </summary>
    /// <param name="left"></param>
    /// <param name="right"></param>
    /// <returns></returns>
    public static Series operator /(Series left, Series right)
    {
        return new Series(PolarsWrapper.SeriesDiv(left.Handle, right.Handle));
    }

    // ==========================================
    // Comparison Methods & Operators (比较)
    // ==========================================

    // C# 的 == 和 != 运算符重载有比较严格的限制（通常用于对象相等性），
    // 且必须成对重载并重写 Equals/GetHashCode。
    // 为了避免混淆（是比较引用还是生成布尔掩码？），我们推荐使用显式的 Eq/Neq 方法，
    // 或者在未来实现复杂的运算符重载策略。目前先暴露方法。
    /// <summary>
    /// Compare whether two Series is equal
    /// </summary>
    /// <param name="other"></param>
    /// <returns></returns>
    public Series Eq(Series other) => new(PolarsWrapper.SeriesEq(Handle, other.Handle));
    /// <summary>
    /// Compare whether two Series is not equal
    /// </summary>
    /// <param name="other"></param>
    /// <returns></returns>
    public Series Neq(Series other) => new(PolarsWrapper.SeriesNeq(Handle, other.Handle));
    /// <summary>
    /// Compare whether left series is greater than right series
    /// </summary>
    /// <param name="left"></param>
    /// <param name="right"></param>
    /// <returns></returns>
    // 大于小于可以用运算符重载，这在 C# 中比较常见用于自定义类型
    public static Series operator >(Series left, Series right) 
        => new(PolarsWrapper.SeriesGt(left.Handle, right.Handle));
    /// <summary>
    /// Compare whether left series is less than right series
    /// </summary>
    /// <param name="left"></param>
    /// <param name="right"></param>
    /// <returns></returns>
    public static Series operator <(Series left, Series right) 
        => new(PolarsWrapper.SeriesLt(left.Handle, right.Handle));
    /// <summary>
    /// Compare whether left series is greater than or equal to right series
    /// </summary>
    /// <param name="left"></param>
    /// <param name="right"></param>
    /// <returns></returns>
    public static Series operator >=(Series left, Series right) 
        => new(PolarsWrapper.SeriesGtEq(left.Handle, right.Handle));
    /// <summary>
    /// Compare whether left series is less than or equal to right series
    /// </summary>
    /// <param name="left"></param>
    /// <param name="right"></param>
    /// <returns></returns>
    public static Series operator <=(Series left, Series right) 
        => new(PolarsWrapper.SeriesLtEq(left.Handle, right.Handle));

    // 显式方法别名 (Fluent API 风格)
    /// <summary>
    /// Compare whether left series is greater than right series
    /// </summary>
    /// <param name="other"></param>
    /// <returns></returns>
    public Series Gt(Series other) => this > other;
    /// <summary>
    /// Compare whether left series is less than right series
    /// </summary>
    /// <param name="other"></param>
    /// <returns></returns>
    public Series Lt(Series other) => this < other;
    /// <summary>
    /// Compare whether left series is greater than or equal to right series
    /// </summary>
    /// <param name="other"></param>
    /// <returns></returns>
    public Series GtEq(Series other) => this >= other;
    /// <summary>
    /// Compare whether left series is less than or equal to right series
    /// </summary>
    /// <param name="other"></param>
    /// <returns></returns>
    public Series LtEq(Series other) => this <= other;

    // ==========================================
    // Aggregations (聚合)
    // ==========================================

    // 注意：Polars 的 Series 聚合通常返回一个长度为 1 的新 Series (Scalar)
    /// <summary>
    /// Sum series into 1 length series(Scalar)
    /// </summary>
    /// <returns></returns>
    public Series Sum() => new(PolarsWrapper.SeriesSum(Handle));
    /// <summary>
    /// Mean series into 1 length series(Scalar)
    /// </summary>
    /// <returns></returns>
    public Series Mean() => new(PolarsWrapper.SeriesMean(Handle));
    /// <summary>
    /// Min series into 1 length series(Scalar)
    /// </summary>
    /// <returns></returns>
    public Series Min() => new(PolarsWrapper.SeriesMin(Handle));
    /// <summary>
    /// Max series into 1 length series(Scalar)
    /// </summary>
    /// <returns></returns>
    public Series Max() => new(PolarsWrapper.SeriesMax(Handle));

    // 泛型辅助方法：直接获取标量值
    /// <summary>
    /// Sum series into scalar
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public T? Sum<T>() => Sum().GetValue<T>(0);
    /// <summary>
    /// Mean series into scalar
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public T? Mean<T>() => Mean().GetValue<T>(0);
    /// <summary>
    /// Min series into scalar
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public T? Min<T>() => Min().GetValue<T>(0);
    /// <summary>
    /// Max series into scalar
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public T? Max<T>() => Max().GetValue<T>(0);

    // ==========================================
    // Constructors
    // ==========================================

    // ------------------------------------------
    // 🚀 1. Fast Path (Primitives)
    // 直接走 P/Invoke，性能最高
    // ------------------------------------------
    
    /// <summary>
    /// Create a Series from an array of integers.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="data"></param>
    /// <param name="validity"></param>
    public Series(string name, int[] data, bool[]? validity = null)
    {
        Handle = PolarsWrapper.SeriesNew(name, data, validity);
    }
    /// <summary>
    /// Create a Series from an array of longs.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="data"></param>
    /// <param name="validity"></param>
    public Series(string name, long[] data, bool[]? validity = null)
    {
        Handle = PolarsWrapper.SeriesNew(name, data, validity);
    }
    /// <summary>
    /// Create a Series from an array of doubles.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="data"></param>
    /// <param name="validity"></param>
    public Series(string name, double[] data, bool[]? validity = null)
    {
        Handle = PolarsWrapper.SeriesNew(name, data, validity);
    }
    /// <summary>
    /// Create a Series from an array of booleans.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="data"></param>
    /// <param name="validity"></param>
    public Series(string name, bool[] data, bool[]? validity = null)
    {
        Handle = PolarsWrapper.SeriesNew(name, data, validity);
    }
    /// <summary>
    /// Create a Series from an array of strings.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="data"></param>
    public Series(string name, string?[] data)
    {
        Handle = PolarsWrapper.SeriesNew(name, data);
    }

    // ------------------------------------------
    // 🐢 2. Universal Path (Complex Types)
    // 委托给 ArrowConverter，逻辑统一
    // ------------------------------------------

    /// <summary>
    /// Create a Series from an array of DateTime values.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="data"></param>
    public Series(string name, DateTime[] data)
    {
        // 1. 转 Arrow
        using var arrowArray = ArrowConverter.Build(data);
        // 2. 导入 Handle (这一步会自动转移所有权给 Rust)
        Handle = ArrowFfiBridge.ImportSeries(name, arrowArray);
    }

    /// <summary>
    /// Create a Series from an array of Nullable DateTime values.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="data"></param>
    public Series(string name, DateTime?[] data)
    {
        using var arrowArray = ArrowConverter.Build(data);
        Handle = ArrowFfiBridge.ImportSeries(name, arrowArray);
    }
    /// <summary>
    /// Create a Series from an array of DateTime with timezone offsets values.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="data"></param>
    public Series(string name, DateTimeOffset[] data)
    {
        // 1. 转 Arrow
        using var arrowArray = ArrowConverter.Build(data);
        Handle = ArrowFfiBridge.ImportSeries(name, arrowArray);
    }

    /// <summary>
    /// Create a Series from an array of Nullable DateTime with timezone offsets values.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="data"></param>
    public Series(string name, DateTimeOffset?[] data)
    {
        // 1. 转 Arrow
        using var arrowArray = ArrowConverter.Build(data);
        Handle = ArrowFfiBridge.ImportSeries(name, arrowArray);
    }
    
    /// <summary>
    /// Create a Series from an array of TimeSpan values.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="data"></param>
    public Series(string name, TimeSpan[] data)
    {
        using var arrowArray = ArrowConverter.Build(data);
        Handle = ArrowFfiBridge.ImportSeries(name, arrowArray);
    }
    /// <summary>
    /// Create a Series from an array of Nullable TimeSpan values.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="data"></param>
    public Series(string name, TimeSpan?[] data)
    {
        using var arrowArray = ArrowConverter.Build(data);
        Handle = ArrowFfiBridge.ImportSeries(name, arrowArray);
    }

    /// <summary>
    /// Create a Series from an array of DateOnly values.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="data"></param>
    public Series(string name, DateOnly[] data)
    {
        using var arrowArray = ArrowConverter.Build(data);
        Handle = ArrowFfiBridge.ImportSeries(name, arrowArray);
    }
    /// <summary>
    /// Create a Series from an array of Nullable DateOnly values.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="data"></param>
    public Series(string name, DateOnly?[] data)
    {
        using var arrowArray = ArrowConverter.Build(data);
        Handle = ArrowFfiBridge.ImportSeries(name, arrowArray);
    }

    /// <summary>
    /// Create a Series from an array of TimeOnly values.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="data"></param>
    public Series(string name, TimeOnly[] data)
    {
        using var arrowArray = ArrowConverter.Build(data);
        Handle = ArrowFfiBridge.ImportSeries(name, arrowArray);
    }
    /// <summary>
    /// Create a Series from an array of Nullable TimeOnly values.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="data"></param>
    public Series(string name, TimeOnly?[] data)
    {
        using var arrowArray = ArrowConverter.Build(data);
        Handle = ArrowFfiBridge.ImportSeries(name, arrowArray);
    }
    /// <summary>
    /// Create a Series from an array of decimals.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="data"></param>
    public Series(string name, decimal[] data)
    {
        using var arrowArray = ArrowConverter.Build(data);
        Handle = ArrowFfiBridge.ImportSeries(name, arrowArray);
    }
    /// <summary>
    /// Create a Series from an array of nullable decimals.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="data"></param>
    public Series(string name, decimal?[] data)
    {
        using var arrowArray = ArrowConverter.Build(data);
        Handle = ArrowFfiBridge.ImportSeries(name, arrowArray);
    }

    // ==========================================
    // Properties
    // ==========================================
    /// <summary>
    /// Length of the Series.
    /// </summary>
    public long Length => PolarsWrapper.SeriesLen(Handle);
    /// <summary>
    /// Name of the Series.
    /// </summary>
    public string Name 
    {
        get => PolarsWrapper.SeriesName(Handle);
        set => PolarsWrapper.SeriesRename(Handle, value);
    }
    /// <summary>
    /// Get the number of null values in the Series.
    /// </summary>
    public long NullCount => PolarsWrapper.SeriesNullCount(Handle);

    // ==========================================
    // Operations
    // ==========================================

    /// <summary>
    /// Cast the Series to a different DataType.
    /// </summary>
    public Series Cast(DataType dtype)
    {
        // SeriesCast 返回一个新的 Series Handle
        return new Series(PolarsWrapper.SeriesCast(Handle, dtype.Handle));
    }
    /// <summary>
    /// Get a slice of this Series.
    /// </summary>
    /// <param name="offset">Start index. Negative values count from the end.</param>
    /// <param name="length">Length of the slice.</param>
    public Series Slice(long offset, long length)
    {
        var newHandle = PolarsWrapper.SeriesSlice(Handle, offset, length);
        return new Series(newHandle);
    }
    /// <summary>
    /// Convert Series to Arrow Array
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public T[] ToArray<T>()
    {
        // 1. 转为 DataFrame (为了用 ToArrow 导出 Batch)
        using var df = new DataFrame(this);
        using var batch = df.ToArrow();
        
        // 2. 取第一列
        var col = batch.Column(0);
        
        // 3. 读取
        return ArrowReader.ReadColumn<T>(col);
    }
    // ==========================================
    // Null Checks & Boolean Masks
    // ==========================================

    /// <summary>
    /// 检查指定索引处的值是否为 Null。
    /// </summary>
    public bool IsNullAt(long index)
    {
        return PolarsWrapper.SeriesIsNullAt(Handle, index);
    }

    /// <summary>
    /// 返回一个布尔 Series，如果元素为 Null 则为 True。
    /// </summary>
    public Series IsNull()
    {
        var newHandle = PolarsWrapper.SeriesIsNull(Handle);
        return new Series(newHandle);
    }

    /// <summary>
    /// 返回一个布尔 Series，如果元素不为 Null 则为 True。
    /// </summary>
    public Series IsNotNull()
    {
        var newHandle = PolarsWrapper.SeriesIsNotNull(Handle);
        return new Series(newHandle);
    }
    // ==========================================
    // Float Checks (数值检查)
    // ==========================================
    /// <summary>
    /// Check whether this series is NaN
    /// </summary>
    /// <returns></returns>
    public Series IsNan() => new(PolarsWrapper.SeriesIsNan(Handle));
    /// <summary>
    /// Check whether this series is not NaN
    /// </summary>
    /// <returns></returns>
    public Series IsNotNan() => new(PolarsWrapper.SeriesIsNotNan(Handle));
    /// <summary>
    /// Check whether this series is finite
    /// </summary>
    /// <returns></returns>
    public Series IsFinite() => new(PolarsWrapper.SeriesIsFinite(Handle));
    /// <summary>
    /// Check whether this series is infinite
    /// </summary>
    /// <returns></returns>
    public Series IsInfinite() => new(PolarsWrapper.SeriesIsInfinite(Handle));
    // ==========================================
    // Unique Ops 
    // ==========================================
    /// <summary>
    /// Count the number of unique values in this Series.
    /// </summary>
    public ulong NUnique => PolarsWrapper.SeriesNUnique(Handle);
    /// <summary>
    /// Get the unique elements of this Series.
    /// </summary>
    public Series Unique() => new(PolarsWrapper.SeriesUnique(Handle));

    /// <summary>
    /// Get the unique elements of this Series, maintaining the order of appearance.
    /// </summary>
    public Series UniqueStable() => new(PolarsWrapper.SeriesUniqueStable(Handle));

    /// <summary>
    /// Get a boolean mask indicating which values are unique.
    /// <para>Implemented via DataFrame expression composition.</para>
    /// </summary>
    public Series IsUnique()
    {
        return ApplyExpr(Polars.Col(Name).IsUnique());
    }

    /// <summary>
    /// Get a boolean mask indicating which values are duplicated.
    /// <para>Implemented via DataFrame expression composition.</para>
    /// </summary>
    public Series IsDuplicated()
    {
        return ApplyExpr(Polars.Col(Name).IsDuplicated());
    }
    // ==========================================
    // Common Ops 
    // ==========================================
    /// <summary>
    /// Sort this Series.
    /// </summary>
    /// <param name="descending">Sort in descending order.</param>
    /// <param name="nullsLast">Place null values last (default behavior depends on ascending/descending).</param>
    /// <param name="multithreaded">Use parallel sorting (default: true).</param>
    /// <param name="maintainOrder">Use stable sort (maintain order of equal elements) (default: false).</param>
    public Series Sort(
        bool descending = false, 
        bool nullsLast = false, 
        bool maintainOrder = false, // 放在这里比较符合直觉
        bool multithreaded = true)
    {
        var h = PolarsWrapper.SeriesSort(
            Handle, 
            descending, 
            nullsLast, 
            multithreaded, 
            maintainOrder
        );
        return new Series(h);
    }
    // ==========================================
    // Conversions (Arrow / DataFrame)
    // ==========================================

    /// <summary>
    /// Zero-copy convert to Apache Arrow Array.
    /// </summary>
    public IArrowArray ToArrow()
    {
        return PolarsWrapper.SeriesToArrow(Handle);
    }
    /// <summary>
    /// Low-level entry point: Create Series from existing Arrow Array.
    /// </summary>
    public static Series FromArrow(string name, IArrowArray arrowArray)
    {
        var handle = ArrowFfiBridge.ImportSeries(name, arrowArray);
        return new Series(handle);
    }

    // ==========================================
    // High-Level Factories
    // ==========================================
    /// <summary>
    /// Create a Series from a list of objects, primitives, or nested lists.
    /// Uses Polars.NET.Core to handle Arrow conversion and FFI transfer.
    /// </summary>
    public static Series From<T>(string name, IEnumerable<T> data) 
    {
        // 1. 调用 Core 层的转换器：IEnumerable<T> -> IArrowArray
        // (原 ArrowArrayFactory.Build)
        using var arrowArray = ArrowConverter.Build(data);

        // 2. 调用 Core 层的 FFI 桥梁：IArrowArray -> SeriesHandle
        // (原 Series.FromArrow 的底层逻辑)
        var handle = ArrowFfiBridge.ImportSeries(name, arrowArray);

        // 3. 封装为 C# API 对象
        return new Series(handle);
    }
    /// <summary>
    /// Convert this single Series into a DataFrame.
    /// </summary>
    public DataFrame ToFrame()
    {
        return new DataFrame(PolarsWrapper.SeriesToFrame(Handle));
    }
    /// <summary>
    /// Dispose the underlying SeriesHandle.
    /// </summary>
    public void Dispose()
    {
        Handle.Dispose();
    }
}