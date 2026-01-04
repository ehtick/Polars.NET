using Polars.NET.Core;
using Apache.Arrow;
using Polars.NET.Core.Arrow;

namespace Polars.CSharp;

/// <summary>
/// Represents a Polars Series.
/// </summary>
public partial class Series : IDisposable
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
    /// <summary>
    /// Rename Series
    /// </summary>
    /// <param name="newName"></param>
    public void Rename(string newName)
    {
        this.Name = newName;
    }

    /// <summary>
    /// Date Ops
    /// </summary>
    public SeriesDtOps Dt => new(this);
    /// <summary>
    /// String Ops
    /// </summary>
    public SeriesStrOps Str => new(this);
    /// <summary>
    /// Access list operations.
    /// </summary>
    public SeriesListOps List => new(this);
    /// <summary>
    /// Access Fixed-Size List (Array) operations.
    /// </summary>
    public SeriesArrayOps Array => new(this);
    /// <summary>
    /// Access struct operations.
    /// </summary>
    public SeriesStructOps Struct => new(this);

    /// <summary>
    /// Clone the Series
    /// </summary>
    /// <returns></returns>
    public Series Clone()
    {
        return new(PolarsWrapper.CloneSeries(Handle));
    }

    internal Series ApplyExpr(Expr expr)
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

    internal Series ApplyBinaryExpr(Series other, Func<Expr, Expr, Expr> op)
    {
        string leftName = this.Name;
        string rightName = other.Name;
        
        // 标记是否创建了临时对象，用于 finally 块清理
        Series? tempRight = null;

        try
        {
            // 1. 处理命名冲突
            // 如果名字一样，Polars 无法在同一个 DF 里放两列同名的
            Series rightSeries;
            
            if (leftName == rightName)
            {
                rightName = "__other_temp__";
                // Clone 出来改名，避免修改原始对象
                tempRight = other.Clone();
                tempRight.Name = rightName;
                rightSeries = tempRight;
            }
            else
            {
                rightSeries = other;
            }

            // 2. 构建 DataFrame (关键修正)
            // 直接将两个 Series 传入构造函数，而不是先创建再 Add
            // 假设你的 DataFrame.FromColumns 支持 params Series[]
            using var df = new DataFrame([this, rightSeries]);

            // 3. 执行表达式
            // Op 接收两个 Expr：Col(this.Name) 和 Col(otherName)
            using var resDf = df.Select(op(Polars.Col(leftName), Polars.Col(rightName)));

            // 4. 返回结果 Series
            // 注意：resDf[0] 返回的新 Series，其 Handle 引用计数 +1，所以 df Dispose 不影响它
            return resDf[0];
        }
        finally
        {
            // 5. 清理临时 Clone 的 Series
            // (rightSeries 如果是 other，不需要 Dispose；如果是 tempRight，需要 Dispose)
            tempRight?.Dispose();
        }
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
        var column = slice.ToArrow();

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
                    DataTypeKind.Array => GetValue<object>(index),
                
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
    /// <summary>
    /// Calculate absolute value.
    /// <para>Implemented via Expr composition.</para>
    /// </summary>
    public Series Abs() => ApplyExpr(Polars.Col(Name).Abs());
    /// <summary>
    /// Calculate square value.
    /// <para>Implemented via Expr composition.</para>
    /// </summary>
    public Series Sqrt() => ApplyExpr(Polars.Col(Name).Sqrt());
    /// <summary>
    /// Calculate the cube root of the expression.
    /// </summary>
    public Series Cbrt() => ApplyExpr(Polars.Col(Name).Cbrt());
    /// <summary>
    /// Calculate exponent value.
    /// <para>Implemented via Expr composition.</para>
    /// </summary>
    public Series Pow(double exponent) => ApplyExpr(Polars.Col(Name).Pow(exponent));
    /// <summary>
    /// Calculate the power of the Euler's number.
    /// </summary>
    public Series Exp() =>  ApplyExpr(Polars.Col(Name).Exp());
    /// <summary>
    /// Calculate the ln of Number 
    /// </summary>
    /// <param name="baseVal"></param>
    /// <returns></returns>
    public Series Ln(double baseVal = Math.E) => ApplyExpr(Polars.Col(Name).Ln(baseVal));
    /// <summary>
    /// Round the number
    /// </summary>
    /// <param name="decimals"></param>
    /// <returns></returns>
    public Series Round(uint decimals) => ApplyExpr(Polars.Col(Name).Round(decimals));
    /// <summary>Compute the element-wise sign (-1, 0, 1).</summary>
    public Series Sign() => ApplyExpr(Polars.Col(Name).Sign());

    /// <summary>Rounds up to the nearest integer.</summary>
    public Series Ceil() => ApplyExpr(Polars.Col(Name).Ceil());

    /// <summary>Rounds down to the nearest integer.</summary>
    public Series Floor() => ApplyExpr(Polars.Col(Name).Floor());
    
    // ==========================================
    // Bitwise Operators (<<, >>)
    // ==========================================

    /// <summary>
    /// Bitwise left shift operation.
    /// </summary>
    public static Series operator <<(Series left, int right)
    {
        // 逻辑：将 Series 视为一个列表达式，应用移位，然后立即求值返回新 Series
        return left.ApplyExpr(Polars.Col(left.Name) << right);
    }

    /// <summary>
    /// Bitwise right shift operation.
    /// <para>
    /// For signed integers, this is arithmetic shift.
    /// For unsigned integers, this is logical shift.
    /// </para>
    /// </summary>
    public static Series operator >>(Series left, int right)
    {
        return left.ApplyExpr(Polars.Col(left.Name) >> right);
    }
    // ==========================================
    // Trigonometry (三角函数)
    // ==========================================

    /// <summary>Compute the element-wise sine.</summary>
    public Series Sin() => ApplyExpr(Polars.Col(Name).Sin());

    /// <summary>Compute the element-wise cosine.</summary>
    public Series Cos() => ApplyExpr(Polars.Col(Name).Cos());

    /// <summary>Compute the element-wise tangent.</summary>
    public Series Tan() => ApplyExpr(Polars.Col(Name).Tan());

    /// <summary>Compute the element-wise inverse sine.</summary>
    public Series ArcSin() => ApplyExpr(Polars.Col(Name).ArcSin());

    /// <summary>Compute the element-wise inverse cosine.</summary>
    public Series ArcCos() => ApplyExpr(Polars.Col(Name).ArcCos());

    /// <summary>Compute the element-wise inverse tangent.</summary>
    public Series ArcTan() => ApplyExpr(Polars.Col(Name).ArcTan());

    // Hyperbolic
    /// <summary>
    /// Compute the element-wise hyperbolic sine.
    /// </summary>
    public Series Sinh() => ApplyExpr(Polars.Col(Name).Sinh());

    /// <summary>
    /// Compute the element-wise hyperbolic cosine.
    /// </summary>
    public Series Cosh() => ApplyExpr(Polars.Col(Name).Cosh());

    /// <summary>
    /// Compute the element-wise hyperbolic tangent.
    /// </summary>
    public Series Tanh() => ApplyExpr(Polars.Col(Name).Tanh());

    /// <summary>
    /// Compute the element-wise inverse hyperbolic sine.
    /// </summary>
    public Series ArcSinh() => ApplyExpr(Polars.Col(Name).ArcSinh());

    /// <summary>
    /// Compute the element-wise inverse hyperbolic cosine.
    /// </summary>
    public Series ArcCosh() => ApplyExpr(Polars.Col(Name).ArcCosh());

    /// <summary>
    /// Compute the element-wise inverse hyperbolic tangent.
    /// </summary>
    public Series ArcTanh() => ApplyExpr(Polars.Col(Name).ArcTanh());

    // ==========================================
    // Comparison Methods & Operators (比较)
    // ==========================================
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
        var col = this.ToArrow();
        // 3. 读取
        return ArrowReader.ReadColumn<T>(col);
    }
    // ==========================================
    // Null Checks & Boolean Masks
    // ==========================================

    /// <summary>
    /// Check whether indexed value is null。
    /// </summary>
    public bool IsNullAt(long index)
    {
        return PolarsWrapper.SeriesIsNullAt(Handle, index);
    }

    /// <summary>
    /// Return a Boolean series, where null value will be masked as true.
    /// </summary>
    public Series IsNull()
    {
        var newHandle = PolarsWrapper.SeriesIsNull(Handle);
        return new Series(newHandle);
    }

    /// <summary>
    /// Return a Boolean series, where null value will be masked as false.
    /// </summary>
    public Series IsNotNull()
    {
        var newHandle = PolarsWrapper.SeriesIsNotNull(Handle);
        return new Series(newHandle);
    }
    // ==========================================
    // Fill Ops
    // ==========================================
    /// <summary>
    /// Fill null values with a specified value.
    /// </summary>
    public Series FillNull(object value) => ApplyExpr(Polars.Col(Name).FillNull(value));
    /// <summary>
    /// Fill null values with a specific strategy (Forward).
    /// </summary>
    public Series ForwardFill(uint? limit = null) => ApplyExpr(Polars.Col(Name).ForwardFill(limit));
    /// <summary>
    /// Fill null values with a specific strategy (Backward).
    /// </summary>
    public Series BackwardFill(uint? limit = null) => ApplyExpr(Polars.Col(Name).BackwardFill(limit));
    /// <summary>
    /// Fill floating point NaN values with a specified value.
    /// Note: This is different from FillNull. It only handles IEEE 754 NaN.
    /// </summary>
    public Series FillNan(object value) => ApplyExpr(Polars.Col(Name).FillNan(value));
    // ==========================================
    // Top-K & Bottom-K
    // ==========================================
    /// <summary>
    /// Get the top k values.
    /// </summary>
    public Series TopK(int k) => ApplyExpr(Polars.Col(Name).TopK(k));

    /// <summary>
    /// Get the bottom k values.
    /// </summary>
    public Series BottomK(int k) => ApplyExpr(Polars.Col(Name).BottomK(k));
    // ==========================================
    // Float Checks
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
    public Series IsUnique() => ApplyExpr(Polars.Col(Name).IsUnique());

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
    /// <summary>
    /// Explode a list column into multiple rows.
    /// The resulting Series will be longer than the original.
    /// </summary>
    public Series Explode()
    {
        return ApplyExpr(Polars.Col(Name).Explode());
    }
    /// <summary>
    /// Aggregate values into a list.
    /// Result is a Series with 1 row containing a List of all values.
    /// </summary>
    public Series Implode() => ApplyExpr(Polars.Col(Name).Implode());
    /// <summary>
    /// Unnest a Struct column into a DataFrame.
    /// Shortcut for <see cref="SeriesStructOps.Unnest"/>.
    /// </summary>
    public DataFrame Unnest() => Struct.Unnest();
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
    // Window & Rolling
    // ==========================================
    /// <summary>
    /// Calculate the difference with the previous value (n-th lag).
    /// </summary>
    public Series Diff(long n = 1) => ApplyExpr(Polars.Col(Name).Diff(n));

    /// <summary>
    /// Shift values by the given number of indices.
    /// </summary>
    public Series Shift(long n = 1) => ApplyExpr(Polars.Col(Name).Shift(n));

    /// <summary>
    /// Check if values are between lower and upper bounds.
    /// </summary>
    public Series IsBetween(object lower, object upper) 
    {
        // 支持传入字面量，更符合直觉
        return ApplyExpr(Polars.Col(Name).IsBetween(Expr.MakeLit(lower), Expr.MakeLit(upper)));
    }
    /// <summary>
    /// Static Rolling Minimum
    /// </summary>
    /// <param name="windowSize"></param>
    /// <param name="minPeriods"></param>
    /// <returns></returns>
    public Series RollingMin(string windowSize, int minPeriods = 1) 
            => ApplyExpr(Polars.Col(Name).RollingMin(windowSize, minPeriods));
    /// <summary>
    /// Static Rolling Minimum, windowSize is timespan
    /// </summary>
    /// <param name="windowSize"></param>
    /// <param name="minPeriods"></param>
    /// <returns></returns>
    public Series RollingMin(TimeSpan windowSize, int minPeriods = 1) 
            => ApplyExpr(Polars.Col(Name).RollingMin(windowSize, minPeriods));
    /// <summary>
    /// Static Rolling Maximum
    /// </summary>
    /// <param name="windowSize"></param>
    /// <param name="minPeriods"></param>
    /// <returns></returns>
    public Series RollingMax(string windowSize, int minPeriods = 1) 
        => ApplyExpr(Polars.Col(Name).RollingMax(windowSize, minPeriods));
    /// <summary>
    /// Static Rolling Maximum
    /// </summary>
    /// <param name="windowSize"></param>
    /// <param name="minPeriods"></param>
    /// <returns></returns>
    public Series RollingMax(TimeSpan windowSize, int minPeriods = 1) 
        => ApplyExpr(Polars.Col(Name).RollingMax(windowSize, minPeriods));
    /// <summary>
    /// Static Rolling Mean
    /// </summary>
    /// <param name="windowSize"></param>
    /// <param name="minPeriods"></param>
    /// <returns></returns>
    public Series RollingMean(string windowSize, int minPeriods = 1) 
        => ApplyExpr(Polars.Col(Name).RollingMean(windowSize, minPeriods));
    /// <summary>
    /// Static Rolling Mean
    /// </summary>
    /// <param name="windowSize"></param>
    /// <param name="minPeriods"></param>
    /// <returns></returns>
    public Series RollingMean(TimeSpan windowSize, int minPeriods = 1) 
        => ApplyExpr(Polars.Col(Name).RollingMean(windowSize, minPeriods));
    /// <summary>
    /// Static Rolling Sum
    /// </summary>
    /// <param name="windowSize"></param>
    /// <param name="minPeriods"></param>
    /// <returns></returns>   
    public Series RollingSum(string windowSize, int minPeriods = 1) 
        => ApplyExpr(Polars.Col(Name).RollingSum(windowSize, minPeriods));
        /// <summary>
    /// Static Rolling Sum
    /// </summary>
    /// <param name="windowSize"></param>
    /// <param name="minPeriods"></param>
    /// <returns></returns>   
    public Series RollingSum(TimeSpan windowSize, int minPeriods = 1) 
        => ApplyExpr(Polars.Col(Name).RollingSum(windowSize, minPeriods));
    // ==========================================
    // UDF
    // ==========================================
    /// <summary>
    /// Apply a custom C# function to the series (element-wise).
    /// <para>Warning: This is slower than native expressions because it runs in the .NET runtime.</para>
    /// </summary>
    public Series Map<TInput, TOutput>(Func<TInput, TOutput> function, DataType outputType)
    {
        // 直接复用 Expr 的 Map
        return ApplyExpr(Polars.Col(Name).Map(function, outputType));
    }

    /// <summary>
    /// Apply a raw Arrow-to-Arrow UDF.
    /// </summary>
    public Series Map(Func<IArrowArray, IArrowArray> function, DataType outputType)
    {
        return ApplyExpr(Polars.Col(Name).Map(function, outputType));
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

/// <summary>
/// Date Ops Namespace
/// </summary>
public class SeriesDtOps
{
    private readonly Series _series;
    internal SeriesDtOps(Series series) { _series = series; }

    private Series Apply(Func<Expr, Expr> op) 
    {
        var expr = op(Polars.Col(_series.Name));
        
        return _series.ApplyExpr(expr);
    }
    /// <summary>Get the year from the underlying date/datetime.</summary>
    public Series Year() => Apply(e => e.Dt.Year());
    /// <summary>Get the quarter from the underlying date/datetime.</summary>
    public Series Quarter() => Apply(e => e.Dt.Quarter());
    /// <summary>Get the month from the underlying date/datetime.</summary>
    public Series Month() => Apply(e => e.Dt.Month());
    /// <summary>Get the day from the underlying date/datetime.</summary>
    public Series Day() => Apply(e => e.Dt.Day());
    /// <summary>Get the ordinal day (day of year) from the underlying date/datetime.</summary>
    public Series OrdinalDay() => Apply(e => e.Dt.OrdinalDay());
    /// <summary>Get the weekday from the underlying date/datetime.</summary>
    public Series WeekDay() => Apply(e => e.Dt.Weekday());
    /// <summary>Get the hour from the underlying datetime.</summary>
    public Series Hour() => Apply(e => e.Dt.Hour());
    /// <summary>Get the minute from the underlying datetime.</summary>
    public Series Minute() => Apply(e => e.Dt.Minute());
    /// <summary>Get the second from the underlying datetime.</summary>
    public Series Second() => Apply(e => e.Dt.Second());
    /// <summary>Get the millisecond from the underlying datetime.</summary>
    public Series Millisecond() => Apply(e => e.Dt.Millisecond());
    /// <summary>Get the microsecond from the underlying datetime.</summary>
    public Series Microsecond() => Apply(e => e.Dt.Microsecond());
    /// <summary>Get the nanosecond from the underlying datetime.</summary>
    public Series Nanosecond() => Apply(e => e.Dt.Nanosecond());

    /// <summary>
    /// Cast to Date (remove time component).
    /// </summary>
    /// <returns></returns>
    public Series Date() => Apply(e => e.Dt.Date());
    /// <summary>
    /// Cast to Time (remove Date component).
    /// </summary>
    /// <returns></returns>
    public Series Time() => Apply(e => e.Dt.Time());
    // ==========================================
    // Truncate & Round (时间对齐)
    // ==========================================

    /// <summary>
    /// Truncate the datetimes to the given interval (e.g. "1d", "1h", "15m").
    /// </summary>
    public Series Truncate(string every) => Apply(e => e.Dt.Truncate(every));
    /// <summary>
    /// Truncate the datetimes to the given timespan
    /// </summary>
    /// <param name="every"></param>
    /// <returns></returns>
    public Series Truncate(TimeSpan every) => Apply(e => e.Dt.Truncate(every));
    /// <summary>
    /// Round the datetimes to the given interval.
    /// </summary>
    public Series Round(string every) => Apply(e => e.Dt.Round(every));
    /// <summary>
    /// Round the datetimes to the given timespan interval.
    /// </summary>
    /// <param name="every"></param>
    /// <returns></returns>
    public Series Round(TimeSpan every) => Apply(e => e.Dt.Round(every));
    // ==========================================
    // Offset (时间平移)
    // ==========================================

    /// <summary>
    /// Offset the datetimes by a given duration expression.
    /// </summary>
    public Series OffsetBy(Expr by) => Apply(e => e.Dt.OffsetBy(by));
    /// <summary>
    /// Offset the datetimes by a constant duration string (e.g., "1d", "-2h").
    /// </summary>
    public Series OffsetBy(string duration) => Apply(e => e.Dt.OffsetBy(duration));
    /// <summary>
    /// Offset the datetimes by TimeSpan
    /// </summary>
    /// <param name="duration"></param>
    /// <returns></returns>
    public Series OffsetBy(TimeSpan duration) => Apply(e => e.Dt.OffsetBy(duration));

    // ==========================================
    // Timestamp (转整数)
    // ==========================================

    /// <summary>
    /// Convert the datetime to an integer timestamp (Unix epoch).
    /// </summary>
    public Series Timestamp(TimeUnit unit = TimeUnit.Microseconds) => Apply(e => e.Dt.Timestamp(unit));

    // ==========================================
    // TimeZone
    // ==========================================
    /// <summary>
    /// Convert from one timezone to another.
    /// Resulting Series will have the given time zone.
    /// </summary>
    /// <param name="tz">Target time zone (e.g. "Asia/Shanghai")</param>    
    public Series ConvertTimeZone(string tz) => Apply(e => e.Dt.ConvertTimeZone(tz));

    /// <summary>
    /// Replace the time zone of a Series.
    /// This does not change the underlying timestamp, only the metadata.
    /// </summary>
    public Series ReplaceTimeZone(string? timeZone, string? ambiguous = null, string? nonExistent = "raise")
         =>Apply(e => e.Dt.ReplaceTimeZone(timeZone,ambiguous,nonExistent));
    // ==========================================
    // BusinessDays
    // ==========================================
    /// <summary>
    /// Add business days to the date column.
    /// </summary>
    /// <param name="n">Number of business days to add (can be negative).</param>
    /// <param name="holidays">List of holidays (dates to skip).</param>
    /// <param name="weekMask">
    /// Array of 7 bools indicating business days, starting from Monday. 
    /// Default is Mon-Fri.
    /// </param>
    /// <param name="roll">Strategy for handling non-business days.</param>
    public Series AddBusinessDays(
        int n, 
        IEnumerable<DateOnly>? holidays = null, 
        bool[]? weekMask = null, 
        Roll roll = Roll.Raise)
        =>Apply(e => e.Dt.AddBusinessDays(n,holidays,weekMask,roll));
    /// <summary>
    /// Add business days to the date column.
    /// </summary>
    public Series AddBusinessDays(
        Expr n, 
        IEnumerable<DateOnly>? holidays = null, 
        bool[]? weekMask = null, 
        Roll roll = Roll.Raise)
        =>Apply(e => e.Dt.AddBusinessDays(n,holidays,weekMask,roll));
        /// <summary>
    /// Check if the date is a business day.
    /// </summary>
    public Series IsBusinessDay(IEnumerable<DateOnly>? holidays = null, bool[]? weekMask = null)
        =>Apply(e => e.Dt.IsBusinessDay(holidays,weekMask));
}

/// <summary>
/// Series String Ops
/// </summary>
public class SeriesStrOps
{
    private readonly Series _series;
    internal SeriesStrOps(Series series) { _series = series; }

    private Series Apply(Func<Expr, Expr> op) 
        => _series.ApplyExpr(op(Polars.Col(_series.Name)));

    /// <summary>
    /// Transfer String to UpperClass.
    /// </summary>
    /// <returns></returns>
    public Series ToUpper() => Apply(e => e.Str.ToUpper());
    /// <summary>
    /// Transfer String to LowerClass.
    /// </summary>
    /// <returns></returns>
    public Series ToLower() => Apply(e => e.Str.ToLower());
    /// <summary>
    /// Get length in bytes.
    /// </summary>
    public Series Len() => Apply(e => e.Str.Len());
    /// <summary>
    /// Check if the string contains a substring that matches a pattern.
    /// </summary>
    /// <param name="pattern"></param>
    /// <returns></returns>
    public Series Contains(string pattern) => Apply(e => e.Str.Contains(pattern));
    /// <summary>
    /// Slice string by length.
    /// </summary>
    /// <param name="offset"></param>
    /// <param name="length"></param>
    /// <returns></returns>
    public Series Slice(long offset, ulong length) => Apply(e => e.Str.Slice(offset, length));
    /// <summary>
    /// Split the string by a substring.
    /// </summary>
    /// <param name="separator"></param>
    /// <returns></returns>
    public Series Split(string separator) => Apply(e => e.Str.Split(separator));
    /// <summary>
    /// Replace charaters in a string.
    /// </summary>
    /// <param name="pattern"></param>
    /// <param name="value"></param>
    /// <param name="useRegex"></param>
    /// <returns></returns>
    public Series ReplaceAll(string pattern, string value, bool useRegex = false)
        => Apply(e => e.Str.ReplaceAll(pattern, value,useRegex));
    /// <summary>
    /// Extract charaters in string by Regex.
    /// </summary>
    /// <param name="pattern"></param>
    /// <param name="groupIndex"></param>
    /// <returns></returns>
    public Series Extract(string pattern, uint groupIndex)
        => Apply(e => e.Str.Extract(pattern, groupIndex));
    // ==========================================
    // Strip / Clean
    // ==========================================

    /// <summary>
    /// Remove leading and trailing characters.
    /// If matches is null, whitespace is removed.
    /// </summary>
    /// <param name="matches">The set of characters to be removed.</param>
    public Series StripChars(string? matches = null)
        => Apply(e => e.Str.StripChars(matches));

    /// <summary>
    /// Remove leading characters.
    /// If matches is null, whitespace is removed.
    /// </summary>
    public Series StripCharsStart(string? matches = null)
        => Apply(e => e.Str.StripCharsStart(matches));

    /// <summary>
    /// Remove trailing characters.
    /// If matches is null, whitespace is removed.
    /// </summary>
    public Series StripCharsEnd(string? matches = null)
        => Apply(e => e.Str.StripCharsEnd(matches));
    /// <summary>
    /// Remove a specific prefix string.
    /// </summary>
    public Series StripPrefix(string prefix)
        => Apply(e => e.Str.StripPrefix(prefix));

    /// <summary>
    /// Remove a specific suffix string.
    /// </summary>
    public Series StripSuffix(string suffix)
        => Apply(e => e.Str.StripPrefix(suffix));

    // ==========================================
    // Boolean Checks (检查)
    // ==========================================

    /// <summary>
    /// Check if the string starts with the given prefix.
    /// </summary>
    public Series StartsWith(string prefix)
        => Apply(e => e.Str.StartsWith(prefix));

    /// <summary>
    /// Check if the string ends with the given suffix.
    /// </summary>
    public Series EndsWith(string suffix)
        => Apply(e => e.Str.StripSuffix(suffix ));

    // ==========================================
    // Temporal Parsing (日期转换)
    // ==========================================

    /// <summary>
    /// Convert string to Date using the specified format.
    /// </summary>
    public Series ToDate(string format)
        => Apply(e => e.Str.ToDate(format));

    /// <summary>
    /// Convert string to Datetime using the specified format.
    /// </summary>
    public Series ToDatetime(string format)
        => Apply(e => e.Str.ToDatetime(format));
}

/// <summary>
/// Series List Ops Namespace
/// </summary>
public class SeriesListOps
{
    private readonly Series _series;
    internal SeriesListOps(Series series) { _series = series; }

    // 复用 ApplyExpr 核心逻辑
    private Series Apply(Func<Expr, Expr> op) 
    {
        // 这里的 ApplyExpr 需要是你 Series 类里的 internal/private 方法
        // 逻辑是: DataFrame.New(this).Select(op(Col(Name)))[0]
        return _series.ApplyExpr(op(Polars.Col(_series.Name)));
    }

    /// <summary>
    /// Get the length of the arrays.
    /// </summary>
    public Series Len() => Apply(e => e.List.Len());

    /// <summary>
    /// Get the first element.
    /// </summary>
    public Series First() => Apply(e => e.List.First());

    /// <summary>
    /// Get the element at the given index.
    /// </summary>
    public Series Get(int index) => Apply(e => e.List.Get(index));

    /// <summary>
    /// Join elements with a separator.
    /// </summary>
    public Series Join(string separator) => Apply(e => e.List.Join(separator));

    /// <summary>
    /// Calculate the sum of the list elements (element-wise).
    /// </summary>
    public Series Sum() => Apply(e => e.List.Sum());

    /// <summary>
    /// Calculate the min of the list elements.
    /// </summary>
    public Series Min() => Apply(e => e.List.Min());

    /// <summary>
    /// Calculate the max of the list elements.
    /// </summary>
    public Series Max() => Apply(e => e.List.Max());

    /// <summary>
    /// Calculate the mean of the list elements.
    /// </summary>
    public Series Mean() => Apply(e => e.List.Mean());

    /// <summary>
    /// Sort the arrays in the list.
    /// </summary>
    public Series Sort(bool descending = false,bool nullsLast=false,bool maintainOrder= false) 
        => Apply(e => e.List.Sort(descending,nullsLast,maintainOrder));

    /// <summary>
    /// Check if the list contains the given item.
    /// </summary>
    public Series Contains(int item) => Apply(e => e.List.Contains(item));
    /// <summary>
    /// Check if the list contains the given item.
    /// </summary>
    public Series Contains(string item) => Apply(e => e.List.Contains(item));
    /// <summary>
    /// Concat this list series with another list series.
    /// Result is a new Series with the lists concatenated.
    /// </summary>
    public Series Concat(Series other)
    {
        // 调用我们刚写的 ApplyBinaryExpr
        // 逻辑：Col(this) .List.Concat ( Col(other) )
        return _series.ApplyBinaryExpr(other, (left, right) => left.List.Concat(right));
    }
    /// <summary>Reverse elements in list.</summary>
    public Series Reverse() => Apply(e => e.List.Reverse());
}
/// <summary>
/// Wrapper for Array (Fixed-Size List) operations on a Series.
/// </summary>
public class SeriesArrayOps
{
    private readonly Series _series;
    internal SeriesArrayOps(Series series) { _series = series; }

    // 复用 ApplyExpr 核心逻辑
    private Series Apply(Func<Expr, Expr> op) 
    {
        return _series.ApplyExpr(op(Polars.Col(_series.Name)));
    }

    // --- Aggregations (聚合：返回的 Series 长度不变，但类型变为标量) ---
    
    /// <summary>Compute the max value of every sub-array.</summary>
    public Series Max() => Apply(e => e.Array.Max());

    /// <summary>Compute the min value of every sub-array.</summary>
    public Series Min() => Apply(e => e.Array.Min());

    /// <summary>Compute the sum of every sub-array.</summary>
    public Series Sum() => Apply(e => e.Array.Sum());

    /// <summary>Compute the mean of every sub-array.</summary>
    public Series Mean() => Apply(e => e.Array.Mean());

    /// <summary>Compute the median of every sub-array.</summary>
    public Series Median() => Apply(e => e.Array.Median());

    /// <summary>Compute the standard deviation of every sub-array.</summary>
    public Series Std(byte ddof = 1) => Apply(e => e.Array.Std(ddof));

    /// <summary>Compute the variance of every sub-array.</summary>
    public Series Var(byte ddof = 1) => Apply(e => e.Array.Var(ddof));

    // --- Boolean (布尔逻辑) ---

    /// <summary>Check if any element in the sub-array is true.</summary>
    public Series Any() => Apply(e => e.Array.Any());

    /// <summary>Check if all elements in the sub-array are true.</summary>
    public Series All() => Apply(e => e.Array.All());

    // --- Sort & Search (排序与搜索) ---

    /// <summary>Sort elements in every sub-array.</summary>
    public Series Sort(bool descending = false, bool nullsLast = false, bool maintainOrder = false) 
        => Apply(e => e.Array.Sort(descending, nullsLast, maintainOrder));

    /// <summary>Reverse elements in every sub-array.</summary>
    public Series Reverse() => Apply(e => e.Array.Reverse());

    /// <summary>Get the index of the minimum value in every sub-array.</summary>
    public Series ArgMin() => Apply(e => e.Array.ArgMin());

    /// <summary>Get the index of the maximum value in every sub-array.</summary>
    public Series ArgMax() => Apply(e => e.Array.ArgMax());

    // --- Structure (结构变换) ---

    /// <summary>Get element at index from every sub-array.</summary>
    public Series Get(int index, bool nullOnOob = true) 
        => Apply(e => e.Array.Get(index, nullOnOob));

    /// <summary>Join elements with a separator.</summary>
    public Series Join(string separator, bool ignoreNulls = true) 
        => Apply(e => e.Array.Join(separator, ignoreNulls));

    /// <summary>
    /// Explode the array column into multiple rows.
    /// The resulting Series will be longer than the original.
    /// </summary>
    public Series Explode() => Apply(e => e.Array.Explode());

    /// <summary>
    /// Convert array to struct. Useful for splitting embeddings into feature columns.
    /// </summary>
    public Series ToStruct() => Apply(e => e.Array.ToStruct());

    /// <summary>
    /// Cast to variable-size List.
    /// </summary>
    public Series ToList() => Apply(e => e.Array.ToList());

    // --- Logic / Set ---

    /// <summary>Check if sub-array contains a specific item.</summary>
    public Series Contains(int item, bool nullsEqual = false) 
        => Apply(e => e.Array.Contains(item, nullsEqual));
    /// <summary>Check if sub-array contains a specific item.</summary>
    public Series Contains(double item, bool nullsEqual = false) 
        => Apply(e => e.Array.Contains(item, nullsEqual));
    /// <summary>Check if sub-array contains a specific item.</summary>   
    public Series Contains(Expr item, bool nullsEqual = false) 
        => Apply(e => e.Array.Contains(item, nullsEqual));

    /// <summary>Get unique elements in every sub-array.</summary>
    public Series Unique(bool stable = false) => Apply(e => e.Array.Unique(stable));
}
/// <summary>
/// Series Struct Ops Namespace
/// </summary>
public class SeriesStructOps
{
    private readonly Series _series;
    internal SeriesStructOps(Series series) { _series = series; }

    private Series Apply(Func<Expr, Expr> op) 
        => _series.ApplyExpr(op(Polars.Col(_series.Name)));

    /// <summary>
    /// Retrieve a field from the struct by name.
    /// Returns a new Series of that field's type.
    /// </summary>
    public Series Field(string name) => Apply(e => e.Struct.Field(name));

    /// <summary>
    /// Retrieve a field from the struct by index.
    /// </summary>
    public Series Field(int index) => Apply(e => e.Struct.Field(index));

    /// <summary>
    /// Rename the fields of the struct.
    /// </summary>
    public Series RenameFields(params string[] names) => Apply(e => e.Struct.RenameFields(names));

    /// <summary>
    /// Convert struct to JSON string.
    /// </summary>
    public Series JsonEncode() => Apply(e => e.Struct.JsonEncode());
    /// <summary>
    /// Unnest the struct column into a DataFrame.
    /// Each field of the struct becomes a separate column.
    /// </summary>
    public DataFrame Unnest()
    {
        // 调用底层 Wrapper
        var dfHandle = PolarsWrapper.SeriesStructUnnest(_series.Handle);
        return new DataFrame(dfHandle);
    }

}