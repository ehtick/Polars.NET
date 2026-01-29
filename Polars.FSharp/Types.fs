namespace Polars.FSharp

open System
open Polars.NET.Core
open Apache.Arrow
open System.Collections.Generic
open Polars.NET.Core.Arrow
open Polars.NET.Core.Data
open Polars.NET.Core.Helpers
open System.Data
open System.Threading.Tasks
open System.Collections.Concurrent
open System.Collections
open System.Reflection
/// --- Series ---
/// <summary>
/// An eager Series holding a single column of data.
/// </summary>
type Series(handle: SeriesHandle) =

    interface IDisposable with member _.Dispose() = handle.Dispose()
    member _.Handle = handle

    member _.Name = PolarsWrapper.SeriesName handle
    member _.Length = PolarsWrapper.SeriesLen handle
    member _.Len = PolarsWrapper.SeriesLen handle
    member _.Count = PolarsWrapper.SeriesLen handle
    member _.NullCount : int64 = PolarsWrapper.SeriesNullCount handle

    // ==========================================
    // Expression Composition (The "ApplyExpr" Pattern)
    // ==========================================

    /// <summary>
    /// Internal Helper: Wrap this Series in a temporary DataFrame, run an Expr, and extract the result.
    /// This allows Series to directly use the full power of the Expression engine without duplicating logic.
    /// </summary>
    member internal this.ApplyExpr(expr: Expr) : Series =
        use dfHandle = PolarsWrapper.SeriesToFrame handle
        use df = new DataFrame(dfHandle)

        use dfRes = df.Select [expr]

        dfRes.[0]
    // ==========================================
    // Binary Op Helper (The "ApplyBinaryExpr" Pattern)
    // ==========================================

    /// <summary>
    /// Internal Helper: Apply a binary expression using two Series.
    /// Handles name collision by creating a temporary renamed series if necessary.
    /// </summary>
    member internal this.ApplyBinaryExpr(other: Series, op: Expr -> Expr -> Expr) : Series =
        let leftName = this.Name
        let rightNameRaw = other.Name
        
        let rightName, rightSeries, tempToDispose =
            if leftName = rightNameRaw then
                let newName = "__other_temp__"
                let cloneHandle = PolarsWrapper.CloneSeries other.Handle 
                let clone = (new Series(cloneHandle)).Rename newName
                newName, clone, Some clone
            else
                rightNameRaw, other, None

        try
            let handles = [| this.Handle; rightSeries.Handle |]
            use dfHandle = PolarsWrapper.DataFrameNew handles
            use df = new DataFrame(dfHandle)

            let expr = op (Expr.Col leftName) (Expr.Col rightName)
            
            use resDf = df.Select [expr]

            resDf.[0]

        finally
            match tempToDispose with
            | Some s -> s.Handle.Dispose()
            | None -> ()
    /// <summary> Access temporal (Date/Time) operations. </summary>
    member this.Dt = SeriesDtNameSpace this
    /// <summary> Access string manipulation operations. </summary>
    member this.Str = SeriesStrNameSpace this
    /// <summary> Access list operations. </summary>
    member this.List = SeriesListNameSpace this
    /// <summary> Access array (fixed-size list) operations. </summary>
    member this.Array = SeriesArrayNameSpace this
    /// <summary> Access struct operations. </summary>
    member this.Struct = SeriesStructNameSpace this
    // --- Basic Operations ---

    /// <summary> Rename the Series in-place. Returns self. </summary>    
    member this.Rename(name: string) = 
        PolarsWrapper.SeriesRename(handle, name)
        this
    /// <summary> Slice the Series. Returns a new Series. </summary>
    /// <param name="offset">Start index.</param>
    /// <param name="length">Length of the slice.</param>
    member this.Slice(offset: int64, length: int64) =
        new Series(PolarsWrapper.SeriesSlice(handle, offset, length))
    /// <summary>
    /// Sort this Series. Returns a new Series.
    /// </summary>
    /// <param name="descending">Sort in descending order (default: false).</param>
    /// <param name="nullsLast">Place null values last (default: false).</param>
    /// <param name="maintainOrder">Maintain the order of equal elements (Stable sort) (default: false).</param>
    /// <param name="multithreaded">Use multiple threads (default: true).</param>
    member this.Sort(
        ?descending: bool,
        ?nullsLast: bool,
        ?maintainOrder: bool,
        ?multithreaded: bool
    ) =
        let desc = defaultArg descending false
        let nLast = defaultArg nullsLast false
        let stable = defaultArg maintainOrder false
        let multi = defaultArg multithreaded true

        new Series(PolarsWrapper.SeriesSort(handle, desc, nLast, multi, stable))

    /// <summary>
    /// Sort this Series in ascending order.
    /// </summary>
    member this.Sort() =
        this.Sort false
    /// <summary>
    /// Get the string representation of the Series Data Type (e.g., "Int64", "String").
    /// </summary>
    member _.DtypeStr = PolarsWrapper.GetSeriesDtypeString handle
    /// <summary> Get the DataType of the Series. </summary>
    member this.DataType : DataType =
        use typeHandle = PolarsWrapper.GetSeriesDataType handle
        
        DataType.FromHandle typeHandle
        
    // ==========================================
    // Missing Data Handling (FillNull & FillNan)
    // ==========================================

    // --- 1. Fill with Scalar (ApplyExpr) ---

    /// <summary> Fill null values with a literal integer. </summary>
    member this.FillNull(fillValue: int) = 
        this.ApplyExpr(Expr.Col(this.Name).FillNull(new Expr(PolarsWrapper.Lit fillValue)))
    /// <summary> Fill null values with a literal double. </summary>
    member this.FillNull(fillValue: double) = 
        this.ApplyExpr(Expr.Col(this.Name).FillNull(new Expr(PolarsWrapper.Lit fillValue)))
    /// <summary> Fill null values with a literal string. </summary>
    member this.FillNull(fillValue: string) = 
        this.ApplyExpr(Expr.Col(this.Name).FillNull(new Expr(PolarsWrapper.Lit fillValue)))
    /// <summary> Fill null values with a literal boolean. </summary>
    member this.FillNull(fillValue: bool) = 
        this.ApplyExpr(Expr.Col(this.Name).FillNull(new Expr(PolarsWrapper.Lit fillValue)))

    /// <summary> Fill floating point NaN values with a literal value. </summary>
    member this.FillNan(fillValue: double) =
        this.ApplyExpr(Expr.Col(this.Name).FillNan(new Expr(PolarsWrapper.Lit fillValue)))

    // --- 2. Fill with Series (ApplyBinaryExpr) ---

    /// <summary>
    /// Fill null values with values from another Series.
    /// Useful for coalescing.
    /// </summary>
    member this.FillNull(fillValue: Series) =
        this.ApplyBinaryExpr(fillValue, fun l r -> l.FillNull r)

    /// <summary>
    /// Fill NaN values with values from another Series.
    /// </summary>
    member this.FillNan(fillValue: Series) =
        this.ApplyBinaryExpr(fillValue, fun l r -> l.FillNan r)
    
    // --- 3. Fill with Expr (Advanced) ---
    
    /// <summary>
    /// Fill nulls using an expression (mostly for internal use or complex literals).
    /// </summary>
    member this.FillNull(expr: Expr) =
        this.ApplyExpr(Expr.Col(this.Name).FillNull expr)
    /// <summary>
    /// Returns a boolean Series indicating which values are null.
    /// </summary>
    member this.IsNull() : Series = 
        new Series(PolarsWrapper.SeriesIsNull handle)
    /// <summary>
    /// Returns a boolean Series indicating which values are not null.
    /// </summary>
    member this.IsNotNull() : Series = 
        new Series(PolarsWrapper.SeriesIsNotNull handle)
    /// <summary>
    /// Drop null values.
    /// </summary>
    member this.DropNulls() : Series =
        new Series(PolarsWrapper.SeriesDropNulls handle)
    /// <summary>
    /// Drop nan values.
    /// </summary>
    member this.DropNans() : Series =
        let expr = Expr.Col(this.Name).DropNans()
        this.ApplyExpr expr
    /// <summary>
    /// Check if the value at the specified index is null.
    /// This is faster than retrieving the value and checking for Option.None.
    /// </summary>
    member _.IsNullAt(index: int) : bool =
        PolarsWrapper.SeriesIsNullAt(handle, int64 index)
    /// <summary>
    /// Get the number of null values in the Series.
    /// This is an O(1) operation (metadata access).
    /// </summary>

    /// <summary> Check if floating point values are NaN. </summary>
    member this.IsNan() = new Series(PolarsWrapper.SeriesIsNan handle)

    /// <summary> Check if floating point values are not NaN. </summary>
    member this.IsNotNan() = new Series(PolarsWrapper.SeriesIsNotNan handle)

    /// <summary> Check if floating point values are finite (not NaN and not Inf). </summary>
    member this.IsFinite() = new Series(PolarsWrapper.SeriesIsFinite handle)

    /// <summary> Check if floating point values are infinite. </summary>
    member this.IsInfinite() = new Series(PolarsWrapper.SeriesIsInfinite handle)
    // ==========================================
    // Uniqueness
    // ==========================================

    /// <summary>
    /// Get unique values (distinct).
    /// </summary>
    member this.Unique() =
        new Series(PolarsWrapper.SeriesUnique handle)

    /// <summary>
    /// Get unique values (distinct), maintaining original order.
    /// </summary>
    member this.UniqueStable() =
        new Series(PolarsWrapper.SeriesUniqueStable handle)

    /// <summary>
    /// Count the number of unique values.
    /// </summary>
    member this.NUnique = 
        PolarsWrapper.SeriesNUnique handle
    /// <summary>
    /// Get a boolean mask indicating which values are unique.
    /// Implemented via Expression engine.
    /// </summary>
    member this.IsUnique() =
        // col(Name).IsUnique()
        let expr = Expr.Col(this.Name).IsUnique()
        this.ApplyExpr expr

    /// <summary>
    /// Get a boolean mask indicating which values are duplicated.
    /// Implemented via Expression engine.
    /// </summary>
    member this.IsDuplicated() =
        let expr = Expr.Col(this.Name).IsDuplicated()
        this.ApplyExpr expr
    // ==========================================
    // UDF / Map (Apply Custom C# / F# Functions)
    // ==========================================

    /// <summary>
    /// Apply a custom function (UDF) to the Series.
    /// Uses Apache Arrow arrays for high-performance data transfer.
    /// </summary>
    /// <param name="func">The compiled UDF (created via Udf.map or Udf.mapOption).</param>
    /// <param name="returnType">The expected output DataType. Required for Polars query planning.</param>
    member this.Map(func: Func<IArrowArray, IArrowArray>, returnType: DataType) =
        // col(Name).Map(func, returnType)
        this.ApplyExpr(Expr.Col(this.Name).Map(func, returnType))

    /// <summary>
    /// Apply a custom function (UDF) assuming the output type is the same as the input.
    /// </summary>
    /// <param name="func">The compiled UDF.</param>
    member this.Map(func: Func<IArrowArray, IArrowArray>) =
        this.Map(func, DataType.SameAsInput)
        
    // ==========================================
    // Optional: High-Level F# Overloads (Sugar)
    // ==========================================
    /// <summary>
    /// Map values using a standard F# function.
    /// Automatically wraps it using Udf.map.
    /// </summary>
    member this.Map<'T, 'U>(f: 'T -> 'U, returnType: DataType) =
        let udf = Udf.map f
        this.Map(udf, returnType)

    /// <summary>
    /// Map values using an F# function that handles Options.
    /// Automatically wraps it using Udf.mapOption.
    /// </summary>
    member this.MapOption<'T, 'U>(f: 'T option -> 'U option, returnType: DataType) =
        let udf = Udf.mapOption f
        this.Map(udf, returnType)
    // ==========================================
    // Math Operations (Forwarding to Expr)
    // ==========================================

    // --- 1. Unary Operations (Scalar / Self) ---

    /// <summary> Round to given decimals. </summary>
    member this.Round(decimals: int) = 
        this.ApplyExpr(Expr.Col(this.Name).Round decimals)

    /// <summary> Round up to the nearest integer. </summary>
    member this.Ceil() = this.ApplyExpr(Expr.Col(this.Name).Ceil())

    /// <summary> Round down to the nearest integer. </summary>
    member this.Floor() = this.ApplyExpr(Expr.Col(this.Name).Floor())

    /// <summary> Absolute value. </summary>
    member this.Abs() = this.ApplyExpr(Expr.Col(this.Name).Abs())

    /// <summary> Element-wise sign. </summary>
    member this.Sign() = this.ApplyExpr(Expr.Col(this.Name).Sign())

    /// <summary> Square root. </summary>
    member this.Sqrt() = this.ApplyExpr(Expr.Col(this.Name).Sqrt())

    /// <summary> Cube root. </summary>
    member this.Cbrt() = this.ApplyExpr(Expr.Col(this.Name).Cbrt())

    /// <summary> Exponential (e^x). </summary>
    member this.Exp() = this.ApplyExpr(Expr.Col(this.Name).Exp())

    /// <summary> Natural logarithm (ln). </summary>
    member this.Ln() = this.ApplyExpr(Expr.Col(this.Name).Ln())

    // --- 2. Binary Operations with Scalar (Treated as Unary Expr) ---

    /// <summary> Power with scalar exponent. </summary>
    member this.Pow(exponent: double) = 
        this.ApplyExpr(Expr.Col(this.Name).Pow exponent)

    /// <summary> Power with integer exponent. </summary>
    member this.Pow(exponent: int) = 
        this.ApplyExpr(Expr.Col(this.Name).Pow exponent)

    /// <summary> Logarithm with scalar base. </summary>
    member this.Log(baseVal: double) = 
        this.ApplyExpr(Expr.Col(this.Name).Log baseVal)
    /// <summary> Bitwise left shift. </summary>
    member this.BitLeftShift(n: int) = 
        this.ApplyExpr(Expr.Col(this.Name).BitLeftShift n)
    /// <summary> Bitwise right shift. </summary>
    member this.BitRightShift(n: int) = 
        this.ApplyExpr(Expr.Col(this.Name).BitRightShift n)

    // --- 3. Binary Operations with Series (Using ApplyBinaryExpr) ---

    /// <summary> Power with Series exponent. </summary>
    member this.Pow(exponent: Series) = 
        this.ApplyBinaryExpr(exponent, fun l r -> l.Pow r)

    /// <summary> Logarithm with Series base. </summary>
    member this.Log(baseVal: Series) = 
        this.ApplyBinaryExpr(baseVal, fun l r -> l.Log r)

    /// <summary> True division (float result). </summary>
    member this.Truediv(other: Series) = 
        this.ApplyBinaryExpr(other, fun l r -> l.Truediv r)
    
    /// <summary> True division (scalar). </summary>
    member this.Truediv(other: double) = 
        this.ApplyExpr(Expr.Col(this.Name).Truediv(new Expr(PolarsWrapper.Lit other)))

    /// <summary> Floor division (integer result). </summary>
    member this.FloorDiv(other: Series) = 
        this.ApplyBinaryExpr(other, fun l r -> l.FloorDiv(r))

    /// <summary> Floor division (scalar). </summary>
    member this.FloorDiv(other: int) = 
        this.ApplyExpr(Expr.Col(this.Name).FloorDiv(new Expr(PolarsWrapper.Lit other)))

    /// <summary> Modulo (remainder). </summary>
    member this.Mod(other: Series) = 
        this.ApplyBinaryExpr(other, fun l r -> l.Mod r)

    /// <summary> Modulo (scalar). </summary>
    member this.Mod(other: int) = 
        this.ApplyExpr(Expr.Col(this.Name).Mod(new Expr(PolarsWrapper.Lit other)))

    // Alias for Mod
    member this.Rem(other: Series) = this.Mod other
    member this.Rem(other: int) = this.Mod other
    // ==========================================
    // Math: Trigonometry
    // ==========================================

    /// <summary> Compute the element-wise sine. </summary>
    member this.Sin() = this.ApplyExpr(Expr.Col(this.Name).Sin())

    /// <summary> Compute the element-wise cosine. </summary>
    member this.Cos() = this.ApplyExpr(Expr.Col(this.Name).Cos())

    /// <summary> Compute the element-wise tangent. </summary>
    member this.Tan() = this.ApplyExpr(Expr.Col(this.Name).Tan())

    /// <summary> Compute the element-wise inverse sine. </summary>
    member this.ArcSin() = this.ApplyExpr(Expr.Col(this.Name).ArcSin())

    /// <summary> Compute the element-wise inverse cosine. </summary>
    member this.ArcCos() = this.ApplyExpr(Expr.Col(this.Name).ArcCos())

    /// <summary> Compute the element-wise inverse tangent. </summary>
    member this.ArcTan() = this.ApplyExpr(Expr.Col(this.Name).ArcTan())

    // ==========================================
    // Math: Hyperbolic
    // ==========================================

    /// <summary> Compute the element-wise hyperbolic sine. </summary>
    member this.Sinh() = this.ApplyExpr(Expr.Col(this.Name).Sinh())

    /// <summary> Compute the element-wise hyperbolic cosine. </summary>
    member this.Cosh() = this.ApplyExpr(Expr.Col(this.Name).Cosh())

    /// <summary> Compute the element-wise hyperbolic tangent. </summary>
    member this.Tanh() = this.ApplyExpr(Expr.Col(this.Name).Tanh())

    /// <summary> Compute the element-wise inverse hyperbolic sine. </summary>
    member this.ArcSinh() = this.ApplyExpr(Expr.Col(this.Name).ArcSinh())

    /// <summary> Compute the element-wise inverse hyperbolic cosine. </summary>
    member this.ArcCosh() = this.ApplyExpr(Expr.Col(this.Name).ArcCosh())

    /// <summary> Compute the element-wise inverse hyperbolic tangent. </summary>
    member this.ArcTanh() = this.ApplyExpr(Expr.Col(this.Name).ArcTanh())
    // ==========================================
    // Shift, Diff & Fill
    // ==========================================

    /// <summary>
    /// Shift the values by a given period.
    /// </summary>
    member this.Shift(n: int64) = 
        this.ApplyExpr(Expr.Col(this.Name).Shift n)

    member this.Shift(n: int) = this.Shift(int64 n)
    
    /// <summary> Shift by 1. </summary>
    member this.Shift() = this.Shift(1L)

    /// <summary>
    /// Calculate the difference with a given period.
    /// </summary>
    member this.Diff(n: int64) = 
        this.ApplyExpr(Expr.Col(this.Name).Diff n)

    member this.Diff(n: int) = this.Diff(int64 n)
    
    /// <summary> Diff by 1. </summary>
    member this.Diff() = this.Diff(1L)

    /// <summary>
    /// Fill null values with the previous non-null value.
    /// </summary>
    /// <param name="limit">Max number of consecutive nulls to fill.</param>
    member this.ForwardFill(?limit: int) =
        this.ApplyExpr(Expr.Col(this.Name).ForwardFill(?limit=limit))

    /// <summary>
    /// Fill null values with the next non-null value.
    /// </summary>
    /// <param name="limit">Max number of consecutive nulls to fill.</param>
    member this.BackwardFill(?limit: int) =
        this.ApplyExpr(Expr.Col(this.Name).BackwardFill(?limit=limit))

    // Alias
    member this.FFill ?limit = this.ForwardFill(?limit=limit)
    member this.BFill ?limit = this.BackwardFill(?limit=limit)

    // ==========================================
    // Rolling Window Functions
    // ==========================================

    // --- Rolling Min ---
    member this.RollingMin(windowSize: string, ?minPeriod: int) =
        this.ApplyExpr(Expr.Col(this.Name).RollingMin(windowSize, ?minPeriod=minPeriod))
    
    member this.RollingMin(windowSize: TimeSpan, ?minPeriod: int) =
        this.ApplyExpr(Expr.Col(this.Name).RollingMin(windowSize, ?minPeriod=minPeriod))

    // --- Rolling Max ---
    member this.RollingMax(windowSize: string, ?minPeriod: int) =
        this.ApplyExpr(Expr.Col(this.Name).RollingMax(windowSize, ?minPeriod=minPeriod))

    member this.RollingMax(windowSize: TimeSpan, ?minPeriod: int) =
        this.ApplyExpr(Expr.Col(this.Name).RollingMax(windowSize, ?minPeriod=minPeriod))

    // --- Rolling Mean ---
    member this.RollingMean(windowSize: string, ?minPeriod: int) =
        this.ApplyExpr(Expr.Col(this.Name).RollingMean(windowSize, ?minPeriod=minPeriod))

    member this.RollingMean(windowSize: TimeSpan, ?minPeriod: int) =
        this.ApplyExpr(Expr.Col(this.Name).RollingMean(windowSize, ?minPeriod=minPeriod))

    // --- Rolling Sum ---
    member this.RollingSum(windowSize: string, ?minPeriod: int) =
        this.ApplyExpr(Expr.Col(this.Name).RollingSum(windowSize, ?minPeriod=minPeriod))

    member this.RollingSum(windowSize: TimeSpan, ?minPeriod: int) =
        this.ApplyExpr(Expr.Col(this.Name).RollingSum(windowSize, ?minPeriod=minPeriod))
    // ==========================================
    // TopK / BottomK
    // ==========================================

    member this.TopK(k: int) = 
        this.ApplyExpr(Expr.Col(this.Name).TopK k)

    member this.BottomK(k: int) = 
        this.ApplyExpr(Expr.Col(this.Name).BottomK k)

    /// <summary>
    /// Get top k elements of this Series, sorted by another Series.
    /// </summary>
    member this.TopKBy(k: int, by: Series, ?reverse: bool) =
        let r = defaultArg reverse false
        this.ApplyBinaryExpr(by, fun me other -> me.TopKBy(k, other, r))

    /// <summary>
    /// Get bottom k elements of this Series, sorted by another Series.
    /// </summary>
    member this.BottomKBy(k: int, by: Series, ?reverse: bool) =
        let r = defaultArg reverse false
        this.ApplyBinaryExpr(by, fun me other -> me.BottomKBy(k, other, r))
    // ==========================================
    // Static Constructors
    // ==========================================
    /// <summary>
    /// Create a Series from any sequence (Array, List, Seq).
    /// Supports:
    /// - Primitives ('T)
    /// - Option types ('T option)
    /// - ValueOption types ('T voption)
    /// </summary>
    static member create(name: string, data: seq<'T>) =

        let arr = Seq.toArray data
        
        let handle = SeriesFactory.Create(name, arr)
        new Series(handle)
    
    // -------------------------------------------------------------------------
    // Fixed Size List / Array (Matrix)
    // -------------------------------------------------------------------------

    /// <summary>
    /// Create a FixedSizeList Series from a 2D Array (Matrix).
    /// Shape: [Rows, Width] -> Array[Width]
    /// Supported Types: Primitives, Decimal, Int128
    /// </summary>
    static member ofArray2D<'T
        when 'T : struct 
        and 'T : unmanaged
        and 'T :> ValueType   
        and 'T : (new : unit -> 'T)> 
        (name: string, data: 'T[,]) =
            new Series(PolarsWrapper.SeriesNewFixedArray(name, data))
    // ========================================================================
    // Unified Entry Points (Delegating to SeriesFactory)
    // ========================================================================

    /// <summary>
    /// Create a Series from a sequence of Options (F# style nullables).
    /// Automatically handles all supported types (int, float, string, datetime, etc.)
    /// </summary>
    static member ofOptionSeq<'T>(name: string, data: seq<'T option>) : Series =
        Series.create(name, data)

    /// <summary>
    /// Create a Series from a sequence of ValueOptions (Struct nullables).
    /// Automatically handles all supported types.
    /// </summary>
    static member ofVOptionSeq<'T>(name: string, data: seq<'T voption>) : Series =
        Series.create(name, data)

    // --- Scalar Access ---
    
    /// <summary> Get value as Int64 Option. Handles Int32/Int64 etc. </summary>
    member _.Int(index: int) : int64 option = 
        PolarsWrapper.SeriesGetInt(handle, int64 index) |> Option.ofNullable

    member _.Int128(index: int) : Int128 option = 
        PolarsWrapper.SeriesGetInt128(handle, int64 index) |> Option.ofNullable

    /// <summary> Get value as Double Option. Handles Float32/Float64. </summary>
    member _.Float(index: int) : float option = 
        PolarsWrapper.SeriesGetDouble(handle, int64 index) |> Option.ofNullable

    /// <summary> Get value as String Option. </summary>
    member _.String(index: int) : string option = 
        PolarsWrapper.SeriesGetString(handle, int64 index) |> Option.ofObj

    /// <summary> Get value as Boolean Option. </summary>
    member _.Bool(index: int) : bool option = 
        PolarsWrapper.SeriesGetBool(handle, int64 index) |> Option.ofNullable

    /// <summary> Get value as Decimal Option. </summary>
    member _.Decimal(index: int) : decimal option = 
        PolarsWrapper.SeriesGetDecimal(handle, int64 index) |> Option.ofNullable

    // Temporal Type
    member _.Date(index: int) : DateOnly option = 
        PolarsWrapper.SeriesGetDate(handle, int64 index) |> Option.ofNullable

    member _.Time(index: int) : TimeOnly option = 
        PolarsWrapper.SeriesGetTime(handle, int64 index) |> Option.ofNullable

    member _.Datetime(index: int) : DateTime option = 
        PolarsWrapper.SeriesGetDatetime(handle, int64 index) |> Option.ofNullable

    member _.Duration(index: int) : TimeSpan option = 
        PolarsWrapper.SeriesGetDuration(handle, int64 index) |> Option.ofNullable
    // --- Aggregations (Returning Series of len 1) ---
    member this.Sum() = new Series(PolarsWrapper.SeriesSum handle)
    member this.Mean() = new Series(PolarsWrapper.SeriesMean handle)
    member this.Min() = new Series(PolarsWrapper.SeriesMin handle)
    member this.Max() = new Series(PolarsWrapper.SeriesMax handle)
    /// <summary>
    /// Get the standard deviation.
    /// </summary>
    /// <param name="ddof">Delta Degrees of Freedom. Default is 1.</param>
    member this.Std(?ddof: int) = 
        this.ApplyExpr(Expr.Col(this.Name).Std(?ddof=ddof))

    /// <summary>
    /// Get the variance.
    /// </summary>
    /// <param name="ddof">Delta Degrees of Freedom. Default is 1.</param>
    member this.Var(?ddof: int) = 
        this.ApplyExpr(Expr.Col(this.Name).Var(?ddof=ddof))

    /// <summary>
    /// Get the median.
    /// </summary>
    member this.Median() = 
        this.ApplyExpr(Expr.Col(this.Name).Median())

    /// <summary>
    /// Get the quantile.
    /// </summary>
    /// <param name="q">Quantile between 0.0 and 1.0.</param>
    /// <param name="interpolation">Interpolation method ("nearest", "higher", "lower", "midpoint", "linear"). Default "linear".</param>
    member this.Quantile(q: float, ?interpolation: QuantileMethod) =
        this.ApplyExpr(Expr.Col(this.Name).Quantile(q, ?interpolation=interpolation))
    /// <summary>
    /// Count the occurrences of unique values.
    /// Similar to SQL `GROUP BY val COUNT(*)`.
    /// </summary>
    /// <param name="sort">Sort the output by count in descending order. Default is true.</param>
    /// <param name="parallel">Execute in parallel. Default is true.</param>
    /// <param name="name">The name of the count column. Default is "count".</param>
    /// <param name="normalize">If true, the count column will contain probabilities instead of counts. Default is false.</param>
    member this.ValueCounts(?sort: bool, ?paralleling: bool, ?name: string, ?normalize: bool) =
        let sort = defaultArg sort true
        let paralleling = defaultArg paralleling true
        let name = defaultArg name "count"
        let normalize = defaultArg normalize false
        
        let dfHandle = PolarsWrapper.SeriesValueCounts(this.Handle, sort, paralleling, name, normalize)
        new DataFrame(dfHandle)

    // --- Operators (Arithmetic) ---

    static member (+) (lhs: Series, rhs: Series) = new Series(PolarsWrapper.SeriesAdd(lhs.Handle, rhs.Handle))
    static member (-) (lhs: Series, rhs: Series) = new Series(PolarsWrapper.SeriesSub(lhs.Handle, rhs.Handle))
    static member (*) (lhs: Series, rhs: Series) = new Series(PolarsWrapper.SeriesMul(lhs.Handle, rhs.Handle))
    static member (/) (lhs: Series, rhs: Series) = new Series(PolarsWrapper.SeriesDiv(lhs.Handle, rhs.Handle))
    static member (%) (lhs: Series, rhs: Series) = lhs.Mod rhs

    // --- Operators (Comparison) ---

    static member (.=) (lhs: Series, rhs: Series) = new Series(PolarsWrapper.SeriesEq(lhs.Handle, rhs.Handle))
    static member (!=) (lhs: Series, rhs: Series) = new Series(PolarsWrapper.SeriesNeq(lhs.Handle, rhs.Handle))
    static member (.>) (lhs: Series, rhs: Series) = new Series(PolarsWrapper.SeriesGt(lhs.Handle, rhs.Handle))
    static member (.<) (lhs: Series, rhs: Series) = new Series(PolarsWrapper.SeriesLt(lhs.Handle, rhs.Handle))

    static member (.>=) (lhs: Series, rhs: Series) = new Series(PolarsWrapper.SeriesGtEq(lhs.Handle, rhs.Handle))
    static member (.<=) (lhs: Series, rhs: Series) = new Series(PolarsWrapper.SeriesLtEq(lhs.Handle, rhs.Handle))

    // --- Broadcasting Helpers (Scalar Ops) ---
    static member (+) (lhs: Series, rhs: int) = lhs + Series.create("lit", [rhs])
    static member (+) (lhs: Series, rhs: double) = lhs + Series.create("lit", [rhs])
    static member (-) (lhs: Series, rhs: int) = lhs - Series.create("lit", [rhs])
    static member (-) (lhs: Series, rhs: double) = lhs - Series.create("lit", [rhs])
    
    static member (*) (lhs: Series, rhs: int) = lhs * Series.create("lit", [rhs])
    static member (*) (lhs: Series, rhs: double) = lhs * Series.create("lit", [rhs])
    
    static member (/) (lhs: Series, rhs: int) = lhs / Series.create("lit", [rhs])
    static member (/) (lhs: Series, rhs: double) = lhs / Series.create("lit", [rhs])
    static member (%) (lhs: Series, rhs: int) = lhs.Mod rhs
    static member (<<<) (lhs: Series, rhs: int) = lhs.BitLeftShift rhs
    static member (>>>) (lhs: Series, rhs: int) = lhs.BitRightShift rhs

    // Comparison with Scalar
    static member (.>) (lhs: Series, rhs: int) = lhs .> Series.create("lit", [rhs])
    static member (.>) (lhs: Series, rhs: double) = lhs .> Series.create("lit", [rhs])
    static member (.<) (lhs: Series, rhs: int) = lhs .< Series.create("lit", [rhs])
    static member (.<) (lhs: Series, rhs: double) = lhs .< Series.create("lit", [rhs])
    static member (.>=) (lhs: Series, rhs: int) = lhs .>= Series.create("lit", [rhs])
    static member (.<=) (lhs: Series, rhs: double) = lhs .<= Series.create("lit", [rhs])
    
    static member (.=) (lhs: Series, rhs: int) = lhs .= Series.create("lit", [rhs])
    static member (.=) (lhs: Series, rhs: double) = lhs .= Series.create("lit", [rhs])
    static member (.=) (lhs: Series, rhs: string) = lhs .= Series.create("lit", [rhs])
    static member (.!=) (lhs: Series, rhs: int) = lhs != Series.create("lit", [rhs])
    static member (.!=) (lhs: Series, rhs: string) = lhs != Series.create("lit", [rhs])
    // ==========================================
    // Unified Accessor (Fast Path + Universal Path)
    // ==========================================

    /// <summary>
    /// Get an item at the specified index.
    /// Supports primitives (int, float, bool, string) via fast native path,
    /// and complex types (Struct, List, DateTime) via Arrow infrastructure.
    /// </summary>
    member this.GetValue<'T>(index: int64) : 'T =
        let len = this.Length
        if index < 0L || index >= len then
            raise (IndexOutOfRangeException(sprintf "Index %d is out of bounds for Series length %d." index len))

        // Consistent Null Check
        if PolarsWrapper.SeriesIsNullAt(handle, index) then
            Unchecked.defaultof<'T>
        else
            // 2. Getvalue
            let t = typeof<'T>
            
            // --- Integer Family ---
            if t = typeof<int> || t = typeof<int option> || t = typeof<Nullable<int>> then
                let v = int (PolarsWrapper.SeriesGetInt(handle, index).Value)
                if t = typeof<int option> then box (Some v) |> unbox<'T>
                else box v |> unbox<'T>

            else if t = typeof<int64> || t = typeof<int64 option> || t = typeof<Nullable<int64>> then
                let v = PolarsWrapper.SeriesGetInt(handle, index).Value
                if t = typeof<int64 option> then box (Some v) |> unbox<'T>
                else box v |> unbox<'T>

            else if t = typeof<Int128> || t = typeof<Int128 option> || t = typeof<Nullable<Int128>> then
                let v = PolarsWrapper.SeriesGetInt128(handle, index).Value
                if t = typeof<Int128 option> then box (Some v) |> unbox<'T>
                else box v |> unbox<'T>

            // --- Float Family ---
            else if t = typeof<double> || t = typeof<double option> || t = typeof<Nullable<double>> then
                let v = PolarsWrapper.SeriesGetDouble(handle, index).Value
                if t = typeof<double option> then box (Some v) |> unbox<'T>
                else box v |> unbox<'T>

            else if t = typeof<float32> || t = typeof<float32 option> || t = typeof<Nullable<float32>> then
                let v = float32 (PolarsWrapper.SeriesGetDouble(handle, index).Value)
                if t = typeof<float32 option> then box (Some v) |> unbox<'T>
                else box v |> unbox<'T>

            // --- Boolean ---
            else if t = typeof<bool> || t = typeof<bool option> || t = typeof<Nullable<bool>> then
                let v = PolarsWrapper.SeriesGetBool(handle, index).Value
                if t = typeof<bool option> then box (Some v) |> unbox<'T>
                else box v |> unbox<'T>

            // --- String ---
            else if t = typeof<string> || t = typeof<string option> then
                let v = PolarsWrapper.SeriesGetString(handle, index)
                if t = typeof<string option> then box (Some v) |> unbox<'T>
                else box v |> unbox<'T>

            // --- Decimal ---
            else if t = typeof<decimal> || t = typeof<decimal option> || t = typeof<Nullable<decimal>> then
                let v = PolarsWrapper.SeriesGetDecimal(handle, index).Value
                if t = typeof<decimal option> then box (Some v) |> unbox<'T>
                else box v |> unbox<'T>

            // --- Temporal ---
            else if t = typeof<DateOnly> || t = typeof<DateOnly option> || t = typeof<Nullable<DateOnly>> then
                let v = PolarsWrapper.SeriesGetDate(handle, index).Value
                if t = typeof<DateOnly option> then box (Some v) |> unbox<'T>
                else box v |> unbox<'T>

            else if t = typeof<TimeOnly> || t = typeof<TimeOnly option> || t = typeof<Nullable<TimeOnly>> then
                let v = PolarsWrapper.SeriesGetTime(handle, index).Value
                if t = typeof<TimeOnly option> then box (Some v) |> unbox<'T>
                else box v |> unbox<'T>
                
            else if t = typeof<TimeSpan> || t = typeof<TimeSpan option> || t = typeof<Nullable<TimeSpan>> then
                let v = PolarsWrapper.SeriesGetDuration(handle, index).Value
                if t = typeof<TimeSpan option> then box (Some v) |> unbox<'T>
                else box v |> unbox<'T>

            // --- Complex Types (Arrow Fallback) ---
            else
                use slicedHandle = PolarsWrapper.SeriesSlice(handle, index, 1L)
                use dfHandle = PolarsWrapper.SeriesToFrame slicedHandle
                use batch = ArrowFfiBridge.ExportDataFrame dfHandle
                let column = batch.Column(0)
                ArrowReader.ReadItem<'T>(column, 0)
    /// <summary>
    /// Get a value as an F# List ('T list).
    /// Automatically handles conversion from .NET List (ResizeArray).
    /// </summary>
    member this.GetList<'Elem>(index: int64) : 'Elem list =
        let netList = this.GetValue<ResizeArray<'Elem>> index
        
        if isNull netList then 
            []
        else 
            netList |> List.ofSeq
                
    /// <summary>
    /// [Indexer] Access value at specific index as boxed object.
    /// Syntax: series.[index]
    /// </summary>
    member this.Item (index: int) : obj =
        let idx = int64 index
        
        match this.DataType with
        | DataType.Boolean -> box (this.GetValue<bool option> idx) 
        
        | DataType.Int8 -> box (this.GetValue<int8 option> idx)
        | DataType.Int16 -> box (this.GetValue<int16 option> idx)
        | DataType.Int32 -> box (this.GetValue<int32 option> idx)
        | DataType.Int64 -> box (this.GetValue<int64 option> idx)
        | DataType.Int128 -> box (this.GetValue<Int128 option> idx)
        
        | DataType.UInt8 -> box (this.GetValue<uint8 option> idx)
        | DataType.UInt16 -> box (this.GetValue<uint16 option> idx)
        | DataType.UInt32 -> box (this.GetValue<uint32 option> idx)
        | DataType.UInt64 -> box (this.GetValue<uint64 option> idx)
        | DataType.UInt128 -> box (this.GetValue<UInt128 option> idx)
        
        | DataType.Float32 -> box (this.GetValue<float32 option> idx)
        | DataType.Float64 -> box (this.GetValue<double option> idx)
        
        | DataType.Decimal _ -> box (this.GetValue<decimal option> idx)
        
        | DataType.String -> box (this.GetValue<string option> idx)
        
        | DataType.Date -> box (this.GetValue<DateOnly option> idx)
        | DataType.Time -> box (this.GetValue<TimeOnly option> idx)
        | DataType.Datetime _ -> box (this.GetValue<DateTime option> idx)
        | DataType.Duration _ -> box (this.GetValue<TimeSpan option> idx)
        
        | DataType.Binary -> box (this.GetValue<byte[] option> idx)

        // Complex Type
        | DataType.List _ -> this.GetValue<obj> idx
        | DataType.Struct _ -> this.GetValue<obj> idx
        | DataType.Array _ -> this.GetValue<obj> idx
        
        | _ -> failwithf "Indexer not fully implemented for type: %A" this.DataType
    /// <summary>
    /// Get an item as an F# Option.
    /// Ideal for safe handling of nulls in Polars series.
    /// </summary>
    member this.GetValueOption<'T>(index: int64) : 'T option =
        this.GetValue<'T option> index
    // ==========================================
    // Interop 
    // ==========================================
    member this.ToFrame() : DataFrame =
        let h = PolarsWrapper.SeriesToFrame handle
        new DataFrame(h)

    member this.Cast(dtype: DataType) : Series =
        use typeHandle = dtype.CreateHandle()
        let newHandle = PolarsWrapper.SeriesCast(handle, typeHandle)
        new Series(newHandle)
    member this.ToArrow() : IArrowArray =
        PolarsWrapper.SeriesToArrow handle
    member this.ToArray<'T>() =
        let col = this.ToArrow()
        ArrowReader.ReadColumn<'T> col
    member this.Show() =
        this.ToFrame().Show()


and SeriesDtNameSpace(parent: Series) =
    
    // Helper: col("Name").Dt.Op(...)
    let apply (op: Expr -> Expr) =
        let expr = Expr.Col parent.Name |> op
        parent.ApplyExpr expr

    // --- Extraction ---
    
    member _.Year() = apply (fun e -> e.Dt.Year())
    member _.Quarter() = apply (fun e -> e.Dt.Quarter())
    member _.Month() = apply (fun e -> e.Dt.Month())
    member _.Day() = apply (fun e -> e.Dt.Day())
    member _.Hour() = apply (fun e -> e.Dt.Hour())
    member _.Minute() = apply (fun e -> e.Dt.Minute())
    member _.Second() = apply (fun e -> e.Dt.Second())
    member _.Millisecond() = apply (fun e -> e.Dt.Millisecond())
    member _.Microsecond() = apply (fun e -> e.Dt.Microsecond())
    member _.Nanosecond() = apply (fun e -> e.Dt.Nanosecond())
    member _.OrdinalDay() = apply (fun e -> e.Dt.OrdinalDay())
    member _.Weekday() = apply (fun e -> e.Dt.Weekday())
    member _.Date() = apply (fun e -> e.Dt.Date())
    member _.Time() = apply (fun e -> e.Dt.Time())

    // --- Formatting ---

    /// <summary> Format datetime to string using the given format string (strftime). </summary>
    member _.ToString(format: string) = 
        apply (fun e -> e.Dt.ToString format)

    /// <summary> Default ISO format. </summary>
    member this.ToString() = 
        this.ToString "%Y-%m-%dT%H:%M:%S%.f"

    // --- Manipulation ---

    member _.Truncate(every: string) = 
        apply (fun e -> e.Dt.Truncate every)

    member _.Round(every: string) = 
        apply (fun e -> e.Dt.Round every)

    member _.OffsetBy(duration: string) =
        apply (fun e -> e.Dt.OffsetBy duration)
    member _.OffsetBy(duration: Expr) =
        apply (fun e -> e.Dt.OffsetBy duration)

    // --- Conversion ---

    member _.TimestampMicros() = apply (fun e -> e.Dt.TimestampMicros())
    member _.TimestampMillis() = apply (fun e -> e.Dt.TimestampMillis())

    // --- TimeZone ---

    member _.ConvertTimeZone(timeZone: string) =
        apply (fun e -> e.Dt.ConvertTimeZone timeZone)

    member _.ReplaceTimeZone(timeZone: string option, ?ambiguous: string, ?nonExistent: string) =
        apply (fun e -> e.Dt.ReplaceTimeZone(timeZone, ?ambiguous=ambiguous, ?nonExistent=nonExistent))

    member this.ReplaceTimeZone(timeZone: string, ?ambiguous, ?nonExistent) =
        this.ReplaceTimeZone(Some timeZone, ?ambiguous=ambiguous, ?nonExistent=nonExistent)

    // --- Business Days ---

    /// <summary>
    /// Add business days (using integer).
    /// </summary>
    member _.AddBusinessDays(n: int, ?weekMask, ?holidays, ?roll) =
        apply (fun e -> 
            e.Dt.AddBusinessDays(
                n, 
                ?weekMask=weekMask, 
                ?holidays=holidays, 
                ?roll=roll
            )
        )

    /// <summary>
    /// Is Business Day check.
    /// </summary>
    member _.IsBusinessDay(?weekMask, ?holidays) =
        apply (fun e -> 
            e.Dt.IsBusinessDay(?weekMask=weekMask, ?holidays=holidays)
        )

and SeriesStrNameSpace(parent: Series) =
    
    let apply (op: Expr -> Expr) =
        let expr = Expr.Col parent.Name |> op
        parent.ApplyExpr expr

    /// <summary> Convert to uppercase. </summary>
    member _.ToUpper() = apply (fun e -> e.Str.ToUpper())

    /// <summary> Convert to lowercase. </summary>
    member _.ToLower() = apply (fun e -> e.Str.ToLower())

    /// <summary> Get length in bytes. </summary>
    member _.Len() = apply (fun e -> e.Str.Len())

    /// <summary> Slice the string. </summary>
    member _.Slice(offset: int64, length: uint64) = 
        apply (fun e -> e.Str.Slice(offset, length))

    /// <summary> Replace all occurrences of a pattern. </summary>
    member _.ReplaceAll(pattern: string, value: string, ?useRegex: bool) =
        apply (fun e -> e.Str.ReplaceAll(pattern, value, ?useRegex=useRegex))

    /// <summary> Extract the target capture group from regex pattern. </summary>
    member _.Extract(pattern: string, groupIndex: int) =
        apply (fun e -> e.Str.Extract(pattern, groupIndex))

    /// <summary> Check if string contains pattern. </summary>
    member _.Contains(pat: string) = 
        apply (fun e -> e.Str.Contains pat)

    /// <summary> Split string by separator. Returns a List column. </summary>
    member _.Split(separator: string) = 
        apply (fun e -> e.Str.Split separator)

    // --- Trimming ---

    /// <summary> Remove leading and trailing characters. </summary>
    member _.Strip(?matches: string) = 
        apply (fun e -> e.Str.Strip(?matches=matches))

    /// <summary> Remove leading characters. </summary>
    member _.LStrip(?matches: string) = 
        apply (fun e -> e.Str.LStrip(?matches=matches))

    /// <summary> Remove trailing characters. </summary>
    member _.RStrip(?matches: string) = 
        apply (fun e -> e.Str.RStrip(?matches=matches))

    member _.StripPrefix(prefix: string) = 
        apply (fun e -> e.Str.StripPrefix prefix)

    member _.StripSuffix(suffix: string) = 
        apply (fun e -> e.Str.StripSuffix suffix)

    // --- Checks ---

    member _.StartsWith(prefix: string) = 
        apply (fun e -> e.Str.StartsWith prefix)

    member _.EndsWith(suffix: string) = 
        apply (fun e -> e.Str.EndsWith suffix)

    // --- Parsing ---

    /// <summary> Parse string to Date. </summary>
    member _.ToDate(format: string) = 
        apply (fun e -> e.Str.ToDate format)

    /// <summary> Parse string to Datetime. </summary>
    member _.ToDatetime(format: string) = 
        apply (fun e -> e.Str.ToDatetime format)

and SeriesListNameSpace(parent: Series) =
    
    // Unary helper
    let apply (op: Expr -> Expr) =
        let expr = Expr.Col parent.Name |> op
        parent.ApplyExpr expr

    // --- Unary Ops (Forward to Expr.List) ---
    
    member _.First() = apply (fun e -> e.List.First())
    member _.Get(index: int) = apply (fun e -> e.List.Get index)
    member _.Join(sep: string) = apply (fun e -> e.List.Join sep)
    member _.Len() = apply (fun e -> e.List.Len())
    member _.Sum() = apply (fun e -> e.List.Sum())
    member _.Min() = apply (fun e -> e.List.Min())
    member _.Max() = apply (fun e -> e.List.Max())
    member _.Mean() = apply (fun e -> e.List.Mean())
    member _.Reverse() = apply (fun e -> e.List.Reverse())
    
    member _.Sort(?descending, ?nullsLast, ?maintainOrder) =
        apply (fun e -> e.List.Sort(?descending=descending, ?nullsLast=nullsLast, ?maintainOrder=maintainOrder))

    // --- Binary Ops ---

    /// <summary>
    /// Concat with another Series.
    /// Logic: [this_val, other_val]
    /// </summary>
    member _.Concat(other: Series) =
        parent.ApplyBinaryExpr(other, (fun l r -> l.List.Concat r))

    // --- Search ---

    member _.Contains(item: int) = apply (fun e -> e.List.Contains item)
    member _.Contains(item: string) = apply (fun e -> e.List.Contains item)

and SeriesArrayNameSpace(parent: Series) =
    
    let apply (op: Expr -> Expr) =
        let expr = Expr.Col parent.Name |> op
        parent.ApplyExpr expr

    // --- Aggregations ---

    member _.Sum() = apply (fun e -> e.Array.Sum())
    member _.Min() = apply (fun e -> e.Array.Min())
    member _.Max() = apply (fun e -> e.Array.Max())
    member _.Mean() = apply (fun e -> e.Array.Mean())
    member _.Median() = apply (fun e -> e.Array.Median())
    
    member _.Std(?ddof: int) = 
        apply (fun e -> e.Array.Std(?ddof=ddof))
    
    member _.Var(?ddof: int) = 
        apply (fun e -> e.Array.Var(?ddof=ddof))

    // --- Boolean / Search ---

    member _.Any() = apply (fun e -> e.Array.Any())
    member _.All() = apply (fun e -> e.Array.All())

    /// <summary> Check if array contains an Item (Expr). </summary>
    member _.Contains(item: Expr, ?nullsEqual: bool) =
        apply (fun e -> e.Array.Contains(item, ?nullsEqual=nullsEqual))

    /// <summary> Check if array contains a literal string. </summary>
    member _.Contains(item: string, ?nullsEqual: bool) =
        apply (fun e -> e.Array.Contains(item, ?nullsEqual=nullsEqual))

    /// <summary> Check if array contains a literal int. </summary>
    member _.Contains(item: int, ?nullsEqual: bool) =
        apply (fun e -> e.Array.Contains(item, ?nullsEqual=nullsEqual))

    // --- Operations ---

    member _.Unique(?stable: bool) = 
        apply (fun e -> e.Array.Unique(?stable=stable))

    member _.Join(separator: string, ?ignoreNulls: bool) =
        apply (fun e -> e.Array.Join(separator, ?ignoreNulls=ignoreNulls))

    member _.Sort(?descending: bool, ?nullsLast: bool, ?maintainOrder: bool) =
        apply (fun e -> 
            e.Array.Sort(
                ?descending=descending, 
                ?nullsLast=nullsLast, 
                ?maintainOrder=maintainOrder
            )
        )

    member _.Reverse() = apply (fun e -> e.Array.Reverse())

    member _.ArgMin() = apply (fun e -> e.Array.ArgMin())
    member _.ArgMax() = apply (fun e -> e.Array.ArgMax())

    member _.Explode() = apply (fun e -> e.Array.Explode())

    // --- Indexing ---

    member _.Get(index: Expr, ?nullOnOob: bool) =
        apply (fun e -> e.Array.Get(index, ?nullOnOob=nullOnOob))

    member _.Get(index: int, ?nullOnOob: bool) =
        apply (fun e -> e.Array.Get(index, ?nullOnOob=nullOnOob))

    // --- Conversion ---

    /// <summary> Convert to variable length List. </summary>
    member _.ToList() = apply (fun e -> e.Array.ToList())

    /// <summary> Convert to Struct. </summary>
    member _.ToStruct() = apply (fun e -> e.Array.ToStruct())

and SeriesStructNameSpace(parent: Series) =
    
    let apply (op: Expr -> Expr) =
        let expr = Expr.Col parent.Name |> op
        parent.ApplyExpr(expr)

    /// <summary> Retrieve a field from the struct by name. </summary>
    member _.Field(name: string) = 
        apply (fun e -> e.Struct.Field name)

    /// <summary> Retrieve a field from the struct by index. </summary>
    member _.Field(index: int) = 
        apply (fun e -> e.Struct.Field index)

    /// <summary> Rename the fields of the struct. </summary>
    member _.RenameFields(names: string list) =
        apply (fun e -> e.Struct.RenameFields names)

    /// <summary> Convert struct to JSON string. </summary>
    member _.JsonEncode() = 
        apply (fun e -> e.Struct.JsonEncode())
// --- Frames ---

/// <summary>
/// An eager DataFrame holding data in memory.
/// <para>
/// DataFrames are 2D tabular data structures with named columns of potentially different types.
/// </para>
/// </summary>
and DataFrame(handle: DataFrameHandle) =
    interface IDisposable with
        member _.Dispose() = handle.Dispose()
    member this.Clone() = new DataFrame(PolarsWrapper.CloneDataFrame handle)
    member internal this.CloneHandle() = PolarsWrapper.CloneDataFrame handle
    member _.Handle = handle
    /// <summary> Create a DataFrame from a list of Series. </summary>
    static member create(series: Series list) : DataFrame =
        let handles = 
            series 
            |> List.map (fun s -> s.Handle) 
            |> List.toArray
            
        let h = PolarsWrapper.DataFrameNew handles
        new DataFrame(h)
    /// <summary> Create a DataFrame from an array of Series. </summary>
    static member create([<ParamArray>] series: Series[]) : DataFrame =
        let handles = series |> Array.map (fun s -> s.Handle)
        let h = PolarsWrapper.DataFrameNew handles
        new DataFrame(h)
    /// <summary>
    /// Read a CSV file into a DataFrame.
    /// </summary>
    /// <param name="path">Path to the CSV file.</param>
    /// <param name="schema">Optional map of column names to DataTypes to enforce schema.</param>
    /// <param name="separator">Character used as separator (default ',').</param>
    /// <param name="hasHeader">Indicate if the first row is a header (default true).</param>
    /// <param name="skipRows">Number of rows to skip at the start.</param>
    /// <param name="tryParseDates">Try to automatically parse date strings (default true).</param>
    static member ReadCsv(
        path: string, 
        ?schema: Map<string, DataType>, 
        ?separator: char,
        ?hasHeader: bool,
        ?skipRows: int,
        ?tryParseDates: bool
    ) : DataFrame =
        
        let sep = defaultArg separator ','
        let header = defaultArg hasHeader true
        let skip = defaultArg skipRows 0 |> uint64 
        let parseDates = defaultArg tryParseDates true

        let mutable dictArg : Dictionary<string, DataTypeHandle> = null
        
        let mutable handlesToDispose = new List<DataTypeHandle>()

        try
            if schema.IsSome then
                dictArg <- new Dictionary<string, DataTypeHandle>()
                for kv in schema.Value do
                    let h = kv.Value.CreateHandle()
                    dictArg.Add(kv.Key, h)
                    handlesToDispose.Add h

            let dfHandle = PolarsWrapper.ReadCsv(path, dictArg, header, sep, skip, parseDates)
            
            new DataFrame(dfHandle)

        finally
            for h in handlesToDispose do
                h.Dispose()
    /// <summary> Asynchronously read a CSV file into a DataFrame. </summary>
    static member ReadCsvAsync(path: string, 
                               ?schema: Map<string, DataType>,
                               ?hasHeader: bool,
                               ?separator: char,
                               ?skipRows: int,
                               ?tryParseDates: bool) : Async<DataFrame> =
        
        let header = defaultArg hasHeader true
        let sep = defaultArg separator ','
        let skip = defaultArg skipRows 0
        let dates = defaultArg tryParseDates true
        
        let schemaDict = 
            match schema with
            | Some m -> 
                let d = Dictionary<string, DataTypeHandle>()
                m |> Map.iter (fun k v -> d.Add(k, v.CreateHandle()))
                d
            | None -> null

        async {
            let! handle = 
                PolarsWrapper.ReadCsvAsync(
                    path, 
                    schemaDict, 
                    header, 
                    sep, 
                    uint64 skip, 
                    dates
                ) |> Async.AwaitTask

            return new DataFrame(handle)
        }

    /// <summary>
    /// [Eager] Create a DataFrame from an IDataReader (e.g. SqlDataReader).
    /// <para>
    /// Uses high-performance streaming ingestion via Apache Arrow.
    /// </para>
    /// </summary>
    /// <param name="reader">The open IDataReader instance.</param>
    /// <param name="batchSize">Number of rows per Arrow batch (default 50,000).</param>
    static member ReadDb(reader: IDataReader, ?batchSize: int) : DataFrame =
        let schema = reader.GetArrowSchema()

        let size = defaultArg batchSize 50_000
        
        let batchStream = reader.ToArrowBatches size
        
        let handle = ArrowStreamInterop.ImportEager(batchStream,schema)
        
        if handle.IsInvalid then

            let emptyBatch = new RecordBatch(schema, System.Array.Empty<IArrowArray>(), 0)

            let safeHandle = ArrowFfiBridge.ImportDataFrame emptyBatch
            new DataFrame(safeHandle)
        else
            new DataFrame(handle)

    /// <summary> Read a parquet file into a DataFrame (Eager). </summary>
    static member ReadParquet (path: string) = new DataFrame(PolarsWrapper.ReadParquet path)
    /// <summary> Asynchronously read a Parquet file. </summary>
    static member ReadParquetAsync (path: string): Async<DataFrame> = 
        async {
            let! handle = PolarsWrapper.ReadParquetAsync path |> Async.AwaitTask
        return new DataFrame(handle)
        }

    /// <summary> Read a JSON file into a DataFrame (Eager). </summary>
    static member ReadJson (path: string) : DataFrame =
        new DataFrame(PolarsWrapper.ReadJson path)
    /// <summary> Read an IPC file into a DataFrame (Eager). </summary>
    static member ReadIpc (path: string) = new DataFrame(PolarsWrapper.ReadIpc path)
    /// <summary> Create a DataFrame from a sequence of objects using Arrow streaming. </summary>
    static member ofSeqStream<'T>(data: seq<'T>, ?batchSize: int) : DataFrame =
        let size = defaultArg batchSize 100_000

        let schema = ArrowConverter.GetSchemaFromType<'T>()
        let batchStream = 
            data
            |> Seq.chunkBySize size
            |> Seq.map ArrowFfiBridge.BuildRecordBatch

        let handle = ArrowStreamInterop.ImportEager(batchStream,schema)

        if handle.IsInvalid then
            let emptyBatch = new Apache.Arrow.RecordBatch(schema, System.Array.Empty<Apache.Arrow.IArrowArray>(), 0)
            let safeHandle = ArrowFfiBridge.ImportDataFrame emptyBatch
            new DataFrame(safeHandle)
        else
            new DataFrame(handle)
    /// <summary>
    /// [ToRecords] Transform DataFrame to F# Records
    /// </summary>
    member this.ToRecords<'T>() : seq<'T> =
        use batch = ArrowFfiBridge.ExportDataFrame this.Handle
        
        ArrowReader.ReadRecordBatch<'T> batch |> Seq.toList |> List.toSeq

    // ==========================================
    // High-Performance Record Converter
    // ==========================================
    /// <summary>
    /// Check if a type is supported by the Fast Columnar Transposition path.
    /// Primitives, Strings, Dates, and their Option/VOption variants, or Arrays with non-null primitive data types are supported.
    /// Lists, Arrays with nullable or option type, and Nested Records must fallback to Arrow.
    /// </summary>
    static member private IsSupportedFastType (t: Type) =
        // 1. Unwrap Option/VOption/Nullable
        let coreType = 
            if t.IsGenericType && (t.GetGenericTypeDefinition() = typedefof<option<_>> || t.GetGenericTypeDefinition() = typedefof<voption<_>> || t.GetGenericTypeDefinition() = typedefof<Nullable<_>>) then
                t.GetGenericArguments().[0]
            else
                t

        if t.IsArray then false 
        else
            if coreType.IsPrimitive then true
            else if coreType = typeof<string> then true
            else if coreType = typeof<decimal> then true
            else if coreType = typeof<DateTime> then true
            else if coreType = typeof<DateOnly> then true
            else if coreType = typeof<TimeOnly> then true
            else if coreType = typeof<TimeSpan> then true
            else if coreType = typeof<DateTimeOffset> then true
            else if coreType = typeof<Int128> then true
            else if coreType = typeof<UInt128> then true
            else false
    /// <summary>
    /// [Internal] Worker method to transpose a single column from Record[] to Series.
    /// This is generic to avoid boxing during array population.
    /// </summary>
    static member private CreateSeriesFromColumn<'Rec, 'Field>(data: 'Rec[], name: string, prop: PropertyInfo) : Series =
        // 1. Create Fast Getter (Delegate)
        let getterMethod = prop.GetGetMethod()
        let getter = Delegate.CreateDelegate(typeof<Func<'Rec, 'Field>>, getterMethod) :?> Func<'Rec, 'Field>
        
        // 2. Transpose: Row-Oriented -> Column-Oriented
        //    (Allocation happens here: O(N))
        let len = data.Length
        let colData = Array.zeroCreate<'Field> len
        
        for i = 0 to len - 1 do
            colData.[i] <- getter.Invoke(data.[i])
            
        // 3. Delegate to C# SeriesFactory (The Magic Step!)
        Series.create(name, colData)

    /// <summary>
    /// Create a DataFrame from a sequence of records.
    /// <para>
    /// Strategy:
    /// 1. Inspects types. If all are simple primitives/strings/dates, uses Fast Columnar Transposition (Zero-Arrow).
    /// 2. If any complex types (Lists, Arrays, Nested Records) are found, falls back to ArrowFfiBridge.
    /// </para>
    /// </summary>
    static member ofRecords<'T>(data: seq<'T>) : DataFrame =
        let recordType = typeof<'T>
        let props = recordType.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)

        // 1. Check Eligibility for Fast Path
        // We only use Fast Path if ALL columns are supported.
        let useFastPath = 
            props 
            |> Array.forall (fun p -> DataFrame.IsSupportedFastType p.PropertyType)

        if useFastPath then
            // ==================================================
            // PATH A: High-Performance Columnar Transposition
            // ==================================================
            let records = Seq.toArray data
            
            // Helper Cache
            let helperMethodDef = 
                typeof<DataFrame>.GetMethod("CreateSeriesFromColumn", BindingFlags.NonPublic ||| BindingFlags.Static)

            let seriesList = 
                props
                |> Array.map (fun prop ->
                    let fieldType = prop.PropertyType
                    let specificHelper = helperMethodDef.MakeGenericMethod(recordType, fieldType)
                    try 
                        specificHelper.Invoke(null, [| records; prop.Name; prop |]) :?> Series
                    with ex ->
                        failwithf "Failed to create series for column '%s': %s" prop.Name ex.InnerException.Message
                )
            DataFrame.create seriesList

        else
            // ==================================================
            // PATH B: Arrow Fallback (The Old Way)
            // Supports Lists, Structs, and complex nesting
            // ==================================================
            let batch = ArrowFfiBridge.BuildRecordBatch data
            let handle = ArrowFfiBridge.ImportDataFrame batch
            new DataFrame(handle)
    /// <summary> Create a DataFrame directly from an Apache Arrow RecordBatch. </summary>
    static member FromArrow (batch: RecordBatch) : DataFrame =
        new DataFrame(PolarsWrapper.FromArrow batch)
    /// <summary> Write DataFrame to CSV. </summary>
    member this.WriteCsv (path: string) = 
        PolarsWrapper.WriteCsv(this.Handle, path)
        this 
    /// <summary> Write DataFrame to Parquet. </summary>
    member this.WriteParquet (path: string) = 
        PolarsWrapper.WriteParquet(this.Handle, path)
        this
    /// <summary>
    /// Write DataFrame to an Arrow IPC (Feather) file.
    /// This is a fast, zero-copy binary format.
    /// </summary>
    member this.WriteIpc(path: string)=
        PolarsWrapper.WriteIpc(this.Handle, path)
        this
    /// <summary>
    /// Write DataFrame to a JSON file (standard array format).
    /// </summary>
    member this.WriteJson(path: string) =
        PolarsWrapper.WriteJson(this.Handle, path)
        this 
    /// <summary>
    /// Export the DataFrame as a stream of Arrow RecordBatches (Zero-Copy).
    /// Calls 'onBatch' for each chunk in the DataFrame.
    /// Useful for custom eager sinks (e.g. WriteDatabase).
    /// </summary>
    member this.ExportBatches(onBatch: Action<RecordBatch>) : unit =
        PolarsWrapper.ExportBatches(this.Handle, onBatch)

    /// <summary>
    /// Stream the DataFrame directly to a database or other IDataReader consumer.
    /// <para>
    /// Uses a producer-consumer pattern. This method blocks until the consumer finishes reading.
    /// Ideal for <c>SqlBulkCopy.WriteToServer</c> or <c>NpgsqlBinaryImporter</c>.
    /// </para>
    /// </summary>
    /// <param name="writerAction">Callback that receives an IDataReader.</param>
    /// <param name="bufferSize">Max number of batches to buffer in memory (default: 5).</param>
    /// <param name="typeOverrides">Dictionary to force specific C# types for columns (optional).</param>
    member this.WriteTo(writerAction: Action<IDataReader>, ?bufferSize: int, ?typeOverrides: IDictionary<string, Type>) : unit =
        let capacity = defaultArg bufferSize 5
        
        use buffer = new BlockingCollection<Apache.Arrow.RecordBatch>(capacity)

        let consumerTask = Task.Run(fun () ->
            let stream = buffer.GetConsumingEnumerable()
            
            let overrides = 
                match typeOverrides with 
                | Some d -> new Dictionary<string, Type>(d) 
                | None -> null
            
            use reader = new ArrowToDbStream(stream, overrides)
            
            writerAction.Invoke reader
        )

        try
            try
                this.ExportBatches(fun batch -> buffer.Add batch)
            finally
                buffer.CompleteAdding()
        with
        | _ -> 
            reraise()

        try
            consumerTask.Wait()
        with
        | :? AggregateException as aggEx ->
            raise (aggEx.Flatten().InnerException)
    
    /// <summary>
    /// Get the schema as Map<ColumnName, DataType>.
    /// </summary>
    member this.Schema : Map<string, DataType> =
        let names = this.ColumnNames
        
        names
        |> Array.map (fun (name: string) ->
            let s = this.Column name
            
            name, s.DataType
        )
        |> Map.ofArray
    member this.Lazy() : LazyFrame =
        let lfHandle = PolarsWrapper.DataFrameToLazy handle
        new LazyFrame(lfHandle)
    /// <summary>
    /// Print schema in a readable format.
    /// </summary>
    member this.PrintSchema() =
        printfn "--- DataFrame Schema ---"
        this.Schema |> Map.iter (fun name dtype -> 
            printfn "%-15s | %A" name dtype
        )
        printfn "------------------------"
    // ==========================================
    // Eager Ops
    // ==========================================
    /// <summary> Add or replace columns using expressions. </summary>
    member this.WithColumns (exprs:Expr list) : DataFrame =
        let handles = exprs |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        let h = PolarsWrapper.WithColumns(this.Handle,handles)
        new DataFrame(h)
    /// <summary> Add or replace columns using generic column expressions (Expr or Selectors). </summary>
    member this.WithColumns (columns:seq<#IColumnExpr>) =
        let exprs = 
            columns 
            |> Seq.collect (fun x -> x.ToExprs()) 
            |> Seq.toList
        
        this.WithColumns exprs
    /// <summary> Add a single column. </summary>
    member this.WithColumn (expr: Expr) : DataFrame =
        let handle = expr.CloneHandle()
        let h = PolarsWrapper.WithColumns(this.Handle,[| handle |])
        new DataFrame(h)
    /// <summary> Select columns using expressions. </summary>
    member this.Select(exprs: Expr list) : DataFrame =
        let handles = exprs |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        let h = PolarsWrapper.Select(this.Handle, handles)
        new DataFrame(h)

    /// <summary> Select columns using generic column expressions (Expr or Selectors). </summary>
    member this.Select(columns: seq<#IColumnExpr>) =
            let exprs = 
                columns 
                |> Seq.collect (fun x -> x.ToExprs()) 
                |> Seq.toList
            
            this.Select exprs
    /// <summary> 
    /// Select a single column using an expression.
    /// Usage: df.Select(pl.col("A"))
    /// </summary>
    member this.Select(expr: Expr) =
        this.Select [expr]

    /// <summary> Filter rows based on a boolean expression (predicate). </summary>
    member this.Filter (expr: Expr) : DataFrame = 
        let h = PolarsWrapper.Filter(this.Handle,expr.CloneHandle())
        new DataFrame(h)
    /// <summary>
    /// Sort the DataFrame.
    /// </summary>
    /// <param name="columns">the column which needs to be sorted (Expr/Selector)。</param>
    /// <param name="descending">sort direction (true=descending).Length must be 1 (broadcasting) or same with columns.</param>
    /// <param name="nullsLast">null value position (true=last).Length must be 1 (broadcasting) or same with columns</param>
    /// <param name="maintainOrder">Stable Sort option</param>
    member this.Sort(
        columns: seq<#IColumnExpr>,
        descending: seq<bool>,
        nullsLast: seq<bool>,
        ?maintainOrder: bool
    ) =
        let exprHandles = 
            columns 
            |> Seq.collect (fun x -> x.ToExprs()) 
            |> Seq.map (fun e -> e.CloneHandle()) 
            |> Seq.toArray
        
        let descArr = descending |> Seq.toArray
        let nullsArr = nullsLast |> Seq.toArray
        let stable = defaultArg maintainOrder false

        let h = PolarsWrapper.DataFrameSort(this.Handle, exprHandles, descArr, nullsArr, stable)
        new DataFrame(h)

    /// <summary> Sort with simple broadcasting options. </summary>
    member this.Sort(
        columns: seq<#IColumnExpr>,
        ?descending: bool,
        ?nullsLast: bool,
        ?maintainOrder: bool
    ) =
        let desc = defaultArg descending false
        let nLast = defaultArg nullsLast false
        
        this.Sort(columns, [| desc |], [| nLast |], ?maintainOrder = maintainOrder)
    /// <summary> Sort by a single expression. </summary>
    member this.Sort(expr: Expr, ?descending: bool, ?nullsLast: bool) =
        this.Sort([expr], ?descending=descending, ?nullsLast=nullsLast)
    /// <summary> Sort by a single column name. </summary>

    member this.Sort(colName: string, ?descending: bool, ?nullsLast: bool) =
        this.Sort([Expr.Col colName], ?descending=descending, ?nullsLast=nullsLast)
    /// <summary> Alias for Sort. </summary>
    member this.Orderby (expr: Expr,desc :bool) : DataFrame =
        this.Sort(expr,desc)
    /// <summary> Group by keys and apply aggregate expressions. </summary>
    member this.GroupBy (keys: Expr list,aggs: Expr list) : DataFrame =
        let kHandles = keys |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        let aHandles = aggs |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        let h = PolarsWrapper.GroupByAgg(this.Handle, kHandles, aHandles)
        new DataFrame(h)
    /// <summary> Group by keys and apply aggregations (Supports Selectors). </summary>
    member this.GroupBy(keys: seq<#IColumnExpr>, aggs: seq<#IColumnExpr>) =
        let kExprs = keys |> Seq.collect (fun x -> x.ToExprs()) |> Seq.toList
        let aExprs = aggs |> Seq.collect (fun x -> x.ToExprs()) |> Seq.toList
        this.GroupBy (kExprs, aExprs)
    /// <summary> Join with another DataFrame. </summary>
    member this.Join (other: DataFrame,leftOn: Expr list,rightOn: Expr list,how: JoinType) : DataFrame =
        let lHandles = leftOn |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        let rHandles = rightOn |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        let h = PolarsWrapper.Join(this.Handle, other.Handle, lHandles, rHandles, how.ToNative())
        new DataFrame(h)
    /// <summary> Concatenate multiple DataFrames. </summary>
    static member Concat (dfs: DataFrame list) (how: ConcatType): DataFrame =
        let handles = dfs |> List.map (fun df -> df.CloneHandle()) |> List.toArray
        new DataFrame(PolarsWrapper.Concat (handles,how.ToNative()))
    /// <summary> Get the first n rows. </summary>
    member this.Head (?rows: int) : DataFrame  =
        let n = defaultArg rows 5
        let h = PolarsWrapper.Head(this.Handle, uint n) 
        new DataFrame(h)
    /// <summary> Get the last n rows. </summary>
    member this.Tail (?n: int) : DataFrame =
        let rows = defaultArg n 5
        let h = PolarsWrapper.Tail(this.Handle, uint rows) 
        new DataFrame(h)


    /// <summary> 
    /// Explode list columns to rows using a Selector.
    /// </summary>
    member this.Explode(selector: Selector) : DataFrame =
        let sh = selector.CloneHandle()
        let h = PolarsWrapper.Explode(this.Handle, sh)
        new DataFrame(h)

    /// <summary> 
    /// Explode list columns to rows using column names.
    /// </summary>
    member this.Explode(columns: seq<string>) =
        let names = Seq.toArray columns
        let h = PolarsWrapper.SelectorCols names
        let sel = new Selector(h)
        this.Explode sel

    /// <summary>Explode a single column by name. </summary>
    member this.Explode(column: string) =
        this.Explode [column]
    /// <summary> Decompose a struct column into multiple columns. </summary>
    member this.UnnestColumn(column: string, ?separator: string) : DataFrame =
        let cols = [| column |]
        let sep = defaultArg separator null
        let newHandle = PolarsWrapper.Unnest(this.Handle, cols, sep)
        new DataFrame(newHandle)
    /// <summary> Decompose multiple struct columns. </summary>
    member this.UnnestColumns(columns: string list, ?separator: string) : DataFrame =
        let cArr = List.toArray columns
        let sep = defaultArg separator null
        let newHandle = PolarsWrapper.Unnest(this.Handle, cArr, sep)
        new DataFrame(newHandle)
    /// <summary>
    /// Pivot the DataFrame from long to wide format.
    /// </summary>
    /// <param name="index">Columns to use as index (keys).</param>
    /// <param name="columns">Column defining the new column names.</param>
    /// <param name="values">Column(s) defining the values.</param>
    /// <param name="aggFn">Aggregation function for duplicates.</param>
    member this.Pivot (index: string list) (columns: string list) (values: string list) (aggFn: PivotAgg) : DataFrame =
        let iArr = List.toArray index
        let cArr = List.toArray columns
        let vArr = List.toArray values
        new DataFrame(PolarsWrapper.Pivot(this.Handle, iArr, cArr, vArr, aggFn.ToNative()))

    /// <summary> 
    /// Unpivot (Melt) the DataFrame from wide to long format using Selectors.
    /// This is the primary implementation backed by native binding.
    /// </summary>
    /// <param name="index">Selector for ID variables (columns to keep)</param>
    /// <param name="on">Selector for Value variables (columns to melt)</param>
    /// <param name="variableName">Name for the variable column (default: "variable")</param>
    /// <param name="valueName">Name for the value column (default: "value")</param>
    member this.Unpivot (index: Selector,on: Selector,variableName: string option,valueName: string option) : DataFrame =
        let hIndex = index.CloneHandle()
        let hOn = on.CloneHandle()
        let varN = Option.toObj variableName
        let valN = Option.toObj valueName
        
        new DataFrame(PolarsWrapper.Unpivot(this.Handle, hIndex, hOn, varN, valN))

    /// <summary> 
    /// Unpivot (Melt) overload for simple string lists.
    /// Auto-converts string lists to Column Selectors.
    /// </summary>
    member this.Unpivot (index: seq<string>,on: seq<string>,variableName: string option,valueName: string option) =
        // 1. Index Selector
        let idxArr = Seq.toArray index
        let sIndex = new Selector(PolarsWrapper.SelectorCols idxArr)

        // 2. On (Value) Selector
        let onArr = Seq.toArray on
        let sOn = new Selector(PolarsWrapper.SelectorCols onArr)

        this.Unpivot(sIndex,sOn,variableName,valueName)
    member this.Unpivot (index: string list,on: string list) =
        this.Unpivot(index,on,None,None)
    /// <summary> Alias for Unpivot. </summary>
    member this.Melt(index: Selector, on: Selector, variableName, valueName) = 
        this.Unpivot(index, on, variableName, valueName)

    member this.Melt(index: seq<string>, on: seq<string>, variableName, valueName) = 
        this.Unpivot(index, on, variableName, valueName)

    member this.Melt(index: string list, on: string list) =
        this.Unpivot(index, on)
    /// <summary>
    /// Slice the DataFrame along the rows.
    /// </summary>
    member this.Slice(offset: int64, length: uint64) = 
        new DataFrame(PolarsWrapper.Slice(this.Handle,offset, length))
    member this.Slice(offset: int64, length: int32) = 
        if length < 0 then raise(ArgumentOutOfRangeException(sprintf "Length must be non-negative."))
        else this.Slice(offset,length)
    // ==========================================
    // Printing / String Representation
    // ==========================================

    /// <summary>
    /// Returns the native Polars string representation of the DataFrame.
    /// Includes shape, header, and truncated data.
    /// </summary>
    override this.ToString() =
        PolarsWrapper.DataFrameToString handle

    /// <summary>
    /// Print the DataFrame to Console (Stdout).
    /// </summary>
    member this.Show() =
        printfn "%s" (this.ToString())
    /// Remove a column by name. Returns a new DataFrame.
    /// </summary>
    member this.Drop(name: string) : DataFrame =
        new DataFrame(PolarsWrapper.Drop(handle, name))

    /// <summary>
    /// Rename a column. Returns a new DataFrame.
    /// </summary>
    member this.Rename(oldName: string, newName: string) : DataFrame =
        new DataFrame(PolarsWrapper.Rename(handle, oldName, newName))

    /// <summary>
    /// Drop rows containing any null values.
    /// subset: Optional list of column names to consider.
    /// </summary>
    member this.DropNulls(?subset: string list) : DataFrame =
        let s = subset |> Option.map List.toArray |> Option.toObj
        new DataFrame(PolarsWrapper.DropNulls(handle, s))

    /// <summary>
    /// Sample n rows from the DataFrame.
    /// </summary>
    member this.Sample(n: int, ?withReplacement: bool, ?shuffle: bool, ?seed: uint64) : DataFrame =
        let replace = defaultArg withReplacement false
        let shuff = defaultArg shuffle true
        let s = Option.toNullable seed
        
        new DataFrame(PolarsWrapper.SampleN(handle, uint64 n, replace, shuff, s))

    /// <summary>
    /// Sample a fraction of rows from the DataFrame.
    /// </summary>
    member this.Sample(frac: double, ?withReplacement: bool, ?shuffle: bool, ?seed: uint64) : DataFrame =
        let replace = defaultArg withReplacement false
        let shuff = defaultArg shuffle true
        let s = Option.toNullable seed
        
        new DataFrame(PolarsWrapper.SampleFrac(handle, frac, replace, shuff, s))
    // Interop
    member this.ToArrow() = ArrowFfiBridge.ExportDataFrame handle
    // Properties
    member _.Rows = PolarsWrapper.DataFrameHeight handle
    member _.Height = PolarsWrapper.DataFrameHeight handle
    member _.Len = PolarsWrapper.DataFrameHeight handle
    member _.Width = PolarsWrapper.DataFrameWidth handle
    /// <summary>
    /// Returns the shape of the DataFrame as (Height, Width).
    /// </summary>
    member this.Shape = this.Len,this.Width
    member _.ColumnNames = PolarsWrapper.GetColumnNames handle
    member _.Columns = PolarsWrapper.GetColumnNames handle
    member this.Int(colName: string, rowIndex: int) : int64 option = 
        let nullableVal = PolarsWrapper.GetInt(handle, colName, int64 rowIndex)
        if nullableVal.HasValue then Some nullableVal.Value else None
    member this.Float(colName: string, rowIndex: int) : float option = 
        let nullableVal = PolarsWrapper.GetDouble(handle, colName, int64 rowIndex)
        if nullableVal.HasValue then Some nullableVal.Value else None
    member this.String(colName: string, rowIndex: int) = PolarsWrapper.GetString(handle, colName, int64 rowIndex) |> Option.ofObj
    member this.StringList(colName: string, rowIndex: int) : string list option =
        use colHandle = PolarsWrapper.Select(handle, [| PolarsWrapper.Col colName |])
        use tempDf = new DataFrame(colHandle)
        use arrowBatch = tempDf.ToArrow()
        
        let col = arrowBatch.Column colName
        
        let extractStrings (valuesArr: IArrowArray) (startIdx: int) (endIdx: int) =
            match valuesArr with
            | :? StringArray as sa ->
                [ for i in startIdx .. endIdx - 1 -> sa.GetString i ]
            | :? StringViewArray as sva ->
                [ for i in startIdx .. endIdx - 1 -> sva.GetString i ]
            | _ -> [] 

        match col with
        // Case A: Arrow.ListArray 
        | :? ListArray as listArr ->
            if listArr.IsNull rowIndex then None
            else
                let start = listArr.ValueOffsets.[rowIndex]
                let end_ = listArr.ValueOffsets.[rowIndex + 1]
                Some (extractStrings listArr.Values start end_)

        // Case B: Large List (64-bit offsets) 
        | :? LargeListArray as listArr ->
            if listArr.IsNull rowIndex then None
            else
                let start = int listArr.ValueOffsets.[rowIndex]
                let end_ = int listArr.ValueOffsets.[rowIndex + 1]
                Some (extractStrings listArr.Values start end_)

        | _ -> 
            // System.Console.WriteLine($"[Debug] Mismatched Array Type: {col.GetType().Name}")
            None
    member this.Decimal(col: string, row: int) : decimal option =
        use s = this.Column col
        s.Decimal row
    // 1. Boolean
    member this.Bool(col: string, row: int) : bool option =
        use s = this.Column col
        s.Bool row
    // 2. Date (DateOnly)
    member this.Date(col: string, row: int) : DateOnly option =
        use s = this.Column col
        s.Date row

    // 3. Time (TimeOnly)
    member this.Time(col: string, row: int) : TimeOnly option =
        use s = this.Column col
        s.Time row

    // 4. Datetime (DateTime)
    member this.Datetime(col: string, row: int) : DateTime option =
        use s = this.Column col
        s.Datetime row

    // 5. Duration (TimeSpan)
    member this.Duration(col: string, row: int) : TimeSpan option =
        use s = this.Column col
        s.Duration row
    member this.Column(name: string) : Series =
        let h = PolarsWrapper.DataFrameGetColumn(this.Handle, name)
        new Series(h)
    member this.Column(index: int) : Series =
        let h = PolarsWrapper.DataFrameGetColumnAt(this.Handle, index)
        new Series(h)

    member this.GetSeries() : Series list =
        [ for i in 0 .. int this.Width - 1 -> this.Column i ]
    /// <summary>
    /// Check if the value at the specified column and row is null.
    /// </summary>
    member this.IsNullAt(col: string, row: int) : bool =
        use s = this.Column col
        s.IsNullAt row
    /// <summary>
    /// Get the number of null values in a specific column.
    /// </summary>
    member this.NullCount(colName: string) : int64 =
        use s = this.Column colName
        s.NullCount
    member this.IsNan(col: string) =
        use s = this.Column col
        s.IsNan()
    member this.IsNotNan (col:string) =
        use s = this.Column col
        s.IsNotNan()
    member this.IsFinite (col:string) =
        use s = this.Column col
        s.IsFinite()
    member this.IsInfinite (col:string) =
        use s = this.Column col
        s.IsInfinite()
    /// <summary>
    /// Helper to get a cell value as an F# List directly.
    /// </summary>
    member this.CellList<'T>(colName: string,row:int) : 'T list =
        let s = this.Column colName
        s.GetList<'T>(int64 row)
    // ==========================================
    // Indexers (Syntax Sugar)
    // ==========================================
    member this.Item (columnName: string) : Series =
        this.Column columnName
    
    member this.Item (columnIndex: int) : Series =
        this.Column columnIndex
    /// <summary>
    /// [Indexer] Access cell value by Row Index and Column Name.
    /// Syntax: df.[rowIndex, "colName"]
    /// </summary>
    member this.Item (rowIndex: int, columnName: string) : obj =
        let series = this.Column columnName
        series.[rowIndex]

    /// <summary>
    /// [Indexer] Access cell value by Row Index and Column Index.
    /// Syntax: df.[rowIndex, colIndex]
    /// </summary>
    member this.Item (rowIndex: int, columnIndex: int) : obj =
        let series = this.Column columnIndex
        series.[rowIndex]

    // ==========================================
    // Row Access
    // ==========================================

    /// <summary>
    /// Get data for a specific row as an object array.
    /// Similar to DataTable.Rows[i].ItemArray.
    /// </summary>
    member this.Row (index: int) : obj[] =
        let h = int64 this.Rows
        if int64 index < 0L || int64 index >= h then
            raise (IndexOutOfRangeException(sprintf "Row index %d is out of bounds. Height: %d" index h))

        let w = this.Columns.Length
        let rowData = Array.zeroCreate<obj> w

        for i in 0 .. w - 1 do
            rowData.[i] <- this.[index, i]

        rowData

    // ==========================================
    // IEnumerable<Series> Support
    // ==========================================
    interface IEnumerable<Series> with
        member this.GetEnumerator() : IEnumerator<Series> =
            let seq = seq {
                let w = this.Columns.Length
                for i in 0 .. w - 1 do
                    yield this.Column(i)
            }
            seq.GetEnumerator()

    interface IEnumerable with
        member this.GetEnumerator() : IEnumerator =
            (this :> IEnumerable<Series>).GetEnumerator() :> IEnumerator
/// <summary>
/// A LazyFrame represents a logical plan of operations that will be optimized and executed only when collected.
/// <para>
/// Operations on LazyFrame are not executed immediately. Instead, they build a query plan.
/// Use <c>Collect()</c> to execute the plan and get a DataFrame.
/// </para>
/// </summary>
and LazyFrame(handle: LazyFrameHandle) =
    member _.Handle = handle
    abstract member Dispose : unit -> unit
    default x.Dispose() = 
        handle.Dispose()

    interface IDisposable with
        member x.Dispose() = x.Dispose()
    member internal this.CloneHandle() = PolarsWrapper.LazyClone handle
    /// <summary> Execute the plan and return a DataFrame. </summary>
    member this.Collect() = 
        let dfHandle = PolarsWrapper.LazyCollect handle
        new DataFrame(dfHandle)
    /// <summary> Execute the plan using the streaming engine. </summary>
    member this.CollectStreaming() =
        let dfHandle = PolarsWrapper.CollectStreaming handle
        new DataFrame(dfHandle)
    /// <summary> Get the schema string of the LazyFrame without executing it. </summary>
    member _.SchemaRaw = PolarsWrapper.GetSchemaString handle

    /// <summary>
    /// Get the schema of the LazyFrame without executing it.
    /// Uses Zero-Copy native introspection.
    /// </summary>
    member _.Schema : Map<string, DataType> =
        use schemaHandle = PolarsWrapper.GetLazySchema handle

        let len = PolarsWrapper.GetSchemaLen schemaHandle

        if len = 0UL then 
            Map.empty
        else
            [| for i in 0UL .. len - 1UL do
                let mutable name = Unchecked.defaultof<string>
                let mutable typeHandle = Unchecked.defaultof<DataTypeHandle>
                
                PolarsWrapper.GetSchemaFieldAt(schemaHandle, i, &name, &typeHandle)
                
                use h = typeHandle
                
                let dtype = DataType.FromHandle h
                
                yield name, dtype
            |]
            |> Map.ofArray

    /// <summary> Print the query plan. </summary>
    member this.Explain(?optimized: bool) = 
        let opt = defaultArg optimized true
        PolarsWrapper.Explain(handle, opt)
    /// <summary>
    /// Lazily scan a CSV file into a LazyFrame.
    /// </summary>
    static member ScanCsv(path: string,
                          ?schema: Map<string, DataType>,
                          ?hasHeader: bool,
                          ?separator: char,
                          ?skipRows: int,
                          ?tryParseDates: bool) : LazyFrame =
        
        let header = defaultArg hasHeader true
        let sep = defaultArg separator ','
        let skip = defaultArg skipRows 0
        let dates = defaultArg tryParseDates true
        
        let schemaDict = 
            match schema with
            | Some m -> 
                let d = Dictionary<string, DataTypeHandle>()
                m |> Map.iter (fun k v -> d.Add(k, v.CreateHandle()))
                d
            | None -> null

        let h = PolarsWrapper.ScanCsv(path, schemaDict, header, sep, uint64 skip, dates)
        new LazyFrame(h)
    /// <summary> Scan a parquet file into a LazyFrame. </summary>
    static member ScanParquet (path: string) = new LazyFrame(PolarsWrapper.ScanParquet path)
    /// <summary> Scan a JSON file into a LazyFrame. </summary>
    static member ScanNdjson (path: string) : LazyFrame =
        new LazyFrame(PolarsWrapper.ScanNdjson path)
    /// <summary> Scan an IPC file into a LazyFrame. </summary>
    static member ScanIpc (path: string) = new LazyFrame(PolarsWrapper.ScanIpc path)
    
    // ==========================================
    // Streaming Scan (Lazy)
    // ==========================================

    /// <summary>
    /// Lazily scan a sequence of objects using Apache Arrow Stream Interface.
    /// This supports predicate pushdown and streaming execution.
    /// Data is pulled from the sequence only when needed.
    /// </summary>
    /// <param name="data">The data source sequence.</param>
    /// <param name="batchSize">Rows per Arrow batch (default: 100,000).</param>
    /// <param name="useBuffered">Choose whether disk buffer file needed (for big data) <param>
    static member scanSeq<'T>(data: seq<'T>, ?batchSize: int, ?useBuffered: bool) : LazyFrame =
            let size = defaultArg batchSize 100_000
            let buffered = defaultArg useBuffered false

            // =========================================================
            // 1. Buffered Mode (Disk IPC)
            // =========================================================
            if buffered then
                let scope = new IpcStreamService.TempIpcScope<'T>(data, size)
                
                // Get FileHandle
                let handle = PolarsWrapper.ScanIpc scope.FilePath
                
                { new LazyFrame(handle) with
                    member this.Dispose() =
                        base.Dispose()
                        scope.Dispose()
                }

            // =========================================================
            // 2. Streaming Mode (Memory Safety & Lazy Fallback)
            // =========================================================
            else
                let schema = ArrowConverter.GetSchemaFromType<'T>()

                let streamFactory = Func<IEnumerable<RecordBatch>>(fun () ->
                    seq {
                        let mutable hasYielded = false
                        
                        let batches = ArrowConverter.ToArrowBatches(data, size)

                        for batch in batches do
                            hasYielded <- true
                            yield batch
                            batch.Dispose()
                        
                        if not hasYielded then
                            let emptyBatch = ArrowConverter.GetEmptyBatch<'T>()
                            yield emptyBatch
                            emptyBatch.Dispose()
                    }
                )

                let handle = ArrowStreamInterop.ScanStream(streamFactory, schema)
                new LazyFrame(handle)

    /// <summary>
    /// [Lazy] Scan a database query lazily.
    /// Requires a factory function to create new IDataReaders for potential multi-pass scans.
    /// </summary>
    static member scanDb(readerFactory: unit -> IDataReader, ?batchSize: int, ?useBuffered: bool) : LazyFrame =
        let size = defaultArg batchSize 50_000
        let buffered = defaultArg useBuffered false

        // =========================================================
        // 1. Buffered Mode (Disk IPC)
        // =========================================================
        if buffered then
            let runBuffer () =
                use reader = readerFactory()
                new IpcStreamService.TempIpcScopeReader(reader, size)

            let scope = runBuffer()
            let handle = PolarsWrapper.ScanIpc scope.FilePath

            { new LazyFrame(handle) with
                member this.Dispose() =
                    base.Dispose()
                    scope.Dispose()
            }

        // =========================================================
        // 2. Streaming Mode (Memory)
        // =========================================================
        else
            // Probe Schema
            let schema = 
                use reader = readerFactory()
                ArrowTypeResolver.GetSchemaFromDataReader reader

            // Stream Factory
            let factory = Func<IEnumerable<RecordBatch>>(fun () ->
                seq {
                    use reader = readerFactory()
                    let batches = DbToArrowStream.ToArrowBatches(reader, size)
                    
                    for batch in batches do
                        yield batch
                        batch.Dispose()
                }
            )

            let handle = ArrowStreamInterop.ScanStream(factory, schema)
            new LazyFrame(handle)

    /// <summary>
    /// [Lazy][Buffered] Scan a database DataReader directly.
    /// <para>Writes to disk IMMEDIATELY because IDataReader is forward-only.</para>
    /// </summary>
    static member scanDb(reader: IDataReader, ?batchSize: int) : LazyFrame =
        let size = defaultArg batchSize 50_000
        
        let scope = new IpcStreamService.TempIpcScopeReader(reader, size)
        let handle = PolarsWrapper.ScanIpc scope.FilePath
        
        // Inline ScopedLazyFrame
        { new LazyFrame(handle) with
            member this.Dispose() =
                base.Dispose()
                scope.Dispose()
        }

    /// <summary> Write LazyFrame execution result to Parquet (Streaming). </summary>
    member this.SinkParquet (path: string) : unit =
        PolarsWrapper.SinkParquet(this.CloneHandle(), path)
    /// <summary> Write LazyFrame execution result to IPC (Streaming). </summary>
    member this.SinkIpc (path: string) = 
        PolarsWrapper.SinkIpc(this.CloneHandle(), path)
    // ==========================================
    // Streaming Sink (Lazy)
    // ==========================================
    /// <summary>
    /// Stream the query result in batches.
    /// This executes the query and calls 'onBatch' for each RecordBatch produced.
    /// </summary>
    member this.SinkBatches(onBatch: Action<RecordBatch>) : unit =
        let newHandle = PolarsWrapper.SinkBatches(this.CloneHandle(), onBatch)
        
        let lfRes = new LazyFrame(newHandle)
        use _ = lfRes.CollectStreaming()
        () 
    /// <summary>
    /// Stream query results directly to a database or other IDataReader consumer.
    /// Uses a producer-consumer pattern with bounded capacity for memory efficiency.
    /// </summary>
    /// <param name="writerAction">Callback to consume the IDataReader (e.g., using SqlBulkCopy).</param>
    /// <param name="bufferSize">Max number of batches to buffer in memory (default: 5).</param>
    /// <param name="typeOverrides">Force specific C# types for columns (e.g. map Date32 to DateTime).</param>
    member this.SinkTo(writerAction: Action<IDataReader>, ?bufferSize: int, ?typeOverrides: IDictionary<string, Type>) : unit =
        let capacity = defaultArg bufferSize 5
        
        use buffer = new BlockingCollection<RecordBatch>(boundedCapacity = capacity)

        let consumerTask = Task.Run(fun () ->
            let stream = buffer.GetConsumingEnumerable()
            
            let overrides = 
                    match typeOverrides with 
                    | Some d -> new Dictionary<string, Type>(d) 
                    | None -> null
            
            use reader = new ArrowToDbStream(stream, overrides)
            
            writerAction.Invoke reader
        )

        try
            try
                this.SinkBatches(fun batch -> buffer.Add batch)
            finally
                buffer.CompleteAdding()
        with
        | _ -> 
            reraise()

        try
            consumerTask.Wait()
        with
        | :? AggregateException as aggEx ->
            raise (aggEx.Flatten().InnerException)
    
    /// <summary>
    /// Join with another LazyFrame.
    /// </summary>
    member this.Join(other: LazyFrame, leftOn: Expr seq, rightOn: Expr seq, how: JoinType) : LazyFrame =
        let lOnArr = leftOn |> Seq.map (fun e -> e.CloneHandle()) |> Seq.toArray
        
        let rOnArr = rightOn |> Seq.map (fun e -> e.CloneHandle()) |> Seq.toArray
        
        let lHandle = this.CloneHandle()
        let rHandle = other.CloneHandle()

        let newHandle = PolarsWrapper.Join(lHandle, rHandle, lOnArr, rOnArr, how.ToNative())
        
        new LazyFrame(newHandle)
    
    member this.Filter (expr: Expr) : LazyFrame =
        let lfClone = this.CloneHandle()
        let exprClone = expr.CloneHandle()
        
        let h = PolarsWrapper.LazyFilter(lfClone, exprClone)
        new LazyFrame(h)
    
    member this.Select (exprs: Expr list) : LazyFrame =
        let lfClone = this.CloneHandle()
        let handles = exprs |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        
        let h = PolarsWrapper.LazySelect(lfClone, handles)
        new LazyFrame(h)
    member this.Select(columns: seq<#IColumnExpr>) =
            let exprs = 
                columns 
                |> Seq.collect (fun x -> x.ToExprs()) 
                |> Seq.toList
            
            this.Select exprs
    /// <summary>
    /// Sort with Parameters
    /// </summary>
    member this.Sort(
        columns: seq<#IColumnExpr>,
        descending: seq<bool>,
        nullsLast: seq<bool>,
        ?maintainOrder: bool
    ) =
        let exprHandles = 
            columns 
            |> Seq.collect (fun x -> x.ToExprs()) 
            |> Seq.map (fun e -> e.CloneHandle()) 
            |> Seq.toArray
        
        let descArr = descending |> Seq.toArray
        let nullsArr = nullsLast |> Seq.toArray
        let stable = defaultArg maintainOrder false

        let lfHandle = this.CloneHandle()

        let h = PolarsWrapper.LazyFrameSort(lfHandle, exprHandles, descArr, nullsArr, stable)
        new LazyFrame(h)

    /// <summary>
    /// Sort with simple options
    /// </summary>
    member this.Sort(
        columns: seq<#IColumnExpr>,
        ?descending: bool,
        ?nullsLast: bool,
        ?maintainOrder: bool
    ) =
        let desc = defaultArg descending false
        let nLast = defaultArg nullsLast false
        
        this.Sort(columns, [| desc |], [| nLast |], ?maintainOrder = maintainOrder)
    member this.Sort(expr: Expr, ?descending: bool, ?nullsLast: bool) =
        this.Sort([expr], ?descending=descending, ?nullsLast=nullsLast)

    member this.Sort(colName: string, ?descending: bool, ?nullsLast: bool) =
        this.Sort([Expr.Col colName], ?descending=descending, ?nullsLast=nullsLast)
    // Alias
    member this.OrderBy(columns: seq<#IColumnExpr>, ?descending: bool, ?nullsLast: bool) = 
        this.Sort(columns, ?descending=descending, ?nullsLast=nullsLast)
    // ==========================================
    // TopK / BottomK
    // ==========================================

    /// <summary>
    /// Get the top k rows based on the given columns.
    /// This is often faster than a full sort followed by a head.
    /// </summary>
    /// <param name="k">Number of rows to return.</param>
    /// <param name="by">Columns to sort by.</param>
    /// <param name="reverse">Sort direction per column. Default is false (no reverse).</param>
    member this.TopK(k: int, by: seq<#IColumnExpr>, ?reverse: seq<bool>) =
        let exprHandles = 
            by 
            |> Seq.collect (fun x -> x.ToExprs()) 
            |> Seq.map (fun e -> e.CloneHandle()) 
            |> Seq.toArray
        
        let descArr = 
            match reverse with
            | Some d -> d |> Seq.toArray
            | None -> [| false |] 

        let lfHandle = this.CloneHandle()

        let h = PolarsWrapper.LazyFrameTopK(lfHandle, uint k, exprHandles, descArr)
        new LazyFrame(h)

    /// <summary>
    /// Get the bottom k rows based on the given columns.
    /// </summary>
    member this.BottomK(k: int, by: seq<#IColumnExpr>, ?reverse: seq<bool>) =
        let exprHandles = 
            by 
            |> Seq.collect (fun x -> x.ToExprs()) 
            |> Seq.map (fun e -> e.CloneHandle()) 
            |> Seq.toArray
        
        let descArr = 
            match reverse with
            | Some d -> d |> Seq.toArray
            | None -> [| false |]

        let lfHandle = this.CloneHandle()
        let h = PolarsWrapper.LazyFrameBottomK(lfHandle, uint k, exprHandles, descArr)
        new LazyFrame(h)

    // [Overload] Sugar for single boolean reversing
    member this.TopK(k: int, by: seq<#IColumnExpr>, reverse: bool) =
        this.TopK(k, by, [| reverse |])
    
    member this.BottomK(k: int, by: seq<#IColumnExpr>, reverse: bool) =
        this.BottomK(k, by, [| reverse |])


    // ==========================================
    // Unnest
    // ==========================================

    /// <summary>
    /// Decompose a struct column into multiple columns.
    /// </summary>
    member this.Unnest(selector: Selector,?separator: string) =
        let lfHandle = this.CloneHandle()
        
        let selHandle = selector.CloneHandle()

        let sep = defaultArg separator null

        let h = PolarsWrapper.LazyFrameUnnest(lfHandle, selHandle, sep)
        new LazyFrame(h)

    /// <summary>
    /// Helper: Unnest columns by name.
    /// </summary>
    member this.Unnest(columns: string list,?separator: string) =
        let columnsArray = columns|> List.toArray
        let handle = PolarsWrapper.SelectorCols columnsArray
        let sel = new Selector(handle)
        this.Unnest (sel,?separator=separator)

    /// <summary>
    /// Helper: Unnest a single column by name.
    /// </summary>
    member this.Unnest(column: string, ?separator: string) =
        this.Unnest ([column], ?separator=separator)
    /// <summary>
    /// Limit the number of rows in the LazyFrame.
    /// This is an optimization hint that pushes down the limit to the scan if possible.
    /// </summary>
    /// <param name="n">Maximum number of rows to return.</param>
    member this.Limit (n: uint) : LazyFrame =
        let lfClone = this.CloneHandle()
        let h = PolarsWrapper.LazyLimit(lfClone, n)
        new LazyFrame(h)
    /// <summary>
    /// Add or replace a single column in the LazyFrame.
    /// </summary>
    /// <param name="expr">The expression defining the new column.</param>
    member this.WithColumn (expr: Expr) : LazyFrame =
        let lfClone = this.CloneHandle()
        let exprClone = expr.CloneHandle()
        let handles = [| exprClone |] 
        let h = PolarsWrapper.LazyWithColumns(lfClone, handles)
        new LazyFrame(h)
    /// <summary>
    /// Add or replace multiple columns in the LazyFrame.
    /// </summary>
    /// <param name="exprs">List of expressions defining the new columns.</param>
    member this.WithColumns (exprs: Expr list) : LazyFrame =
        let lfClone = this.CloneHandle()
        let handles = exprs |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        let h = PolarsWrapper.LazyWithColumns(lfClone, handles)
        new LazyFrame(h)
    /// <summary>
    /// Add or replace columns using generic column expressions (Expr or Selectors).
    /// </summary>
    member this.WithColumns (columns:seq<#IColumnExpr>) =
        let exprs = 
            columns 
            |> Seq.collect (fun x -> x.ToExprs()) 
            |> Seq.toList
        
        this.WithColumns exprs
    /// <summary>
    /// Group by keys and apply aggregate expressions.
    /// </summary>
    /// <param name="keys">Grouping keys.</param>
    /// <param name="aggs">Aggregation expressions to apply per group.</param>
    member this.GroupBy (keys: Expr list,aggs: Expr list) : LazyFrame =
        let lfClone = this.CloneHandle()
        let kHandles = keys |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        let aHandles = aggs |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        let h = PolarsWrapper.LazyGroupByAgg(lfClone, kHandles, aHandles)
        new LazyFrame(h)
    /// <summary>
    /// Group by keys and apply aggregations (Supports Selectors).
    /// </summary>
    member this.GroupBy(keys: seq<#IColumnExpr>, aggs: seq<#IColumnExpr>) =
            let kExprs = keys |> Seq.collect (fun x -> x.ToExprs()) |> Seq.toList
            let aExprs = aggs |> Seq.collect (fun x -> x.ToExprs()) |> Seq.toList
            this.GroupBy(kExprs, aExprs)
    /// <summary>
    /// Unpivot (Melt) the LazyFrame using Selectors.
    /// Primary overload backed by native binding.
    /// </summary>
    member this.Unpivot(index: Selector, on: Selector, variableName: string option, valueName: string option) : LazyFrame =
        let lfClone = this.CloneHandle()
        
        let hIndex = index.CloneHandle()
        let hOn = on.CloneHandle()
        let varN = Option.toObj variableName
        let valN = Option.toObj valueName
        
        new LazyFrame(PolarsWrapper.LazyUnpivot(lfClone, hIndex, hOn, varN, valN))

    /// <summary>
    /// Unpivot (Melt) overload for simple string lists.
    /// Auto-converts to Selectors.
    /// </summary>
    member this.Unpivot(index: seq<string>, on: seq<string>, variableName: string option, valueName: string option) =
        // 1. Convert Index strings to Selector
        let idxArr = Seq.toArray index
        let sIndex = new Selector(PolarsWrapper.SelectorCols idxArr)

        // 2. Convert On strings to Selector
        let onArr = Seq.toArray on
        let sOn = new Selector(PolarsWrapper.SelectorCols onArr)

        // 3. Route to main logic
        this.Unpivot(sIndex, sOn, variableName, valueName)

    member this.Unpivot(index: string list, on: string list) =
        this.Unpivot(index, on, None, None)

    // ==========================================
    // Aliases (Melt)
    // ==========================================
    
    member this.Melt(index: Selector, on: Selector, variableName, valueName) = 
        this.Unpivot(index, on, variableName, valueName)

    member this.Melt(index: seq<string>, on: seq<string>, variableName, valueName) = 
        this.Unpivot(index, on, variableName, valueName)

    member this.Melt(index: string list, on: string list) =
        this.Unpivot(index, on)
    member this.Explode(selector: Selector) : LazyFrame =
        let lfClone = this.CloneHandle()
        let sh = selector.CloneHandle()
        new LazyFrame(PolarsWrapper.LazyExplode(lfClone, sh))

    member this.Explode(columns: seq<string>) =
        let names = Seq.toArray columns
        let h = PolarsWrapper.SelectorCols names
        let sel = new Selector(h)
        this.Explode sel

    member this.Explode(column: string) = 
        this.Explode [column]

    /// <summary>
    /// JoinAsOf with string tolerance (e.g., "2d", "1h").
    /// </summary>
    member this.JoinAsOf(other: LazyFrame, 
                         leftOn: Expr, 
                         rightOn: Expr, 
                         byLeft: Expr list, 
                         byRight: Expr list, 
                         strategy: string option, 
                         tolerance: string option) : LazyFrame =
        
        let lClone = this.CloneHandle()
        let rClone = other.CloneHandle()
        
        let lOn = leftOn.CloneHandle()
        let rOn = rightOn.CloneHandle()
        
        let lByArr = byLeft |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        let rByArr = byRight |> List.map (fun e -> e.CloneHandle()) |> List.toArray

        let strat = defaultArg strategy "backward"
        let tol = Option.toObj tolerance 

        let h = PolarsWrapper.JoinAsOf(
            lClone, rClone, 
            lOn, rOn, 
            lByArr, rByArr,
            strat, tol
        )
        new LazyFrame(h)
    
    /// <summary>
    /// Slice the LazyFrame along the rows.
    /// </summary>
    member this.Slice(offset: int64, length: uint32) = 
        new LazyFrame(PolarsWrapper.LazySlice(this.Handle,offset, length))
    member this.Slice(offset: int64, length: int32) = 
        if length < 0 then raise(ArgumentOutOfRangeException(sprintf "Length must be non-negative."))
        else this.Slice(offset,length)

    /// <summary>
    /// JoinAsOf with TimeSpan tolerance.
    /// </summary>
    member this.JoinAsOf(other: LazyFrame, 
                         leftOn: Expr, 
                         rightOn: Expr, 
                         byLeft: Expr list, 
                         byRight: Expr list, 
                         strategy: string option, 
                         tolerance: TimeSpan option) : LazyFrame =
        
        let tolStr = 
            tolerance 
            |> Option.map DurationFormatter.ToPolarsString

        this.JoinAsOf(other, leftOn, rightOn, byLeft, byRight, strategy, tolStr)
    static member Concat  (lfs: LazyFrame list) (how: ConcatType) : LazyFrame =
        let handles = lfs |> List.map (fun lf -> lf.CloneHandle()) |> List.toArray
        new LazyFrame(PolarsWrapper.LazyConcat(handles, how.ToNative(), false, true))
    /// <summary>
    /// Perform a dynamic group-by (rolling window) and aggregation in one step.
    /// </summary>
    /// <param name="indexCol">Name of the time index column.</param>
    /// <param name="every">Period of the window (step size).</param>
    /// <param name="aggs">List of aggregation expressions to apply.</param>
    /// <param name="period">Size of the window. Defaults to 'every'.</param>
    /// <param name="offset">Offset of the window. Defaults to 0.</param>
    /// <param name="by">Additional grouping keys (e.g. ID, Category).</param>
    /// <param name="label">Label of the window (Left, Right, DataPoint).</param>
    /// <param name="includeBoundaries">Whether to include the window boundaries.</param>
    /// <param name="closedWindow">Which side of the window is closed.</param>
    /// <param name="startBy">Strategy to determine the start of the first window.</param>
    member this.GroupByDynamic(
        indexCol: string,
        every: TimeSpan,
        aggs: seq<#IColumnExpr>, 
        ?period: TimeSpan,
        ?offset: TimeSpan,
        ?by: seq<#IColumnExpr>, 
        ?label: Label,
        ?includeBoundaries: bool,
        ?closedWindow: ClosedWindow,
        ?startBy: StartBy
    ) : LazyFrame =
        let periodVal = defaultArg period every
        let offsetVal = defaultArg offset TimeSpan.Zero
        let labelVal = defaultArg label Label.Left
        let includeBoundariesVal = defaultArg includeBoundaries false
        let closedWindowVal = defaultArg closedWindow ClosedWindow.Left
        let startByVal = defaultArg startBy StartBy.WindowBound

        let everyStr = DurationFormatter.ToPolarsString every
        let periodStr = DurationFormatter.ToPolarsString periodVal
        let offsetStr = DurationFormatter.ToPolarsString offsetVal

        let keyExprs = 
            match by with
            | Some cols -> cols |> Seq.collect (fun x -> x.ToExprs()) |> Seq.toList
            | None -> []
        let keyHandles = keyExprs |> List.map (fun e -> e.CloneHandle()) |> List.toArray

        let aggExprs = 
            aggs 
            |> Seq.collect (fun x -> x.ToExprs()) 
            |> Seq.toList
        let aggHandles = aggExprs |> List.map (fun e -> e.CloneHandle()) |> List.toArray

        let lfHandle = this.CloneHandle()

        let newH = PolarsWrapper.LazyGroupByDynamic(
            lfHandle,
            indexCol,
            everyStr,
            periodStr,
            offsetStr,
            labelVal.ToNative(),
            includeBoundariesVal,
            closedWindowVal.ToNative(),
            startByVal.ToNative(),
            keyHandles,
            aggHandles
        )

        new LazyFrame(newH)

/// <summary>
/// SQL Context for executing SQL queries on registered LazyFrames.
/// </summary>
type SqlContext() =
    let handle = PolarsWrapper.SqlContextNew()
    
    interface IDisposable with
        member _.Dispose() = handle.Dispose()

    /// <summary> Register a LazyFrame as a table for SQL querying. </summary>
    member _.Register(name: string, lf: LazyFrame) =
        PolarsWrapper.SqlRegister(handle, name, lf.CloneHandle())

    /// <summary> Execute a SQL query and return a LazyFrame. </summary>
    member _.Execute(query: string) =
        new LazyFrame(PolarsWrapper.SqlExecute(handle, query))

type private TempSchema(schema: Map<string, DataType>) =
    let handles = 
        schema 
        |> Seq.map (fun kv -> 
            kv.Key, kv.Value.CreateHandle()
        )
        |> dict
        |> fun d -> new Dictionary<string, DataTypeHandle>(d)

    member this.Dictionary = handles

    interface IDisposable with
        member _.Dispose() =
            for h in handles.Values do h.Dispose()