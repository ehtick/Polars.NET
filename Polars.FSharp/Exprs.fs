namespace Polars.FSharp

open System
open Polars.NET.Core
open Apache.Arrow

type IColumnExpr =
    abstract member ToExprs : unit -> Expr list

/// <summary>
/// Represents a Polars Expression, which can be a column reference, a literal value, or a computation.
/// </summary>
and Expr(handle: ExprHandle) =
    member _.Handle = handle
    member internal this.CloneHandle() = PolarsWrapper.CloneExpr handle

    interface IColumnExpr with
        member this.ToExprs() = [this]

    // --- Namespaces ---
    /// <summary> Access naming operations (prefix/suffix). </summary>
    member this.Name = new NameOps(this.CloneHandle())
    /// <summary> Access list operations. </summary>
    member this.List = new ListOps(this.CloneHandle())
    /// <summary> Access struct operations. </summary>
    member this.Struct = new StructOps(this.CloneHandle())
    /// <summary> Access temporal (date/time) operations. </summary>
    member this.Dt = new DtOps(this.CloneHandle())
    /// <summary> Access string manipulation operations. </summary>
    member this.Str = new StringOps(this.CloneHandle())
    member this.Array = new ArrayOps(this.CloneHandle())

    // --- Column ---
    static member Col (name: string) = new Expr(PolarsWrapper.Col name)
    /// <summary> Select multiple columns (returns a Wildcard Expression). </summary>
    static member Cols (names: string list) =
        let arr = List.toArray names
        new Expr(PolarsWrapper.Cols arr)

    // --- Rounding & Sign ---
    member this.Round(decimals: int) = new Expr(PolarsWrapper.Round(this.CloneHandle(), uint decimals))
    /// <summary> Compute the element-wise sign. </summary>
    member this.Sign() = new Expr(PolarsWrapper.Sign(this.CloneHandle()))

    /// <summary> Round up to the nearest integer. </summary>
    member this.Ceil() = new Expr(PolarsWrapper.Ceil(this.CloneHandle()))

    /// <summary> Round down to the nearest integer. </summary>
    member this.Floor() = new Expr(PolarsWrapper.Floor(this.CloneHandle()))
    // ==========================================
    // Bitwise Operations (Custom Extension)
    // ==========================================

    /// <summary>
    /// Bitwise left shift (<<).
    /// </summary>
    member this.BitLeftShift(n: int) = 
        new Expr(PolarsWrapper.BitLeftShift(this.CloneHandle(), n))

    /// <summary>
    /// Bitwise right shift (>>).
    /// </summary>
    member this.BitRightShift(n: int) = 
        new Expr(PolarsWrapper.BitRightShift(this.CloneHandle(), n))

    // --- Operators ---
    /// <summary> Greater than. </summary>
    static member (.>) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Gt(lhs.CloneHandle(), rhs.CloneHandle()))
    /// <summary> Less than. </summary>
    static member (.<) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Lt(lhs.CloneHandle(), rhs.CloneHandle()))
    /// <summary> Greater than or equal to. </summary>
    static member (.>=) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.GtEq(lhs.CloneHandle(), rhs.CloneHandle()))
    /// <summary> Less than or equal to. </summary>
    static member (.<=) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.LtEq(lhs.CloneHandle(), rhs.CloneHandle()))
    /// <summary> Equal to. </summary>
    static member (.==) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Eq(lhs.CloneHandle(), rhs.CloneHandle()))
    /// <summary> Not equal to. </summary>
    static member (.!=) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Neq(lhs.CloneHandle(), rhs.CloneHandle()))
    // 运算符重载, Arithmetic
    static member ( + ) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Add(lhs.CloneHandle(), rhs.CloneHandle()))
    static member ( - ) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Sub(lhs.CloneHandle(), rhs.CloneHandle()))
    static member ( * ) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Mul(lhs.CloneHandle(), rhs.CloneHandle()))
    static member ( / ) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Div(lhs.CloneHandle(), rhs.CloneHandle()))
    static member ( % ) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Rem(lhs.CloneHandle(), rhs.CloneHandle()))
    /// <summary> Power / Exponentiation. </summary>
    static member (.**) (baseExpr: Expr, exponent: Expr) = baseExpr.Pow exponent
    /// <summary> Logical AND. </summary>
    static member (.&&) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.And(lhs.CloneHandle(), rhs.CloneHandle()))
    /// <summary> Logical OR. </summary>
    static member (.||) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Or(lhs.CloneHandle(), rhs.CloneHandle()))
    /// <summary> Logical NOT. </summary>
    static member (!!) (e: Expr) = new Expr(PolarsWrapper.Not (e.CloneHandle()))
    static member (.^) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Xor(lhs.CloneHandle(), rhs.CloneHandle()))
    
    /// <summary> Bitwise left shift operator (expr <<< n). </summary>
    static member (<<<) (lhs: Expr, rhs: int) = lhs.BitLeftShift rhs

    /// <summary> Bitwise right shift operator (expr >>> n). </summary>
    static member (>>>) (lhs: Expr, rhs: int) = lhs.BitRightShift rhs
    // --- Methods ---
    /// <summary> Rename the output column. </summary>
    member this.Alias(name: string) = new Expr(PolarsWrapper.Alias(this.CloneHandle(), name))

    /// <summary> Cast the expression to a different data type. </summary>
    member this.Cast(dtype: DataType, ?strict: bool) =
        let isStrict = defaultArg strict false
        
        // 1. 创建 Type Handle
        use typeHandle = dtype.CreateHandle()
        
        // 2. 调用更新后的 Wrapper
        let newHandle = PolarsWrapper.ExprCast(this.CloneHandle(), typeHandle, isStrict)
        
        new Expr(newHandle)
    // Aggregations
    member this.Sum() = new Expr(PolarsWrapper.Sum (this.CloneHandle()))
    member this.Mean() = new Expr(PolarsWrapper.Mean (this.CloneHandle()))
    member this.Max() = new Expr(PolarsWrapper.Max (this.CloneHandle()))
    member this.Min() = new Expr(PolarsWrapper.Min (this.CloneHandle()))
    // Math
    member this.Abs() = new Expr(PolarsWrapper.Abs (this.CloneHandle()))
    member this.Sqrt() = new Expr(PolarsWrapper.Sqrt(this.CloneHandle()))
    member this.Cbrt() = new Expr(PolarsWrapper.Cbrt(this.CloneHandle()))
    member this.Exp() = new Expr(PolarsWrapper.Exp(this.CloneHandle()))
    member this.Pow(exponent: Expr) = 
        new Expr(PolarsWrapper.Pow(this.CloneHandle(), exponent.CloneHandle()))
    member this.Pow(exponent: double) = 
        this.Pow(PolarsWrapper.Lit exponent |> fun h -> new Expr(h))
    member this.Pow(exponent: int) = 
        this.Pow(PolarsWrapper.Lit exponent |> fun h -> new Expr(h))
    /// <summary> Calculate the logarithm with the given base. </summary>
    member this.Log(baseVal: double) = 
        new Expr(PolarsWrapper.Log(this.CloneHandle(), baseVal))
    member this.Log(baseExpr: Expr) = 
        this.Ln() / baseExpr.Ln()
    /// <summary> Calculate the natural logarithm (base e). </summary>
    member this.Ln() = 
        this.Log Math.E

    /// <summary>
    /// Divide this expression by another.
    /// Result is always a float (True Division).
    /// </summary>
    member this.Truediv(other: Expr) =
        // 必须 CloneHandle，因为 Wrapper/Rust 会消耗掉它们
        new Expr(PolarsWrapper.Div(this.CloneHandle(), other.CloneHandle()))

    /// <summary>
    /// Integer division (floor division).
    /// </summary>
    member this.FloorDiv(other: Expr) =
        new Expr(PolarsWrapper.FloorDiv(this.CloneHandle(), other.CloneHandle()))

    /// <summary>
    /// Modulo operator (remainder).
    /// </summary>
    member this.Mod(other: Expr) =
        new Expr(PolarsWrapper.Rem(this.CloneHandle(), other.CloneHandle()))
    member this.Rem(other: Expr) = 
        this.Mod other
        
    // ==========================================
    // Math: Trigonometry (三角函数)
    // ==========================================
    member this.Sin() = new Expr(PolarsWrapper.Sin(this.CloneHandle()))
    member this.Cos() = new Expr(PolarsWrapper.Cos(this.CloneHandle()))
    member this.Tan() = new Expr(PolarsWrapper.Tan(this.CloneHandle()))
    
    member this.ArcSin() = new Expr(PolarsWrapper.ArcSin(this.CloneHandle()))
    member this.ArcCos() = new Expr(PolarsWrapper.ArcCos(this.CloneHandle()))
    member this.ArcTan() = new Expr(PolarsWrapper.ArcTan(this.CloneHandle()))

    // ==========================================
    // Math: Hyperbolic (双曲函数)
    // ==========================================

    member this.Sinh() = new Expr(PolarsWrapper.Sinh(this.CloneHandle()))
    member this.Cosh() = new Expr(PolarsWrapper.Cosh(this.CloneHandle()))
    member this.Tanh() = new Expr(PolarsWrapper.Tanh(this.CloneHandle()))
    
    member this.ArcSinh() = new Expr(PolarsWrapper.ArcSinh(this.CloneHandle()))
    member this.ArcCosh() = new Expr(PolarsWrapper.ArcCosh(this.CloneHandle()))
    member this.ArcTanh() = new Expr(PolarsWrapper.ArcTanh(this.CloneHandle()))
    // Stats
    /// <summary>
    /// Count the number of valid (non-null) values.
    /// </summary>
    member this.Count() = new Expr(PolarsWrapper.Count (this.CloneHandle()))
    member this.Std(?ddof: int) = 
        let d = defaultArg ddof 1 // Default sample std dev
        new Expr(PolarsWrapper.Std(this.CloneHandle(), d))
        
    member this.Var(?ddof: int) = 
        let d = defaultArg ddof 1
        new Expr(PolarsWrapper.Var(this.CloneHandle(), d))
        
    member this.Median() = new Expr(PolarsWrapper.Median (this.CloneHandle()))
    
    member this.Quantile(q: float, ?interpolation: string) =
        let method = defaultArg interpolation "linear"
        new Expr(PolarsWrapper.Quantile(this.CloneHandle(), q, method))
    // Logic
    /// <summary> Check if the value is between lower and upper bounds (inclusive). </summary>
    member this.IsBetween(lower: Expr, upper: Expr) =
        new Expr(PolarsWrapper.IsBetween(this.CloneHandle(), lower.CloneHandle(), upper.CloneHandle()))
    member this.FillNull(fillValue: Expr) = 
        new Expr(PolarsWrapper.FillNull(this.CloneHandle(), fillValue.CloneHandle()))
    member this.FillNan(fillValue:Expr) =
        new Expr(PolarsWrapper.FillNan(this.CloneHandle(), fillValue.CloneHandle()));
    member this.IsNull() = 
        new Expr(PolarsWrapper.IsNull(this.CloneHandle()))
    member this.IsNotNull() = 
        new Expr(PolarsWrapper.IsNotNull(this.CloneHandle()))
    // UDF
    /// <summary>
    /// Apply a custom C#/F# function (UDF) to the expression.
    /// The function receives an Apache Arrow Array and returns an Arrow Array.
    /// </summary>
    member this.Map(func: Func<IArrowArray, IArrowArray>, outputType: DataType) =
        use typeHandle = outputType.CreateHandle()
        let newHandle = PolarsWrapper.Map(this.CloneHandle(), func, typeHandle)
        new Expr(newHandle)
    member this.Map(func: Func<IArrowArray, IArrowArray>) =
        this.Map(func, DataType.SameAsInput)
    /// Advanced
    /// <summary> Explode a list column into multiple rows. </summary>
    member this.Explode() = new Expr(PolarsWrapper.Explode(this.CloneHandle()))
    /// <summary> Implode multiple rows to a list. </summary>
    member this.Implode() = new Expr(PolarsWrapper.Implode(this.CloneHandle()))
        // ==========================================
    // TopK / BottomK
    // ==========================================

    /// <summary>
    /// Get the k largest elements.
    /// Result is sorted descending.
    /// </summary>
    member this.TopK(k: int) = 
        new Expr(PolarsWrapper.TopK(this.CloneHandle(), uint k))

    /// <summary>
    /// Get the k smallest elements.
    /// Result is sorted ascending.
    /// </summary>
    member this.BottomK(k: int) = 
        new Expr(PolarsWrapper.BottomK(this.CloneHandle(), uint k))

    // ==========================================
    // TopKBy / BottomKBy
    // ==========================================

    /// <summary>
    /// Get the top k elements determined by the values in the 'by' columns.
    /// </summary>
    /// <param name="k">Number of elements to return.</param>
    /// <param name="by">Columns to sort by.</param>
    /// <param name="reverse">Reverse the sort order for each by column. Default false.</param>
    member this.TopKBy(k: int, by: seq<#IColumnExpr>, ?reverse: seq<bool>) =
        // 1. 准备 By Expr Handles
        let byHandles = 
            by 
            |> Seq.collect (fun x -> x.ToExprs()) 
            |> Seq.map (fun e -> e.CloneHandle()) 
            |> Seq.toArray
        
        // 2. 准备 Reverse 数组
        let revArr = 
            match reverse with
            | Some r -> r |> Seq.toArray
            | None -> [| false |] // C# 端会广播

        // 3. 调用 Wrapper
        // 注意：Wrapper 会接管 byHandles 的所有权
        new Expr(PolarsWrapper.TopKBy(this.CloneHandle(), uint k, byHandles, revArr))

    /// <summary>
    /// Get the bottom k elements determined by the values in the 'by' columns.
    /// </summary>
    member this.BottomKBy(k: int, by: seq<#IColumnExpr>, ?reverse: seq<bool>) =
        let byHandles = 
            by 
            |> Seq.collect (fun x -> x.ToExprs()) 
            |> Seq.map (fun e -> e.CloneHandle()) 
            |> Seq.toArray
        
        let revArr = 
            match reverse with
            | Some r -> r |> Seq.toArray
            | None -> [| false |]

        new Expr(PolarsWrapper.BottomKBy(this.CloneHandle(), uint k, byHandles, revArr))

    // --- Sugar Overloads (Single Column By) ---

    member this.TopKBy(k: int, by: #IColumnExpr, reverse: bool) =
        this.TopKBy(k, [by], [| reverse |])
    
    member this.TopKBy(k: int, by: #IColumnExpr) =
        this.TopKBy(k, [by], [| false |])

    member this.BottomKBy(k: int, by: #IColumnExpr, reverse: bool) =
        this.BottomKBy(k, [by], [| reverse |])

    member this.BottomKBy(k: int, by: #IColumnExpr) =
        this.BottomKBy(k, [by], [| false |])
    /// <summary> Apply a window function over specific partition columns. </summary>
    member this.Over(partitionBy: Expr list) =
        let mainHandle = this.CloneHandle()
        let partHandles = partitionBy |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        new Expr(PolarsWrapper.Over(mainHandle, partHandles))

    member this.Over(partitionCol: Expr) =
        this.Over [partitionCol]
    // Shift
    member this.Shift(n: int64) = new Expr(PolarsWrapper.Shift(this.CloneHandle(), n))
    // Default shift 1
    member this.Shift() = this.Shift 1L

    // Diff
    member this.Diff(n: int64) = new Expr(PolarsWrapper.Diff(this.CloneHandle(), n))
    // Default diff 1
    member this.Diff() = this.Diff 1L

    // Fill
    // limit: 0 means fill infinitely
    member this.ForwardFill(?limit: int) = 
        let l = defaultArg limit 0
        new Expr(PolarsWrapper.ForwardFill(this.CloneHandle(), uint l))

    member this.BackwardFill(?limit: int) = 
        let l = defaultArg limit 0
        new Expr(PolarsWrapper.BackwardFill(this.CloneHandle(), uint l))
    
    member this.FillNullStrategy(strategy: string) =
        match strategy.ToLower() with
        | "forward" | "ffill" -> this.ForwardFill()
        | "backward" | "bfill" -> this.BackwardFill()
        | _ -> failwith "Unsupported strategy"

    // ==========================================
    // Uniqueness & Duplication
    // ==========================================

    /// <summary>
    /// Get a boolean mask indicating which values are unique.
    /// </summary>
    member this.IsUnique() =
        new Expr(PolarsWrapper.ExprIsUnique(this.CloneHandle()))

    /// <summary>
    /// Get a boolean mask indicating which values are duplicated.
    /// </summary>
    member this.IsDuplicated() =
        new Expr(PolarsWrapper.ExprIsDuplicated(this.CloneHandle()))

    /// <summary>
    /// Get unique values of this expression.
    /// </summary>
    member this.Unique() =
        new Expr(PolarsWrapper.ExprUnique(this.CloneHandle()))

    /// <summary>
    /// Get unique values of this expression, maintaining order (Stable).
    /// </summary>
    member this.UniqueStable() =
        new Expr(PolarsWrapper.ExprUniqueStable(this.CloneHandle()))
    member this.RollingMin(windowSize: string, ?minPeriod: int) =
        let m = defaultArg minPeriod 1
        new Expr(PolarsWrapper.RollingMin(this.CloneHandle(), windowSize,m))
    member this.RollingMin(windowSize: TimeSpan, ?minPeriod: int) =
        let m = defaultArg minPeriod 1
        this.RollingMin(DurationFormatter.ToPolarsString windowSize,m)
        
    member this.RollingMax(windowSize: string, ?minPeriod: int) =
        let m = defaultArg minPeriod 1 
        new Expr(PolarsWrapper.RollingMax(this.CloneHandle(), windowSize,m))
    member this.RollingMax(windowSize: TimeSpan, ?minPeriod: int) =
        let m = defaultArg minPeriod 1
        this.RollingMax(DurationFormatter.ToPolarsString windowSize,m)

    member this.RollingMean(windowSize: string, ?minPeriod: int) = 
        let m = defaultArg minPeriod 1 
        new Expr(PolarsWrapper.RollingMean(this.CloneHandle(), windowSize, m))
    member this.RollingMean(windowSize: TimeSpan, ?minPeriod: int) =
        let m = defaultArg minPeriod 1
        this.RollingMean(DurationFormatter.ToPolarsString windowSize,m)
        
    member this.RollingSum(windowSize: string, ?minPeriod: int) =
        let m = defaultArg minPeriod 1  
        new Expr(PolarsWrapper.RollingSum(this.CloneHandle(), windowSize, m))
    member this.RollingSum(windowSize: TimeSpan, ?minPeriod: int) =
        let m = defaultArg minPeriod 1
        this.RollingSum(DurationFormatter.ToPolarsString windowSize,m)
    // 用法: col("price").RollingMeanBy("1d", col("date"))
    member this.RollingMeanBy(windowSize: string, by: Expr,?closed: string,?minPeriod: int) =
        let c = defaultArg closed "left"
        let m = defaultArg minPeriod 1
        new Expr(PolarsWrapper.RollingMeanBy(this.CloneHandle(), windowSize, m, by.CloneHandle(), c))
    member this.RollingMeanBy(windowSize: TimeSpan, by: Expr,?closed: string,?minPeriod: int) =
        let c = defaultArg closed "left"
        let m = defaultArg minPeriod 1
        this.RollingMeanBy(DurationFormatter.ToPolarsString windowSize,by,c,m)
    member this.RollingSumBy(windowSize: string, by: Expr, ?closed: string,?minPeriod: int) =
        let c = defaultArg closed "left"
        let m = defaultArg minPeriod 1 
        new Expr(PolarsWrapper.RollingSumBy(this.CloneHandle(), windowSize, m, by.CloneHandle(), c))
    member this.RollingSumBy(windowSize: TimeSpan, by: Expr,?closed: string,?minPeriod: int) =
        let c = defaultArg closed "left"
        let m = defaultArg minPeriod 1
        this.RollingSumBy(DurationFormatter.ToPolarsString windowSize,by,c,m)
    // 用法: col("price").RollingMeanBy("1d", col("date"))
    member this.RollingMaxBy(windowSize: string, by: Expr, ?closed: string, ?minPeriod: int) =
        let c = defaultArg closed "left"
        let m = defaultArg minPeriod 1 
        new Expr(PolarsWrapper.RollingMaxBy(this.CloneHandle(), windowSize, m, by.CloneHandle(), c))
    member this.RollingMaxBy(windowSize: TimeSpan, by: Expr,?closed: string,?minPeriod: int) =
        let c = defaultArg closed "left"
        let m = defaultArg minPeriod 1
        this.RollingMaxBy(DurationFormatter.ToPolarsString windowSize,by,c,m)
    member this.RollingMinBy(windowSize: string, by: Expr, ?closed: string, ?minPeriod: int) =
        let c = defaultArg closed "left"
        let m = defaultArg minPeriod 1 
        new Expr(PolarsWrapper.RollingMinBy(this.CloneHandle(), windowSize, m, by.CloneHandle(), c))
    member this.RollingMinBy(windowSize: TimeSpan, by: Expr,?closed: string,?minPeriod: int) =
        let c = defaultArg closed "left"
        let m = defaultArg minPeriod 1
        this.RollingMinBy(DurationFormatter.ToPolarsString windowSize,by,c,m)

// --- Namespace Helpers ---

and DtOps(handle: ExprHandle) =
    let wrap op = new Expr(op handle)
    member _.Year() = wrap PolarsWrapper.DtYear
    member _.Quarter() = wrap PolarsWrapper.DtQuarter
    member _.Month() = wrap PolarsWrapper.DtMonth
    member _.Day() = wrap PolarsWrapper.DtDay
    member _.Hour() = wrap PolarsWrapper.DtHour
    member _.Minute() = wrap PolarsWrapper.DtMinute
    member _.Second() = wrap PolarsWrapper.DtSecond
    member _.Millisecond() = wrap PolarsWrapper.DtMillisecond
    member _.Microsecond() = wrap PolarsWrapper.DtMicrosecond
    member _.Nanosecond() = wrap PolarsWrapper.DtNanosecond
    member _.OrdinalDay() = wrap PolarsWrapper.DtOrdinalDay
    member _.Weekday() = wrap PolarsWrapper.DtWeekday
    member _.Date() = wrap PolarsWrapper.DtDate
    member _.Time() = wrap PolarsWrapper.DtTime

    /// <summary> Format datetime to string using the given format string (strftime). </summary>
    member _.ToString(format: string) = 
        new Expr(PolarsWrapper.DtToString(handle, format)) // 注意这里 handle 是 Clone 进来的，Wrapper 会消耗它

    // col("date").Dt.ToString()
    member this.ToString() = 
        // 这是一个常见的 ISO 格式，或者你可以选择其他默认值
        this.ToString "%Y-%m-%dT%H:%M:%S%.f"
    // --- Manipulation ---

    /// <summary>
    /// Truncate dates to the specified interval (e.g., "1d", "1h", "15m").
    /// </summary>
    member _.Truncate(every: string) = 
        new Expr(PolarsWrapper.DtTruncate(handle, every))

    /// <summary>
    /// Round dates to the nearest interval.
    /// </summary>
    member _.Round(every: string) = 
        new Expr(PolarsWrapper.DtRound(handle, every))

    /// <summary>
    /// Offset the date by a given duration string (e.g., "1d", "-2h").
    /// </summary>
    member _.OffsetBy(duration: string) =
        // 自动创建 lit(duration)
        let durExpr = PolarsWrapper.Lit(duration) // 假设 Lit(string) wrapper 存在
        new Expr(PolarsWrapper.DtOffsetBy(handle, durExpr))

    /// <summary>
    /// Offset the date by a duration expression (dynamic offset).
    /// </summary>
    member _.OffsetBy(duration: Expr) =
        new Expr(PolarsWrapper.DtOffsetBy(handle, duration.CloneHandle()))

    // --- Conversion ---

    /// <summary>
    /// Convert to integer timestamp (Microseconds).
    /// </summary>
    member _.TimestampMicros() = 
        new Expr(PolarsWrapper.DtTimestamp(handle, 1))

    /// <summary>
    /// Convert to integer timestamp (Milliseconds).
    /// </summary>
    member _.TimestampMillis() = 
        new Expr(PolarsWrapper.DtTimestamp(handle, 2))
    /// <summary>
    /// Convert the datetime to a different time zone.
    /// The underlying physical value (UTC timestamp) remains the same, but the display time changes.
    /// </summary>
    member _.ConvertTimeZone(timeZone: string) =
        new Expr(PolarsWrapper.DtConvertTimeZone(handle, timeZone))

    /// <summary>
    /// Replace the time zone of a datetime.
    /// Use None (null) to make it TimeZone-Naive.
    /// ambiguous: Strategy for DST overlaps ("raise", "earliest", "latest", "null").
    /// nonExistent: Strategy for missing DST times ("raise", "null").
    /// </summary>
    member _.ReplaceTimeZone(timeZone: string option, ?ambiguous: string, ?nonExistent: string) =
        let tz = Option.toObj timeZone
        let amb = Option.toObj ambiguous
        let ne = Option.toObj nonExistent
        new Expr(PolarsWrapper.DtReplaceTimeZone(handle, tz, amb, ne))

    /// <summary>
    /// Helper: Replace time zone with a specific string.
    /// </summary>
    member this.ReplaceTimeZone(timeZone: string, ?ambiguous: string, ?nonExistent: string) =
        this.ReplaceTimeZone(Some timeZone, ?ambiguous=ambiguous, ?nonExistent=nonExistent)
    /// <summary>
    /// Add business days to a date column.
    /// </summary>
    /// <param name="n">Number of business days to add.</param>
    /// <param name="weekMask">Array of 7 bools (Mon-Sun) indicating business days. Default: [true, true, true, true, true, false, false].</param>
    /// <param name="holidays">List of holidays to skip.</param>
    /// <param name="roll">Strategy for handling non-business start dates. Default: Raise.</param>
    member this.AddBusinessDays(
        n: Expr, 
        ?weekMask: bool[], 
        ?holidays: seq<DateOnly>, 
        ?roll: Roll
    ) =
        // 1. 处理默认参数
        let mask = defaultArg weekMask [| true; true; true; true; true; false; false |]
        let r = defaultArg roll Roll.Raise
        
        // 2. 处理假期 (DateOnly -> int days since 1970-01-01)
        let epoch = DateOnly(1970, 1, 1).DayNumber
        let holidayInts = 
            match holidays with
            | Some hols -> hols |> Seq.map (fun d -> d.DayNumber - epoch) |> Seq.toArray
            | None -> [||]

        // 3. 调用 Wrapper
        new Expr(PolarsWrapper.DtAddBusinessDays(
            handle, 
            n.CloneHandle(), 
            mask, 
            holidayInts, 
            r.ToNative()
        ))

    /// <summary>
    /// Overload: Add business days using an integer literal.
    /// </summary>
    member this.AddBusinessDays(n: int, ?weekMask, ?holidays, ?roll) =
        let expr = new Expr(PolarsWrapper.Lit n)
        this.AddBusinessDays(
            expr, 
            ?weekMask = weekMask, 
            ?holidays = holidays, 
            ?roll = roll
        )

    /// <summary>
    /// Check if the date is a business day.
    /// </summary>
    /// <param name="weekMask">Array of 7 bools (Mon-Sun). Default: Mon-Fri are business days.</param>
    /// <param name="holidays">List of holidays.</param>
    member this.IsBusinessDay(?weekMask: bool[], ?holidays: seq<DateOnly>) =
        let mask = defaultArg weekMask [| true; true; true; true; true; false; false |]
        
        let epoch = DateOnly(1970, 1, 1).DayNumber
        let holidayInts = 
            match holidays with
            | Some hols -> hols |> Seq.map (fun d -> d.DayNumber - epoch) |> Seq.toArray
            | None -> [||]

        new Expr(PolarsWrapper.DtIsBusinessDay(
            handle,
            mask,
            holidayInts
        ))
        
and StringOps(handle: ExprHandle) =
    let wrap op = new Expr(op handle)
    
    /// <summary> Convert to uppercase. </summary>
    member _.ToUpper() = wrap PolarsWrapper.StrToUpper
    /// <summary> Convert to lowercase. </summary>
    member _.ToLower() = wrap PolarsWrapper.StrToLower
    /// <summary> Get length in bytes. </summary>
    member _.Len() = wrap PolarsWrapper.StrLenBytes
    // F# uint64 = C# ulong
    member _.Slice(offset: int64, length: uint64) = 
        new Expr(PolarsWrapper.StrSlice(handle, offset, length))
    member _.ReplaceAll(pattern: string, value: string, ?useRegex: bool) =
        let regex = defaultArg useRegex false
        new Expr(PolarsWrapper.StrReplaceAll(handle, pattern, value,regex))
    member _.Extract(pattern: string, groupIndex: int) =
        new Expr(PolarsWrapper.StrExtract(handle, pattern, uint groupIndex))
    member _.Contains(pat: string) = 
        new Expr(PolarsWrapper.StrContains(handle, pat))
    member _.Split(separator: string) = new Expr(PolarsWrapper.StrSplit(handle, separator))
    /// <summary>
    /// Remove leading and trailing characters.
    /// If 'matches' is omitted, whitespace is removed.
    /// </summary>
    member _.Strip(?matches: string) = 
        // Option.toObj: None -> null, Some s -> s
        new Expr(PolarsWrapper.StrStripChars(handle, Option.toObj matches))

    /// <summary>
    /// Remove leading characters (Left Trim).
    /// If 'matches' is omitted, whitespace is removed.
    /// </summary>
    member _.LStrip(?matches: string) = 
        new Expr(PolarsWrapper.StrStripCharsStart(handle, Option.toObj matches))

    /// <summary>
    /// Remove trailing characters (Right Trim).
    /// If 'matches' is omitted, whitespace is removed.
    /// </summary>
    member _.RStrip(?matches: string) = 
        new Expr(PolarsWrapper.StrStripCharsEnd(handle, Option.toObj matches))

    /// <summary>
    /// Remove a specific prefix string.
    /// </summary>
    member _.StripPrefix(prefix: string) = 
        new Expr(PolarsWrapper.StrStripPrefix(handle, prefix))

    /// <summary>
    /// Remove a specific suffix string.
    /// </summary>
    member _.StripSuffix(suffix: string) = 
        new Expr(PolarsWrapper.StrStripSuffix(handle, suffix))

    /// <summary>
    /// Check if string starts with a specific prefix.
    /// </summary>
    member _.StartsWith(prefix: string) = 
        new Expr(PolarsWrapper.StrStartsWith(handle, prefix))

    /// <summary>
    /// Check if string ends with a specific suffix.
    /// </summary>
    member _.EndsWith(suffix: string) = 
        new Expr(PolarsWrapper.StrEndsWith(handle, suffix))

    /// <summary>
    /// Parse string to Date using a format string (e.g., "%Y-%m-%d").
    /// </summary>
    member _.ToDate(format: string) = 
        new Expr(PolarsWrapper.StrToDate(handle, format))

    /// <summary>
    /// Parse string to Datetime using a format string.
    /// </summary>
    member _.ToDatetime(format: string) = 
        new Expr(PolarsWrapper.StrToDatetime(handle, format))

and NameOps(handle: ExprHandle) =
    let wrap op arg = new Expr(op(handle, arg))
    member _.Prefix(p: string) = wrap PolarsWrapper.Prefix p
    member _.Suffix(s: string) = wrap PolarsWrapper.Suffix s

and ListOps(handle: ExprHandle) =
    /// <summary> Get the first element of the list. </summary>
    member _.First() = new Expr(PolarsWrapper.ListFirst handle)
    /// <summary> Get element at index. </summary>
    member _.Get(index: int) = new Expr(PolarsWrapper.ListGet(handle, int64 index))
    /// <summary> Join list elements with separator. </summary>
    member _.Join(separator: string) = new Expr(PolarsWrapper.ListJoin(handle, separator))
    /// <summary> Get list length. </summary>
    member _.Len() = new Expr(PolarsWrapper.ListLen handle)
    /// <summary> Reverse the list. </summary>
    member _.Reverse() = new Expr(PolarsWrapper.ListReverse handle)
    // Aggregations within list
    member _.Sum() = new Expr(PolarsWrapper.ListSum handle)
    member _.Min() = new Expr(PolarsWrapper.ListMin handle)
    member _.Max() = new Expr(PolarsWrapper.ListMax handle)
    member _.Mean() = new Expr(PolarsWrapper.ListMean handle)
    /// <summary> Sort the list. </summary>
    member _.Sort(?descending: bool, ?nullsLast:bool,?maintainOrder: bool) =
        let desc = defaultArg descending false
        let nullsLastOption = defaultArg nullsLast false 
        let maintainOrderOption = defaultArg maintainOrder false 
        new Expr(PolarsWrapper.ListSort(handle, desc,nullsLastOption,maintainOrderOption))
    /// <summary>
    /// Combine the current expression with other expressions into a List.
    /// Result: [parent_val, other_val_1, other_val_2, ...]
    /// Equivalent to: pl.concatList([parent, others...])
    /// </summary>
    member _.Concat(others: seq<#IColumnExpr>) =
        // 1. 收集所有 Expr Handles
        let handles = 
            seq {
                // 第一个是自己 (Parent)
                yield handle
                
                // 后续是传入的 others (展开 Selector)
                yield! others 
                       |> Seq.collect (fun x -> x.ToExprs()) 
                       |> Seq.map (fun e -> e.CloneHandle())
            }
            |> Seq.toArray

        // 2. 调用 Wrapper
        new Expr(PolarsWrapper.ConcatList(handles))

    /// <summary>
    /// Overload: Concat a single expression/column.
    /// </summary>
    member this.Concat(other: #IColumnExpr) =
        this.Concat [other]
    // Contains
    member _.Contains(item: Expr) : Expr = 
        new Expr(PolarsWrapper.ListContains(handle, item.CloneHandle()))
    member _.Contains(item: int) = 
        let itemHandle = PolarsWrapper.Lit item
        new Expr(PolarsWrapper.ListContains(PolarsWrapper.CloneExpr handle, itemHandle))
    member _.Contains(item: string) = 
        let itemHandle = PolarsWrapper.Lit item
        new Expr(PolarsWrapper.ListContains(PolarsWrapper.CloneExpr handle, itemHandle))

and ArrayOps(handle: ExprHandle) = 
    // --- Aggregations ---

    /// <summary> Compute the sum of the values in the array. </summary>
    member _.Sum() = new Expr(PolarsWrapper.ArraySum(handle))

    /// <summary> Compute the minimum value in the array. </summary>
    member _.Min() = new Expr(PolarsWrapper.ArrayMin(handle))

    /// <summary> Compute the maximum value in the array. </summary>
    member _.Max() = new Expr(PolarsWrapper.ArrayMax(handle))

    /// <summary> Compute the mean value in the array. </summary>
    member _.Mean() = new Expr(PolarsWrapper.ArrayMean(handle))

    /// <summary> Compute the median value in the array. </summary>
    member _.Median() = new Expr(PolarsWrapper.ArrayMedian(handle))

    /// <summary> Compute the standard deviation of the values in the array. </summary>
    member _.Std(?ddof: int) = 
        let d = defaultArg ddof 1 |> byte
        new Expr(PolarsWrapper.ArrayStd(handle, d))

    /// <summary> Compute the variance of the values in the array. </summary>
    member _.Var(?ddof: int) = 
        let d = defaultArg ddof 1 |> byte
        new Expr(PolarsWrapper.ArrayVar(handle, d))

    // --- Boolean / Search ---

    /// <summary> Check if any value in the array is true. </summary>
    member _.Any() = new Expr(PolarsWrapper.ArrayAny handle)

    /// <summary> Check if all values in the array are true. </summary>
    member _.All() = new Expr(PolarsWrapper.ArrayAll handle)

    /// <summary> Check if the array contains a specific item. </summary>
    member _.Contains(item: Expr, ?nullsEqual: bool) =
        let eq = defaultArg nullsEqual false
        // Wrapper consumes item ownership
        new Expr(PolarsWrapper.ArrayContains(handle, item.CloneHandle(), eq))

    /// <summary> Check if the array contains a literal value. </summary>
    member this.Contains(item: string, ?nullsEqual) =
        let eq = defaultArg nullsEqual false 
        let itemHandle = PolarsWrapper.Lit item
        new Expr(PolarsWrapper.ArrayContains(handle, itemHandle, eq))
    member this.Contains(item: int, ?nullsEqual) =
        let eq = defaultArg nullsEqual false 
        let itemHandle = PolarsWrapper.Lit item
        new Expr(PolarsWrapper.ArrayContains(handle, itemHandle, eq))
    // --- Operations ---

    /// <summary> Get unique values in the array. </summary>
    member _.Unique(?stable: bool) = 
        let s = defaultArg stable false
        new Expr(PolarsWrapper.ArrayUnique(handle, s))

    /// <summary> Join array elements into a string. </summary>
    member _.Join(separator: string, ?ignoreNulls: bool) =
        let ign = defaultArg ignoreNulls true
        new Expr(PolarsWrapper.ArrayJoin(handle, separator, ign))

    /// <summary> Sort the array. </summary>
    member _.Sort(?descending: bool, ?nullsLast: bool, ?maintainOrder: bool) =
        let desc = defaultArg descending false
        let nLast = defaultArg nullsLast false
        let stable = defaultArg maintainOrder false 
        new Expr(PolarsWrapper.ArraySort(handle, desc, nLast, stable))

    /// <summary> Reverse the array. </summary>
    member _.Reverse() = new Expr(PolarsWrapper.ArrayReverse handle)

    /// <summary> Get the index of the minimum value. </summary>
    member _.ArgMin() = new Expr(PolarsWrapper.ArrayArgMin handle)

    /// <summary> Get the index of the maximum value. </summary>
    member _.ArgMax() = new Expr(PolarsWrapper.ArrayArgMax handle)

    /// <summary> Explode the array to rows. </summary>
    member _.Explode() = new Expr(PolarsWrapper.ArrayExplode handle)

    // --- Indexing ---

    /// <summary> Get value at index. </summary>
    member _.Get(index: Expr, ?nullOnOob: bool) =
        let oob = defaultArg nullOnOob false
        new Expr(PolarsWrapper.ArrayGet(handle, index.CloneHandle(), oob))

    /// <summary> Get value at integer index. </summary>
    member this.Get(index: int, ?nullOnOob) =
        let oob = defaultArg nullOnOob false
        let indexHandle = PolarsWrapper.Lit index
        new Expr(PolarsWrapper.ArrayGet(handle, indexHandle, oob))

    // --- Conversion ---

    /// <summary> Convert Array to List (variable length). </summary>
    member _.ToList() = new Expr(PolarsWrapper.ArrayToList handle)

    /// <summary> Convert Array to Struct. </summary>
    member _.ToStruct() = new Expr(PolarsWrapper.ArrayToStruct handle)

and StructOps(handle: ExprHandle) =
    /// <summary> Retrieve a field from the struct by name. </summary>
    member _.Field(name: string) = 
        new Expr(PolarsWrapper.StructFieldByName(handle, name))
    member _.Field(index: int) = 
        new Expr(PolarsWrapper.StructFieldByIndex(handle, index))
    member _.RenameFields(names: string list) =
        let cArr = List.toArray names
        new Expr(PolarsWrapper.StructRenameFields(handle, cArr));
    member _.JsonEncode() = 
        new Expr(PolarsWrapper.StructJsonEncode handle);


/// <summary>
/// A column selection strategy (e.g., all columns, or specific columns).
/// </summary>
and Selector(handle: SelectorHandle) =
    member _.Handle = handle
    
    member internal this.CloneHandle() = 
        PolarsWrapper.CloneSelector handle

    // ==========================================
    // Methods
    // ==========================================

    /// <summary> Exclude columns from a wildcard selection (col("*")). </summary>
    member this.Exclude(names: string list) =
        let arr = List.toArray names
        new Selector(PolarsWrapper.SelectorExclude(this.CloneHandle(), arr))
        
    /// <summary>
    /// Convert the Selector to an Expression.
    /// Selectors are essentially dynamic Expressions that expand to column names.
    /// </summary>
    member this.ToExpr() =
        new Expr(PolarsWrapper.SelectorToExpr(this.CloneHandle()))

    interface IColumnExpr with
        member this.ToExprs() = [this.ToExpr()]

    // ==========================================
    // Operators (The Magic 🪄)
    // ==========================================

    /// <summary> NOT operator: ~selector </summary>
    /// <example> ~~~pl.cs.numeric() </example>
    static member (~~~) (s: Selector) = 
        new Selector(PolarsWrapper.SelectorNot(s.CloneHandle()))

    /// <summary> AND operator: s1 &&& s2 (Intersection) </summary>
    /// <example> pl.cs.numeric() &&& pl.cs.matches("Val") </example>
    static member (&&&) (l: Selector, r: Selector) = 
        new Selector(PolarsWrapper.SelectorAnd(l.CloneHandle(), r.CloneHandle()))

    /// <summary> OR operator: s1 ||| s2 (Union) </summary>
    /// <example> pl.cs.startsWith("A") ||| pl.cs.endsWith("Z") </example>
    static member (|||) (l: Selector, r: Selector) = 
        new Selector(PolarsWrapper.SelectorOr(l.CloneHandle(), r.CloneHandle()))

    /// <summary> subtraction operator: s1 - s2 (Difference) </summary>
    /// <remarks> Some Polars versions support this as a shorthand for Exclude or Difference </remarks>
    static member (-) (l: Selector, r: Selector) =
        // 逻辑通常等同于: l &&& (~~~r)
        // 或者如果 Rust 有专门的 diff 接口
         new Selector(PolarsWrapper.SelectorAnd(l.CloneHandle(), PolarsWrapper.SelectorNot(r.CloneHandle())))

/// <summary>
/// 高级列选择表达式 DSL。
/// 允许包装 Expr，或者对 Selector 结果应用函数。
/// </summary>
type ColumnExpr =
    /// <summary> 普通表达式 </summary>
    | Plain of Expr
    
    /// <summary> 普通 Selector </summary>
    | Select of Selector
    
    /// <summary> 带映射的 Selector (先选列，再计算) </summary>
    /// <example> Map(pl.cs.numeric(), fun e -> e * pl.lit(2)) </example>
    | MapCols of Selector * (Expr -> Expr)

    // 实现接口：这是核心魔法
    interface IColumnExpr with
        member this.ToExprs() =
            match this with
            | Plain e -> [ e ]
            
            | Select s -> [ s.ToExpr() ]
            
            | MapCols (s, mapper) -> 
                // 1. Selector -> Wildcard Expr (e.g. col("*"))
                let wildcard = s.ToExpr()
                // 2. 应用映射函数 (e.g. col("*") * 2)
                let mappedExpr = mapper wildcard
                // 3. 返回列表
                [ mappedExpr ]