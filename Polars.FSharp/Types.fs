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
open System.Text
open System.IO
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
    /// Explode a list column into multiple rows.
    /// The resulting Series will be longer than the original.
    /// </summary>
    member this.Explode() =
        this.ApplyExpr(Expr.Col(this.Name).Explode())
    /// <summary>
    /// Aggregate values into a list.
    /// Result is a Series with 1 row containing a List of all values.
    /// </summary>
    member this.Implode() =
        this.ApplyExpr(Expr.Col(this.Name).Implode())
    /// <summary>
    /// Unnest a Struct column into a DataFrame.
    /// Shortcut for <see cref="SeriesStructOps.Unnest"/>.
    /// </summary>
    member this.Unnest() =
        this.Struct.Unnest()
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
    // Uniqueness & Boolean Masl
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
    /// <summary>
    /// Check if values are between lower and upper bounds.
    /// </summary>
    member this.IsBetween(lower:Expr, upper:Expr) = 
        this.ApplyExpr(Expr.Col(this.Name).IsBetween(lower,upper))
    /// <summary>
    /// Check if the value is in given collection.
    /// </summary>
    member this.IsIn(other:Expr, ?nullsEqual:bool) =
        this.ApplyExpr(Expr.Col(this.Name).IsIn(other=other,?nullsEqual=nullsEqual))
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

    /// <summary>
    /// Apply a rolling min (moving min) over a window.
    /// </summary>
    /// <param name="windowSize">
    /// The size of the window formatted as a string duration.
    /// <para>Examples: <c>"3i"</c> (3 index rows), <c>"1d"</c> (1 day), <c>"1h"</c> (1 hour).</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a value (otherwise <c>null</c>).
    /// </param>
    /// <param name="weights">
    /// Optional weights to apply to the window.
    /// <para>The length of the array should match the window size (if using fixed row windows).</para>
    /// <para>Default is <c>null</c> (unweighted).</para>
    /// </param>
    /// <param name="center">
    /// If <c>true</c>, the window is centered on the current observation.
    /// <para>Default is <c>false</c> (right-aligned window, <c>[i-window, i]</c>).</para>
    /// </param>
    /// <returns>A new <see cref="Series"/> with the rolling minimum.</returns>
    member this.RollingMin(windowSize: string, ?minPeriod: int,?weights: float[], ?center: bool) =
        this.ApplyExpr(Expr.Col(this.Name).RollingMin(windowSize, ?minPeriod=minPeriod,?weights=weights,?center=center))
    /// <summary>
    /// Apply a rolling min (moving min) over a window.
    /// </summary>
    /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a value (otherwise <c>null</c>).
    /// </param>
    /// <param name="weights">
    /// Optional weights to apply to the window.
    /// <para>The length of the array should match the window size (if using fixed row windows).</para>
    /// <para>Default is <c>null</c> (unweighted).</para>
    /// </param>
    /// <param name="center">
    /// If <c>true</c>, the window is centered on the current observation.
    /// <para>Default is <c>false</c> (right-aligned window, <c>[i-window, i]</c>).</para>
    /// </param>
    /// <returns>A new <see cref="Series"/> with the rolling minimum.</returns>
    member this.RollingMin(windowSize: TimeSpan, ?minPeriod: int,?weights: float[], ?center:bool) =
        this.ApplyExpr(Expr.Col(this.Name).RollingMin(windowSize, ?minPeriod=minPeriod,?weights=weights,?center=center))

    /// <summary>
    /// Apply a rolling max (moving max) over a window.
    /// </summary>
    /// <param name="windowSize">
    /// The size of the window formatted as a string duration.
    /// <para>Examples: <c>"3i"</c> (3 index rows), <c>"1d"</c> (1 day), <c>"1h"</c> (1 hour).</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a value (otherwise <c>null</c>).
    /// </param>
    /// <param name="weights">
    /// Optional weights to apply to the window.
    /// <para>The length of the array should match the window size (if using fixed row windows).</para>
    /// <para>Default is <c>null</c> (unweighted).</para>
    /// </param>
    /// <param name="center">
    /// If <c>true</c>, the window is centered on the current observation.
    /// <para>Default is <c>false</c> (right-aligned window, <c>[i-window, i]</c>).</para>
    /// </param>
    /// <returns>A new <see cref="Series"/> with the rolling maximum.</returns>
    member this.RollingMax(windowSize: string, ?minPeriod: int,?weights: float[], ?center:bool) =
        this.ApplyExpr(Expr.Col(this.Name).RollingMax(windowSize, ?minPeriod=minPeriod,?weights=weights,?center=center))
    /// <summary>
    /// Apply a rolling max (moving max) over a window.
    /// </summary>
    /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a value (otherwise <c>null</c>).
    /// </param>
    /// <param name="weights">
    /// Optional weights to apply to the window.
    /// <para>The length of the array should match the window size (if using fixed row windows).</para>
    /// <para>Default is <c>null</c> (unweighted).</para>
    /// </param>
    /// <param name="center">
    /// If <c>true</c>, the window is centered on the current observation.
    /// <para>Default is <c>false</c> (right-aligned window, <c>[i-window, i]</c>).</para>
    /// </param>
    /// <returns>A new <see cref="Series"/> with the rolling maximum.</returns>
    member this.RollingMax(windowSize: TimeSpan, ?minPeriod: int,?weights: float[], ?center:bool) =
        this.ApplyExpr(Expr.Col(this.Name).RollingMax(windowSize, ?minPeriod=minPeriod,?weights=weights,?center=center))

    /// <summary>
    /// Apply a rolling average (moving average) over a window.
    /// </summary>
    /// <param name="windowSize">
    /// The size of the window formatted as a string duration.
    /// <para>Examples: <c>"3i"</c> (3 index rows), <c>"1d"</c> (1 day), <c>"1h"</c> (1 hour).</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a value (otherwise <c>null</c>).
    /// </param>
    /// <param name="weights">
    /// Optional weights to apply to the window.
    /// <para>The length of the array should match the window size (if using fixed row windows).</para>
    /// <para>Default is <c>null</c> (unweighted).</para>
    /// </param>
    /// <param name="center">
    /// If <c>true</c>, the window is centered on the current observation.
    /// <para>Default is <c>false</c> (right-aligned window, <c>[i-window, i]</c>).</para>
    /// </param>
    /// <returns>A new <see cref="Series"/> with the rolling average.</returns>
    member this.RollingMean(windowSize: string, ?minPeriod: int,?weights: float[], ?center:bool) =
        this.ApplyExpr(Expr.Col(this.Name).RollingMean(windowSize, ?minPeriod=minPeriod,?weights=weights,?center=center))
    /// <summary>
    /// Apply a rolling average (moving average) over a window.
    /// </summary>
    /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a value (otherwise <c>null</c>).
    /// </param>
    /// <param name="weights">
    /// Optional weights to apply to the window.
    /// <para>The length of the array should match the window size (if using fixed row windows).</para>
    /// <para>Default is <c>null</c> (unweighted).</para>
    /// </param>
    /// <param name="center">
    /// If <c>true</c>, the window is centered on the current observation.
    /// <para>Default is <c>false</c> (right-aligned window, <c>[i-window, i]</c>).</para>
    /// </param>
    /// <returns>A new <see cref="Series"/> with the rolling average.</returns>
    member this.RollingMean(windowSize: TimeSpan, ?minPeriod: int,?weights: float[], ?center:bool) =
        this.ApplyExpr(Expr.Col(this.Name).RollingMean(windowSize, ?minPeriod=minPeriod,?weights=weights,?center=center))

    /// <summary>
    /// Apply a rolling sum (moving sum) over a window.
    /// </summary>
    /// <param name="windowSize">
    /// The size of the window formatted as a string duration.
    /// <para>Examples: <c>"3i"</c> (3 index rows), <c>"1d"</c> (1 day), <c>"1h"</c> (1 hour).</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a value (otherwise <c>null</c>).
    /// </param>
    /// <param name="weights">
    /// Optional weights to apply to the window.
    /// <para>The length of the array should match the window size (if using fixed row windows).</para>
    /// <para>Default is <c>null</c> (unweighted).</para>
    /// </param>
    /// <param name="center">
    /// If <c>true</c>, the window is centered on the current observation.
    /// <para>Default is <c>false</c> (right-aligned window, <c>[i-window, i]</c>).</para>
    /// </param>
    /// <returns>A new <see cref="Series"/> with the rolling sum.</returns>
    member this.RollingSum(windowSize: string, ?minPeriod: int,?weights: float[], ?center:bool) =
        this.ApplyExpr(Expr.Col(this.Name).RollingSum(windowSize, ?minPeriod=minPeriod,?weights=weights,?center=center))
    /// <summary>
    /// Apply a rolling sum (moving sum) over a window.
    /// </summary>
    /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a value (otherwise <c>null</c>).
    /// </param>
    /// <param name="weights">
    /// Optional weights to apply to the window.
    /// <para>The length of the array should match the window size (if using fixed row windows).</para>
    /// <para>Default is <c>null</c> (unweighted).</para>
    /// </param>
    /// <param name="center">
    /// If <c>true</c>, the window is centered on the current observation.
    /// <para>Default is <c>false</c> (right-aligned window, <c>[i-window, i]</c>).</para>
    /// </param>
    /// <returns>A new <see cref="Series"/> with the rolling sum.</returns>
    member this.RollingSum(windowSize: TimeSpan, ?minPeriod: int,?weights: float[], ?center:bool) =
        this.ApplyExpr(Expr.Col(this.Name).RollingSum(windowSize, ?minPeriod=minPeriod,?weights=weights,?center=center))

    /// <summary>
    /// Apply a rolling median (moving median) over a window.
    /// </summary>
    /// <param name="windowSize">
    /// The size of the window formatted as a string duration.
    /// <para>Examples: <c>"3i"</c> (3 index rows), <c>"1d"</c> (1 day), <c>"1h"</c> (1 hour).</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a value (otherwise <c>null</c>).
    /// </param>
    /// <param name="weights">
    /// Optional weights to apply to the window.
    /// <para>The length of the array should match the window size (if using fixed row windows).</para>
    /// <para>Default is <c>null</c> (unweighted).</para>
    /// </param>
    /// <param name="center">
    /// If <c>true</c>, the window is centered on the current observation.
    /// <para>Default is <c>false</c> (right-aligned window, <c>[i-window, i]</c>).</para>
    /// </param>
    /// <returns>A new <see cref="Series"/> with the rolling median.</returns>
    member this.RollingMedian(windowSize: string, ?minPeriod: int,?weights: float[], ?center:bool) =
        this.ApplyExpr(Expr.Col(this.Name).RollingMedian(windowSize, ?minPeriod=minPeriod,?weights=weights,?center=center))
    /// <summary>
    /// Apply a rolling median (moving median) over a window.
    /// </summary>
    /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a value (otherwise <c>null</c>).
    /// </param>
    /// <param name="weights">
    /// Optional weights to apply to the window.
    /// <para>The length of the array should match the window size (if using fixed row windows).</para>
    /// <para>Default is <c>null</c> (unweighted).</para>
    /// </param>
    /// <param name="center">
    /// If <c>true</c>, the window is centered on the current observation.
    /// <para>Default is <c>false</c> (right-aligned window, <c>[i-window, i]</c>).</para>
    /// </param>
    /// <returns>A new <see cref="Series"/> with the rolling median.</returns>
    member this.RollingMedian(windowSize: TimeSpan, ?minPeriod: int,?weights: float[], ?center:bool) =
        this.ApplyExpr(Expr.Col(this.Name).RollingMedian(windowSize, ?minPeriod=minPeriod,?weights=weights,?center=center))
    /// <summary>
    /// Apply a rolling standard deviation (moving standard deviation) over a window.
    /// </summary>
    /// <param name="windowSize">
    /// The size of the window formatted as a string duration.
    /// <para>Examples: <c>"3i"</c> (3 index rows), <c>"1d"</c> (1 day), <c>"1h"</c> (1 hour).</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a value (otherwise <c>null</c>).
    /// </param>
    /// <param name="weights">
    /// Optional weights to apply to the window.
    /// <para>The length of the array should match the window size (if using fixed row windows).</para>
    /// <para>Default is <c>null</c> (unweighted).</para>
    /// </param>
    /// <param name="center">
    /// If <c>true</c>, the window is centered on the current observation.
    /// <para>Default is <c>false</c> (right-aligned window, <c>[i-window, i]</c>).</para>
    /// </param>
    /// <returns>A new <see cref="Series"/> with the rolling standard deviation.</returns>
    member this.RollingStd(windowSize: string, ?minPeriod: int,?weights: float[], ?center:bool) =
        this.ApplyExpr(Expr.Col(this.Name).RollingStd(windowSize, ?minPeriod=minPeriod,?weights=weights,?center=center))

    /// <summary>
    /// Apply a rolling standard deviation (moving standard deviation) over a window.
    /// </summary>
    /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a value (otherwise <c>null</c>).
    /// </param>
    /// <param name="weights">
    /// Optional weights to apply to the window.
    /// <para>The length of the array should match the window size (if using fixed row windows).</para>
    /// <para>Default is <c>null</c> (unweighted).</para>
    /// </param>
    /// <param name="center">
    /// If <c>true</c>, the window is centered on the current observation.
    /// <para>Default is <c>false</c> (right-aligned window, <c>[i-window, i]</c>).</para>
    /// </param>
    /// <returns>A new <see cref="Series"/> with the rolling standard deviation.</returns>
    member this.RollingStd(windowSize: TimeSpan, ?minPeriod: int,?weights: float[], ?center:bool) =
        this.ApplyExpr(Expr.Col(this.Name).RollingStd(windowSize, ?minPeriod=minPeriod,?weights=weights,?center=center))
    /// <summary>
    /// Apply a rolling variance (moving variance) over a window.
    /// </summary>
    /// <param name="windowSize">
    /// The size of the window formatted as a string duration.
    /// <para>Examples: <c>"3i"</c> (3 index rows), <c>"1d"</c> (1 day), <c>"1h"</c> (1 hour).</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a value (otherwise <c>null</c>).
    /// </param>
    /// <param name="weights">
    /// Optional weights to apply to the window.
    /// <para>The length of the array should match the window size (if using fixed row windows).</para>
    /// <para>Default is <c>null</c> (unweighted).</para>
    /// </param>
    /// <param name="center">
    /// If <c>true</c>, the window is centered on the current observation.
    /// <para>Default is <c>false</c> (right-aligned window, <c>[i-window, i]</c>).</para>
    /// </param>
    /// <returns>A new <see cref="Series"/> with the rolling variance.</returns>
    member this.RollingVar(windowSize: string, ?minPeriod: int,?weights: float[], ?center:bool,?ddof:uint8) =
        this.ApplyExpr(Expr.Col(this.Name).RollingVar(windowSize, ?minPeriod=minPeriod,?weights=weights,?center=center,?ddof=ddof))
    /// <summary>
    /// Apply a rolling variance (moving variance) over a window.
    /// </summary>
    /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a value (otherwise <c>null</c>).
    /// </param>
    /// <param name="weights">
    /// Optional weights to apply to the window.
    /// <para>The length of the array should match the window size (if using fixed row windows).</para>
    /// <para>Default is <c>null</c> (unweighted).</para>
    /// </param>
    /// <param name="center">
    /// If <c>true</c>, the window is centered on the current observation.
    /// <para>Default is <c>false</c> (right-aligned window, <c>[i-window, i]</c>).</para>
    /// </param>
    /// <param name="ddof">
    /// “Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. 
    /// <para>By default ddof is 1.</para>
    /// </param>
    /// <returns>A new <see cref="Series"/> with the rolling variance.</returns>
    member this.RollingVar(windowSize: TimeSpan, ?minPeriod: int,?weights: float[], ?center:bool,?ddof:uint8) =
        this.ApplyExpr(Expr.Col(this.Name).RollingVar(windowSize, ?minPeriod=minPeriod,?weights=weights,?center=center,?ddof=ddof))
    /// <summary>
    /// Apply a rolling skew (moving skew) over a window.
    /// </summary>
    /// <param name="windowSize">
    /// The size of the window formatted as a string duration.
    /// <para>Examples: <c>"3i"</c> (3 index rows), <c>"1d"</c> (1 day), <c>"1h"</c> (1 hour).</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a value (otherwise <c>null</c>).
    /// </param>
    /// <param name="weights">
    /// Optional weights to apply to the window.
    /// <para>The length of the array should match the window size (if using fixed row windows).</para>
    /// <para>Default is <c>null</c> (unweighted).</para>
    /// </param>
    /// <param name="center">
    /// If <c>true</c>, the window is centered on the current observation.
    /// <para>Default is <c>false</c> (right-aligned window, <c>[i-window, i]</c>).</para>
    /// </param>
    /// <param name="bias">If False, the calculations are corrected for statistical bias.</param>
    /// <returns>A new <see cref="Series"/> with the rolling skew.</returns>
    member this.RollingSkew(windowSize: string, ?minPeriod: int,?weights: float[], ?center:bool,?bias:bool) =
        this.ApplyExpr(Expr.Col(this.Name).RollingSkew(windowSize, ?minPeriod=minPeriod,?weights=weights,?center=center,?bias=bias))
    /// <summary>
    /// Apply a rolling skew (moving skew) over a window.
    /// </summary>
    /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a value (otherwise <c>null</c>).
    /// </param>
    /// <param name="weights">
    /// Optional weights to apply to the window.
    /// <para>The length of the array should match the window size (if using fixed row windows).</para>
    /// <para>Default is <c>null</c> (unweighted).</para>
    /// </param>
    /// <param name="center">
    /// If <c>true</c>, the window is centered on the current observation.
    /// <para>Default is <c>false</c> (right-aligned window, <c>[i-window, i]</c>).</para>
    /// </param>
    /// <param name="bias">If False, the calculations are corrected for statistical bias.</param>
    /// <returns>A new <see cref="Series"/> with the rolling skew.</returns>
    member this.RollingSkew(windowSize: TimeSpan, ?minPeriod: int,?weights: float[], ?center:bool,?bias:bool) =
        this.ApplyExpr(Expr.Col(this.Name).RollingSkew(windowSize, ?minPeriod=minPeriod,?weights=weights,?center=center,?bias=bias))
    /// <summary>
    /// Apply a rolling skew (moving skew) over a window.
    /// </summary>
    /// <param name="windowSize">
    /// The size of the window formatted as a string duration.
    /// <para>Examples: <c>"3i"</c> (3 index rows), <c>"1d"</c> (1 day), <c>"1h"</c> (1 hour).</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a value (otherwise <c>null</c>).
    /// </param>
    /// <param name="weights">
    /// Optional weights to apply to the window.
    /// <para>The length of the array should match the window size (if using fixed row windows).</para>
    /// <para>Default is <c>null</c> (unweighted).</para>
    /// </param>
    /// <param name="center">
    /// If <c>true</c>, the window is centered on the current observation.
    /// <para>Default is <c>false</c> (right-aligned window, <c>[i-window, i]</c>).</para>
    /// </param>
    /// <param name="fisher">If True, Fisher’s definition is used (normal ==> 0.0). If False, Pearson’s definition is used (normal ==> 3.0).</param>
    /// <param name="bias">If False, the calculations are corrected for statistical bias.</param>
    /// <returns>A new <see cref="Series"/> with the rolling skew.</returns>
    member this.RollingKurtosis(windowSize: string, ?minPeriod: int,?weights: float[], ?center:bool,?fisher:bool,?bias:bool) =
        this.ApplyExpr(Expr.Col(this.Name).RollingKurtosis(windowSize, ?minPeriod=minPeriod,?weights=weights,?center=center,?fisher=fisher,?bias=bias))
    /// <summary>
    /// Apply a rolling skew (moving skew) over a window.
    /// </summary>
    /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a value (otherwise <c>null</c>).
    /// </param>
    /// <param name="weights">
    /// Optional weights to apply to the window.
    /// <para>The length of the array should match the window size (if using fixed row windows).</para>
    /// <para>Default is <c>null</c> (unweighted).</para>
    /// </param>
    /// <param name="center">
    /// If <c>true</c>, the window is centered on the current observation.
    /// <para>Default is <c>false</c> (right-aligned window, <c>[i-window, i]</c>).</para>
    /// </param>
    /// <param name="fisher">If True, Fisher’s definition is used (normal ==> 0.0). If False, Pearson’s definition is used (normal ==> 3.0).</param>
    /// <param name="bias">If False, the calculations are corrected for statistical bias.</param>
    /// <returns>A new <see cref="Series"/> with the rolling skew.</returns>
    member this.RollingKurtosis(windowSize: TimeSpan, ?minPeriod: int,?weights: float[], ?center:bool,?fisher:bool,?bias:bool) =
        this.ApplyExpr(Expr.Col(this.Name).RollingKurtosis(windowSize, ?minPeriod=minPeriod,?weights=weights,?center=center,?fisher=fisher,?bias=bias))
    /// <summary>
    /// Apply a rolling rank (moving rank) over a window.
    /// </summary>
    /// <param name="method">
    /// The method used to assign ranks to tied elements. See <see cref="RankMethod"/> for details.
    /// Default is <see cref="RankMethod.Average"/>.</param>
    /// <param name="seed">If method="random", use this as seed.
    /// </param>
    /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a value (otherwise <c>null</c>).
    /// </param>
    /// <param name="weights">
    /// Optional weights to apply to the window.
    /// <para>The length of the array should match the window size (if using fixed row windows).</para>
    /// <para>Default is <c>null</c> (unweighted).</para>
    /// </param>
    /// <param name="center">
    /// If <c>true</c>, the window is centered on the current observation.
    /// <para>Default is <c>false</c> (right-aligned window, <c>[i-window, i]</c>).</para>
    /// </param>
    member this.RollingRank(windowSize: string, ?minPeriod: int,?method:RankMethod,?seed:uint64,?weights: float[], ?center:bool) =
        this.ApplyExpr(Expr.Col(this.Name).RollingRank(windowSize, ?minPeriod=minPeriod,?method=method,?seed=seed,?weights=weights,?center=center))
    /// <summary>
    /// Apply a rolling rank (moving rank) over a window.
    /// </summary>
    /// <param name="method">
    /// The method used to assign ranks to tied elements. See <see cref="RankMethod"/> for details.
    /// Default is <see cref="RankMethod.Average"/>.</param>
    /// <param name="seed">If method="random", use this as seed.
    /// </param>
    /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a value (otherwise <c>null</c>).
    /// </param>
    /// <param name="weights">
    /// Optional weights to apply to the window.
    /// <para>The length of the array should match the window size (if using fixed row windows).</para>
    /// <para>Default is <c>null</c> (unweighted).</para>
    /// </param>
    /// <param name="center">
    /// If <c>true</c>, the window is centered on the current observation.
    /// <para>Default is <c>false</c> (right-aligned window, <c>[i-window, i]</c>).</para>
    /// </param>
    member this.RollingRank(windowSize: TimeSpan, ?minPeriod: int,?method:RankMethod,?seed:uint64,?weights: float[], ?center:bool) =
        this.ApplyExpr(Expr.Col(this.Name).RollingRank(windowSize, ?minPeriod=minPeriod,?method=method,?seed=seed,?weights=weights,?center=center))
    /// <summary>
    /// Apply a rolling quantile over a fixed window.
    /// </summary>
    /// <param name="quantile">Quantile between 0.0 and 1.0 (e.g., 0.5 for median).</param>
    /// <param name="method">Interpolation method when the quantile lies between two data points.</param>
    /// <param name="windowSize">
    /// The size of the window. 
    /// <para>Format: <c>"3i"</c> (3 rows) or just a number string <c>"3"</c>.</para>
    /// <para>For time-based windows (e.g. "2h"), use <see cref="RollingQuantileBy(double,QuantileMethod,string,Expr,int,ClosedWindow)"/> instead.</para>
    /// </param>
    /// <param name="weights">
    /// Optional weights for the window. The length must match the parsed window size.
    /// <para>If <c>null</c>, equal weights are used.</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a value (otherwise <c>null</c>).
    /// </param>
    /// <param name="center">
    /// If <c>true</c>, the window is centered on the current observation.
    /// <para>Default is <c>false</c> (right-aligned window, <c>[i-window, i]</c>).</para>
    /// </param>
    /// <returns>A new series representing the rolling quantile.</returns>
    member this.RollingQuantile(quantile:float,method:QuantileMethod,windowSize: string, ?minPeriod: int,?weights: float[], ?center:bool) =
        this.ApplyExpr(Expr.Col(this.Name).RollingQuantile(quantile,method,windowSize, ?minPeriod=minPeriod,?weights=weights,?center=center))
    /// <summary>
    /// Apply a rolling quantile over a fixed window.
    /// </summary>
    /// <param name="quantile">Quantile between 0.0 and 1.0 (e.g., 0.5 for median).</param>
    /// <param name="method">Interpolation method when the quantile lies between two data points.</param>
    /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// <param name="weights">
    /// Optional weights for the window. The length must match the parsed window size.
    /// <para>If <c>null</c>, equal weights are used.</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a value (otherwise <c>null</c>).
    /// </param>
    /// <param name="center">
    /// If <c>true</c>, the window is centered on the current observation.
    /// <para>Default is <c>false</c> (right-aligned window, <c>[i-window, i]</c>).</para>
    /// </param>
    /// <returns>A new series representing the rolling quantile.</returns>
    member this.RollingQuantile(quantile:float,method:QuantileMethod,windowSize: TimeSpan, ?minPeriod: int,?weights: float[], ?center:bool) =
        this.ApplyExpr(Expr.Col(this.Name).RollingQuantile(quantile,method,windowSize, ?minPeriod=minPeriod,?weights=weights,?center=center))
    // ==========================================
    // Rolling ... By
    // ==========================================

    /// <summary>
    /// Apply a rolling mean (moving average) over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// <para>
    /// Unlike standard fixed-size rolling windows (which operate on row counts), this operates on values (typically time).
    /// </para>
    /// </summary>
    /// <remarks>
    /// The <paramref name="by"/> column must be sorted in ascending order.
    /// </remarks>
    /// <param name="windowSize">
    /// The size of the dynamic window.
    /// <para>Supported duration strings: <c>"1d"</c>, <c>"2h"</c>, <c>"10s"</c>, <c>"500ms"</c>, etc.</para>
    /// </param>
    /// <param name="by">
    /// The column used to define the window (the "time" axis). 
    /// <para>Typically a <c>Date</c> or <c>DateTime</c> column, but can also be monotonic integers.</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a non-null result.</param>
    /// <param name="closed">
    /// Defines how the window interval is closed. 
    /// Default is <see cref="ClosedWindow.Left"/> <c>[t - window, t)</c>.
    /// </param>
    /// <returns>A new series representing the dynamic rolling mean.</returns>
    member this.RollingMeanBy(windowSize: string, by: Expr, ?closed: ClosedWindow, ?minPeriod: int) =
        this.ApplyExpr(Expr.Col(this.Name).RollingMeanBy(windowSize, by, ?closed=closed, ?minPeriod=minPeriod))
    /// <summary>
    /// Apply a rolling mean (moving average) over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// <para>
    /// Unlike standard fixed-size rolling windows (which operate on row counts), this operates on values (typically time).
    /// </para>
    /// </summary>
    /// <remarks>
    /// The <paramref name="by"/> column must be sorted in ascending order.
    /// </remarks>
    /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// <param name="by">
    /// The column used to define the window (the "time" axis). 
    /// <para>Typically a <c>Date</c> or <c>DateTime</c> column, but can also be monotonic integers.</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a non-null result.</param>
    /// <param name="closed">
    /// Defines how the window interval is closed. 
    /// Default is <see cref="ClosedWindow.Left"/> <c>[t - window, t)</c>.
    /// </param>
    /// <returns>A new series representing the dynamic rolling mean.</returns>
    member this.RollingMeanBy(windowSize: TimeSpan, by: Expr, ?closed: ClosedWindow, ?minPeriod: int) =
        this.ApplyExpr(Expr.Col(this.Name).RollingMeanBy(windowSize, by, ?closed=closed, ?minPeriod=minPeriod))

    /// <summary>
    /// Apply a rolling sum (moving sum) over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// <para>
    /// Unlike standard fixed-size rolling windows (which operate on row counts), this operates on values (typically time).
    /// </para>
    /// </summary>
    /// <remarks>
    /// The <paramref name="by"/> column must be sorted in ascending order.
    /// </remarks>
    /// <param name="windowSize">
    /// The size of the dynamic window.
    /// <para>Supported duration strings: <c>"1d"</c>, <c>"2h"</c>, <c>"10s"</c>, <c>"500ms"</c>, etc.</para>
    /// </param>
    /// <param name="by">
    /// The column used to define the window (the "time" axis). 
    /// <para>Typically a <c>Date</c> or <c>DateTime</c> column, but can also be monotonic integers.</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a non-null result.</param>
    /// <param name="closed">
    /// Defines how the window interval is closed. 
    /// Default is <see cref="ClosedWindow.Left"/> <c>[t - window, t)</c>.
    /// </param>
    /// <returns>A new series representing the dynamic rolling sum.</returns>
    member this.RollingSumBy(windowSize: string, by: Expr, ?closed: ClosedWindow, ?minPeriod: int) =
        this.ApplyExpr(Expr.Col(this.Name).RollingSumBy(windowSize, by, ?closed=closed, ?minPeriod=minPeriod))
    /// <summary>
    /// Apply a rolling sum (moving sum) over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// <para>
    /// Unlike standard fixed-size rolling windows (which operate on row counts), this operates on values (typically time).
    /// </para>
    /// </summary>
    /// <remarks>
    /// The <paramref name="by"/> column must be sorted in ascending order.
    /// </remarks>
    /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// <param name="by">
    /// The column used to define the window (the "time" axis). 
    /// <para>Typically a <c>Date</c> or <c>DateTime</c> column, but can also be monotonic integers.</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a non-null result.</param>
    /// <param name="closed">
    /// Defines how the window interval is closed. 
    /// Default is <see cref="ClosedWindow.Left"/> <c>[t - window, t)</c>.
    /// </param>
    /// <returns>A new series representing the dynamic rolling sum.</returns>
    member this.RollingSumBy(windowSize: TimeSpan, by: Expr, ?closed: ClosedWindow, ?minPeriod: int) =
        this.ApplyExpr(Expr.Col(this.Name).RollingSumBy(windowSize, by, ?closed=closed, ?minPeriod=minPeriod))

    /// <summary>
    /// Apply a rolling min (moving min) over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// <para>
    /// Unlike standard fixed-size rolling windows (which operate on row counts), this operates on values (typically time).
    /// </para>
    /// </summary>
    /// <remarks>
    /// The <paramref name="by"/> column must be sorted in ascending order.
    /// </remarks>
    /// <param name="windowSize">
    /// The size of the dynamic window.
    /// <para>Supported duration strings: <c>"1d"</c>, <c>"2h"</c>, <c>"10s"</c>, <c>"500ms"</c>, etc.</para>
    /// </param>
    /// <param name="by">
    /// The column used to define the window (the "time" axis). 
    /// <para>Typically a <c>Date</c> or <c>DateTime</c> column, but can also be monotonic integers.</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a non-null result.</param>
    /// <param name="closed">
    /// Defines how the window interval is closed. 
    /// Default is <see cref="ClosedWindow.Left"/> <c>[t - window, t)</c>.
    /// </param>
    /// <returns>A new series representing the dynamic rolling min.</returns>
    member this.RollingMinBy(windowSize: string, by: Expr, ?closed: ClosedWindow, ?minPeriod: int) =
        this.ApplyExpr(Expr.Col(this.Name).RollingMinBy(windowSize, by, ?closed=closed, ?minPeriod=minPeriod))
    /// <summary>
    /// Apply a rolling min (moving min) over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// <para>
    /// Unlike standard fixed-size rolling windows (which operate on row counts), this operates on values (typically time).
    /// </para>
    /// </summary>
    /// <remarks>
    /// The <paramref name="by"/> column must be sorted in ascending order.
    /// </remarks>
    /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// <param name="by">
    /// The column used to define the window (the "time" axis). 
    /// <para>Typically a <c>Date</c> or <c>DateTime</c> column, but can also be monotonic integers.</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a non-null result.</param>
    /// <param name="closed">
    /// Defines how the window interval is closed. 
    /// Default is <see cref="ClosedWindow.Left"/> <c>[t - window, t)</c>.
    /// </param>
    /// <returns>A new series representing the dynamic rolling min.</returns>
    member this.RollingMinBy(windowSize: TimeSpan, by: Expr, ?closed: ClosedWindow, ?minPeriod: int) =
        this.ApplyExpr(Expr.Col(this.Name).RollingMinBy(windowSize, by, ?closed=closed, ?minPeriod=minPeriod))

    /// <summary>
    /// Apply a rolling max (moving max) over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// <para>
    /// Unlike standard fixed-size rolling windows (which operate on row counts), this operates on values (typically time).
    /// </para>
    /// </summary>
    /// <remarks>
    /// The <paramref name="by"/> column must be sorted in ascending order.
    /// </remarks>
    /// <param name="windowSize">
    /// The size of the dynamic window.
    /// <para>Supported duration strings: <c>"1d"</c>, <c>"2h"</c>, <c>"10s"</c>, <c>"500ms"</c>, etc.</para>
    /// </param>
    /// <param name="by">
    /// The column used to define the window (the "time" axis). 
    /// <para>Typically a <c>Date</c> or <c>DateTime</c> column, but can also be monotonic integers.</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a non-null result.</param>
    /// <param name="closed">
    /// Defines how the window interval is closed. 
    /// Default is <see cref="ClosedWindow.Left"/> <c>[t - window, t)</c>.
    /// </param>
    /// <returns>A new series representing the dynamic rolling max.</returns>
    member this.RollingMaxBy(windowSize: string, by: Expr, ?closed: ClosedWindow, ?minPeriod: int) =
        this.ApplyExpr(Expr.Col(this.Name).RollingMaxBy(windowSize, by, ?closed=closed, ?minPeriod=minPeriod))
    /// <summary>
    /// Apply a rolling median (moving median) over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// <para>
    /// Unlike standard fixed-size rolling windows (which operate on row counts), this operates on values (typically time).
    /// </para>
    /// </summary>
    /// <remarks>
    /// The <paramref name="by"/> column must be sorted in ascending order.
    /// </remarks>
    /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// <param name="by">
    /// The column used to define the window (the "time" axis). 
    /// <para>Typically a <c>Date</c> or <c>DateTime</c> column, but can also be monotonic integers.</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a non-null result.</param>
    /// <param name="closed">
    /// Defines how the window interval is closed. 
    /// Default is <see cref="ClosedWindow.Left"/> <c>[t - window, t)</c>.
    /// </param>
    /// <returns>A new series representing the dynamic rolling median.</returns>
    member this.RollingMedianBy(windowSize: TimeSpan, by: Expr, ?closed: ClosedWindow, ?minPeriod: int) =
        this.ApplyExpr(Expr.Col(this.Name).RollingMedianBy(windowSize, by, ?closed=closed, ?minPeriod=minPeriod))
    /// <summary>
    /// Apply a rolling median (moving median) over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// <para>
    /// Unlike standard fixed-size rolling windows (which operate on row counts), this operates on values (typically time).
    /// </para>
    /// </summary>
    /// <remarks>
    /// The <paramref name="by"/> column must be sorted in ascending order.
    /// </remarks>
    /// <param name="windowSize">
    /// The size of the dynamic window.
    /// <para>Supported duration strings: <c>"1d"</c>, <c>"2h"</c>, <c>"10s"</c>, <c>"500ms"</c>, etc.</para>
    /// </param>
    /// <param name="by">
    /// The column used to define the window (the "time" axis). 
    /// <para>Typically a <c>Date</c> or <c>DateTime</c> column, but can also be monotonic integers.</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a non-null result.</param>
    /// <param name="closed">
    /// Defines how the window interval is closed. 
    /// Default is <see cref="ClosedWindow.Left"/> <c>[t - window, t)</c>.
    /// </param>
    /// <returns>A new series representing the dynamic rolling max.</returns>
    member this.RollingMedianBy(windowSize: string, by: Expr, ?closed: ClosedWindow, ?minPeriod: int) =
        this.ApplyExpr(Expr.Col(this.Name).RollingMedianBy(windowSize, by, ?closed=closed, ?minPeriod=minPeriod))
    /// <summary>
    /// Apply a rolling max (moving max) over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// <para>
    /// Unlike standard fixed-size rolling windows (which operate on row counts), this operates on values (typically time).
    /// </para>
    /// </summary>
    /// <remarks>
    /// The <paramref name="by"/> column must be sorted in ascending order.
    /// </remarks>
    /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// <param name="by">
    /// The column used to define the window (the "time" axis). 
    /// <para>Typically a <c>Date</c> or <c>DateTime</c> column, but can also be monotonic integers.</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a non-null result.</param>
    /// <param name="closed">
    /// Defines how the window interval is closed. 
    /// Default is <see cref="ClosedWindow.Left"/> <c>[t - window, t)</c>.
    /// </param>
    /// <returns>A new series representing the dynamic rolling max.</returns>
    member this.RollingMaxBy(windowSize: TimeSpan, by: Expr, ?closed: ClosedWindow, ?minPeriod: int) =
        this.ApplyExpr(Expr.Col(this.Name).RollingMaxBy(windowSize, by, ?closed=closed, ?minPeriod=minPeriod))
    /// <summary>
    /// Apply a rolling standard deviation (moving standard deviation) over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// <para>
    /// Unlike standard fixed-size rolling windows (which operate on row counts), this operates on values (typically time).
    /// </para>
    /// </summary>
    /// <remarks>
    /// The <paramref name="by"/> column must be sorted in ascending order.
    /// </remarks>
    /// <param name="windowSize">
    /// The size of the dynamic window.
    /// <para>Supported duration strings: <c>"1d"</c>, <c>"2h"</c>, <c>"10s"</c>, <c>"500ms"</c>, etc.</para>
    /// </param>
    /// <param name="by">
    /// The column used to define the window (the "time" axis). 
    /// <para>Typically a <c>Date</c> or <c>DateTime</c> column, but can also be monotonic integers.</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a non-null result.</param>
    /// <param name="closed">
    /// Defines how the window interval is closed. 
    /// Default is <see cref="ClosedWindow.Left"/> <c>[t - window, t)</c>.
    /// </param>
    /// <returns>A new series representing the dynamic rolling standard deviation.</returns>
    member this.RollingStdBy(windowSize: string, by: Expr, ?closed: ClosedWindow, ?minPeriod: int) =
        this.ApplyExpr(Expr.Col(this.Name).RollingStdBy(windowSize, by, ?closed=closed, ?minPeriod=minPeriod))
    /// <summary>
    /// Apply a rolling standard deviation (moving standard deviation) over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// <para>
    /// Unlike standard fixed-size rolling windows (which operate on row counts), this operates on values (typically time).
    /// </para>
    /// </summary>
    /// <remarks>
    /// The <paramref name="by"/> column must be sorted in ascending order.
    /// </remarks>
    /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// <param name="by">
    /// The column used to define the window (the "time" axis). 
    /// <para>Typically a <c>Date</c> or <c>DateTime</c> column, but can also be monotonic integers.</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a non-null result.</param>
    /// <param name="closed">
    /// Defines how the window interval is closed. 
    /// Default is <see cref="ClosedWindow.Left"/> <c>[t - window, t)</c>.
    /// </param>
    /// <returns>A new series representing the dynamic rolling standard deviation.</returns>
    member this.RollingStdBy(windowSize: TimeSpan, by: Expr, ?closed: ClosedWindow, ?minPeriod: int) =
        this.ApplyExpr(Expr.Col(this.Name).RollingStdBy(windowSize, by, ?closed=closed, ?minPeriod=minPeriod))
    /// <summary>
    /// Apply a rolling variance (moving variance) over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// <para>
    /// Unlike standard fixed-size rolling windows (which operate on row counts), this operates on values (typically time).
    /// </para>
    /// </summary>
    /// <remarks>
    /// The <paramref name="by"/> column must be sorted in ascending order.
    /// </remarks>
    /// <param name="windowSize">
    /// The size of the dynamic window.
    /// <para>Supported duration strings: <c>"1d"</c>, <c>"2h"</c>, <c>"10s"</c>, <c>"500ms"</c>, etc.</para>
    /// </param>
    /// <param name="by">
    /// The column used to define the window (the "time" axis). 
    /// <para>Typically a <c>Date</c> or <c>DateTime</c> column, but can also be monotonic integers.</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a non-null result.</param>
    /// <param name="closed">
    /// Defines how the window interval is closed. 
    /// Default is <see cref="ClosedWindow.Left"/> <c>[t - window, t)</c>.
    /// </param>
    /// <param name="ddof">
    /// “Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. 
    /// <para>By default ddof is 1.</para>
    /// </param>
    /// <returns>A new series representing the dynamic rolling variance.</returns>
    member this.RollingVarBy(windowSize: string, by: Expr, ?closed: ClosedWindow, ?minPeriod: int,?ddof:uint8) =
        this.ApplyExpr(Expr.Col(this.Name).RollingVarBy(windowSize, by, ?closed=closed, ?minPeriod=minPeriod,?ddof=ddof))
    /// <summary>
    /// Apply a rolling variance (moving variance) over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// <para>
    /// Unlike standard fixed-size rolling windows (which operate on row counts), this operates on values (typically time).
    /// </para>
    /// </summary>
    /// <remarks>
    /// The <paramref name="by"/> column must be sorted in ascending order.
    /// </remarks>
    /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// <param name="by">
    /// The column used to define the window (the "time" axis). 
    /// <para>Typically a <c>Date</c> or <c>DateTime</c> column, but can also be monotonic integers.</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a non-null result.</param>
    /// <param name="closed">
    /// Defines how the window interval is closed. 
    /// Default is <see cref="ClosedWindow.Left"/> <c>[t - window, t)</c>.
    /// </param>
    /// <param name="ddof">
    /// “Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. 
    /// <para>By default ddof is 1.</para>
    /// </param>
    /// <returns>A new series representing the dynamic rolling variance.</returns>
    member this.RollingVarBy(windowSize: TimeSpan, by: Expr, ?closed: ClosedWindow, ?minPeriod: int,?ddof:uint8) =
        this.ApplyExpr(Expr.Col(this.Name).RollingVarBy(windowSize, by, ?closed=closed, ?minPeriod=minPeriod,?ddof=ddof))
    /// <summary>
    /// Apply a rolling rank (moving rank) over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// <para>
    /// Unlike standard fixed-size rolling windows (which operate on row counts), this operates on values (typically time).
    /// </para>
    /// </summary>
    /// <remarks>
    /// The <paramref name="by"/> column must be sorted in ascending order.
    /// </remarks>
    /// <param name="windowSize">
    /// The size of the dynamic window.
    /// <para>Supported duration strings: <c>"1d"</c>, <c>"2h"</c>, <c>"10s"</c>, <c>"500ms"</c>, etc.</para>
    /// </param>
    /// <param name="by">
    /// The column used to define the window (the "time" axis). 
    /// <para>Typically a <c>Date</c> or <c>DateTime</c> column, but can also be monotonic integers.</para>
    /// </param>
    /// <param name="method">The method used to assign ranks to tied elements.
    /// </param>
    /// <param name="seed">Seed for the random method (only relevant when method is Random).
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a non-null result.</param>
    /// <param name="closed">
    /// Defines how the window interval is closed. 
    /// Default is <see cref="ClosedWindow.Left"/> <c>[t - window, t)</c>.
    /// </param>
    /// <returns>A new series representing the dynamic rolling rank.</returns>
    member this.RollingRankBy(windowSize: string, by: Expr, ?method:RollingRankMethod,?seed:uint64,?closed: ClosedWindow, ?minPeriod: int) =
        this.ApplyExpr(Expr.Col(this.Name).RollingRankBy(windowSize, by,?method=method,?seed=seed, ?closed=closed, ?minPeriod=minPeriod))
    /// <summary>
    /// Apply a rolling rank (moving rank) over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// <para>
    /// Unlike standard fixed-size rolling windows (which operate on row counts), this operates on values (typically time).
    /// </para>
    /// </summary>
    /// <remarks>
    /// The <paramref name="by"/> column must be sorted in ascending order.
    /// </remarks>
    /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// <param name="by">
    /// The column used to define the window (the "time" axis). 
    /// <para>Typically a <c>Date</c> or <c>DateTime</c> column, but can also be monotonic integers.</para>
    /// </param>
    /// <param name="method">The method used to assign ranks to tied elements.
    /// </param>
    /// <param name="seed">Seed for the random method (only relevant when method is Random).
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a non-null result.</param>
    /// <param name="closed">
    /// Defines how the window interval is closed. 
    /// Default is <see cref="ClosedWindow.Left"/> <c>[t - window, t)</c>.
    /// </param>
    /// <returns>A new series representing the dynamic rolling rank.</returns>
    member this.RollingRankBy(windowSize: TimeSpan, by: Expr, ?closed: ClosedWindow, ?minPeriod: int) =
        this.ApplyExpr(Expr.Col(this.Name).RollingRankBy(windowSize, by, ?closed=closed, ?minPeriod=minPeriod))
    /// <summary>
    /// Apply a rolling quantile (moving quantile) over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// <para>
    /// Unlike standard fixed-size rolling windows (which operate on row counts), this operates on values (typically time).
    /// </para>
    /// </summary>
    /// <remarks>
    /// The <paramref name="by"/> column must be sorted in ascending order.
    /// </remarks>
    /// <param name="quantile">Quantile between 0.0 and 1.0 (e.g., 0.5 for median).
    /// </param>
    /// <param name="method">Interpolation method when the quantile lies between two data points.
    /// </param>
    /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// <param name="by">
    /// The column used to define the window (the "time" axis). 
    /// <para>Typically a <c>Date</c> or <c>DateTime</c> column, but can also be monotonic integers.</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a non-null result.</param>
    /// <param name="closed">
    /// Defines how the window interval is closed. 
    /// Default is <see cref="ClosedWindow.Left"/> <c>[t - window, t)</c>.
    /// </param>
    /// <returns>A new series representing the dynamic rolling quantile.</returns>
    member this.RollingQuantileBy(quantile: float, method: QuantileMethod, windowSize: string, by: Expr, ?closed: ClosedWindow, ?minPeriod: int) =
        this.ApplyExpr(Expr.Col(this.Name).RollingQuantileBy(quantile, method, windowSize, by, ?closed=closed, ?minPeriod=minPeriod))
    /// <summary>
    /// Apply a rolling quantile (moving quantile) over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// <para>
    /// Unlike standard fixed-size rolling windows (which operate on row counts), this operates on values (typically time).
    /// </para>
    /// </summary>
    /// <remarks>
    /// The <paramref name="by"/> column must be sorted in ascending order.
    /// </remarks>
    /// <param name="quantile">Quantile between 0.0 and 1.0 (e.g., 0.5 for median).
    /// </param>
    /// <param name="method">Interpolation method when the quantile lies between two data points.
    /// </param>
    /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// <param name="by">
    /// The column used to define the window (the "time" axis). 
    /// <para>Typically a <c>Date</c> or <c>DateTime</c> column, but can also be monotonic integers.</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a non-null result.</param>
    /// <param name="closed">
    /// Defines how the window interval is closed. 
    /// Default is <see cref="ClosedWindow.Left"/> <c>[t - window, t)</c>.
    /// </param>
    /// <returns>A new series representing the dynamic rolling quantile.</returns>
    member this.RollingQuantileBy(quantile: float, method: QuantileMethod, windowSize: TimeSpan, by: Expr, ?closed: ClosedWindow, ?minPeriod: int) =
        this.ApplyExpr(Expr.Col(this.Name).RollingQuantileBy(quantile, method, windowSize, by, ?closed=closed, ?minPeriod=minPeriod))
    // ==========================================
    // TopK / BottomK
    // ==========================================
    /// <summary>
    /// Get the k largest elements.
    /// Result is sorted descending.
    /// </summary>
    member this.TopK(k: int) = 
        this.ApplyExpr(Expr.Col(this.Name).TopK k)
    /// <summary>
    /// Get the k smallest elements.
    /// Result is sorted ascending.
    /// </summary>
    member this.BottomK(k: int) = 
        this.ApplyExpr(Expr.Col(this.Name).BottomK k)

    /// <summary>
    /// Get top k elements of this Series, sorted by another Series.
    /// </summary>
    member this.TopKBy(k: int, by: Series, ?reverse: bool) =
        let r = defaultArg reverse false
        this.ApplyBinaryExpr(by, fun me other -> me.TopKBy(k, other, r))
    /// <summary>
    /// Get top k elements of this Series, sorted by another Expr.
    /// </summary>
    member this.TopKBy(k: int, by: Expr, ?reverse: bool) =
        let r = defaultArg reverse false
        this.ApplyExpr(Expr.Col(this.Name).TopKBy(k, by, r))

    member this.TopKBy(k: int, by: seq<#IColumnExpr>, ?reverse: seq<bool>) =
        this.ApplyExpr(Expr.Col(this.Name).TopKBy(k, by, ?reverse=reverse))

    /// <summary>
    /// Get bottom k elements of this Series, sorted by another Series.
    /// </summary>
    member this.BottomKBy(k: int, by: Series, ?reverse: bool) =
        let r = defaultArg reverse false
        this.ApplyBinaryExpr(by, fun me other -> me.BottomKBy(k, other, r))
    /// <summary>
    /// Get bottom k elements of this Series, sorted by another Expr.
    /// </summary>
    member this.BottomKBy(k: int, by: Expr, ?reverse: bool) =
        let r = defaultArg reverse false
        this.ApplyExpr(Expr.Col(this.Name).BottomKBy(k, by, r))
    member this.BottomKBy(k: int, by: seq<#IColumnExpr>, ?reverse: seq<bool>) =
        this.ApplyExpr(Expr.Col(this.Name).BottomKBy(k, by, ?reverse=reverse))
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
    /// High-performance creation from any sequence. 
    /// Supports nested lists, structs, and F# Options.
    /// </summary>
    static member ofSeq<'T>(name: string, data: seq<'T>) : Series =
        let arrowArray = ArrowConverter.Build data
        
        let handle = ArrowFfiBridge.ImportSeries(name, arrowArray)
        
        new Series(handle)

    /// <summary>
    /// Convert Series to a typed sequence of Options.
    /// Uses high-performance Arrow reader (Zero-Copy).
    /// Supports: Primitives, String, DateTime, DateOnly, TimeOnly, List, Struct.
    /// </summary>
    member this.AsSeq<'T>() : seq<'T option> =
        seq {
            use cArray = PolarsWrapper.SeriesToArrow this.Handle
            
            let accessor = ArrowReader.GetSeriesAccessor<'T> cArray
            let len = cArray.Length

            for i in 0 .. len - 1 do
                let valObj = accessor.Invoke i
                if isNull valObj then 
                    None 
                else 
                    Some(unbox<'T> valObj)
        }
    /// <summary>
    /// Get values as a list (forces evaluation).
    /// </summary>
    member this.ToList<'T>() = this.AsSeq<'T>() |> Seq.toList
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

    member _.DateTime(index: int) : DateTime option = 
        PolarsWrapper.SeriesGetDatetime(handle, int64 index) |> Option.ofNullable

    member _.Duration(index: int) : TimeSpan option = 
        PolarsWrapper.SeriesGetDuration(handle, int64 index) |> Option.ofNullable
    // --- Aggregations (Returning Series of len 1) ---
    member this.First() = this.ApplyExpr(Expr.Col(this.Name).First())
    member this.Last() = this.ApplyExpr(Expr.Col(this.Name).Last())
    member this.Sum() = new Series(PolarsWrapper.SeriesSum handle)
    member this.Mean() = new Series(PolarsWrapper.SeriesMean handle)
    member this.Min() = new Series(PolarsWrapper.SeriesMin handle)
    member this.Max() = new Series(PolarsWrapper.SeriesMax handle)
    member this.Product() = this.ApplyExpr(Expr.Col(this.Name).Product())
    // ==========================================
    // Statistical Ops
    // ==========================================
    member this.Count() = this.ApplyExpr(Expr.Col(this.Name).Count())
    /// <summary>
    /// Get the standard deviation.
    /// </summary>
    /// <param name="ddof">Delta Degrees of Freedom. Default is 1.</param>
    /// <returns>A new <see cref="Series"/> containing the Std (length 1).</returns>
    member this.Std(?ddof: int) = 
        let d = defaultArg ddof 1
        this.ApplyExpr(Expr.Col(this.Name).Std d)

    /// <summary>
    /// Get the variance.
    /// </summary>
    /// <param name="ddof">Delta Degrees of Freedom. Default is 1.</param>
    /// <returns>A new <see cref="Series"/> containing the Var (length 1).</returns>
    member this.Var(?ddof: int) = 
        let d = defaultArg ddof 1
        this.ApplyExpr(Expr.Col(this.Name).Var d)

    /// <summary>
    /// Get the median.
    /// </summary>
    /// <returns>A new <see cref="Series"/> containing the Median (length 1).</returns>
    member this.Median() = 
        this.ApplyExpr(Expr.Col(this.Name).Median())
    /// <summary>
    /// Get the Skew.
    /// </summary>
    /// <param name="bias">If False, the calculations are corrected for statistical bias.</param>
    /// <returns>A new <see cref="Series"/> containing the Skew (length 1).</returns>
    member this.Skew(?bias:bool) = 
        let b = defaultArg bias true
        this.ApplyExpr(Expr.Col(this.Name).Skew b)
    /// <summary>
    /// Get the Kurtosis.
    /// </summary>
    /// <param name="fisher">If True, Fisher’s definition is used (normal ==> 0.0). If False, Pearson’s definition is used (normal ==> 3.0).</param>
    /// <param name="bias">If False, the calculations are corrected for statistical bias.</param>
    /// <returns>A new <see cref="Series"/> containing the Skew (length 1).</returns>
    member this.Kurtosis(?fisher:bool,?bias:bool) = 
        let b = defaultArg bias true
        let f = defaultArg fisher true
        this.ApplyExpr(Expr.Col(this.Name).Kurtosis(f,b))
    /// <summary>
    /// Get the quantile.
    /// </summary>
    /// <param name="q">Quantile between 0.0 and 1.0.</param>
    /// <param name="interpolation">Interpolation method ("nearest", "higher", "lower", "midpoint", "linear"). Default "linear".</param>
    member this.Quantile(q: float, ?interpolation: QuantileMethod) =
        this.ApplyExpr(Expr.Col(this.Name).Quantile(q, ?interpolation=interpolation))
    /// <summary>
    /// Computes percentage change between values. 
    /// Percentage change (as fraction) between current element and most-recent non-null element at least n period(s) before the current element. 
    /// Computes the change from the previous row by default.
    /// </summary>
    /// <param name="n">Periods to shift for forming percent change.Default:1</param>
    /// <returns>A new <see cref="Series"/> containing the Var (length 1).</returns>
    member this.PctChange(?n: int) = 
        let nd = defaultArg n 1
        this.ApplyExpr(Expr.Col(this.Name).PctChange nd)
    /// <summary>
    /// Assign ranks to data, dealing with ties appropriately.
    /// </summary>
    /// <param name="method">
    /// The method used to assign ranks to tied elements. See <see cref="RankMethod"/> for details.
    /// Default is <see cref="RankMethod.Average"/>.</param>
    /// <param name="descending">Rank in descending order.</param>
    /// <param name="seed">If method="random", use this as seed.</param>
    /// <returns></returns>
    member this.Rank(?method: RankMethod, ?descending: bool, ?seed: uint64) = 
        this.ApplyExpr(Expr.Col(this.Name).Rank(?method=method, ?descending=descending, ?seed=seed))
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
    // ==========================================
    // Cumulative Functions
    // ==========================================
    /// <summary>
    /// Get an array with the cumulative sum computed at every element.
    /// </summary>
    /// <param name="reverse">Reverse the operation.</param>
    /// <returns></returns>
    member this.CumSum(?reverse:bool) = 
        this.ApplyExpr(Expr.Col(this.Name).CumSum(?reverse=reverse))
    /// <summary>
    /// Get an array with the cumulative min computed at every element.
    /// </summary>
    /// <param name="reverse">Reverse the operation.</param>
    /// <returns></returns>
    member this.CumMin(?reverse:bool) = 
        this.ApplyExpr(Expr.Col(this.Name).CumMin(?reverse=reverse))
    /// <summary>
    /// Get an array with the cumulative max computed at every element.
    /// </summary>
    /// <param name="reverse">Reverse the operation.</param>
    /// <returns></returns>
    member this.CumMax(?reverse:bool) = 
        this.ApplyExpr(Expr.Col(this.Name).CumMax(?reverse=reverse))
    /// <summary>
    /// Get an array with the cumulative prod computed at every element.
    /// </summary>
    /// <param name="reverse">Reverse the operation.</param>
    /// <returns></returns>
    member this.CumProd(?reverse:bool) = 
        this.ApplyExpr(Expr.Col(this.Name).CumProd(?reverse=reverse))    
    /// <summary>
    /// Get an array with the cumulative count computed at every element.
    /// </summary>
    /// <param name="reverse">Reverse the operation.</param>
    /// <returns></returns>
    member this.CumCount(?reverse:bool) = 
        this.ApplyExpr(Expr.Col(this.Name).CumCount(?reverse=reverse)) 
    // ==========================================
    // EWM Functions
    // ==========================================
    /// <summary>
    /// Compute exponentially-weighted moving average.
    /// </summary>
    /// <param name="alpha">
    /// Specify smoothing factor alpha directly. 
    /// <para>Constraint: <c>0 &lt; alpha &lt;= 1</c></para>
    /// </param>
    /// <param name="adjust">
    /// If <c>true</c>, divide by decaying adjustment factor in beginning periods to account for imbalance in relative weightings (viewing data as finite history). 
    /// If <c>false</c>, assume infinite history.
    /// </param>
    /// <param name="bias">
    /// If <c>true</c>, use a biased estimator (Standard deviation uses <c>N</c> in denominator). 
    /// If <c>false</c>, use an unbiased estimator (Standard deviation uses <c>N-1</c>).
    /// <para>Note: This is primarily relevant for Variance/StdDev. For Mean, it typically defaults to true.</para>
    /// </param>
    /// <param name="minPeriods">Minimum number of observations in window required to have a value (otherwise result is null).</param>
    /// <param name="ignoreNulls">Ignore missing values when calculating weights.</param>
    /// <returns>A new expression representing the EWM mean.</returns>
    member this.EwmMean(alpha:float,?adjust:bool,?bias:bool,?minPeriods:int,?ignoreNulls:bool) =
        this.ApplyExpr(Expr.Col(this.Name).EwmMean(alpha=alpha,?adjust=adjust,?bias=bias,?minPeriods=minPeriods,?ignoreNulls=ignoreNulls))
    /// <summary>
    /// Compute exponentially-weighted moving standard deviation.
    /// </summary>
    /// <param name="alpha">
    /// Specify smoothing factor alpha directly. 
    /// <para>Constraint: <c>0 &lt; alpha &lt;= 1</c></para>
    /// </param>
    /// <param name="adjust">
    /// If <c>true</c>, divide by decaying adjustment factor in beginning periods to account for imbalance in relative weightings (viewing data as finite history). 
    /// If <c>false</c>, assume infinite history.
    /// </param>
    /// <param name="bias">
    /// If <c>true</c>, use a biased estimator (Standard deviation uses <c>N</c> in denominator). 
    /// If <c>false</c>, use an unbiased estimator (Standard deviation uses <c>N-1</c>).
    /// <para>Note: This is primarily relevant for Variance/StdDev. For Mean, it typically defaults to true.</para>
    /// </param>
    /// <param name="minPeriods">Minimum number of observations in window required to have a value (otherwise result is null).</param>
    /// <param name="ignoreNulls">Ignore missing values when calculating weights.</param>
    /// <returns>A new expression representing the EWM standard deviation.</returns>
    member this.EwmStd(alpha:float,?adjust:bool,?bias:bool,?minPeriods:int,?ignoreNulls:bool) =
        this.ApplyExpr(Expr.Col(this.Name).EwmStd(alpha=alpha,?adjust=adjust,?bias=bias,?minPeriods=minPeriods,?ignoreNulls=ignoreNulls))
    /// <summary>
    /// Compute exponentially-weighted moving variance.
    /// </summary>
    /// <param name="alpha">
    /// Specify smoothing factor alpha directly. 
    /// <para>Constraint: <c>0 &lt; alpha &lt;= 1</c></para>
    /// </param>
    /// <param name="adjust">
    /// If <c>true</c>, divide by decaying adjustment factor in beginning periods to account for imbalance in relative weightings (viewing data as finite history). 
    /// If <c>false</c>, assume infinite history.
    /// </param>
    /// <param name="bias">
    /// If <c>true</c>, use a biased estimator (Standard deviation uses <c>N</c> in denominator). 
    /// If <c>false</c>, use an unbiased estimator (Standard deviation uses <c>N-1</c>).
    /// <para>Note: This is primarily relevant for Variance/StdDev. For Mean, it typically defaults to true.</para>
    /// </param>
    /// <param name="minPeriods">Minimum number of observations in window required to have a value (otherwise result is null).</param>
    /// <param name="ignoreNulls">Ignore missing values when calculating weights.</param>
    /// <returns>A new expression representing the EWM variance.</returns>
    member this.EwmVar(alpha:float,?adjust:bool,?bias:bool,?minPeriods:int,?ignoreNulls:bool) =
        this.ApplyExpr(Expr.Col(this.Name).EwmVar(alpha=alpha,?adjust=adjust,?bias=bias,?minPeriods=minPeriods,?ignoreNulls=ignoreNulls))
    /// <summary>
    /// Compute exponentially-weighted moving average based on a temporal or index column.
    /// </summary>
    /// <param name="by">
    /// The column used to determine the distance between observations.
    /// <para>Supported data types: <c>Date</c>, <c>DateTime</c>, <c>UInt64</c>, <c>UInt32</c>, <c>Int64</c>, or <c>Int32</c>.</para>
    /// </param>
    /// <param name="halfLife">
    /// The unit over which an observation decays to half its value.
    /// <para>Supported string formats:</para>
    /// <list type="bullet">
    ///     <item><term>Time units</term><description><c>ns</c> (nanosecond), <c>us</c> (microsecond), <c>ms</c> (millisecond), <c>s</c> (second), <c>m</c> (minute), <c>h</c> (hour), <c>d</c> (day), <c>w</c> (week).</description></item>
    ///     <item><term>Index units</term><description><c>i</c> (index count). Example: <c>"2i"</c> means decay by half every 2 index steps.</description></item>
    ///     <item><term>Compound</term><description>Example: <c>"3d12h4m25s"</c>.</description></item>
    /// </list>
    /// <para>
    /// <b>Warning:</b> <paramref name="halfLife"/> is treated as a constant duration. 
    /// Calendar durations such as months (<c>mo</c>) or years (<c>y</c>) are <b>NOT</b> supported because they vary in length. 
    /// Please express such durations in hours (e.g. use <c>'730h'</c> instead of <c>'1mo'</c>).
    /// </para>
    /// </param>
    /// <returns>A new expression representing the time/index-based EWM mean.</returns>
    member this.EwmMeanBy(by:Expr,halfLife:string) =
        this.ApplyExpr(Expr.Col(this.Name).EwmMeanBy(by=by,halfLife=halfLife))

    // ==========================================
    // Operators (Arithmetic) 
    // ==========================================

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
    member this.FromArrow(name:string,arrowArray:IArrowArray) : Series = 
        new Series(ArrowFfiBridge.ImportSeries(name,arrowArray))
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
    member _.Combine(time:Expr,timeUnit:TimeUnit) = apply (fun e -> e.Dt.Combine(time,timeUnit))

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
        parent.ApplyExpr expr

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
    /// <summary>
    /// Unnest the struct column into a DataFrame.
    /// Each field of the struct becomes a separate column.
    /// </summary>
    member _.Unnest() =
        let dfHandle = PolarsWrapper.SeriesStructUnnest parent.Handle
        new DataFrame(dfHandle)
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
    /// <param name="columns">Columns to select.</param>
    /// <param name="schema">Overwrite the schema of the dataset.</param>
    /// <param name="hasHeader">Indicate if the CSV file has a header line (default: true).</param>
    /// <param name="separator">Character used as separator (default: ',').</param>
    /// <param name="quoteChar">Character used for quoting (default: '"'). Set to '\0' to disable.</param>
    /// <param name="eolChar">Character used as End-Of-Line (default: '\n').</param>
    /// <param name="ignoreErrors">Ignore parsing errors (default: false).</param>
    /// <param name="tryParseDates">Try to automatically parse dates (default: true).</param>
    /// <param name="lowMemory">Reduce memory usage at expense of performance (default: false).</param>
    /// <param name="skipRows">Number of rows to skip (default: 0).</param>
    /// <param name="nRows">Stop reading after n rows.</param>
    /// <param name="inferSchemaLength">Number of rows to scan for schema inference (default: 100).</param>
    /// <param name="encoding">File encoding (UTF8 or LossyUTF8).</param>
    /// <param name="nullValues">List of strings to consider as null values.</param>
    /// <param name="missingIsNull">Treat missing fields as null (default: true).</param>
    /// <param name="commentPrefix">Lines starting with this prefix will be ignored.</param>
    /// <param name="decimalComma">Use comma as decimal separator (default: false).</param>
    /// <param name="truncateRaggedLines">Truncate lines longer than schema (default: false).</param>
    /// <param name="rowIndexName">If provided, add a column with the row index.</param>
    /// <param name="rowIndexOffset">Offset for the row index (default: 0).</param>
    static member ReadCsv
        (
            path: string,
            ?columns: string list,
            ?schema: PolarsSchema,
            ?hasHeader: bool,
            ?separator: char,
            ?quoteChar: char,          
            ?eolChar: char,            
            ?ignoreErrors: bool,
            ?tryParseDates: bool,
            ?lowMemory: bool,
            ?skipRows: int64,
            ?nRows: int64,
            ?inferSchemaLength: int64,
            ?encoding: CsvEncoding,    
            ?nullValues: string list,  
            ?missingIsNull: bool,      
            ?commentPrefix: string,    
            ?decimalComma: bool,       
            ?truncateRaggedLines: bool,
            ?rowIndexName: string,     
            ?rowIndexOffset: uint64    
        ) : DataFrame =
        
        // 1. Defaults
        let pHeader = defaultArg hasHeader true
        let pSep = defaultArg separator ',' |> byte
        let pQuote = defaultArg quoteChar '"' |> byte
        let pEol = defaultArg eolChar '\n' |> byte
        let pIgnoreErrors = defaultArg ignoreErrors false
        let pTryParseDates = defaultArg tryParseDates true
        let pLowMem = defaultArg lowMemory false
        let pSkipRows = defaultArg skipRows 0L |> unativeint
        let pEncoding = defaultArg encoding CsvEncoding.UTF8
        
        let pMissingIsNull = defaultArg missingIsNull true
        let pDecimalComma = defaultArg decimalComma false
        let pTruncateRagged = defaultArg truncateRaggedLines false
        let pRowIndexOffset = defaultArg rowIndexOffset 0UL |> unativeint

        // 2. Complex Conversions
        
        // Columns: string list -> string[]
        let pCols = 
            columns 
            |> Option.map List.toArray 
            |> Option.toObj

        // NullValues: string list -> string[]
        let pNullValues = 
            nullValues
            |> Option.map List.toArray
            |> Option.toObj

        // Schema: Schema obj -> SchemaHandle
        let hSchema = 
            schema 
            |> Option.map (fun s -> s.Handle) 
            |> Option.toObj

        // Nullable ulongs
        let pNRows = 
            nRows 
            |> Option.map unativeint 
            |> Option.toNullable
        let pInfer = 
            inferSchemaLength 
            |> Option.map unativeint 
            |> Option.toNullable

        // 3. Call Wrapper
        let handle = PolarsWrapper.ReadCsv(
            path,
            pCols,
            pHeader,
            pSep,
            pQuote,
            pEol,
            pIgnoreErrors,
            pTryParseDates,
            pLowMem,
            pSkipRows,
            pNRows,
            pInfer,
            hSchema,
            pEncoding.ToNative(),
            pNullValues,
            pMissingIsNull,
            Option.toObj commentPrefix,
            pDecimalComma,
            pTruncateRagged,
            Option.toObj rowIndexName,
            pRowIndexOffset
        )

        new DataFrame(handle)

    /// <summary>
    /// Read a CSV file asynchronously into a DataFrame.
    /// </summary>
    static member ReadCsvAsync
        (
            path: string,
            ?columns: string list,
            ?schema: PolarsSchema,
            ?hasHeader: bool,
            ?separator: char,
            ?quoteChar: char,
            ?eolChar: char,
            ?ignoreErrors: bool,
            ?tryParseDates: bool,
            ?lowMemory: bool,
            ?skipRows: int64,
            ?nRows: int64,
            ?inferSchemaLength: int64,
            ?encoding: CsvEncoding,
            ?nullValues: string list,
            ?missingIsNull: bool,
            ?commentPrefix: string,
            ?decimalComma: bool,
            ?truncateRaggedLines: bool,
            ?rowIndexName: string,
            ?rowIndexOffset: uint64
        ) =
        task {
             return DataFrame.ReadCsv(
                path,
                ?columns = columns,
                ?schema = schema,
                ?hasHeader = hasHeader,
                ?separator = separator,
                ?quoteChar = quoteChar,
                ?eolChar = eolChar,
                ?ignoreErrors = ignoreErrors,
                ?tryParseDates = tryParseDates,
                ?lowMemory = lowMemory,
                ?skipRows = skipRows,
                ?nRows = nRows,
                ?inferSchemaLength = inferSchemaLength,
                ?encoding = encoding,
                ?nullValues = nullValues,
                ?missingIsNull = missingIsNull,
                ?commentPrefix = commentPrefix,
                ?decimalComma = decimalComma,
                ?truncateRaggedLines = truncateRaggedLines,
                ?rowIndexName = rowIndexName,
                ?rowIndexOffset = rowIndexOffset
            )
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
    static member ReadParquet(path: string, 
                              ?columns: string list, 
                              ?nRows: uint64, 
                              ?parallelStrategy: ParallelStrategy,
                              ?lowMemory: bool,
                              ?rowIndexName: string,
                              ?rowIndexOffset: uint32) =
        
        let cols = defaultArg columns [] |> List.toArray
        let para = defaultArg parallelStrategy ParallelStrategy.Auto
        let lowMem = defaultArg lowMemory false
        let rName = defaultArg rowIndexName null
        let rOff = defaultArg rowIndexOffset 0u
        
        let nRowsNullable = Option.toNullable nRows
        
        let h = PolarsWrapper.ReadParquet(
            path, 
            cols, 
            nRowsNullable, 
            para.ToNative(), 
            lowMem, 
            rName, 
            rOff
        )

        new DataFrame(h)
    static member ReadParquet(buffer: byte[], 
                              ?columns: string list, 
                              ?nRows: uint64, 
                              ?parallelStrategy: ParallelStrategy, 
                              ?lowMemory: bool,
                              ?rowIndexName: string,
                              ?rowIndexOffset: uint32) : DataFrame =
        
        let cols = defaultArg columns [] |> List.toArray
        let para = defaultArg parallelStrategy ParallelStrategy.Auto
        let lowMem = defaultArg lowMemory false
        let rName = defaultArg rowIndexName null
        let rOff = defaultArg rowIndexOffset 0u
        
        let nRowsNullable = Option.toNullable nRows

        let h = PolarsWrapper.ReadParquet(
            buffer, 
            cols, 
            nRowsNullable, 
            para.ToNative(), 
            lowMem, 
            rName, 
            rOff
        )

        new DataFrame(h)
    /// <summary> Asynchronously read a Parquet file. </summary>
    static member ReadParquetAsync (path: string,
                            ?columns: string list, 
                            ?nRows: uint64, 
                            ?parallelStrategy: ParallelStrategy, 
                            ?lowMemory: bool,
                            ?rowIndexName: string,
                            ?rowIndexOffset: uint32): Async<DataFrame> = 
        let cols = defaultArg columns [] |> List.toArray
        let para = defaultArg parallelStrategy ParallelStrategy.Auto
        let lowMem = defaultArg lowMemory false
        let rName = defaultArg rowIndexName null
        let rOff = defaultArg rowIndexOffset 0u
        let nRowsNullable = Option.toNullable nRows
        async {
            let! handle = PolarsWrapper.ReadParquetAsync(path,cols,nRowsNullable,para.ToNative(),lowMem,rName,rOff) |> Async.AwaitTask
        return new DataFrame(handle)
        }

    /// <summary>
    /// Read a JSON file into a DataFrame.
    /// </summary>
    static member ReadJson(path: string, 
                           ?columns: string seq, 
                           ?schema: PolarsSchema, 
                           ?inferSchemaLen: uint64, 
                           ?batchSize: uint64, 
                           ?ignoreErrors: bool,
                           ?jsonFormat: JsonFormat) : DataFrame =
        
        let cols = columns |> Option.map Seq.toArray |> Option.toObj
        
        let schemaHandle = match schema with Some s -> s.Handle | None -> null
        
        let inferLen = Option.toNullable inferSchemaLen
        let batch = Option.toNullable batchSize
        
        let ignoreErr = defaultArg ignoreErrors false
        
        let fmt = defaultArg jsonFormat JsonFormat.Json

        let h = PolarsWrapper.ReadJson(path, cols, schemaHandle, inferLen, batch, ignoreErr, fmt.ToNative())
        new DataFrame(h)

    /// <summary>
    /// Read JSON from in-memory bytes.
    /// </summary>
    static member ReadJson(buffer: byte[], 
                           ?columns: string seq, 
                           ?schema: PolarsSchema, 
                           ?inferSchemaLen: uint64, 
                           ?batchSize: uint64, 
                           ?ignoreErrors: bool,
                           ?jsonFormat: JsonFormat) : DataFrame =
        
        let cols = columns |> Option.map Seq.toArray |> Option.toObj
        let schemaHandle = match schema with Some s -> s.Handle | None -> null
        let inferLen = Option.toNullable inferSchemaLen
        let batch = Option.toNullable batchSize
        let ignoreErr = defaultArg ignoreErrors false
        
        let fmt = defaultArg jsonFormat JsonFormat.Json

        let h = PolarsWrapper.ReadJson(buffer, cols, schemaHandle, inferLen, batch, ignoreErr, fmt.ToNative())
        new DataFrame(h)

    /// <summary>
    /// Read JSON from a Stream.
    /// </summary>
    static member ReadJson(stream: Stream, 
                           ?columns: string seq, 
                           ?schema: PolarsSchema, 
                           ?inferSchemaLen: uint64, 
                           ?batchSize: uint64, 
                           ?ignoreErrors: bool,
                           ?jsonFormat: JsonFormat) : DataFrame =
        
        use ms = new MemoryStream()
        stream.CopyTo ms
        let bytes = ms.ToArray()
        
        DataFrame.ReadJson(
            bytes, 
            ?columns=columns, 
            ?schema=schema, 
            ?inferSchemaLen=inferSchemaLen, 
            ?batchSize=batchSize, 
            ?ignoreErrors=ignoreErrors, 
            ?jsonFormat=jsonFormat
        )
    /// <summary>
    /// Read an Arrow IPC (Feather v2) file into a DataFrame.
    /// </summary>
    static member ReadIpc(path: string,
                          ?columns: string seq,
                          ?nRows: uint64,
                          ?rowIndexName: string,
                          ?rowIndexOffset: uint32,
                          ?rechunk: bool,
                          ?memoryMap: bool,
                          ?includePathColumn: string) : DataFrame =
        
        let cols = columns |> Option.map Seq.toArray |> Option.toObj
        let rows = Option.toNullable nRows
        let idxName = Option.toObj rowIndexName
        let idxOffset = defaultArg rowIndexOffset 0u
        let rechk = defaultArg rechunk false
        let mmap = defaultArg memoryMap false 
        let pathCol = Option.toObj includePathColumn

        let h = PolarsWrapper.ReadIpc(path, cols, rows, idxName, idxOffset, rechk, mmap, pathCol)
        new DataFrame(h)

    /// <summary>
    /// Read IPC from in-memory bytes.
    /// </summary>
    static member ReadIpc(buffer: byte[],
                          ?columns: string seq,
                          ?nRows: uint64,
                          ?rowIndexName: string,
                          ?rowIndexOffset: uint32,
                          ?rechunk: bool,
                          ?includePathColumn: string) : DataFrame =
        
        let cols = columns |> Option.map Seq.toArray |> Option.toObj
        let rows = Option.toNullable nRows
        let idxName = Option.toObj rowIndexName
        let idxOffset = defaultArg rowIndexOffset 0u
        let rechk = defaultArg rechunk false
        let pathCol = Option.toObj includePathColumn

        let h = PolarsWrapper.ReadIpc(buffer, cols, rows, idxName, idxOffset, rechk, pathCol)
        new DataFrame(h)

    /// <summary>
    /// Read IPC from a Stream.
    /// </summary>
    static member ReadIpc(stream: Stream,
                          ?columns: string seq,
                          ?nRows: uint64,
                          ?rowIndexName: string,
                          ?rowIndexOffset: uint32,
                          ?rechunk: bool) : DataFrame =
        
        use ms = new MemoryStream()
        stream.CopyTo ms
        let bytes = ms.ToArray()

        DataFrame.ReadIpc(
            bytes, 
            ?columns=columns, 
            ?nRows=nRows, 
            ?rowIndexName=rowIndexName, 
            ?rowIndexOffset=rowIndexOffset, 
            ?rechunk=rechunk
        )
    /// <summary>
    /// Read an Excel file (.xlsx) into a DataFrame using the high-performance Rust 'calamine' engine.
    /// </summary>
    /// <param name="path">Path to the .xlsx file.</param>
    /// <param name="sheetName">Name of the sheet to read. If specified, takes precedence over sheetIndex.</param>
    /// <param name="sheetIndex">Index of the sheet to read (0-based). Default is 0.</param>
    /// <param name="schema">Optional schema overrides to enforce specific column types.</param>
    /// <param name="hasHeader">Indicates if the first row contains header names. Default is true.</param>
    /// <param name="inferSchemaLen">Number of rows to use for schema inference. Default is 100.</param>
    /// <param name="dropEmptyRows">If true, drop rows where all cells are empty/null. Default is true.</param>
    /// <param name="raiseIfEmpty">If true, raises an error if the sheet is empty. Default is true.</param>
    static member ReadExcel(path: string,
                            ?sheetName: string,
                            ?sheetIndex: uint64,
                            ?schema: PolarsSchema,
                            ?hasHeader: bool,
                            ?inferSchemaLen: uint64,
                            ?dropEmptyRows: bool,
                            ?raiseIfEmpty: bool) : DataFrame =
        
        let sName = Option.toObj sheetName
        let sIdx = defaultArg sheetIndex 0UL
        
        let sHandle = 
            match schema with 
            | Some s -> s.Handle 
            | None -> null

        let header = defaultArg hasHeader true
        let infer = defaultArg inferSchemaLen 100UL
        let dropEmpty = defaultArg dropEmptyRows true
        let raiseEmpty = defaultArg raiseIfEmpty true

        let h = PolarsWrapper.ReadExcel(
            path, 
            sName, 
            sIdx, 
            sHandle, 
            header, 
            infer, 
            dropEmpty, 
            raiseEmpty
        )
        
        new DataFrame(h)
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
            let emptyBatch = new RecordBatch(schema, System.Array.Empty<Apache.Arrow.IArrowArray>(), 0)
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
    /// <summary>
    /// Write DataFrame to a comma-separated values (CSV) file.
    /// </summary>
    /// <param name="path">The output file path.</param>
    /// <param name="hasHeader">Whether to include the header row (default: true).</param>
    /// <param name="useBom">Whether to include the UTF-8 Byte Order Mark (BOM) (default: false).</param>
    /// <param name="separator">Character used as separator (default: ',').</param>
    /// <param name="quoteChar">Character used for quoting (default: '"').</param>
    /// <param name="quoteStyle">The quoting style to use (default: Necessary).</param>
    /// <param name="nullValue">String representation for null values (default: "").</param>
    /// <param name="lineTerminator">Character sequence used to terminate lines (default: "\n").</param>
    /// <param name="floatScientific">Always use scientific notation for floats.</param>
    /// <param name="floatPrecision">Number of decimal places to write for floats.</param>
    /// <param name="decimalComma">Use comma as decimal separator (default: false).</param>
    /// <param name="dateFormat">Format string for Date columns.</param>
    /// <param name="timeFormat">Format string for Time columns.</param>
    /// <param name="datetimeFormat">Format string for Datetime columns.</param>
    /// <param name="batchSize">Batch size for writing rows (default: 0 = Polars default).</param>
    member this.WriteCsv
        (
            path: string,
            ?hasHeader: bool,
            ?useBom: bool,
            ?separator: char,
            ?quoteChar: char,
            ?quoteStyle: QuoteStyle, 
            ?nullValue: string,
            ?lineTerminator: string,
            ?floatScientific: bool,
            ?floatPrecision: int,
            ?decimalComma: bool,
            ?dateFormat: string,
            ?timeFormat: string,
            ?datetimeFormat: string,
            ?batchSize: int
        ) = 
        
        // 1. Defaults
        let pHeader = defaultArg hasHeader true
        let pBom = defaultArg useBom false
        let pSep = defaultArg separator ',' 
        let pQuote = defaultArg quoteChar '"' 
        let pStyle = defaultArg quoteStyle QuoteStyle.Necessary
        let pLineTerm = defaultArg lineTerminator "\n"
        let pDecimalComma = defaultArg decimalComma false
        let pBatchSize = defaultArg batchSize 0

        // 2. Optionals (Strings)
        let pNullVal = Option.toObj nullValue
        let pDateFmt = Option.toObj dateFormat
        let pTimeFmt = Option.toObj timeFormat
        let pDateTimeFmt = Option.toObj datetimeFormat

        // 3. Nullables (Primitive Types)
        // Option<bool> -> Nullable<bool>
        let pFloatSci = Option.toNullable floatScientific
        // Option<int> -> Nullable<int>
        let pFloatPrec = Option.toNullable floatPrecision

        // 4. Call Wrapper
        PolarsWrapper.WriteCsv(
            this.Handle,
            path,
            pHeader,
            pBom,
            pBatchSize,
            pSep,
            pQuote,
            pStyle.ToNative(),
            pNullVal,
            pLineTerm,
            pDateFmt,
            pTimeFmt,
            pDateTimeFmt,
            pFloatSci,
            pFloatPrec,
            pDecimalComma
        )
        
        // Return self for fluent API
        this
    /// <summary>
    /// Write DataFrame to Parquet file.
    /// </summary>
    /// <param name="path">Output file path.</param>
    /// <param name="compression">Compression method. Defaults to Snappy.</param>
    /// <param name="compressionLevel">Compression level for Gzip/Brotli/Zstd. -1 means default. Defaults to -1.</param>
    /// <param name="statistics">Compute and write column statistics. Defaults to false.</param>
    /// <param name="rowGroupSize">Number of rows per row group. 0 means use default.</param>
    /// <param name="dataPageSize">Size of data page in bytes. 0 means use default.</param>
    /// <param name="parallel">Write in parallel. Defaults to true.</param>
    member this.WriteParquet(path: string, ?compression: ParquetCompression, ?compressionLevel: int, ?statistics: bool, ?rowGroupSize: int, ?dataPageSize: int, ?parallelOn: bool) =
        // 1. 处理默认值
        let compression = defaultArg compression ParquetCompression.Snappy
        let compressionLevel = defaultArg compressionLevel -1
        let statistics = defaultArg statistics false
        let rowGroupSize = defaultArg rowGroupSize 0
        let dataPageSize = defaultArg dataPageSize 0
        let p = defaultArg parallelOn true

        PolarsWrapper.WriteParquet(
            this.Handle, 
            path, 
            compression.ToNative(), 
            compressionLevel, 
            statistics, 
            rowGroupSize, 
            dataPageSize, 
            p
        )
        
        this
    /// <summary>
    /// Write DataFrame to IPC (Arrow) file.
    /// </summary>
    /// <param name="path">The file path to write to.</param>
    /// <param name="compression">Compression method (NoCompression, LZ4, ZSTD). Defaults to NoCompression.</param>
    /// <param name="parallel">Whether to use parallel writing. Defaults to true.</param>
    /// <param name="compatLevel">Arrow compatibility level. -1 means newest. Defaults to -1.</param>
    member this.WriteIpc(path: string, ?compression: IpcCompression, ?parallelStrategy: bool, ?compatLevel: int) =
        let compression = defaultArg compression IpcCompression.NoCompression
        let parallelOn = defaultArg parallelStrategy true
        let compatLevel = defaultArg compatLevel -1
        
        PolarsWrapper.WriteIpc(this.Handle, path, compression.ToNative(), parallelOn, compatLevel)
    /// <summary>   
    /// Write DataFrame to a JSON file.
    /// </summary>
    member this.WriteJson(path: string, ?format: JsonFormat) =
        let format = defaultArg format JsonFormat.Json
        PolarsWrapper.WriteJson(this.Handle, path, format.ToNative())

    /// <summary>
    /// Write DataFrame to a NDJSON (JsonLines) file.
    /// </summary>
    member this.WriteNdJson(path: string) =
        this.WriteJson(path, JsonFormat.JsonLines)
    // ---------------------------------------------------------
    // Write Excel (Export)
    // ---------------------------------------------------------
    
    /// <summary>
    /// Write the DataFrame to an Excel file (.xlsx) using the native high-performance engine.
    /// <para>UInt64, Int128, UInt128 are automatically written as Text to preserve precision.</para>
    /// </summary>
    /// <param name="path">Destination file path.</param>
    /// <param name="sheetName">Optional sheet name (default: "Sheet1").</param>
    /// <param name="dateFormat">Optional Excel format string for Date columns (e.g. "yyyy-mm-dd").</param>
    /// <param name="datetimeFormat">Optional Excel format string for Datetime columns (e.g. "yyyy-mm-dd hh:mm:ss").</param>
    member this.WriteExcel(path: string, 
                           ?sheetName: string, 
                           ?dateFormat: string, 
                           ?datetimeFormat: string) =
        
        // F# Option -> C# null handling
        let sName = Option.toObj sheetName
        let dFmt = Option.toObj dateFormat
        let dtFmt = Option.toObj datetimeFormat
        
        PolarsWrapper.WriteExcel(this.Handle, path, sName, dFmt, dtFmt)
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
        
        use buffer = new BlockingCollection<RecordBatch>(capacity)

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
    member this.Schema =
        let h = PolarsWrapper.GetDataFrameSchema this.Handle 
        new PolarsSchema(h)
    member this.Lazy() : LazyFrame =
        let lfHandle = PolarsWrapper.DataFrameToLazy handle
        new LazyFrame(lfHandle)
    /// <summary>
    /// Print schema in a readable format.
    /// </summary>
    member this.PrintSchema() =
        printfn "--- DataFrame Schema ---"
        
        use sc = this.Schema

        sc.ToMap() 
        |> Map.iter (fun name dtype -> 
            printfn "%-15s | %O" name dtype
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
    member this.Join (other: DataFrame,
                      leftOn: Expr list,
                      rightOn: Expr list,
                      how: JoinType,
                      // --- New Optional Parameters ---
                      ?suffix: string,
                      ?validation: JoinValidation,
                      ?coalesce: JoinCoalesce,
                      ?maintainOrder: JoinMaintainOrder,
                      ?nullsEqual: bool,
                      ?sliceOffset: int64,
                      ?sliceLen: uint64) : DataFrame =
        
        let lHandles = leftOn |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        let rHandles = rightOn |> List.map (fun e -> e.CloneHandle()) |> List.toArray
        
        // Handle Defaults
        let suff = defaultArg suffix null // Pass null to let Rust use default ("_right")
        let valid = defaultArg validation JoinValidation.ManyToMany
        let coal = defaultArg coalesce JoinCoalesce.JoinSpecific
        let mo = defaultArg maintainOrder JoinMaintainOrder.NotMaintainOrder
        let ne = defaultArg nullsEqual false
        
        // Slice logic
        let so = Option.toNullable sliceOffset
        let sl = defaultArg sliceLen 0UL

        let h = PolarsWrapper.Join(
            this.Handle, 
            other.Handle, 
            lHandles, 
            rHandles, 
            how.ToNative(),
            suff,
            valid.ToNative(),
            coal.ToNative(),
            mo.ToNative(),
            ne,
            so,
            sl
        )
        new DataFrame(h)
    member internal this.JoinAsOfInternal(other: DataFrame, 
                         leftOn: Expr, 
                         rightOn: Expr, 
                         // --- Optional Parameters ---
                         ?byLeft: Expr list, 
                         ?byRight: Expr list, 
                         ?strategy: AsofStrategy, 
                         ?tolerance: string,      // String
                         ?toleranceInt: int64,    // Int
                         ?toleranceFloat: float,  // Float
                         ?allowEq: bool,
                         ?checkSorted: bool,
                         ?suffix: string,
                         ?validation: JoinValidation,
                         ?coalesce: JoinCoalesce,
                         ?maintainOrder: JoinMaintainOrder,
                         ?nullsEqual: bool,
                         ?sliceOffset: int64,
                         ?sliceLen: uint64) : DataFrame =
        
        // 1. Convert to Lazy
        let lfSelf = this.Lazy()
        let lfOther = other.Lazy()

        // 2. Delegate to LazyFrame.JoinAsOfInternal
        // F# allows passing optional arguments directly via ?arg=val
        let resLf = lfSelf.JoinAsOfInternal(
            lfOther, leftOn, rightOn,
            ?byLeft = byLeft, 
            ?byRight = byRight, 
            ?strategy = strategy, 
            ?tolerance = tolerance, 
            ?toleranceInt = toleranceInt, 
            ?toleranceFloat = toleranceFloat, 
            ?allowEq = allowEq, 
            ?checkSorted = checkSorted, 
            ?suffix = suffix, 
            ?validation = validation, 
            ?coalesce = coalesce, 
            ?maintainOrder = maintainOrder, 
            ?nullsEqual = nullsEqual, 
            ?sliceOffset = sliceOffset, 
            ?sliceLen = sliceLen
        )

        // 3. Collect back to DataFrame
        resLf.Collect()

    // ==========================================
    // Public Overloads (Facade)
    // ==========================================

    // 1. String Tolerance
    /// <summary>
    /// Join with tolerance as string (e.g. "2h", "10s").
    /// </summary>
    member this.JoinAsOf(other: DataFrame, leftOn: Expr, rightOn: Expr, tolerance: string, 
                         ?strategy: AsofStrategy, ?byLeft: Expr list, ?byRight: Expr list) =
        this.JoinAsOfInternal(
            other, leftOn, rightOn, 
            tolerance = tolerance, 
            ?strategy = strategy, ?byLeft = byLeft, ?byRight = byRight
        )

    // 2. TimeSpan Tolerance
    /// <summary>
    /// Join with tolerance as TimeSpan.
    /// </summary>
    member this.JoinAsOf(other: DataFrame, leftOn: Expr, rightOn: Expr, tolerance: System.TimeSpan, 
                         ?strategy: AsofStrategy, ?byLeft: Expr list, ?byRight: Expr list) =
        let tolStr = DurationFormatter.ToPolarsString(tolerance)
        this.JoinAsOfInternal(
            other, leftOn, rightOn, 
            tolerance = tolStr, 
            ?strategy = strategy, ?byLeft = byLeft, ?byRight = byRight
        )

    // 3. Int64 Tolerance
    /// <summary>
    /// Join with tolerance as integer (e.g. timestamp or simple counter).
    /// </summary>
    member this.JoinAsOf(other: DataFrame, leftOn: Expr, rightOn: Expr, tolerance: int64, 
                         ?strategy: AsofStrategy, ?byLeft: Expr list, ?byRight: Expr list) =
        this.JoinAsOfInternal(
            other, leftOn, rightOn, 
            toleranceInt = tolerance, 
            ?strategy = strategy, ?byLeft = byLeft, ?byRight = byRight
        )

    // 4. Float Tolerance
    /// <summary>
    /// Join with tolerance as float.
    /// </summary>
    member this.JoinAsOf(other: DataFrame, leftOn: Expr, rightOn: Expr, tolerance: float, 
                         ?strategy: AsofStrategy, ?byLeft: Expr list, ?byRight: Expr list) =
        this.JoinAsOfInternal(
            other, leftOn, rightOn, 
            toleranceFloat = tolerance, 
            ?strategy = strategy, ?byLeft = byLeft, ?byRight = byRight
        )
    /// <summary>
    /// General Concat method.
    /// checkDuplicates is only used when how = ConcatType.Horizontal.
    /// </summary>
    static member internal Concat (dfs: seq<DataFrame>, how: ConcatType, ?checkDuplicates: bool) : DataFrame =
        let handles = dfs |> Seq.map (fun df -> df.CloneHandle()) |> Seq.toArray
        
        let check = defaultArg checkDuplicates true
        let h = PolarsWrapper.Concat(handles, how.ToNative(), check)
        new DataFrame(h)

    /// <summary>
    /// Horizontal concatenation (Index alignment).
    /// </summary>
    static member ConcatHorizontal (dfs: seq<DataFrame>, ?checkDuplicates: bool) : DataFrame =
        DataFrame.Concat(dfs, ConcatType.Horizontal, ?checkDuplicates = checkDuplicates)

    /// <summary>
    /// Vertical concatenation (Column alignment).
    /// </summary>
    static member ConcatVertical (dfs: seq<DataFrame>) : DataFrame =
        DataFrame.Concat(dfs, ConcatType.Vertical)

    /// <summary>
    /// Diagonal concatenation.
    /// </summary>
    static member ConcatDiagonal (dfs: seq<DataFrame>) : DataFrame =
        DataFrame.Concat(dfs, ConcatType.Diagonal)
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
    member this.Pivot (index: string list, 
                       columns: string list, 
                       values: string list, 
                       aggFn: PivotAgg, 
                       ?sortColumns: bool, // 建议改名 sortColumns 跟 C# 保持一致
                       ?separator: string) : DataFrame =
        
        let iArr = List.toArray index
        let cArr = List.toArray columns
        let vArr = List.toArray values
        let sort = defaultArg sortColumns false
        let sep = defaultArg separator null

        let h = PolarsWrapper.Pivot(
            this.Handle, 
            iArr, 
            cArr, 
            vArr, 
            null,               // aggExpr = null
            aggFn.ToNative(), 
            sort, 
            sep
        )
        new DataFrame(h)

    // 2. Expr Version
    member this.Pivot (index: string list, 
                       columns: string list, 
                       values: string list, 
                       aggExpr: Expr,      
                       ?sortColumns: bool,
                       ?separator: string) : DataFrame =
        
        let iArr = List.toArray index
        let cArr = List.toArray columns
        let vArr = List.toArray values
        let sort = defaultArg sortColumns false
        let sep = defaultArg separator null
        let exprH = aggExpr.CloneHandle()

        // 调用 Wrapper
        // aggFn 传 0uy (Dummy value)
        let h = PolarsWrapper.Pivot(
            this.Handle, 
            iArr, 
            cArr, 
            vArr, 
            exprH,              // aggExpr handle
            PivotAgg.First.ToNative(),                // aggFn (ignored)
            sort, 
            sep
        )
        new DataFrame(h)

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
    /// <summary>
    /// Horizontally stack columns to the DataFrame.
    /// Returns a new DataFrame with the new columns appended.
    /// </summary>
    member this.HStack(columns: Series list) : DataFrame =
        let handles = 
            columns 
            |> List.map (fun s -> s.Handle) 
            |> List.toArray
        
        new DataFrame(PolarsWrapper.HStack(this.Handle, handles))

    /// <summary>
    /// Vertically stack another DataFrame to this one.
    /// Checks that the schema matches.
    /// </summary>
    member this.VStack(other: DataFrame) : DataFrame =
        new DataFrame(PolarsWrapper.VStack(this.Handle, other.Handle))

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

    // 4. DateTime (DateTime)
    member this.DateTime(col: string, row: int) : DateTime option =
        use s = this.Column col
        s.DateTime row

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
    /// <summary>
    /// Get a value from the DataFrame using a generic type argument.
    /// Eliminates the need for unbox, but throws if type mismatches.
    /// </summary>
    member this.Cell<'T>(colName: string ,rowIndex: int) : 'T =
        let s = this.Column colName
        s.GetValue<'T>(int64 rowIndex)
    /// <summary>
    /// Get a value from the DataFrame using a generic type argument.
    /// Eliminates the need for unbox, but throws if type mismatches.
    /// </summary>
    member this.Cell<'T>(rowIndex: int,colName: string ) : 'T =
        let s = this.Column colName
        s.GetValue<'T>(int64 rowIndex)

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
    member _.CollectStreaming() =
        let dfHandle = PolarsWrapper.CollectStreaming handle
        new DataFrame(dfHandle)

    /// <summary>
    /// Get the schema of the LazyFrame without executing it.
    /// Uses Zero-Copy native introspection.
    /// </summary>
    member this.Schema =
        let h = PolarsWrapper.GetLazySchema this.Handle 
        new PolarsSchema(h)
    member this.PrintSchema() =
        printfn "--- LazyFrame Schema ---"
        
        use sc = this.Schema

        sc.ToMap() 
        |> Map.iter (fun name dtype -> 
            printfn "%-15s | %O" name dtype
        )
        
        printfn "------------------------"

    /// <summary> Print the query plan. </summary>
    member this.Explain(?optimized: bool) = 
        let opt = defaultArg optimized true
        PolarsWrapper.Explain(handle, opt)
    /// <summary>
    /// Lazily read from a CSV file.
    /// </summary>
    static member ScanCsv
        (
            path: string,
            ?separator: char,
            ?hasHeader: bool,
            ?quoteChar: char,          // [NEW]
            ?eolChar: char,            // [NEW]
            ?ignoreErrors: bool,
            ?skipRows: int64,
            ?nRows: int64,
            ?cache: bool,
            ?rechunk: bool,
            ?lowMemory: bool,
            ?inferSchemaLength: int64,
            ?schema: PolarsSchema,
            ?tryParseDates: bool,
            ?rowIndexName: string,
            ?rowIndexOffset: uint64,   // [FIX] uint32 -> uint64 to match C# nuint
            ?encoding: CsvEncoding,    // [FIX] Use new CsvEncoding enum
            ?nullValues: string list,  // [NEW]
            ?missingIsNull: bool,      // [NEW]
            ?commentPrefix: string,    // [NEW]
            ?decimalComma: bool        // [NEW]
        ) =
        // 1. Defaults
        let pSep = defaultArg separator ','
        let pHeader = defaultArg hasHeader true
        let pQuote = defaultArg quoteChar '"'
        let pEol = defaultArg eolChar '\n'
        let pIgnoreErrors = defaultArg ignoreErrors false
        
        let pSkipRows = defaultArg skipRows 0L |> uint64
        let pCache = defaultArg cache true
        let pRechunk = defaultArg rechunk false
        let pLowMem = defaultArg lowMemory false
        let pTryParseDates = defaultArg tryParseDates true
        
        let pRowIndexOffset = defaultArg rowIndexOffset 0UL
        let pEncoding = defaultArg encoding CsvEncoding.UTF8
        let pMissingIsNull = defaultArg missingIsNull true
        let pDecimalComma = defaultArg decimalComma false

        // 2. Options -> Nullables / Objects
        let pNRows = nRows |> Option.map uint64 |> Option.toNullable
        let pInfer = inferSchemaLength |> Option.map uint64 |> Option.toNullable
        
        // NullValues: string list -> string[]
        let pNullValues = 
            nullValues
            |> Option.map List.toArray
            |> Option.toObj

        // Schema Handle (Reference Type)
        let hSchema = 
            schema 
            |> Option.map (fun s -> s.Handle) 
            |> Option.toObj
            
        // 3. Call Wrapper
        let handle = PolarsWrapper.ScanCsv(
            path,
            hSchema,
            pHeader,
            pSep,
            pQuote,
            pEol,
            pIgnoreErrors,
            pTryParseDates,
            pLowMem,
            pCache,
            pRechunk,
            pSkipRows,
            pNRows,
            pInfer,
            Option.toObj rowIndexName,
            pRowIndexOffset,
            pEncoding.ToNative(),
            pNullValues,
            pMissingIsNull,
            Option.toObj commentPrefix,
            pDecimalComma
        )

        new LazyFrame(handle)

    /// <summary>
    /// [Memory] Lazily read CSV from a byte array.
    /// </summary>
    static member ScanCsv
        (
            buffer: byte[],
            ?separator: char,
            ?hasHeader: bool,
            ?quoteChar: char,          // [NEW]
            ?eolChar: char,            // [NEW]
            ?ignoreErrors: bool,
            ?skipRows: int64,
            ?nRows: int64,
            ?cache: bool,
            ?rechunk: bool,
            ?lowMemory: bool,
            ?inferSchemaLength: int64,
            ?schema: PolarsSchema,
            ?tryParseDates: bool,
            ?rowIndexName: string,
            ?rowIndexOffset: uint64,   // [FIX] uint32 -> uint64
            ?encoding: CsvEncoding,
            ?nullValues: string list,  // [NEW]
            ?missingIsNull: bool,      // [NEW]
            ?commentPrefix: string,    // [NEW]
            ?decimalComma: bool        // [NEW]
        ) =
        // 1. Defaults
        let pSep = defaultArg separator ','
        let pHeader = defaultArg hasHeader true
        let pQuote = defaultArg quoteChar '"'
        let pEol = defaultArg eolChar '\n'
        let pIgnoreErrors = defaultArg ignoreErrors false
        
        let pSkipRows = defaultArg skipRows 0L |> uint64
        let pCache = defaultArg cache true
        let pRechunk = defaultArg rechunk true
        let pLowMem = defaultArg lowMemory false
        let pTryParseDates = defaultArg tryParseDates true
        
        let pRowIndexOffset = defaultArg rowIndexOffset 0UL |> uint64
        let pEncoding = defaultArg encoding CsvEncoding.UTF8
        let pMissingIsNull = defaultArg missingIsNull true
        let pDecimalComma = defaultArg decimalComma false

        // 2. Options -> Nullables
        let pNRows = nRows |> Option.map uint64 |> Option.toNullable
        let pInfer = inferSchemaLength |> Option.map uint64 |> Option.toNullable
        
        let pNullValues = 
            nullValues
            |> Option.map List.toArray
            |> Option.toObj

        // 3. Schema Handle
        let hSchema = 
            schema 
            |> Option.map (fun s -> s.Handle) 
            |> Option.toObj

        // 4. Call C# Wrapper (Memory Overload)
        let handle = PolarsWrapper.ScanCsv(
            buffer,
            hSchema,
            pHeader,
            pSep,
            pQuote,
            pEol,
            pIgnoreErrors,
            pTryParseDates,
            pLowMem,
            pCache,
            pRechunk,
            pSkipRows,
            pNRows,
            pInfer,
            Option.toObj rowIndexName,
            pRowIndexOffset,
            pEncoding.ToNative(),
            pNullValues,
            pMissingIsNull,
            Option.toObj commentPrefix,
            pDecimalComma
        )

        new LazyFrame(handle)
    /// <summary> Helper: Scan CSV with default settings </summary>
    static member ScanCsv(path: string) = 
        LazyFrame.ScanCsv(path, hasHeader=true)
    /// <summary> Scan a parquet file into a LazyFrame. </summary>
    /// <summary>
    /// Lazily read from a parquet file or a common cloud store (S3, GCS, Azure, etc.).
    /// </summary>
    /// <param name="path">Path to file or cloud location.</param>
    /// <param name="nRows">Stop reading after n rows.</param>
    /// <param name="parallel">Parallel strategy (Auto, Columns, RowGroups, None).</param>
    /// <param name="lowMemory">Reduce memory pressure at the expense of performance.</param>
    /// <param name="useStatistics">Use parquet statistics to prune row groups.</param>
    /// <param name="glob">Expand path using globbing rules.</param>
    /// <param name="allowMissingColumns">If true, do not fail if columns are missing.</param>
    /// <param name="rowIndexName">If provided, add a row index column with this name.</param>
    /// <param name="rowIndexOffset">Start index for the row index column.</param>
    /// <param name="includePathColumn">If provided, add a column with the path of the file.</param>
    /// <param name="schema">Overwrite the schema of the dataset.</param>
    /// <param name="hiveSchema">The schema of the hive partitions.</param>
    /// <param name="tryParseHiveDates">Attempt to parse hive values as Date/Datetime.</param>
    static member ScanParquet
        (
            path: string,
            ?nRows: int64,
            ?parallelStrategy: ParallelStrategy,
            ?lowMemory: bool,
            ?useStatistics: bool,
            ?glob: bool,
            ?allowMissingColumns: bool,
            ?rowIndexName: string,
            ?rowIndexOffset: uint32,
            ?includePathColumn: string,
            ?schema: PolarsSchema,
            ?hiveSchema: PolarsSchema,
            ?tryParseHiveDates: bool
        ) =
        // Defaults
        let pParallel = defaultArg parallelStrategy ParallelStrategy.Auto
        let pLowMem = defaultArg lowMemory false
        let pStats = defaultArg useStatistics true
        let pGlob = defaultArg glob true
        let pAllowMissing = defaultArg allowMissingColumns false
        let pRowIndexOffset = defaultArg rowIndexOffset 0u
        let pTryHive = defaultArg tryParseHiveDates false

        //  F# Types -> C# Interop Types
        
        // nRows: int64 option -> ulong? (Nullable<ulong>)
        let pNRows = 
            nRows 
            |> Option.map uint64 
            |> Option.toNullable

        // Schema: Schema Object -> SchemaHandle (Raw Pointer holder)
        let hSchema = 
            schema 
            |> Option.map (fun s -> s.Handle) 
            |> Option.toObj

        let hHiveSchema = 
            hiveSchema 
            |> Option.map (fun s -> s.Handle) 
            |> Option.toObj

        let handle = PolarsWrapper.ScanParquet(
            path,
            pNRows,
            pParallel.ToNative(),
            pLowMem,
            pStats,
            pGlob,
            pAllowMissing,
            Option.toObj rowIndexName,
            pRowIndexOffset,
            Option.toObj includePathColumn,
            hSchema,
            hHiveSchema,
            pTryHive
        )

        new LazyFrame(handle)
    /// <summary>
    /// [Memory] Lazily read parquet from a byte array (in-memory buffer).
    /// </summary>
    /// <param name="buffer">The byte array containing parquet data.</param>
    /// <param name="nRows">Stop reading after n rows.</param>
    /// <param name="parallel">Parallel strategy (Auto, Columns, RowGroups, None).</param>
    /// <param name="lowMemory">Reduce memory pressure at the expense of performance.</param>
    /// <param name="useStatistics">Use parquet statistics to prune row groups.</param>
    /// <param name="glob">Globbing patterns (usually irrelevant for memory scan, defaults to true).</param>
    /// <param name="allowMissingColumns">If true, do not fail if columns are missing.</param>
    /// <param name="rowIndexName">If provided, add a row index column with this name.</param>
    /// <param name="rowIndexOffset">Start index for the row index column.</param>
    /// <param name="includePathColumn">If provided, add a column with the path (usually irrelevant for memory).</param>
    /// <param name="schema">Overwrite the schema of the dataset.</param>
    /// <param name="hiveSchema">The schema of the hive partitions.</param>
    /// <param name="tryParseHiveDates">Attempt to parse hive values as Date/Datetime.</param>
    static member ScanParquet
        (
            buffer: byte[],
            ?nRows: int64,
            ?parallelStrategy: ParallelStrategy,
            ?lowMemory: bool,
            ?useStatistics: bool,
            ?glob: bool,
            ?allowMissingColumns: bool,
            ?rowIndexName: string,
            ?rowIndexOffset: uint32,
            ?includePathColumn: string,
            ?schema: PolarsSchema,       
            ?hiveSchema: PolarsSchema,   
            ?tryParseHiveDates: bool
        ) =
        let pParallel = defaultArg parallelStrategy ParallelStrategy.Auto
        let pLowMem = defaultArg lowMemory false
        let pStats = defaultArg useStatistics true
        let pGlob = defaultArg glob true
        let pAllowMissing = defaultArg allowMissingColumns false
        let pRowIndexOffset = defaultArg rowIndexOffset 0u
        let pTryHive = defaultArg tryParseHiveDates false

        // 2. Type Conversions
        let pNRows = 
            nRows 
            |> Option.map uint64 
            |> Option.toNullable

        // Extract Handle from PolarsSchema wrapper
        let hSchema = 
            schema 
            |> Option.map (fun s -> s.Handle) 
            |> Option.toObj

        let hHiveSchema = 
            hiveSchema 
            |> Option.map (fun s -> s.Handle) 
            |> Option.toObj

        // 3. Call C# Wrapper (Memory Overload)
        let handle = PolarsWrapper.ScanParquet(
            buffer,
            pNRows,
            pParallel.ToNative(),
            pLowMem,
            pStats,
            pGlob,
            pAllowMissing,
            Option.toObj rowIndexName,
            pRowIndexOffset,
            Option.toObj includePathColumn,
            hSchema,
            hHiveSchema,
            pTryHive
        )

        new LazyFrame(handle)
    /// <summary>
    /// Lazily read a NDJSON file.
    /// </summary>
    static member ScanNdjson(path: string,
                             ?schema: PolarsSchema,
                             ?inferSchemaLen: uint64,
                             ?batchSize: uint64,
                             ?nRows: uint64,
                             ?lowMemory: bool,
                             ?rechunk: bool,
                             ?ignoreErrors: bool,
                             ?rowIndexName: string,
                             ?rowIndexOffset: uint32,
                             ?includePathColumn: string) : LazyFrame =
        
        let schemaHandle = match schema with Some s -> s.Handle | None -> null
        let inferLen = Option.toNullable inferSchemaLen
        let batch = Option.toNullable batchSize
        let rows = Option.toNullable nRows
        
        let lowMem = defaultArg lowMemory false
        let rechk = defaultArg rechunk false
        let ignoreErr = defaultArg ignoreErrors false
        
        let idxName = Option.toObj rowIndexName
        let idxOffset = defaultArg rowIndexOffset 0u
        let pathCol = Option.toObj includePathColumn

        let h = PolarsWrapper.ScanNdjson(
            path, 
            schemaHandle, 
            batch, 
            inferLen, 
            rows, 
            lowMem, 
            rechk, 
            ignoreErr, 
            idxName, 
            idxOffset, 
            pathCol
        )
        new LazyFrame(h)

    /// <summary>
    /// Lazily read NDJSON from in-memory bytes.
    /// </summary>
    static member ScanNdjson(buffer: byte[],
                             ?schema: PolarsSchema,
                             ?inferSchemaLen: uint64,
                             ?batchSize: uint64,
                             ?nRows: uint64,
                             ?lowMemory: bool,
                             ?rechunk: bool,
                             ?ignoreErrors: bool,
                             ?rowIndexName: string,
                             ?rowIndexOffset: uint32) : LazyFrame =
        
        let schemaHandle = match schema with Some s -> s.Handle | None -> null
        let inferLen = Option.toNullable inferSchemaLen
        let batch = Option.toNullable batchSize
        let rows = Option.toNullable nRows
        
        let lowMem = defaultArg lowMemory false
        let rechk = defaultArg rechunk false
        let ignoreErr = defaultArg ignoreErrors false
        
        let idxName = Option.toObj rowIndexName
        let idxOffset = defaultArg rowIndexOffset 0u
        
        let h = PolarsWrapper.ScanNdjson(
            buffer, 
            schemaHandle, 
            batch, 
            inferLen, 
            rows, 
            lowMem, 
            rechk, 
            ignoreErr, 
            idxName, 
            idxOffset, 
            null 
        )
        new LazyFrame(h)

    /// <summary>
    /// Lazily read NDJSON from a Stream.
    /// </summary>
    static member ScanNdjson(stream: Stream,
                             ?schema: PolarsSchema,
                             ?inferSchemaLen: uint64,
                             ?batchSize: uint64,
                             ?nRows: uint64,
                             ?lowMemory: bool,
                             ?rechunk: bool,
                             ?ignoreErrors: bool,
                             ?rowIndexName: string,
                             ?rowIndexOffset: uint32) : LazyFrame =
        
        use ms = new MemoryStream()
        stream.CopyTo ms
        let bytes = ms.ToArray()

        LazyFrame.ScanNdjson(
            bytes,
            ?schema=schema,
            ?inferSchemaLen=inferSchemaLen,
            ?batchSize=batchSize,
            ?nRows=nRows,
            ?lowMemory=lowMemory,
            ?rechunk=rechunk,
            ?ignoreErrors=ignoreErrors,
            ?rowIndexName=rowIndexName,
            ?rowIndexOffset=rowIndexOffset
        )
    /// <summary>
    /// Lazily read an Arrow IPC (Feather v2) file.
    /// </summary>
    static member ScanIpc(path: string,
                          ?schema: PolarsSchema,
                          ?nRows: uint64,
                          ?rechunk: bool,
                          ?cache: bool,
                          ?rowIndexName: string,
                          ?rowIndexOffset: uint32,
                          ?includePathColumn: string,
                          ?hivePartitioning: bool) : LazyFrame =
        
        let schemaHandle = match schema with Some s -> s.Handle | None -> null
        let rows = Option.toNullable nRows
        let rechk = defaultArg rechunk false
        let useCache = defaultArg cache true 
        let idxName = Option.toObj rowIndexName
        let idxOffset = defaultArg rowIndexOffset 0u
        let pathCol = Option.toObj includePathColumn
        let hive = defaultArg hivePartitioning false

        let h = PolarsWrapper.ScanIpc(
            path, 
            schemaHandle, 
            rows, 
            rechk, 
            useCache, 
            idxName, 
            idxOffset, 
            pathCol, 
            hive
        )
        new LazyFrame(h)

    /// <summary>
    /// Lazily read Arrow IPC (Feather v2) from in-memory bytes.
    /// </summary>
    static member ScanIpc(buffer: byte[],
                          ?schema: PolarsSchema,
                          ?nRows: uint64,
                          ?rechunk: bool,
                          ?cache: bool,
                          ?rowIndexName: string,
                          ?rowIndexOffset: uint32,
                          ?hivePartitioning: bool) : LazyFrame =
        
        let schemaHandle = match schema with Some s -> s.Handle | None -> null
        let rows = Option.toNullable nRows
        let rechk = defaultArg rechunk false
        let useCache = defaultArg cache true
        let idxName = Option.toObj rowIndexName
        let idxOffset = defaultArg rowIndexOffset 0u
        let hive = defaultArg hivePartitioning false

        let h = PolarsWrapper.ScanIpc(
            buffer, 
            schemaHandle, 
            rows, 
            rechk, 
            useCache, 
            idxName, 
            idxOffset, 
            hive
        )
        new LazyFrame(h)

    /// <summary>
    /// Lazily read Arrow IPC (Feather v2) from a Stream.
    /// </summary>
    static member ScanIpc(stream: Stream,
                          ?schema: PolarsSchema,
                          ?nRows: uint64,
                          ?rechunk: bool,
                          ?cache: bool,
                          ?rowIndexName: string,
                          ?rowIndexOffset: uint32,
                          ?hivePartitioning: bool) : LazyFrame =
        
        use ms = new MemoryStream()
        stream.CopyTo(ms)
        let bytes = ms.ToArray()

        LazyFrame.ScanIpc(
            bytes,
            ?schema=schema,
            ?nRows=nRows,
            ?rechunk=rechunk,
            ?cache=cache,
            ?rowIndexName=rowIndexName,
            ?rowIndexOffset=rowIndexOffset,
            ?hivePartitioning=hivePartitioning
        )
    
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
                let handle = LazyFrame.ScanIpc(scope.FilePath).Handle
                
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
    /// Scan a database query lazily.
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
            let handle = LazyFrame.ScanIpc(scope.FilePath).Handle

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
        let handle = LazyFrame.ScanIpc(scope.FilePath).Handle
        
        // Inline ScopedLazyFrame
        { new LazyFrame(handle) with
            member this.Dispose() =
                base.Dispose()
                scope.Dispose()
        }

    /// <summary>   
    /// Write LazyFrame execution result to Parquet (Streaming). 
    /// </summary>
    /// <param name="path">Output file path.</param>
    /// <param name="compression">Compression method. Defaults to Snappy.</param>
    /// <param name="compressionLevel">Compression level for Gzip/Brotli/Zstd. -1 means default. Defaults to -1.</param>
    /// <param name="statistics">Compute and write column statistics. Defaults to false.</param>
    /// <param name="rowGroupSize">Number of rows per row group. 0 means use default.</param>
    /// <param name="dataPageSize">Size of data page in bytes. 0 means use default.</param>
    /// <param name="maintainOrder">Whether to maintain the order of the data. Defaults to true.</param>
    /// <param name="syncOnClose">File synchronization behavior on close. Defaults to None.</param>
    /// <param name="mkdir">Recursively create the directory if it does not exist. Defaults to false.</param>
    member this.SinkParquet(
        path: string,
        ?compression: ParquetCompression,
        ?compressionLevel: int,
        ?statistics: bool,
        ?rowGroupSize: int,
        ?dataPageSize: int,
        ?maintainOrder: bool,
        ?syncOnClose: SyncOnClose,
        ?mkdir: bool
    ) : unit =
        let compression = defaultArg compression ParquetCompression.Snappy
        let compressionLevel = defaultArg compressionLevel -1
        let statistics = defaultArg statistics false
        let rowGroupSize = defaultArg rowGroupSize 0
        let dataPageSize = defaultArg dataPageSize 0
        let maintainOrder = defaultArg maintainOrder true
        let syncOnClose = defaultArg syncOnClose SyncOnClose.NoSync
        let mkdir = defaultArg mkdir false

        PolarsWrapper.SinkParquet(
            this.CloneHandle(), 
            path,
            compression.ToNative(),
            compressionLevel,
            statistics,
            rowGroupSize,
            dataPageSize,
            maintainOrder,
            syncOnClose.ToNative(),
            mkdir
        )
    /// <summary>
    /// Sink the LazyFrame to a JSON file.
    /// </summary>
    member this.SinkJson(
        path: string,
        ?format: JsonFormat,
        ?maintainOrder: bool,
        ?syncOnClose: SyncOnClose,
        ?mkdir: bool
    ) =
        let format = defaultArg format JsonFormat.Json
        let maintainOrder = defaultArg maintainOrder true
        let syncOnClose = defaultArg syncOnClose SyncOnClose.NoSync
        let mkdir = defaultArg mkdir false

        PolarsWrapper.SinkJson(
            this.CloneHandle(), 
            path,
            format.ToNative(),
            maintainOrder,
            syncOnClose.ToNative(),
            mkdir
        )

    /// <summary>
    /// Sink the LazyFrame to a NDJSON (JsonLines) file.
    /// </summary>
    member this.SinkNdJson(
        path: string,
        ?maintainOrder: bool,
        ?syncOnClose: SyncOnClose,
        ?mkdir: bool
    ) =
        this.SinkJson(
            path, 
            format = JsonFormat.JsonLines, 
            ?maintainOrder = maintainOrder, 
            ?syncOnClose = syncOnClose, 
            ?mkdir = mkdir
        )
    /// <summary>
    /// Sink the LazyFrame to an IPC (Arrow) file.
    /// </summary>
    /// <param name="path">Output file path.</param>
    /// <param name="compression">Compression method (NoCompression, LZ4, ZSTD). Defaults to NoCompression.</param>
    /// <param name="maintainOrder">Whether to maintain the order of the data. Defaults to true.</param>
    /// <param name="syncOnClose">File synchronization behavior on close. Defaults to None.</param>
    /// <param name="mkdir">Recursively create the directory if it does not exist. Defaults to false.</param>
    /// <param name="compatLevel">Arrow compatibility level. -1 means newest. Defaults to -1.</param>
    member this.SinkIpc(
        path: string, 
        ?compression: IpcCompression, 
        ?maintainOrder: bool, 
        ?syncOnClose: SyncOnClose, 
        ?mkdir: bool, 
        ?compatLevel: int
    ) =
        let compression = defaultArg compression IpcCompression.NoCompression
        let maintainOrder = defaultArg maintainOrder true
        let syncOnClose = defaultArg syncOnClose SyncOnClose.NoSync
        let mkdir = defaultArg mkdir false
        let compatLevel = defaultArg compatLevel -1

        PolarsWrapper.SinkIpc(
            this.CloneHandle(), 
            path,
            compression.ToNative(),
            compatLevel,
            maintainOrder,
            syncOnClose.ToNative(),
            mkdir
        )
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
    member this.Join(other: LazyFrame, 
                     leftOn: Expr seq, 
                     rightOn: Expr seq, 
                     how: JoinType,
                     // --- New Optional Parameters ---
                     ?suffix: string,
                     ?validation: JoinValidation,
                     ?coalesce: JoinCoalesce,
                     ?maintainOrder: JoinMaintainOrder,
                     ?nullsEqual: bool,
                     ?sliceOffset: int64,
                     ?sliceLen: uint64) : LazyFrame =

        let lOnArr = leftOn |> Seq.map (fun e -> e.CloneHandle()) |> Seq.toArray
        let rOnArr = rightOn |> Seq.map (fun e -> e.CloneHandle()) |> Seq.toArray
        
        let lHandle = this.CloneHandle()
        let rHandle = other.CloneHandle()

        // Handle Defaults
        let suff = defaultArg suffix null
        let valid = defaultArg validation JoinValidation.ManyToMany
        let coal = defaultArg coalesce JoinCoalesce.JoinSpecific
        let mo = defaultArg maintainOrder JoinMaintainOrder.NotMaintainOrder
        let ne = defaultArg nullsEqual false
        
        let so = Option.toNullable sliceOffset
        let sl = defaultArg sliceLen 0UL

        let newHandle = PolarsWrapper.Join(
            lHandle, 
            rHandle, 
            lOnArr, 
            rOnArr, 
            how.ToNative(),
            suff,
            valid.ToNative(),
            coal.ToNative(),
            mo.ToNative(),
            ne,
            so,
            sl
        )
        
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
    member internal this.JoinAsOfInternal(other: LazyFrame, 
                         leftOn: Expr, 
                         rightOn: Expr, 
                         // --- Optional Parameters ---
                         ?byLeft: Expr list, 
                         ?byRight: Expr list, 
                         ?strategy: AsofStrategy, 
                         ?tolerance: string,      // String (e.g. "2h")
                         ?toleranceInt: int64,    // Int (e.g. timestamp)
                         ?toleranceFloat: float,  // Float
                         ?allowEq: bool,
                         ?checkSorted: bool,
                         ?suffix: string,
                         ?validation: JoinValidation,
                         ?coalesce: JoinCoalesce,
                         ?maintainOrder: JoinMaintainOrder,
                         ?nullsEqual: bool,
                         ?sliceOffset: int64,
                         ?sliceLen: uint64) : LazyFrame =
        
        // 1. Clone Handles (Mandatory)
        let lClone = this.CloneHandle()
        let rClone = other.CloneHandle()
        let lOn = leftOn.CloneHandle()
        let rOn = rightOn.CloneHandle()
        
        // 2. Handle 'By' keys (Optional List -> Handle Array)
        let toHandleArr (exprs: Expr list option) =
            match exprs with
            | Some es -> es |> List.map (fun e -> e.CloneHandle()) |> List.toArray
            | None -> [||]

        let lByArr = toHandleArr byLeft
        let rByArr = toHandleArr byRight

        // 3. Handle Enums & Defaults
        let strat = defaultArg strategy AsofStrategy.Backward
        let valid = defaultArg validation JoinValidation.ManyToMany
        let coal = defaultArg coalesce JoinCoalesce.JoinSpecific
        let mo = defaultArg maintainOrder JoinMaintainOrder.NotMaintainOrder
        
        // 4. Handle Bools & Strings
        let ae = defaultArg allowEq true
        let cs = defaultArg checkSorted true
        let ne = defaultArg nullsEqual false
        let suff = defaultArg suffix null // Rust default is "_right"
        
        // 5. Handle Nullables (Tolerances & Slice)
        // Option.toObj converts string option -> string (null if None)
        let tolStr = Option.toObj tolerance 
        // Option.toNullable converts int option -> Nullable<int>
        let tolInt = Option.toNullable toleranceInt
        let tolFloat = Option.toNullable toleranceFloat
        let sOff = Option.toNullable sliceOffset
        let sLen = defaultArg sliceLen 0UL

        // 6. Call Wrapper
        let h = PolarsWrapper.JoinAsOf(
            lClone, rClone, 
            [| lOn |], [| rOn |], // Wrapper expects arrays
            lByArr, rByArr,
            strat.ToNative(),     // Enum -> PlAsofStrategy
            tolStr,
            tolInt,
            tolFloat,
            ae,
            cs,
            suff,
            valid.ToNative(),
            coal.ToNative(),
            mo.ToNative(),
            ne,
            sOff,
            sLen
        )
        
        new LazyFrame(h)

    /// <summary>
    /// Join with tolerance as string (e.g. "2h", "10s").
    /// </summary>
    member this.JoinAsOf(other: LazyFrame, leftOn: Expr, rightOn: Expr, tolerance: string, 
                         ?strategy: AsofStrategy, ?byLeft: Expr list, ?byRight: Expr list) =
        this.JoinAsOfInternal(
            other, leftOn, rightOn, 
            tolerance = tolerance, // String
            ?strategy = strategy, ?byLeft = byLeft, ?byRight = byRight
        )

    /// <summary>
    /// Join with tolerance as TimeSpan.
    /// </summary>
    member this.JoinAsOf(other: LazyFrame, leftOn: Expr, rightOn: Expr, tolerance: System.TimeSpan, 
                         ?strategy: AsofStrategy, ?byLeft: Expr list, ?byRight: Expr list) =
        let tolStr = DurationFormatter.ToPolarsString(tolerance)
        this.JoinAsOfInternal(
            other, leftOn, rightOn, 
            tolerance = tolStr, // Converted String
            ?strategy = strategy, ?byLeft = byLeft, ?byRight = byRight
        )

    /// <summary>
    /// Join with tolerance as integer (e.g. timestamp or simple counter).
    /// </summary>
    member this.JoinAsOf(other: LazyFrame, leftOn: Expr, rightOn: Expr, tolerance: int64, 
                         ?strategy: AsofStrategy, ?byLeft: Expr list, ?byRight: Expr list) =
        this.JoinAsOfInternal(
            other, leftOn, rightOn, 
            toleranceInt = tolerance, // Int64
            ?strategy = strategy, ?byLeft = byLeft, ?byRight = byRight
        )

    /// <summary>
    /// Join with tolerance as float.
    /// </summary>
    member this.JoinAsOf(other: LazyFrame, leftOn: Expr, rightOn: Expr, tolerance: float, 
                         ?strategy: AsofStrategy, ?byLeft: Expr list, ?byRight: Expr list) =
        this.JoinAsOfInternal(
            other, leftOn, rightOn, 
            toleranceFloat = tolerance, // Float
            ?strategy = strategy, ?byLeft = byLeft, ?byRight = byRight
        )
    /// <summary>
    /// Slice the LazyFrame along the rows.
    /// </summary>
    member this.Slice(offset: int64, length: uint32) = 
        new LazyFrame(PolarsWrapper.LazySlice(this.Handle,offset, length))
    member this.Slice(offset: int64, length: int32) = 
        if length < 0 then raise(ArgumentOutOfRangeException(sprintf "Length must be non-negative."))
        else this.Slice(offset,length)

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
/// Polars Schema definition (Name -> DataType).
/// </summary>
and PolarsSchema (handle: SchemaHandle) =
    
    // --- Property ---
  member val Handle = handle

    // --- Constructors ---
    static member private CreateHandleFromFields(fields: seq<string * DataType>) =
            let names = fields |> Seq.map fst |> Seq.toArray
            let typeHandles = fields |> Seq.map (fun (_, t) -> t.CreateHandle()) |> Seq.toArray
            
            try
                PolarsWrapper.NewSchema(names, typeHandles)
            finally
                for th in typeHandles do th.Dispose()
    /// <summary> Create an empty schema </summary>
    new () = new PolarsSchema(PolarsWrapper.SchemaCreate())

    /// <summary> Create schema from field definitions </summary>
    new (fields: seq<string * DataType>) =
        new PolarsSchema(PolarsSchema.CreateHandleFromFields(fields))

    static member ofMap (m: Map<string, DataType>) = new PolarsSchema(m |> Map.toSeq)
    static member ofList (fields: (string * DataType) list) = new PolarsSchema(fields)

    // --- Inspection API (Alignment with C#) ---

    member this.Len() = PolarsWrapper.GetSchemaLen(this.Handle)

    /// <summary> Get column name and type at specific index </summary>
    member private this.GetFieldAt(index: uint64) =
        let mutable name = Unchecked.defaultof<string>
        let mutable typeHandle = Unchecked.defaultof<DataTypeHandle>
        
        PolarsWrapper.GetSchemaFieldAt(this.Handle, index, &name, &typeHandle)
        
        try
            let dt = DataType.FromHandle typeHandle
            name, dt
        finally
            if not typeHandle.IsInvalid then 
                typeHandle.Dispose()

    /// <summary> Get all column names </summary>
    member this.Names =
        let len = this.Len()
        [ for i in 0UL .. (len - 1UL) -> 
            let mutable name = Unchecked.defaultof<string>
            let mutable _th = Unchecked.defaultof<DataTypeHandle>
            PolarsWrapper.GetSchemaFieldAt(this.Handle, i, &name, &_th)
            if not _th.IsInvalid then _th.Dispose() 
            name 
        ]

    /// <summary> Convert to F# Map </summary>
    member this.ToMap() =
        let len = this.Len()
        [ for i in 0UL .. (len - 1UL) do
            yield this.GetFieldAt(i) 
        ] |> Map.ofList

    /// <summary> Convert to Dictionary </summary>
    member this.ToDictionary() =
        let len = this.Len()
        let dict = Dictionary<string, DataType>(int len)
        for i in 0UL .. (len - 1UL) do
            let name, dtype = this.GetFieldAt(i)
            dict.[name] <- dtype
        dict

    /// <summary> Indexer: schema["col_name"] </summary>
    member this.Item 
        with get(name: string) =
            let len = this.Len()
            let rec find i =
                if i >= len then raise (KeyNotFoundException $"Column '{name}' not found in Schema.")
                else
                    let colName, dtype = this.GetFieldAt(i)
                    if colName = name then dtype
                    else 
                        find (i + 1UL)
            find 0UL

    // --- Display ---
    
    override this.ToString() =
        if this.Handle.IsInvalid then "Schema: {}"
        else
            let sb = StringBuilder "Schema: {"
            let len = this.Len()
            for i in 0UL .. (len - 1UL) do
                let name, dtype = this.GetFieldAt(i)
                sb.Append $"{name}: {dtype}" |> ignore
                if i < len - 1UL then sb.Append(", ") |> ignore
            sb.Append "}" |> ignore
            sb.ToString()

    // --- Interface ---
    
    interface IDisposable with
        member this.Dispose() = 
            if not (isNull (box this.Handle)) && not this.Handle.IsInvalid then
                this.Handle.Dispose()

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

