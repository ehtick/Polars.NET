namespace Polars.FSharp

open System
open Apache.Arrow
open Polars.NET.Core
open Polars.NET.Core.Arrow

// =========================================================================================
// MODULE: Series Extensions (Data Conversion & Computation)
// =========================================================================================
[<AutoOpen>]
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module SeriesExtensions =
    // -----------------------------------------------------------
    // 1. Data Conversion (Series <-> Seq)
    // -----------------------------------------------------------
    type Series with
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

    // -----------------------------------------------------------
    // 3. UDF Support (Direct Map on Series)
    // -----------------------------------------------------------
    type Series with
        /// <summary>
        /// Apply a C# UDF (Arrow->Arrow) directly to this Series.
        /// Returns a new Series.
        /// </summary>
        member this.Map(func: Func<IArrowArray, IArrowArray>) : Series =
            let inputArrow = this.ToArrow()
            
            let outputArrow = func.Invoke inputArrow

            let field = new Field(this.Name, outputArrow.Data.DataType, true)
            let schema = new Schema([| field |], null)
            use batch = new RecordBatch(schema, [| outputArrow |], outputArrow.Length)
            
            use df = DataFrame.FromArrow batch
            
            let res = df.Column 0
            
            res.Rename this.Name

// =========================================================================================
// MODULE: DataFrame Serialization (Record <-> DataFrame)
// =========================================================================================
[<AutoOpen>]
module Serialization =

    // ==========================================
    // Extensions (Exposed Methods)
    // ==========================================
    
    type DataFrame with
        member this.Describe() : DataFrame =
            let numericCols = 
                this.Schema 
                |> Map.filter (fun _ dtype -> dtype.IsNumeric)
                |> Map.keys
                |> Seq.toList

            if numericCols.IsEmpty then
                failwith "No numeric columns to describe."

            let metrics = [
                "count",      fun (c: string) -> pl.col(c).Count().Cast Float64
                "null_count", fun c -> pl.col(c).IsNull().Sum().Cast Float64
                "mean",       fun c -> pl.col(c).Mean()
                "std",        fun c -> pl.col(c).Std()
                "min",        fun c -> pl.col(c).Min().Cast Float64
                "25%",        fun c -> pl.col(c).Quantile 0.25
                "50%",        fun c -> pl.col(c).Median().Cast Float64 
                "75%",        fun c -> pl.col(c).Quantile 0.75
                "max",        fun c -> pl.col(c).Max().Cast Float64
            ]

            let rowFrames = 
                metrics 
                |> List.map (fun (statName, op) ->
                    let exprs = 
                        [ pl.lit(statName).Alias "statistic" ] @
                        (numericCols |> List.map (fun c -> op c))
                    
                    this |> pl.select exprs
                )

            pl.concat rowFrames Vertical

        /// <summary>
        /// Get a value from the DataFrame using a generic type argument.
        /// Eliminates the need for unbox, but throws if type mismatches.
        /// </summary>
        member this.Cell<'T>(colName: string ,rowIndex: int) : 'T =
            let s = this.Column colName
            s.GetValue<'T>(int64 rowIndex)

        member this.Cell<'T>(rowIndex: int,colName: string ) : 'T =
            let s = this.Column colName
            s.GetValue<'T>(int64 rowIndex)