namespace Polars.FSharp
open System.Runtime.CompilerServices

[<AutoOpen>]
module Describe =
    
    type DataFrame with
        /// <summary>
        /// Generate a summary statistics DataFrame (count, mean, std, min, 25%, 50%, 75%, max).
        /// Similar to pandas/polars describe().
        /// </summary>
        member this.Describe() : DataFrame =
            use schema = this.Schema
            
            let numericCols = 
                schema.ToMap()
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

            pl.concat rowFrames

[<Extension>]
type LazyFrameDeltaExtensions =

    [<Extension>]
    static member MergeDeltaOrdered(
        this: LazyFrame,
        path: string,
        mergeKeys: seq<string>,
        ?canEvolve: bool,
        ?cloudOptions: CloudOptions
    ) : DeltaMergeBuilder =
        let keysArr = mergeKeys |> Seq.toArray
        let evolve = defaultArg canEvolve false
        new DeltaMergeBuilder(this, path, keysArr, evolve, cloudOptions)

[<Extension>]
type DataFrameDeltaExtensions =

    [<Extension>]
    static member MergeDeltaOrdered(
        this: DataFrame,
        path: string,
        mergeKeys: seq<string>,
        ?canEvolve: bool,
        ?cloudOptions: CloudOptions
    ) : DeltaMergeBuilder =
        let keysArr = mergeKeys |> Seq.toArray
        let evolve = defaultArg canEvolve false
        new DeltaMergeBuilder(this.Lazy(), path, keysArr, evolve, cloudOptions)