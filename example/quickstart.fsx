// ==========================================
// Polars.NET F# Quick Start Script
// ==========================================

#r "../Polars.NET.Core/bin/Debug/net10.0/Polars.NET.Core.dll"
#r "../Polars.FSharp/bin/Debug/net10.0/Polars.FSharp.dll"
#r "nuget: Apache.Arrow, 22.1.0" 

open System
open Polars.FSharp

let printHeader (title: string) (data: obj) =
    Console.ForegroundColor <- ConsoleColor.Cyan
    printfn "\n>>> %s" title
    Console.ResetColor()
    printfn "%O" data

// ==========================================
// Create DataFrame by F# Records
// ==========================================
printHeader "Creating DataFrame from Records (Idiomatic F#)"

type WeatherData = { 
    Date: string
    City: string
    Temperature: float
    Rain: bool 
}

let data = [
    { Date="2023-01-01"; City="London";     Temperature=10.5; Rain=true }
    { Date="2023-01-01"; City="Manchester"; Temperature=9.0;  Rain=true }
    { Date="2023-01-02"; City="London";     Temperature=12.1; Rain=false }
    { Date="2023-01-02"; City="Manchester"; Temperature=8.5;  Rain=false }
    { Date="2023-01-03"; City="London";     Temperature=13.5; Rain=false }
]

let df = 
    DataFrame.ofRecords data
    |> pl.withColumn (pl.col("Date").Str.ToDate "%Y-%m-%d")

df.Show()

// ==========================================
// 2. Filter
// ==========================================
printHeader "Filtering: London Only"

let londonDf = 
    df 
    |> pl.filter (pl.col "City" .== pl.lit "London")

londonDf.Show()

// ==========================================
// 3. GroupBy & Aggregation
// ==========================================
printHeader "Aggregation: Stats per City"

let aggDf = 
    df
    |> pl.groupBy [pl.col "City"] [
        pl.col("Temperature").Mean().Alias "Avg_Temp"
        pl.col("Temperature").Max().Alias "Max_Temp"
        pl.col("Rain").Sum().Alias "Rainy_Days" // bool sum -> count of true
        pl.count().Alias "Total_Records"
    ]

aggDf.Show()

// ==========================================
// 4. Window Functions
// ==========================================
printHeader "Window: Diff from Daily Average"

let windowDf = 
    df
    |> pl.select [
        pl.col "Date"
        pl.col "City"
        pl.col "Temperature"
        
        // Over(Date): Calculate mean value by group
        (pl.col("Temperature").Mean().Over(pl.col "Date"))
            .Alias "Daily_Avg"
            
        (pl.col "Temperature" - pl.col("Temperature").Mean().Over(pl.col "Date"))
            .Alias "Diff"
    ]
    |> pl.sort (pl.col "Date", false)

windowDf.Show()

// ==========================================
// 5. Lazy API
// ==========================================
printHeader "Lazy Execution & Query Plan"

// .Lazy() won't execuate eagerly, only execution plan will be built
let lf = 
    df.Lazy()
        .Filter(pl.col "Temperature" .> pl.lit 10.0)
        .GroupBy(
            [pl.col "City"],                                         
            [pl.mean(pl.col "Temperature").Alias "Lazy_Avg_Temp"]   
        )

// Print Exection Optimized Plan 
printfn "%s" (lf.Explain(optimized=true))

printHeader "Lazy Execution Result"
// Execuate after Collect
let result = lf.Collect()
result.Show()