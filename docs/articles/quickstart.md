# Getting Started
This guide will walk you through the core functionalities of Polars.NET: creating DataFrames, inspecting data, applying transformations, and leveraging the Lazy engine for maximum performance.

# Prerequisites
First, install the NuGet packages.
```bash
dotnet add package Polars.NET

# For F# users (includes idiomatic extensions)
dotnet add package Polars.FSharp
```
## 1. Creating a DataFrame
Polars.NET supports creating DataFrames from CSV, Parquet, JSON, Database, or in-memory objects.
C# (Fluent API)
```cs
using Polars;
using Polars.CSharp;
using static Polars.CSharp.Polars; // Allows using Col(), Lit() directly

// Create from in-memory columns
var df = DataFrame.FromColumns(new 
{
    Date = new[] { "2023-01-01", "2023-01-01", "2023-01-02" },
    City = new[] { "London", "Manchester", "London" },
    Temperature = new[] { 10.5, 9.0, 12.1 },
    Rain = new[] { true, true, false }
});

// Parse date string to Date32 type
df = df.WithColumn(Col("Date").Str.ToDate("%Y-%m-%d"));

// Preview
df.Show();
```
F# (Pipeline API)
```fsharp
open Polars.FSharp

type WeatherData = { Date: string; City: string; Temperature: float; Rain: bool }

let data = [
    { Date="2023-01-01"; City="London";     Temperature=10.5; Rain=true }
    { Date="2023-01-01"; City="Manchester"; Temperature=9.0;  Rain=true }
    { Date="2023-01-02"; City="London";     Temperature=12.1; Rain=false }
]

let df = 
    DataFrame.ofRecords data
    |> pl.withColumn (pl.col("Date").Str.ToDate "%Y-%m-%d")

// In F#, the DataFrame prints nicely in interactive sessions
df
```
## 2. Selection & Filtering
Polars expressions allow you to define transformations efficiently. Notice how C# uses method chaining, while F# uses the pipe operator (|>).

C#
```cs
var londonData = df
    .Filter(Col("City") == "London")
    .Select(
        Col("Date"),
        // Arithmetic on columns
        (Col("Temperature") * 1.8 + 32).Alias("Temp_F") 
    );
```
F#
```fs
let londonData =
    df 
    |> pl.filter (pl.col "City" .== pl.lit "London")
    |> pl.select [
        pl.col "Date"
        // Arithmetic using overloaded operators
        (pl.col "Temperature" * pl.lit 1.8 + pl.lit 32.0).Alias "Temp_F"
    ]
```
## 3. Aggregation (GroupBy)
Compute summary statistics by groups. Polars executes aggregations in parallel.

C#
```cs
var aggDf = df
    .GroupBy("City")
    .Agg(
        Col("Temperature").Mean().Alias("Avg_Temp"),
        Col("Temperature").Max().Alias("Max_Temp"),
        Col("Rain").Sum().Alias("Rainy_Days"), // Sum of bool = count true
        Col("City").Count().Alias("Total_Records")
    );
```
F#
```fs
let aggDf = 
    df
    |> pl.groupBy [pl.col "City"] [
        pl.col("Temperature").Mean().Alias "Avg_Temp"
        pl.col("Temperature").Max().Alias "Max_Temp"
        pl.col("Rain").Sum().Alias "Rainy_Days"
        pl.count().Alias "Total_Records"
    ]
```
## 4. Window Functions
Window functions allow you to compute values relative to a group without collapsing the rows (unlike GroupBy). This is extremely powerful for time-series analysis or calculating differences from averages.

C#
```cs
// Calculate daily average and the difference from that average
var windowDf = df.Select(
    Col("Date"),
    Col("City"),
    Col("Temperature"),
    
    // Window Expression: Mean over Date
    Col("Temperature").Mean().Over("Date").Alias("Daily_Avg"),
    
    // Calculate difference immediately
    (Col("Temperature") - Col("Temperature").Mean().Over("Date")).Alias("Diff")
).Sort("Date", descending: false);
```
F#
```fs
let windowDf = 
    df
    |> pl.select [
        pl.col "Date"
        pl.col "City"
        pl.col "Temperature"
        
        (pl.col("Temperature").Mean().Over(pl.col "Date")).Alias "Daily_Avg"
        
        (pl.col "Temperature" - pl.col("Temperature").Mean().Over(pl.col "Date")).Alias "Diff"
    ]
    |> pl.sort (pl.col "Date", false)
```
## 5. The Lazy API (Performance)
This is where Polars.NET shines. Instead of executing every step immediately, Lazy() builds a query plan. Collect() optimizes and runs it.

#### Why Lazy?

1. Predicate Pushdown: Filters are applied before loading data (if possible).

2. Projection Pushdown: Only necessary columns are loaded.

3. Parallel Execution: Branching logic is executed in parallel.

C#
```cs
var lf = df.Lazy()
    .Filter(Col("Temperature") > 10.0)
    .GroupBy("City")
    .Agg(Col("Temperature").Mean().Alias("Lazy_Avg_Temp"));

// See the optimized query plan
Console.WriteLine(lf.Explain());

// Execute
var finalDf = lf.Collect();
```
F#
```fs
let lf = 
    df.Lazy()
    |> LazyFrame.filter (pl.col "Temperature" .> pl.lit 10.0)
    |> LazyFrame.groupBy [pl.col "City"] [
         pl.mean(pl.col "Temperature").Alias "Lazy_Avg_Temp"
    ]

printfn "%s" (lf.Explain(optimized=true))

let finalDf = lf.Collect()
```
## Next Steps
- IO Operations: Learn how to read/write CSV, Parquet, and Excel.

- User Defined Functions: Learn how to map C# functions for custom logic.

- API Reference: Explore the full list of available Expressions.