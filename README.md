[![NuGet](https://img.shields.io/nuget/v/Polars.NET.svg)](https://www.nuget.org/packages/Polars.NET)
[![NuGet Downloads](https://img.shields.io/nuget/dt/Polars.NET.svg)](https://www.nuget.org/packages/Polars.NET)
[![NuGet](https://img.shields.io/nuget/v/Polars.FSharp.svg)](https://www.nuget.org/packages/Polars.FSharp)
[![NuGet Downloads](https://img.shields.io/nuget/dt/Polars.FSharp.svg)](https://www.nuget.org/packages/Polars.FSharp)
[![License](https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square)](LICENSE)
[![Docs](https://img.shields.io/badge/docs-online-brightgreen.svg?style=flat-square&logo=read-the-docs&logoColor=white)](https://errorlsc.github.io/Polars.NET/index.html)

# Polars.NET

**High-Performance, DataFrame Engine for .NET, powered by Rust & Apache Arrow.**

Supported Platforms: Windows (x64), Linux (x64/ARM64, glibc/musl), macOS (ARM64).

## Why Polars.NET exists

This is the game I'd like to play: binding the lightning-fast Polars engine to the .NET ecosystem.
And it means a lot of fun to do this.

## Architecture

3-Layer Architecture ensures stability even when the underlying Rust engine changes.

1. Hand-written Rust C ABI layer bridging .NET and Polars. (native_shim)
2. .NET Core layer for dirty works like `unsafe` ops, wrappers, `LibraryImports`. (Polars.NET.Core)
3. C# and F# API layer here. No `unsafe` blocks. (Polars.CSharp & Polars.FSharp)

## Installation

C# Users:
```Bash
dotnet add package Polars.NET 
```
F# Users:
```Bash
dotnet add package Polars.FSharp
```

Requirements: .NET 8+.
Hardware: CPU with AVX2 support (x86-64-v3). Roughly Intel Haswell (2013+) or AMD Excavator (2015+).

## Quick Start

### C# Example

```csharp
using Polars.CSharp;
using static Polars.CSharp.Polars; // For Col(), Lit() helpers

// 1. Create a DataFrame
var data = new[] {
    new { Name = "Alice", Age = 25, Dept = "IT" },
    new { Name = "Bob", Age = 30, Dept = "HR" },
    new { Name = "Charlie", Age = 35, Dept = "IT" }
};
var df = DataFrame.From(data);

// 2. Filter & Aggregate
var res = df
    .Filter(Col("Age") > 28)
    .GroupBy("Dept")
    .Agg(
        Col("Age").Mean().Alias("AvgAge"),
        Col("Name").Count().Alias("Count")
    )
    .Sort("AvgAge", descending: true);

// 3. Output
res.Show();
// shape: (2, 3)
// ┌──────┬────────┬───────┐
// │ Dept ┆ AvgAge ┆ Count │
// │ ---  ┆ ---    ┆ ---   │
// │ str  ┆ f64    ┆ u32   │
// ╞══════╪════════╪═══════╡
// │ IT   ┆ 35.0   ┆ 1     │
// │ HR   ┆ 30.0   ┆ 1     │
// └──────┴────────┴───────┘
```
### F# Example

```fsharp

open Polars.FSharp

// 1. Scan CSV (Lazy)
let lf = LazyFrame.ScanCsv "users.csv"

// 2. Transform Pipeline
let res = 
    lf
    |> pl.filterLazy (pl.col "age" .> pl.lit 28)
    |> pl.groupByLazy 
        [ pl.col "dept" ]
        [ 
            pl.col("age").Mean().Alias "AvgAge" 
            pl.col("name").Count().Alias "Count"
        ]
    |> pl.collect
    |> pl.sort ("AvgAge", false)

// 3. Output
res.Show()

```
## Polyglot Notebook Support

Polars.NET works seamlessly with .NET Interactive. Please refer to the example folder for a Polyglot Notebook demo.

## Benchmark

Environment:

- OS: Fedora Linux 43 (KDE Plasma)

- CPU & Memory: Intel Core i7-10700 @ 2.90GHz， 32G RAM

- Runtime: .NET 10.0.2 (RyuJIT X64)

1. CSV Parsing & GroupBy (100M Rows)

Winner: Polars.NET

| Method                  | Mean      | Error     | StdDev   | Ratio  | RatioSD | Rank | Gen0          | Gen1          | Gen2       | Allocated       | Alloc Ratio    |
|------------------------ |----------:|----------:|---------:|-------:|--------:|-----:|--------------:|--------------:|-----------:|----------------:|---------------:|
| Polars_Lazy             |   1.688 s |  0.7931 s | 0.0435 s |   1.00 |    0.03 |    1 |             - |             - |          - |         2.41 KB |           1.00 |
| DuckDB_SQL              |   2.298 s |  0.7716 s | 0.0423 s |   1.36 |    0.04 |    2 |             - |             - |          - |         5.86 KB |           2.43 |
| Microsoft_Data_Analysis | 171.973 s | 32.3325 s | 1.7723 s | 101.93 |    2.48 |    4 | 60349000.0000 | 15096000.0000 | 17000.0000 | 497648378.43 KB | 206,145,606.60 |
| Native_PLINQ            |  38.723 s | 19.5104 s | 1.0694 s |  22.95 |    0.76 |    3 |  1559000.0000 |   782000.0000 |  6000.0000 |  15323745.74 KB |   6,347,700.50 |

2. Cosine Similarity (100M Elements)

Winner: TensorPrimitives / AVX2 For pure dense numerical computations, use .NET's TensorPrimitives.
Polars incurs overhead for expression planning.

| Method                  | Mean        | Error     | StdDev   | Ratio | RatioSD | Gen0       | Allocated   | Alloc Ratio |
|------------------------ |------------:|----------:|---------:|------:|--------:|-----------:|------------:|------------:|
| TensorPrimitives_CosSim |    37.31 ms |  0.551 ms | 0.030 ms |  1.00 |    0.00 |          - |           - |          NA |
| Polars_CosSim           |   261.96 ms | 19.628 ms | 1.076 ms |  7.02 |    0.03 |          - |      2024 B |          NA |
| MathNet_CosSim          | 1,381.02 ms | 47.750 ms | 2.617 ms | 37.02 |    0.07 | 70000.0000 | 592000112 B |          NA |
| Linq_CosSim             |   150.00 ms |  2.566 ms | 0.141 ms |  4.02 |    0.00 |          - |       104 B |          NA |

3. Decimal Rolling Average (100M Rows)

Winner: PLINQ. To prevent OOM exception, DuckDB.NET and Polars.NET are recommended.

| Method             | Mean     | Error     | StdDev   | Ratio | RatioSD | Gen0        | Gen1       | Gen2      | Allocated     | Alloc Ratio |
|------------------- |---------:|----------:|---------:|------:|--------:|------------:|-----------:|----------:|--------------:|------------:|
| Polars_Rolling_MA  | 16.739 s | 16.8846 s | 0.9255 s |  5.00 |    0.24 |           - |          - |         - |       2.73 KB |       0.000 |
| DuckDB_Rolling_MA  |  5.677 s |  0.2891 s | 0.0158 s |  1.70 |    0.01 |           - |          - |         - |       1.16 KB |       0.000 |
| Linq_Rolling_100M  | 28.110 s |  5.1386 s | 0.2817 s |  8.39 |    0.10 |  84000.0000 | 83000.0000 | 6000.0000 | 1288413.37 KB |       0.164 |
| PLinq_Rolling_100M |  3.349 s |  0.5944 s | 0.0326 s |  1.00 |    0.01 | 176000.0000 | 97000.0000 | 1000.0000 | 7869481.27 KB |       1.000 |

4. NDJSON Parsing (5M Rows)

Winner: DuckDB DuckDB's projection pushdown on JSON is superior.
Polars.NET is still 3x faster than System.Text.Json and 10x faster than Newtonsoft.

| Method                 | Mean        | Error      | StdDev    | Ratio | RatioSD | Gen0         | Gen1        | Allocated      | Alloc Ratio   |
|----------------------- |------------:|-----------:|----------:|------:|--------:|-------------:|------------:|---------------:|--------------:|
| DuckDB_ScanNdjson      |    764.1 ms |   316.3 ms |  17.34 ms |  1.00 |    0.03 |            - |           - |        2.73 KB |          1.00 |
| Polars_ScanNdjson      |  2,279.5 ms |   885.9 ms |  48.56 ms |  2.98 |    0.08 |            - |           - |         3.6 KB |          1.32 |
| SystemTextJson_Process |  7,267.9 ms |   457.3 ms |  25.07 ms |  9.52 |    0.19 |  674000.0000 |   2000.0000 |  5510787.13 KB |  2,021,148.29 |
| Newtonsoft_Dynamic     | 25,970.3 ms | 5,027.7 ms | 275.58 ms | 34.00 |    0.74 | 5237000.0000 | 132000.0000 | 42776424.27 KB | 15,688,774.52 |

5. Excel Parsing (1M Rows, 20 Cols)

Winner: Polars.NET Leveraging Rust's calamine crate via FFI allows Polars.NET to outperform dedicated .NET Excel libraries.

| Method          | Mean    | Error   | StdDev  | Ratio | RatioSD | Gen0         | Gen1        | Gen2       | Allocated      | Alloc Ratio  |
|---------------- |--------:|--------:|--------:|------:|--------:|-------------:|------------:|-----------:|---------------:|-------------:|
| Polars_Mixed    | 14.74 s | 2.509 s | 0.138 s |  1.00 |    0.01 |            - |           - |          - |        2.74 KB |         1.00 |
| MiniExcel_Mixed | 28.26 s | 0.974 s | 0.053 s |  1.92 |    0.02 | 1950000.0000 |  26000.0000 |          - | 15935125.02 KB | 5,811,099.72 |
| EPPlus_Mixed    | 34.84 s | 2.042 s | 0.112 s |  2.36 |    0.02 |  822000.0000 | 186000.0000 | 16000.0000 | 10328914.66 KB | 3,766,669.73 |

6. Join (20M Rows)

Winner: DuckDB / Polars.NET Both utilize high-performance Hash Joins. 
Traditional C# approaches (LINQ/MDA) are not viable for this scale.

| Method      | Mean        | Error       | StdDev   | Ratio | RatioSD | Gen0        | Gen1        | Gen2      | Allocated     | Alloc Ratio  |
|------------ |------------:|------------:|---------:|------:|--------:|------------:|------------:|----------:|--------------:|-------------:|
| Polars_Join |    386.4 ms |   155.52 ms |  8.52 ms |  1.35 |    0.03 |           - |           - |         - |       3.45 KB |         2.89 |
| DuckDB_Join |    286.9 ms |    85.11 ms |  4.67 ms |  1.00 |    0.02 |           - |           - |         - |        1.2 KB |         1.00 |
| Linq_Join   |  9,254.0 ms | 1,797.34 ms | 98.52 ms | 32.26 |    0.54 | 102000.0000 | 101000.0000 | 5000.0000 | 1157332.91 KB |   968,226.23 |
| MDA_Join    | 15,346.4 ms | 1,534.58 ms | 84.12 ms | 53.49 |    0.79 | 312000.0000 |  27000.0000 | 4000.0000 | 5838266.01 KB | 4,884,300.97 |

7. Polars.FSharp vs Deedle(Quant, same as 3)

Winner: Polars.NET 
45x faster than Deedle.

| Method                      | Mean        | Error      | StdDev    | Ratio | RatioSD | Gen0        | Gen1        | Gen2      | Allocated    | Alloc Ratio |
|---------------------------- |------------:|-----------:|----------:|------:|--------:|------------:|------------:|----------:|-------------:|------------:|
| Deedle_Decimal_Rolling      | 1,824.86 ms | 882.497 ms | 48.373 ms | 1.000 |    0.03 | 491000.0000 | 124000.0000 | 7000.0000 | 4240150912 B |       1.000 |
| Polars_Rolling              |    41.89 ms |   4.345 ms |  0.238 ms | 0.023 |    0.00 |           - |           - |         - |       1352 B |       0.000 |

8. Polars.NET vs Python brothers(pandas and pypolars)

- UDF

Polars.NET wins. RyuJIT outperforms Python interpreter.

```csharp
// Generated Data: "ID_0_OK", "ID_1_OK" ...
var udfExpr = Col("log")
    .Map<string, int>(str => 
    {
        var parts = str.Split('_'); 
        return int.Parse(parts[1]);
    }, DataType.Int32)
    .Alias("parsed_id");

using var res = _df.Select(udfExpr);
```
```python
# pandas
res_pandas = pdf["log"].apply(lambda x: int(x.split('_')[1]))
# pypolars
res_polars = pl_df.select(
    pl.col("log").map_elements(lambda x: int(x.split('_')[1]), return_dtype=pl.Int64)
)
```
| Method                 | Mean     | Error   | StdDev  | Gen0       | Gen1      | Gen2      | Allocated |
|----------------------- |---------:|--------:|--------:|-----------:|----------:|----------:|----------:|
| PolarsDotNet_StringUdf | 177.6 ms | 0.92 ms | 0.82 ms | 28000.0000 | 1333.3333 | 1333.3333 | 228.74 MB |
| Pandas                 | 656.6 ms |         |         |            |           |           |           |
| PyPolars               | 515.1 ms |         |         |            |           |           |           |

- GroupBy (10M Rows)

Polars.NET wins.

```csharp
var res = _df.Lazy()
    .GroupBy("id")
    .Agg(
        Col("val").Sum().Alias("total")
    )
    .Collect();
```
```python
# polars
res = (
    df.lazy()
    .group_by("id")
    .agg(pl.col("val").sum().alias("total"))
    .collect() 
)
# pandas
res_pd = df_pd.groupby("id")["val"].sum()
```
| Method               | Mean      | Error    | StdDev   | Allocated |
|--------------------- |----------:|---------:|---------:|----------:|
| PolarsDotNet_GroupBy | 16.75 ms  | 0.268 ms | 0.251 ms |     741 B |
| Pandas_GroupBy       | 17.70 ms  |          |          |           |
| PolarsPython_GroupBy | 145.73 ms |          |          |           |

## 🗺️ Roadmap & Documentation

- Expanded SQL Support: Full coverage of Polars SQL capabilities (CTEs, Window Functions) to replace in-memory DataTable SQL queries.

- Cloud Support : Direct IO integration with AWS S3, Azure Blob Storage, and Data Lakes.

- Documentation: [📖 **Read the Comprehensive API Reference**](https://errorlsc.github.io/Polars.NET/index.html)

## 🤝 Contributing

Contributions are welcome! Whether it's adding new expression mappings, improving documentation, or optimizing the FFI layer.

1. Fork the repo.

2. Create your feature branch.

3. Submit a Pull Request.

## 📄 License

MIT License. See LICENSE for details.