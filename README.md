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

## Installation

C# Users:
```Bash
dotnet add package Polars.NET 
```
F# Users:
```Bash
dotnet add package Polars.FSharp
```

- Requirements: .NET 8+.
- Hardware: CPU with AVX2 support (x86-64-v3). Roughly Intel Haswell (2013+) or AMD Excavator (2015+). If you have AVX-512 supported CPU, please try to compile Rust core on your machine use RUSTFLAGS='-C target-cpu=native'

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

- Benchmark Code [Here](https://github.com/ErrorLSC/Polars.NET-Benchmark)

- Environment:

    - OS: Fedora Linux 43 (KDE Plasma)

    - CPU & Memory: Intel Core i7-10700 @ 2.90GHz，32G RAM

    - Runtime: .NET 10.0.2 (RyuJIT X64), Python 3.14.2

    - Polars.NET & Polars.FSharp Version: v0.2.1-beta1

#### 1. CSV Parsing & GroupBy (100M Rows)

- Winner: Polars.NET

| Method                  | Mean      | Managed Mem |
|------------------------ |----------:| -----------:|
| Polars_Streaming        |   1.688 s |     2.41 KB |
| DuckDB_SQL              |   2.298 s |     5.86 KB |
| Microsoft_Data_Analysis | 171.973 s |      498 GB |
| Native_PLINQ            |  38.723 s |    15.32 GB |

#### 2. Cosine Similarity (100M Elements)

- Winner: TensorPrimitives / AVX2 For pure dense numerical computations, use .NET's TensorPrimitives. 
    Polars.NET incurs overhead for expression planning.

| Method                  | Mean        | Managed Mem |
|------------------------ |------------:|------------:|
| TensorPrimitives_CosSim |    37.31 ms |         0 B |
| Polars_CosSim           |   261.96 ms |      2024 B |
| MathNet_CosSim          | 1,381.02 ms |      592 MB |
| Linq_CosSim             |   150.00 ms |      104  B |

#### 3. Decimal Rolling Average (100M Rows)

- Winner: PLINQ. To prevent OOM exception, DuckDB.NET and Polars.NET are recommended here.

| Method             | Mean     | Managed Mem |
|------------------- |---------:|------------:|
| Polars_Rolling_MA  | 16.739 s |     2.73 KB |
| DuckDB_Rolling_MA  |  5.677 s |     1.16 KB |
| Linq_Rolling_100M  | 28.110 s |     1.29 GB |
| PLinq_Rolling_100M |  3.349 s |     7.87 GB |

#### 4. NDJSON Parsing (5M Rows)

- Winner: DuckDB DuckDB's projection pushdown on JSON is superior.
    Polars.NET is still 3x faster than System.Text.Json and 10x faster than Newtonsoft.

| Method                 | Mean        | Managed Mem |
|----------------------- |------------:|------------:|
| DuckDB_ScanNdjson      |    764.1 ms |     2.73 KB |
| Polars_ScanNdjson      |  2,279.5 ms |      3.6 KB |
| SystemTextJson_Process |  7,267.9 ms |     5.51 GB |
| Newtonsoft_Dynamic     | 25,970.3 ms |    42.78 GB |

#### 5. Excel Parsing (1M Rows, 20 Cols)

- Winner: Polars.NET Leveraging Rust's calamine crate via FFI allows Polars.NET to outperform dedicated .NET Excel libraries.

| Method          | Mean    | Managed Mem |
|---------------- |--------:|------------:|
| Polars_Mixed    | 14.74 s |     2.74 KB |
| MiniExcel_Mixed | 28.26 s |    15.94 GB |
| EPPlus_Mixed    | 34.84 s |    10.33 GB |

#### 6. Join (20M Rows)

- Winner: DuckDB / Polars.NET Both utilize high-performance Hash Joins. 
    Traditional C# approaches (LINQ/MDA) are not viable for this scale.

| Method      | Mean        | Managed Mem |
|------------ |------------:|------------:|
| Polars_Join |    386.4 ms |     3.45 KB |
| DuckDB_Join |    286.9 ms |      1.2 KB | 
| Linq_Join   |  9,254.0 ms |     1.16 GB |  
| MDA_Join    | 15,346.4 ms |     5.83 GB |

#### 7. Polars.FSharp vs Deedle(Quant, same as 3)

- Winner: Polars.NET 
    45x faster than Deedle.

| Method                      | Mean        | Managed Mem |
|---------------------------- |------------:|------------:|
| Deedle_Decimal_Rolling      | 1,824.86 ms |     4.24 GB |
| Polars_Rolling              |    41.89 ms |     1352  B |

#### 8. Polars.NET vs Python brothers(pandas and pypolars)

- UDF

    Winner: Polars.NET. RyuJIT outperforms Python interpreter in UDF case.

| Method         | Mean     | Managed Mem |
|--------------- |---------:|------------:|
| Polars.NET_UDF | 177.6 ms |   228.74 MB |
| Pandas_UDF     | 656.6 ms |             |
| PyPolars_UDF   | 515.1 ms |             |

- GroupBy (10M Rows)

    Winner: Polars.NET. Very little diff between Polars.NET and pypolars. 

| Method             | Mean      | Managed Mem |
|--------------------|----------:|------------:|
| Polars.NET_GroupBy |  16.75 ms |       741 B |
| Pandas_GroupBy     |  17.70 ms |             |
| PyPolars_GroupBy   | 145.73 ms |             |

## Architecture

3-Layer Architecture ensures stability even when the underlying Rust engine changes.

1. Hand-written Rust C ABI layer bridging .NET and Polars. (native_shim)
2. .NET Core layer for dirty works like unsafe ops, wrappers, LibraryImports. (Polars.NET.Core)
3. High level C# and F# API layer here. No unsafe blocks. (Polars.CSharp & Polars.FSharp)

## Roadmap & Documentation

- Expanded SQL Support: Full coverage of Polars SQL capabilities (CTEs, Window Functions) to replace in-memory DataTable SQL queries.

- Cloud Support : Direct IO integration with AWS S3, Azure Blob Storage, and Data Lakes.

- Documentation: [**Docs Here**](https://errorlsc.github.io/Polars.NET/index.html)

## Contributing

Contributions are welcome! Whether it's adding new expression mappings, improving documentation, or optimizing the FFI layer.

1. Fork the repo.

2. Create your feature branch.

3. Submit a Pull Request.

## License

MIT License. See LICENSE for details.