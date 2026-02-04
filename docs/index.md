---
layout: home
title: Polars.NET - Blazingly Fast DataFrames for .NET
hero:
  name: Polars.NET
  text: The High-Performance DataFrame Library
  tagline: Rust Core. Arrow Memory. Native Speed.
  actions:
    - theme: brand
      text: Get Started
      link: /article/quickstart
    - theme: alt
      text: View on GitHub
      link: https://github.com/errorlsc/polars.net
features:
  - title: 🚀 Rust Powered
    details: Built on top of the Polars engine. Multi-threaded, SIMD-optimized, and query-optimized out of the box.
  - title: ⚡ Zero-Copy FFI
    details: Leverages Apache Arrow C Data Interface for O(1) memory transfer between C# and Rust.
  - title: 🛡️ Memory Safe
    details: Handles 400M+ row ETL pipelines with stable, constant memory usage via Streaming Engine.
  - title: ⚔️ Dual API
    details: Fluent/LINQ-style API for C# developers and a functional Pipeline API for F# lovers.
---

# Why Polars.NET?

**Polars.NET** brings the lightning-fast [Polars](https://pola.rs/) data processing engine to the .NET ecosystem. It is not just a wrapper; it is a high-performance bridge connecting the safety of .NET with the raw power of Rust.

<div align="center">
  <img src="images/icon.png" alt="Polars.NET Logo" width="200"/>
</div>

### The "Void" is Filled
For too long, .NET developers have envied the Python ecosystem's data tools. `Microsoft.Data.Analysis` is stagnant, `Python.NET` is heavy, and `Spark` requires a JVM.

**Polars.NET changes the game.** No Python interpreter. No JVM. Just pure, native performance delivered via a single NuGet package.

---

## 🔥 Key Features

### 1. The Power of Lazy Evaluation
Unlike traditional Eager DataFrames (Pandas, Deedle), Polars.NET features a robust **Query Optimizer**. It sees your entire query, optimizes predicates, pushes down filters, and executes efficiently.

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
var res = df.
    Lazy()
    .Filter(Col("Age") > 28)
    .GroupBy("Dept")
    .Agg(
        Col("Age").Mean().Alias("AvgAge"),
        Col("Name").Count().Alias("Count")
    )
    .Sort("AvgAge");

// 3. Output
res.CollectStreaming().Show();
///shape: (2, 3)
/// ┌──────┬────────┬───────┐
/// │ Dept ┆ AvgAge ┆ Count │
/// │ ---  ┆ ---    ┆ ---   │
/// │ str  ┆ f64    ┆ u32   │
/// ╞══════╪════════╪═══════╡
/// │ HR   ┆ 30.0   ┆ 1     │
/// │ IT   ┆ 35.0   ┆ 1     │
/// └──────┴────────┴───────┘
```
### 2.Functional F# Bliss

```fsharp
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
/// result
/// shape: (3, 5)
/// ┌────────────┬────────────┬─────────────┬───────────┬───────┐
/// │ Date       ┆ City       ┆ Temperature ┆ Daily_Avg ┆ Diff  │
/// │ ---        ┆ ---        ┆ ---         ┆ ---       ┆ ---   │
/// │ date       ┆ str        ┆ f64         ┆ f64       ┆ f64   │
/// ╞════════════╪════════════╪═════════════╪═══════════╪═══════╡
/// │ 2023-01-01 ┆ London     ┆ 10.5        ┆ 9.75      ┆ 0.75  │
/// │ 2023-01-01 ┆ Manchester ┆ 9.0         ┆ 9.75      ┆ -0.75 │
/// │ 2023-01-02 ┆ London     ┆ 12.1        ┆ 12.1      ┆ 0.0   │
/// └────────────┴────────────┴─────────────┴───────────┴───────┘
```

### 3.Unbeatable I/O

- Excel: Powered by calamine (Rust), reading Excel files 10x-50x faster than EPPlus/NPOI without GC pressure.

- CSV/Parquet: Multi-threaded SIMD parsing that saturates your NVMe SSD.

- SQL: Read directly from databases into Arrow memory.

### 4.Zero-Copy Architecture

We don't marshal objects. We share pointers.

- Rust FFI: Handwritten unsafe shim ensuring precise memory layout control.

- .NET Core: Uses .NET 7+ LibraryImport for minimal runtime overhead.

- Apache Arrow: The lingua franca of in-memory data. C# sees what Rust allocates instantly.
