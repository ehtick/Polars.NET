# Polars.NET

🚀 **High-Performance, AI-Ready DataFrames for .NET, powered by Rust & Apache Arrow.**

Polars.NET is not just a binding; it is a production-grade data engineering toolkit for the .NET ecosystem. It brings the lightning-fast performance of the Polars Rust engine to C# and F#, while adding unique, enterprise-ready features missing from official bindings—like seamless Database Streaming, Zero-Copy Interop, and AI-native Vector support.

## Why Polars.NET exists

The .NET ecosystem deserves a first-class, production-grade DataFrame engine —
not a thin wrapper, not a toy binding, and not a Python dependency in disguise.

Polars.NET is designed for engineers who care about:

- predictable performance

- strong typing

- streaming data at scale

- and long-term system evolution

## Why Polars.NET?

1. ⚡ Unmatched Performance
- Rust Core: Built on the blazing fast Polars query engine (written in Rust).

- Lazy Evaluation: Intelligent query optimizer with predicate pushdown, projection pushdown, and parallel execution.

- Zero-Copy: Built on Apache Arrow, enabling zero-copy data transfer between C#, Python, and databases.

2. 🛡️ Enterprise & AI Ready
- Database Streaming (Unique):

   - Read: Stream millions of rows from any IDataReader (SQL Server, Postgres, SQLite) directly into Polars without loading everything into RAM.

   - Write: Stream processed data back to databases via IBulkCopy interfaces using our unique ArrowToDbStream adapter.

- AI / Vector Support: First-class support for FixedSizeList (Array) columns. Perform high-performance vector operations (Dot Product, Cosine Similarity, Aggregations) directly in the engine—perfect for RAG and Embedding workflows.

3. 🧶 .NET Native Experience

    - Fluent API: Intuitive, LINQ-like API design for C#.

    - Functional API: Idiomatic, pipe-forward (|>) API for F# lovers.

    - Type Safety: Leveraging .NET's strong type system to prevent runtime errors.

## 📦 Installation

```Bash
dotnet add package Polars.NET
```

## Target Framework

.NET 8 and 

## 🏁 Quick Start

### C# Example

```C#
using Polars.CSharp;
using static Polars.CSharp.Polars; // For Col(), Lit() helpers

// 1. Create a DataFrame
var data = new[] {
    new { Name = "Alice", Age = 25, Dept = "IT" },
    new { Name = "Bob", Age = 30, Dept = "HR" },
    new { Name = "Charlie", Age = 35, Dept = "IT" }
};
using var df = DataFrame.From(data);

// 2. Filter & Aggregate
using var res = df
    .Filter(Col("Age") > 28)
    .GroupBy("Dept")
    .Agg(
        Col("Age").Mean().Alias("AvgAge"),
        Col("Name").Count().Alias("Count")
    )
    .Sort("AvgAge", descending: true);

// 3. Output
res.Show();
```
### F# Example

```F#

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
## 🔥 Killer Features (The "Missing" Parts)

1. 🌊 Streaming ETL: Database -> Polars -> Database

Process millions of rows with constant memory usage using our unique streaming adapters.

```C#
// 1. Source: Stream from Database (e.g., SqlDataReader)
// We scan the DB via a factory, pulling 50k rows at a time into Apache Arrow batches.
var lf = LazyFrame.ScanDb(() => mySqlCommand.ExecuteReader(), batchSize: 50_000);

// 2. Transform: Lazy Evaluation (Rust Engine)
// No data is loaded yet. We are building a query plan.
var pipeline = lf
    .Filter(Col("Region") == Lit("US"))
    .WithColumn((Col("Amount") * 1.08).Alias("TaxedAmount"))
    .Select("OrderId", "TaxedAmount", "OrderDate");

// 3. Sink: Stream back to Database (e.g., SqlBulkCopy)
// We expose the processed stream as an IDataReader implementation!
pipeline.SinkTo((IDataReader reader) => 
{
    // This reader pulls data from the Rust engine on-demand.
    // Perfect for SqlBulkCopy.WriteToServer(reader)
    using var bulk = new SqlBulkCopy(connectionString);
    bulk.DestinationTableName = "ProcessedOrders";
    bulk.WriteToServer(reader);
});
```
```
┌──────────────┐     Arrow Batches     ┌────────────┐
│  Database    │ ───────────────────▶ │ Polars Core│
│ (IDataReader)│                       │   (Rust)   │
└─────▲────────┘ ◀─────────────────── │            │
      │        Zero-Copy Stream        └─────▲──────┘
      │                                      │
      │                                      │ FFI
┌─────┴──────┐                               │
│   .NET API │ ◀────────────────────────────┘
│ (C# / F#)  │
└────────────┘
```

2. 🧠 Vector / Embedding Operations

Native support for Array (Fixed-Size List) types, enabling high-performance AI workflows.

```C#
// Scenario: RAG - Calculate Cosine Similarity between Query and Document Embeddings
using var df = DataFrame.FromColumns(new { 
    DocId = new[] { 1, 2 },
    Embedding = new[] { new[] {0.1, 0.2}, new[] {0.5, 0.8} } // Array<Float64, 2>
});

var queryVec = new[] { 0.1, 0.2 }; // Query Embedding

var res = df.Lazy()
    .Select(
        Col("DocId"),
        // Calculate Dot Product & Similarity efficiently
        (Col("Embedding") * Lit(queryVec)).Array.Sum().Alias("Score")
    )
    .TopK(1, "Score") // Get Top 1 match
    .Collect();
```

3. 🕒 Time Series Intelligence

Robust support for time-series data, including As-Of Joins and Dynamic Rolling Windows.

```C#
// As-Of Join: Match trades to the nearest quote within 2 seconds
var trades = dfTrades.Lazy(); // timestamp, ticker, price
var quotes = dfQuotes.Lazy(); // timestamp, ticker, bid

var enriched = trades.JoinAsOf(
    quotes, 
    leftOn: Col("timestamp"), 
    rightOn: Col("timestamp"),
    by: [Col("ticker")],      // Match on same Ticker
    tolerance: "2s",          // Look back max 2 seconds
    strategy: "backward"      // Find previous quote
);
```

```F#
// F# Dynamic Rolling Window
lf
|> pl.groupByDynamic "time" (TimeSpan.FromHours 1.0)
    [ pl.col("value").Mean().Alias("hourly_mean") ]
|> pl.collect
```

## 🗺️ Roadmap & Documentation

We are actively working on detailed API documentation.

    - Auto-generated API Reference (HTML)

    - Advanced Recipes (Time Series, Vector Search)

## 🤝 Contributing

Contributions are welcome! Whether it's adding new expression mappings, improving documentation, or optimizing the FFI layer.

1. Fork the repo.

2. Create your feature branch.

3. Submit a Pull Request.

## 📄 License

MIT License. See LICENSE for details.