#!/bin/bash
set -e

echo "=== 1. Building Projects (Release) ==="
dotnet build Polars.FSharp/Polars.FSharp.fsproj -c Release

echo "=== 2. Generating F# Markdown (Custom Script) ==="
dotnet fsi GenerateFSharpDocs.fsx

echo "=== 3. Organizing Docs ==="

rm -rf docs/api/csharp
rm -rf docs/api/fsharp
rm -rf docs/_site


mkdir -p docs/api/fsharp

cp -r docs/api/fsharp_generated/* docs/api/fsharp/

echo "=== 4. Building Site (DocFX) ==="
docfx metadata docs/docfx.json
docfx build docs/docfx.json
rm -rf docs/api/fsharp_generated
echo "=== Done! ==="