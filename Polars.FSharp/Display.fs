namespace Polars.FSharp

open System
open System.IO
open System.Text
open Microsoft.DotNet.Interactive.Formatting
open Polars.NET.Core.Arrow
/// <summary>
/// Display utilities for DataFrame and LazyFrame in interactive environments.
/// </summary>
[<AutoOpen>]
module Display =

    /// <summary>
    /// Display DataFrame as Html Table
    /// </summary>
    let toHtml (df: DataFrame) =
        let rowsToShow = 10
        let totalRows = df.Rows
        let n = Math.Min(int64 rowsToShow, totalRows)
        
        use previewDf = df.Head(int n)
        use batch = ArrowFfiBridge.ExportDataFrame previewDf.Handle
        let schema = batch.Schema

        let sb = StringBuilder()
        
        // CSS style
        sb.Append("""<style>
            .pl-frame { font-family: "Consolas", "Monaco", monospace; font-size: 13px; border-collapse: collapse; border: 1px solid #e0e0e0; }
            .pl-frame th { background-color: #f0f0f0; font-weight: bold; text-align: left; padding: 6px 12px; border-bottom: 2px solid #ccc; }
            .pl-frame td { padding: 6px 12px; border-bottom: 1px solid #f0f0f0; white-space: pre; } /* white-space: pre fomart */
            .pl-frame tr:nth-child(even) { background-color: #f9f9f9; }
            .pl-frame tr:hover { background-color: #f1f1f1; }
            .pl-dim { font-family: sans-serif; font-size: 12px; color: #666; margin-bottom: 8px; }
            .pl-type { font-size: 10px; color: #999; display: block; margin-top: 2px; font-weight: normal; }
            .pl-null { color: #d0d0d0; font-style: italic; }
        </style>""") |> ignore

        // 3. dimension info
        sb.AppendFormat("<div class='pl-dim'>Polars DataFrame: <b>({0} rows, {1} columns)</b></div>", totalRows, df.ColumnNames) |> ignore
        
        // 4. build table
        sb.Append "<div style='overflow-x:auto'><table class='pl-frame'>" |> ignore
        
        // --- head ---
        sb.Append "<thead><tr>" |> ignore
        for field in schema.FieldsList do
            // Arrow DataType Name (e.g., Timestamp, Int64)
            let typeName = field.DataType.Name 
            sb.AppendFormat("<th>{0}<span class='pl-type'>{1}</span></th>", 
                System.Net.WebUtility.HtmlEncode field.Name, 
                typeName) |> ignore
        sb.Append "</tr></thead>" |> ignore

        // --- 表体 ---
        sb.Append "<tbody>" |> ignore
        let rowCount = batch.Length
        let colCount = batch.ColumnCount

        for i in 0 .. rowCount - 1 do
            sb.Append "<tr>" |> ignore
            for j in 0 .. colCount - 1 do
                let colArray = batch.Column j
                
                let valStr = colArray.FormatValue i
                
                if valStr = "null" then
                    sb.Append "<td class='pl-null'>null</td>" |> ignore
                else
                    sb.AppendFormat("<td>{0}</td>", System.Net.WebUtility.HtmlEncode valStr) |> ignore
            sb.Append "</tr>" |> ignore
        
        if totalRows > int64 rowsToShow then
             let remaining = totalRows - int64 rowsToShow
             sb.AppendFormat("<tr><td colspan='{0}' style='text-align:center; font-style:italic; color:#999; padding: 10px'>... {1} more rows ...</td></tr>", colCount, remaining) |> ignore

        sb.Append "</tbody></table></div>" |> ignore
        sb.ToString()

    /// <summary>
    /// Init notebook support
    /// </summary>
    let init () =
        Formatter.Register<DataFrame>(
            Action<DataFrame, TextWriter>(fun df writer -> 
                writer.Write(toHtml df)
            ),
            "text/html"
        )
        
        Formatter.Register<LazyFrame>(
            Action<LazyFrame, TextWriter>(fun lf writer -> 
                let plan = lf.Explain true 
                
                let schemaStr = lf.SchemaRaw 
                
                let html = $"""
                <div style="font-family: monospace;">
                    <div style="background:#f4f4f4; padding:5px; border-bottom:1px solid #ddd; font-weight:bold">Polars LazyFrame</div>
                    <div style="padding:10px">
                        <strong>Optimized Plan:</strong>
                        <pre style="background:#f9f9f9; padding:10px; border:1px solid #eee;">{System.Net.WebUtility.HtmlEncode plan}</pre>
                    </div>
                </div>
                """
                writer.Write html
            ),
            "text/html"
        )
    