namespace Polars.FSharp.Tests

open System
open System.IO
open Xunit
open Polars.FSharp
open MiniExcelLibs // 这是一个 .NET 库，F# 调用完全没问题

// 定义一个 Record Type，MiniExcel 会自动将其属性映射为 Excel 列
type TestExcelRow = {
    Id: int
    Name: string
    Score: float
    IsActive: bool
    JoinDate: DateTime
}

type FSharpExcelTests() =

    [<Fact>]
    member _.``IO: Read Excel (Native Roundtrip with MiniExcel)`` () =
        let tempFile = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString() + ".xlsx")
        
        try
            // ---------------------------------------------------
            // 1. 准备数据 (F# List of Records)
            // ---------------------------------------------------
            let data = [
                { Id = 1; Name = "Alice"; Score = 99.5; IsActive = true;  JoinDate = DateTime(2022, 1, 1) }
                { Id = 2; Name = "Bob";   Score = 88.0; IsActive = false; JoinDate = DateTime(2023, 5, 20) }
                { Id = 3; Name = "C";     Score = 60.0; IsActive = true;  JoinDate = DateTime(2024, 12, 31) }
            ]

            // MiniExcel 支持 IEnumerable，F# List 实现了它
            MiniExcel.SaveAs(tempFile, data) |> ignore
            // =================================================================
            // 2. 测试默认读取
            // =================================================================
            let testDefaultRead() =
                use df = DataFrame.ReadExcel tempFile
                
                Assert.Equal(3L, df.Rows)
                
                // 验证 F# Extension Methods 读取值
                // 注意：Excel 数字默认是 f64 (Double)
                // 验证 Id: 1 -> 1.0 (Polars 默认推断)
                Assert.Equal(1.0, df.Float("Id", 0).Value) 
                
                // 验证 String
                Assert.Equal("Alice", df.String("Name", 0).Value)
                
                // 验证 DateTime (Rust 解析 OADate -> Polars Datetime)
                Assert.Equal(DateTime(2022, 1, 1), df.DateTime("JoinDate", 0).Value)

            testDefaultRead()

            // =================================================================
            // 3. 测试 Schema Overrides (强类型控制)
            // =================================================================
            let testSchemaRead() =
                // 场景：我希望 Id 被读作 Int64，而不是默认的 Float64
                // 场景：我希望 Score 被读作 String
                use schema = PolarsSchema.ofList([
                    "Id", DataType.Int64
                    "Score", DataType.String
                ])
                use df = DataFrame.ReadExcel(tempFile, schema=schema)

                // 验证类型是否生效
                Assert.Equal(DataType.Int64, df.Schema.["Id"])
                Assert.Equal(DataType.String, df.Schema.["Score"])

                // 验证值转换逻辑 (Rust 层 match 逻辑)
                // Id: Float(1.0) -> Int64(1)
                Assert.Equal(1L, df.Int("Id", 0).Value)
                
                // Score: Float(99.5) -> String("99.5")
                Assert.Equal("99.5", df.String("Score", 0).Value)

            testSchemaRead()

        finally
            if File.Exists(tempFile) then File.Delete(tempFile)
    [<Fact>]
    member _.``IO: Excel Roundtrip (Precision & Formats)`` () =
        let tempFile = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString() + ".xlsx")
        
        try
            // 1. 构造极端数据
            // 使用 Series 构造器创建 DataFrame
            // F# 中 uint64 对应 System.UInt64 (Rust u64)
            // 这是一个远超 Excel 53-bit 精度的数
            let bigId = 18446744073709551615UL 
            
            use sId = Series.create("Id", [| 1 |])
            use sBigId = Series.create("BigId", [| bigId |])
            use sDate = Series.create("MyDate", [| DateTime(2023, 10, 1) |])
            
            use df = DataFrame.create [sId; sBigId; sDate]

            // 2. 写入 Excel (带格式)
            // 我们希望 BigId 被自动转为文本，MyDate 按照 dd-mm-yyyy 显示
            df.WriteExcel(
                tempFile, 
                sheetName="Data", 
                dateFormat="dd-mm-yyyy"
            )

            // 3. 读回 Excel (带 Schema 校验)
            // 强制 BigId 为 String，看看它是否保持了原样
            use schema = PolarsSchema.ofList ["BigId", DataType.String]

            use dfRead = DataFrame.ReadExcel(tempFile, schema=schema)

            // 4. 验证
            // BigId 应该是 String 类型，且内容完全一致 (没有被 Excel 截断为 ...1615 -> ...0000)
            Assert.Equal(DataType.String, dfRead.Schema.["BigId"])
            
            // 获取值 (注意：F# 获取 String Series 的值)
            let readBigIdStr = dfRead.String("BigId", 0).Value
            Assert.Equal(bigId.ToString(), readBigIdStr)

            // 验证日期值 (逻辑值应该是对的)
            let readDate = dfRead.DateTime("MyDate", 0).Value
            Assert.Equal(DateTime(2023, 10, 1), readDate)

        finally
            if File.Exists(tempFile) then File.Delete(tempFile)