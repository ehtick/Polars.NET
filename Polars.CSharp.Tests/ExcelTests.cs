#nullable enable
using MiniExcelLibs;

namespace Polars.CSharp.Tests;

public class ExcelTests
{
    [Fact]
    public void Test_ReadExcel_RoundTrip_With_MiniExcel()
    {
        // ---------------------------------------------------
        // 1. 准备数据 (出题)
        // ---------------------------------------------------
        var tempFile = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid()}.xlsx");

        var data = new[]
        {
            // 混合数据类型，测试推断能力
            new { Id = 1, Name = "Alice", JoinDate = new DateTime(2022, 1, 1), Score = 99.5, IsActive = true,  Phone = 13800138000 },
            new { Id = 2, Name = "Bob",   JoinDate = new DateTime(2023, 5, 20),Score = 88.0, IsActive = false, Phone = 13900139000 },
            new { Id = 3, Name = "Charlie",JoinDate= new DateTime(2024, 12, 31),Score= 60.0, IsActive = true,  Phone = 13700137000 },
        };

        try
        {
            // 用 MiniExcel 生成标准的 .xlsx 文件
            MiniExcel.SaveAs(tempFile, data);

            // =================================================================
            // 2. 测试默认读取 (自然推断)
            // =================================================================
            {
                using var df = DataFrame.ReadExcel(tempFile);
                // df.Show();

                Assert.Equal(3, df.Height);
                
                // 验证日期 (Rust OADate -> Polars Datetime)
                // 注意：Polars 默认 Datetime 是微秒级，C# DateTime 也能对应上
                var dateVal = df.GetValue<DateTime>(0, "JoinDate");
                Assert.Equal(new DateTime(2022, 1, 1), dateVal);

                // 验证数值
                Assert.Equal(99.5, df.GetValue<double>(0, "Score"));
                
                // 验证布尔
                Assert.True(df.GetValue<bool>(0, "IsActive"));
                
                // 验证 Phone (MiniExcel 默认写入为数字，Polars 自然推断为 Int64)
                Assert.Equal(13800138000, df.GetValue<double>(0, "Phone"));
            }

            // =================================================================
            // 3. 测试 Schema Overrides (强转测试)
            // =================================================================
            // 场景：电话号码在 Excel 里存的是数字，但我们需要它作为 String 读入
            {
                // 定义 Schema：强制 Phone 列为 String
                using var schema = new PolarsSchema();
                schema.Add("Phone", DataType.String); 

                using var df = DataFrame.ReadExcel(tempFile, schema: schema);

                // 验证 Phone 现在是不是 String 类型
                Assert.Equal(DataType.String, df.Schema["Phone"]);
                
                // 验证内容是否正确转成了字符串
                Assert.Equal("13800138000", df.GetValue<string>(0, "Phone"));
                
                // 验证其他未指定的列是否依然正常推断 (Score 应该是 Float64)
                Assert.Equal(DataType.Float64, df.Schema["Score"]);
            }
        }
        finally
        {
            if (File.Exists(tempFile)) File.Delete(tempFile);
        }
    }

    [Fact]
    public void Test_ReadExcel_EmptyRows_And_Nulls()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid()}.xlsx");

        // 构造带空行和 Null 的数据
        // MiniExcel 写 null 会产生空单元格
        var data = new[]
        {
            new { Col1 = (string?)"A", Col2 = (int?)1 },
            new { Col1 = (string?)null, Col2 = (int?)null },
            new { Col1 = (string?)"C", Col2 = (int?)3 },
        };

        try
        {
            MiniExcel.SaveAs(tempFile, data);

            // 测试 dropEmptyRows = true (默认)
            using var dfDropped = DataFrame.ReadExcel(tempFile, dropEmptyRows: true);
            Assert.Equal(2, dfDropped.Height); // 中间那行应该没了
            Assert.Equal("A", dfDropped.GetValue<string>(0, "Col1"));
            Assert.Equal("C", dfDropped.GetValue<string>(1, "Col1"));

            // 测试 dropEmptyRows = false
            using var dfKept = DataFrame.ReadExcel(tempFile, dropEmptyRows: false);
            Assert.Equal(3, dfKept.Height);
            Assert.Null(dfKept.GetValue<string>(1, "Col1")); // 第二行存在，且为 Null
        }
        finally
        {
            if (File.Exists(tempFile)) File.Delete(tempFile);
        }
    }
    [Fact]
    public void Test_WriteExcel_Precision_And_Formats()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid()}.xlsx");

        try
        {
            // 1. 构造极其刁钻的数据
            // UInt64.MaxValue 是 18446744073709551615，远超 Excel 精度
            // Date 和 Datetime 用于测试格式化
            using var df = DataFrame.From(
            [
                new 
                { 
                    Id = 1, 
                    BigId = ulong.MaxValue, // 极端大数
                    JoinDate = new DateTime(2023, 10, 1),
                    LogTime = new DateTime(2023, 10, 1, 14, 30, 0)
                }
            ]);

            // 2. 写入 Excel (自定义格式)
            df.WriteExcel(
                tempFile, 
                sheetName: "Data", 
                dateFormat: "dd/mm/yyyy",          // 英国格式
                datetimeFormat: "hh:mm AM/PM"      // 12小时制
            );

            // 3. 验证 (这里我们“回读”来验证)
            // 注意：回读时，BigId 应该是 String 类型，因为我们在 Write 时强转了
            using var dfRead = DataFrame.ReadExcel(
                tempFile, 
                schema: new PolarsSchema().Add("BigId", DataType.String) // 强制按 String 读回以验证内容
            );

            // 验证大数精度：如果转成了数字，末尾肯定不是 ...1615
            // 如果转成了 String，应该完全一致
            var bigIdStr = dfRead.GetValue<string>(0, "BigId");
            Assert.Equal(ulong.MaxValue.ToString(), bigIdStr);

            // 验证日期值 (格式化只影响显示，ReadExcel 读回来的应该是正确的 DateTime 值)
            var joinDate = dfRead.GetValue<DateTime>(0, "JoinDate");
            Assert.Equal(new DateTime(2023, 10, 1), joinDate);
        }
        finally
        {
            if (File.Exists(tempFile)) File.Delete(tempFile);
        }
    }
}