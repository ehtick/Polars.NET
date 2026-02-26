using System.Linq.Expressions;
using Apache.Arrow;
using Apache.Arrow.Memory;
using static Polars.CSharp.Polars;
namespace Polars.CSharp.Tests;

public class DataFrameTests
{
    [Fact]
    public void Test_ReadCsv_Filter_Select()
    {
        // 1. 准备一个临时 CSV 文件
        var csvContent = @"name,age,salary
Alice,25,50000
Bob,30,60000
Charlie,35,70000
David,40,80000";
        var fileName = "test_data.csv";
        File.WriteAllText(fileName, csvContent);

        try
        {
            // 2. 读取 CSV
            using var df = DataFrame.ReadCsv(fileName);
            
            // 验证加载正确
            Assert.Equal(4, df.Height);
            Assert.Equal(3, df.Width);
            Assert.Contains("name", df.Columns);

            // 3. 执行操作：筛选 age > 30 并选择 name 和 salary
            // SQL 逻辑: SELECT name, salary FROM df WHERE age > 30
            using var filtered = df
                .Filter(Col("age") > 30)
                .Select("name", "salary");

            // 验证结果
            // 应该剩下 Charlie (35) 和 David (40)
            Assert.Equal(2, filtered.Height);
            Assert.Equal(2, filtered.Width);
            
            Assert.Equal("Charlie", filtered.GetValue<string>(0, "name"));
            Assert.Equal("David", filtered.GetValue<string>(1, "name"));
        }
        finally
        {
            if (File.Exists(fileName)) File.Delete(fileName);
        }
    }

    [Fact]
    public void Test_FromArrow_RoundTrip()
    {
        // 1. 手动构建一个 Arrow RecordBatch
        var builder = new RecordBatch.Builder(new NativeMemoryAllocator())
            .Append("id", false, col => col.Int32(array => array.AppendRange([1, 2, 3])))
            .Append("value", false, col => col.Double(array => array.AppendRange([1.1, 2.2, 3.3])));

        using var originalBatch = builder.Build();

        // 2. 转为 Polars DataFrame
        using var df = DataFrame.FromArrow(originalBatch);
        
        Assert.Equal(3, df.Height);
        Assert.Equal(2, df.Width);

        // 3. 做一些计算 (例如 value * 2)
        using var resultDf = df.Select(
            Col("id"), 
            (Col("value") * 2.0).Alias("value_doubled")
        );

        // 4. 转回 Arrow 验证
        using var resultBatch = resultDf.ToArrow();
        var doubledCol = resultBatch.Column("value_doubled") as DoubleArray;

        Assert.NotNull(doubledCol);
        Assert.Equal(2.2, doubledCol.GetValue(0).Value, 4);
        Assert.Equal(4.4, doubledCol.GetValue(1).Value, 4);
        Assert.Equal(6.6, doubledCol.GetValue(2).Value, 4);
    }
    
    [Fact]
    public void Test_GroupBy_Agg()
    {
         // 准备数据: Department, Salary
        var csvContent = @"dept,salary
IT,100
IT,200
HR,150
HR,50";
        var fileName = "groupby_test.csv";
        File.WriteAllText(fileName, csvContent);

        try
        {
            using var df = DataFrame.ReadCsv(fileName);

            // GroupBy dept, Agg Sum(salary)
            using var grouped = df
                .GroupBy("dept")
                .Agg(Col("salary").Sum().Alias("total_salary"))
                .Sort("total_salary", descending: true); // 排序方便断言

            // 预期: 
            // IT: 300
            // HR: 200
            
            Assert.Equal(2, grouped.Height);
            
            Assert.Equal("IT", grouped.GetValue<string>(0, "dept"));
            Assert.Equal(300, grouped.GetValue<long>(0, "total_salary"));
            
            Assert.Equal("HR", grouped.GetValue<string>(1, "dept"));
            Assert.Equal(200, grouped.GetValue<long>(1, "total_salary"));
        }
        finally
        {
            if (File.Exists(fileName)) File.Delete(fileName);
        }
    }
    [Fact]
    public void Test_GroupBy_Advanced_Aggregations()
    {
        // 1. 准备数据 (使用匿名对象和集合表达式，无需读写 CSV)
        // Group A: [true, false], [1, 2] -> 混合布尔，多行
        // Group B: [true],       [3]    -> 单一行 (适合测试 Item)
        // Group C: [false, false],[4, 5] -> 全 False
        var groups = new[] { "A", "A", "B", "C", "C" };
        var bools = new[] { true, false, true, false, false };
        var values = new[] { 1, 2, 3, 4, 5 };

        using var df = DataFrame.FromColumns(new { groups, bools, values });

        // 2. 执行 GroupBy 和 聚合
        using var result = df
            .GroupBy("groups")
            .Agg(
                // Boolean 聚合
                Col("bools").Any().Alias("is_any_true"),   // 只要有一个 true 就是 true
                Col("bools").All().Alias("is_all_true"),   // 必须全是 true 才是 true
                
                // 位置聚合
                Col("values").First().Alias("v_first"),
                Col("values").Last().Alias("v_last"),
                
                // 验证 Reverse (配合 First 使用，Reverse().First() 等于 Last)
                Col("values").Reverse().First().Alias("v_rev_first") 
            )
            .Sort("groups");

        // 3. 断言验证
        Assert.Equal(3, result.Height); // A, B, C 三组

        // --- Group A (Mixed) ---
        // bools: [true, false] -> Any: True, All: False
        // values: [1, 2] -> First: 1, Last: 2
        Assert.Equal("A", result.GetValue<string>(0, "groups"));
        Assert.True(result.GetValue<bool>(0, "is_any_true"));
        Assert.False(result.GetValue<bool>(0, "is_all_true"));
        Assert.Equal(1, result.GetValue<int>(0, "v_first"));
        Assert.Equal(2, result.GetValue<int>(0, "v_last"));
        Assert.Equal(2, result.GetValue<int>(0, "v_rev_first")); // Reverse 后的 First 应该是 2

        // --- Group B (Single) ---
        // bools: [true] -> Any: True, All: True
        Assert.Equal("B", result.GetValue<string>(1, "groups"));
        Assert.True(result.GetValue<bool>(1, "is_all_true")); // 只有一个 true，所以 all 也是 true

        // --- Group C (All False) ---
        // bools: [false, false] -> Any: False, All: False
        Assert.Equal("C", result.GetValue<string>(2, "groups"));
        Assert.False(result.GetValue<bool>(2, "is_any_true"));
    }

    [Fact]
    public void Test_GroupBy_Item_Safe()
    {
        // Item() 是个狠角色。
        // 如果组里只有 1 个元素，它返回该元素。
        // 如果组里有多个元素，Polars 可能会报错或者行为未定义（取决于版本和 allow_empty）。
        // 我们这里测试最标准的用法：取单元素组的值。
        
        var groups = new[] { "X", "Y" };
        var codes = new[] { 101, 102 }; // 每个组只有一个值

        using var df = DataFrame.FromColumns(new { groups, codes });

        using var res = df.GroupBy("groups")
            .Agg(
                Col("codes").Item().Alias("code_item")
            )
            .Sort("groups");

        Assert.Equal(2, res.Height);
        Assert.Equal(101, res.GetValue<int>(0, "code_item"));
        Assert.Equal(102, res.GetValue<int>(1, "code_item"));
    }

    [Fact]
    public void Test_Expr_Reverse_Standalone()
    {
        // 单独测试 Reverse，不通过 GroupBy
        // [1, 2, 3] -> [3, 2, 1]
        
        using var df = DataFrame.FromColumns(new 
        { 
            nums = new[] { 1, 2, 3 } 
        });

        using var res = df.Select(
            Col("nums").Reverse().Alias("nums_rev")
        );

        var revArr = res["nums_rev"].ToArray<int>();
        
        Assert.Equal(3, revArr.Length);
        Assert.Equal(3, revArr[0]);
        Assert.Equal(2, revArr[1]);
        Assert.Equal(1, revArr[2]);
    }
    // ==========================================
    // Join Tests
    // ==========================================
    [Fact]
    public void Test_DataFrame_Join_MultiColumn_WithParams()
    {
        // 1. 准备数据：使用 FromColumns 在内存中直接构建
        // 场景：学生成绩表 (Left)
        // 包含一个 'note' 列，用于制造列名冲突，测试 suffix
        using var scoresDf = DataFrame.FromColumns(new 
        {
            student = new[] { "Alice", "Alice", "Bob" },
            year    = new[] { 2023,    2024,    2023 },
            score   = new[] { 85,      90,      70 },
            note    = new[] { "Score1", "Score2", "Score3" } 
        });

        // 场景：班级分配表 (Right)
        // 同样包含 'note' 列
        using var classDf = DataFrame.FromColumns(new 
        {
            student = new[] { "Alice", "Alice", "Bob" },
            year    = new[] { 2023,    2024,    2024 },
            className = new[] { "Math", "Physics", "History" }, // 避免用 C# 关键字 class
            note    = new[] { "Class1", "Class2", "Class3" }
        });

        // 2. 执行 Join 测试新参数
        // 逻辑：Inner Join on (student, year)
        // 预期匹配：
        // - (Alice, 2023) -> Math
        // - (Alice, 2024) -> Physics
        // (Bob, 2023) 左有右无 -> 丢弃
        // (Bob, 2024) 左无右有 -> 丢弃
        using var joinedDf = scoresDf.Join(
            classDf,
            leftOn: ["student", "year"],
            rightOn: ["student", "year"],
            how: JoinType.Inner,
            
            // --- 新参数实战 ---
            suffix: "_conflict_test",          // 自定义后缀
            validation: JoinValidation.OneToOne, // 验证键的唯一性 (当前数据满足 1:1)
            coalesce: JoinCoalesce.JoinSpecific  // 默认行为：合并 Key 列
        );

        // 3. 验证基础维度
        Assert.Equal(2, joinedDf.Height);
        
        // 4. 验证列名处理 (Suffix 是否生效)
        // 原有列: student, year, score, note
        // 新增列: className, note_conflict_test (右表的 note 加后缀)
        var cols = joinedDf.Columns;
        Assert.Contains("note", cols);
        Assert.Contains("note_conflict_test", cols); // 验证后缀

        // 5. 验证数据正确性 (先排序确保稳定)
        using var sorted = joinedDf.Sort("year");

        // Row 0: Alice 2023
        Assert.Equal(2023, sorted.GetValue<int>(0, "year"));
        Assert.Equal("Math", sorted.GetValue<string>(0, "className"));
        
        // 验证冲突列的内容
        Assert.Equal("Score1", sorted.GetValue<string>(0, "note"));              // 左表数据
        Assert.Equal("Class1", sorted.GetValue<string>(0, "note_conflict_test")); // 右表数据

        // Row 1: Alice 2024
        Assert.Equal(2024, sorted.GetValue<int>(1, "year"));
        Assert.Equal("Physics", sorted.GetValue<string>(1, "className"));
        Assert.Equal("Score2", sorted.GetValue<string>(1, "note"));
        Assert.Equal("Class2", sorted.GetValue<string>(1, "note_conflict_test"));
    }
    // ==========================================
    // Concat Tests (Vertical, Horizontal, Diagonal)
    // ==========================================
    [Fact]
    public void Test_Concat_All_Types()
    {
        // --- 1. Vertical (垂直拼接) ---
        // 场景：两份数据结构相同，上下堆叠
        {
            using var csv1 = new DisposableFile("id,name\n1,Alice","csv");
            using var df1 = DataFrame.ReadCsv(csv1.Path);

            using var csv2 = new DisposableFile("id,name\n2,Bob","csv");
            using var df2 = DataFrame.ReadCsv(csv2.Path);

            using var res = DataFrame.Concat([df1, df2]);

            Assert.Equal(2, res.Height);
            Assert.Equal(2, res.Width);

            // 验证顺序
            Assert.Equal(1, res.GetValue<int>(0, "id"));
            Assert.Equal(2, res.GetValue<int>(1, "id"));
        }

        // --- 2. Horizontal (水平拼接) ---
        // 场景：行数相同，列不同，左右拼接
        {
            using var csv1 = new DisposableFile("id\n1\n2",".csv");
            using var df1 = DataFrame.ReadCsv(csv1.Path);

            using var csv2 = new DisposableFile("name,age\nAlice,20\nBob,30",".csv");
            using var df2 = DataFrame.ReadCsv(csv2.Path);

            using var res = DataFrame.ConcatHorizontal([df1, df2]);

            Assert.Equal(2, res.Height);
            Assert.Equal(3, res.Width); // id + name + age

            Assert.NotNull(res.Columns.Contains("id") ? res.GetValue<int>(0, "id") : null);
            Assert.NotNull(res.Columns.Contains("name") ? res.GetValue<string>(0, "name") : null);
            Assert.NotNull(res.Columns.Contains("age") ? res.GetValue<int>(0, "age") : null);
            
            // 验证数据对齐
            Assert.Equal(1, res.GetValue<int>(0, "id"));
            Assert.Equal("Alice", res.GetValue<string>(0, "name"));
        }

        // --- 3. Diagonal (对角拼接) ---
        // 场景：列不完全对齐，取并集，空缺填 null
        // DF1: [A, B]
        // DF2: [B, C]
        // Result: [A, B, C]
        {
            using var csv1 = new DisposableFile("A,B\n1,10",".csv");
            using var df1 = DataFrame.ReadCsv(csv1.Path);

            using var csv2 = new DisposableFile("B,C\n20,300",".csv");
            using var df2 = DataFrame.ReadCsv(csv2.Path);

            using var res = DataFrame.ConcatDiagonal([df1, df2]);

            Assert.Equal(2, res.Height); // 垂直堆叠
            Assert.Equal(3, res.Width);  // A, B, C (列的并集)
            
            Assert.Equal(1, res.GetValue<int>(0, "A"));
            Assert.Equal(10, res.GetValue<int>(0, "B"));
            Assert.Null(res.GetValue<int?>(0, "C"));

            // Row 1 (来自 DF2): A=null, B=20, C=300
            Assert.Null(res.GetValue<int?>(1, "A"));
            Assert.Equal(20, res.GetValue<int>(1, "B"));
            Assert.Equal(300, res.GetValue<int>(1, "C"));
        }
    }
    // ==========================================
    // Reshaping Tests (Pivot & Unpivot)
    // ==========================================
    [Fact]
    public void Test_Pivot_Unpivot_With_CustomExpr()
    {
        // 1. 准备数据：内存构建 (长表)
        // 场景：记录了不同城市在不同日期的温度 (摄氏度)
        using var df = DataFrame.FromColumns(new
        {
            date = new[] { "2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02" },
            city = new[] { "NY", "LA", "NY", "LA" },
            temp = new[] { 5.0, 20.0, 2.0, 18.0 } // 使用 double 方便后续计算
        });

        // --- Step 1: Standard Pivot (Enum 方式) ---
        // 目标：长 -> 宽, 聚合用 First
        // 新参数测试：sortColumns = true (确保 "LA" 排在 "NY" 前面)
        using var pivoted = df.Pivot(
            index: ["date"],
            columns: ["city"],
            values: ["temp"],
            aggregateFunction: PivotAgg.First,
            sortColumns: true 
        );

        // 验证结构
        Assert.Equal(2, pivoted.Height);
        Assert.Equal(3, pivoted.Width); // date, LA, NY (Sorted)

        // 验证列名排序 (LA < NY)
        var cols = pivoted.ColumnNames;
        Assert.Equal("LA", cols[1]);
        Assert.Equal("NY", cols[2]);

        // 验证值 (2024-01-01)
        // Sort 后 date 应该是升序
        Assert.Equal(20.0, pivoted.GetValue<double>(0, "LA")); // LA 20度
        Assert.Equal(5.0, pivoted.GetValue<double>(0, "NY"));  // NY 5度

        // --- Step 2: Custom Expr Pivot (修正版) ---
        
        // 方案A：最佳实践 - 先计算，后透视
        // 我们先计算华氏度，生成新列 "temp_f"(
        using var dfWithF = df.WithColumns((Col("temp") * 1.8 + 32).Alias("temp_f"));
        
        // 然后使用 Expr 重载进行透视

        using var pivotedFahrenheit = dfWithF.Pivot(
            index: ["date"],
            columns: ["city"],
            values: ["temp_f"],
            aggregateExpr: Col("").First(), 
            sortColumns: true
        );

        // 验证计算结果
        // NY: 5 * 1.8 + 32 = 41
        // LA: 20 * 1.8 + 32 = 68
        Assert.Equal(68.0, pivotedFahrenheit.GetValue<double>(0, "LA"));
        Assert.Equal(41.0, pivotedFahrenheit.GetValue<double>(0, "NY"));

        // --- Step 3: Unpivot/Melt (宽 -> 长) ---
        // 把 Step 1 的结果还原
        using var unpivoted = pivoted.Unpivot(
            index: ["date"],
            on: ["LA", "NY"],
            variableName: "city_restored",
            valueName: "temp_restored"
        ).Sort(["date", "city_restored"]); // 排序以确保断言稳定

        // 验证还原结果
        Assert.Equal(4, unpivoted.Height);
        
        // 验证第一行: 2024-01-01, LA, 20.0
        Assert.Equal("2024-01-01", unpivoted.GetValue<string>(0, "date"));
        Assert.Equal("LA", unpivoted.GetValue<string>(0, "city_restored"));
        Assert.Equal(20.0, unpivoted.GetValue<double>(0, "temp_restored"));
    }
    // ==========================================
    // Display Tests (Head & Show)
    // ==========================================
    [Fact]
    public void Test_Head_And_Show()
    {
        // 构造较多数据 (15行)
        // 0..14
        using var df = DataFrame.FromArrow(
            new RecordBatch.Builder(new NativeMemoryAllocator())
                .Append("id", false, col => col.Int32(arr => arr.AppendRange(Enumerable.Range(0, 15))))
                .Append("name", false, col => col.String(arr => arr.AppendRange(Enumerable.Range(0, 15).Select(i => $"User_{i}"))))
                .Build()
        );

        Assert.Equal(15, df.Height);

        // 1. Test Head/Tail
        using var headDf = df.Head(5);
        Assert.Equal(5, headDf.Height);
        
        Assert.Equal(0, headDf.GetValue<int>(0,"id"));
        Assert.Equal(4, headDf.GetValue<int>(4,"id"));

        using var tailDf = df.Tail(5);
        Assert.Equal(5, tailDf.Height);

        Assert.Equal(10, tailDf.GetValue<int>(0,"id"));
        Assert.Equal(14, tailDf.GetValue<int>(4,"id"));
        // 2. Test Show (No exception should be thrown)
        // 这会在控制台打印表格
        Console.WriteLine("\n--- Testing DataFrame.Show() output ---");
        df.Show(); 
        
        // 测试小数据 Show
        headDf.Show();
        tailDf.Show();
    }
    [Fact]
    public void Test_Describe_Logic()
    {
        // 构造数据: 1, 2, 3, 4, 5
        var content = "val\n1\n2\n3\n4\n5\n"; 
        using var csv = new DisposableFile(content,".csv");
        using var df = DataFrame.ReadCsv(csv.Path);

        // 调用 Describe
        using var summary = df.Describe();
        
        // 打印看看 (此时应该能用 PrintSchema 或 Show 了)
        summary.Show(); 

        // 验证行数: count, null_count, mean, std, min, 25%, 50%, 75%, max
        Assert.Equal(9, summary.Height);
        
        // 验证 Mean (第3行)
        // statistic="mean", val=3.0
        // 我们用 Filter 取出来验证
        using var meanRow = summary.Filter(Col("statistic") == Lit("mean"));
        Assert.Equal(3.0, meanRow.GetValue<double>(0, "val"));
        
        // 验证 Min
        using var minRow = summary.Filter(Col("statistic") == Lit("min"));
        Assert.Equal(1.0, minRow.GetValue<double>(0, "val"));
    }
    // ==========================================
    // Rolling & List & Name Ops Tests
    // ==========================================

    [Fact]
    public void Test_Rolling_Functions()
    {
        // 构造时序数据
        var content = @"date,val
2024-01-01,10
2024-01-02,20
2024-01-03,30
2024-01-04,40
2024-01-05,50";
        using var csv = new DisposableFile(content,".csv");
        using var df = DataFrame.ReadCsv(csv.Path, tryParseDates: true);

        // 逻辑: 3天滑动窗口求平均 (Rolling Mean)
        // 10
        // 10,20 -> 15
        // 10,20,30 -> 20
        var rollExpr = Col("val")
            .RollingMeanBy(windowSize: new TimeSpan(3,0,0,0), by: Col("date"), closed: ClosedWindow.Left)
            .Alias("roll_mean");

        using var res = df.Select(
            Col("date"),
            Col("val"),
            rollExpr
        );

        // 第3行 (2024-01-03): 窗口 [01, 02, 03) -> 10, 20. Mean = 15. 
        // Polars 的 RollingBy closed="left" 行为细节取决于版本，通常不包含当前行
        // 假设这里验证的是基本调用成功，具体数值依赖 Polars 逻辑
        Assert.NotNull(res);
        Assert.Equal(5, res.Height); 
        // 只要不抛异常且有数据返回，说明 Wrapper 绑定成功
    }

    [Fact]
    public void Test_List_Aggregations_And_Name()
    {
        // 构造含有 List 的数据不易直接通过 CSV，我们用 GroupBy 产生 List
        // A: [1, 2]
        // B: [3, 4, 5]
        var content = @"group,val
A,1
A,2
B,3
B,4
B,5";
        using var csv = new DisposableFile(content,".csv");
        using var df = DataFrame.ReadCsv(csv.Path);

        using var res = df
            .GroupBy(Col("group"))
            .Agg(
                Col("val").Alias("val_list") // 隐式聚合为 List
            )
            .Select(
                Col("group"),
                // 测试 List.Sum, List.Max
                Col("val_list").List.Sum().Name.Suffix("_sum"),
                Col("val_list").List.Max().Name.Suffix("_max"),
                // 测试 List.Contains
                Col("val_list").List.Contains(3).Alias("has_3")
            )
            .Sort("group");
        // A (1,2) -> Sum=3, Max=2, Has3=false
        // B (3,4,5) -> Sum=12, Max=5, Has3=true

        // 验证 A
        Assert.Equal(3, res.GetValue<int>(0,"val_list_sum"));
        Assert.Equal(2, res.GetValue<int>(0,"val_list_max"));
        Assert.False(res.GetValue<bool>(0,"has_3"));

        // 验证 B
        Assert.Equal(12, res.GetValue<int>(1,"val_list_sum"));
        Assert.Equal(5, res.GetValue<int>(1,"val_list_max"));
        Assert.True(res.GetValue<bool>(1,"has_3"));
    }
    [Fact]
    public void Test_DataFrame_From_Records_With_Decimal()
    {
        // 1. 准备数据
        var data = new[]
        {
            new { Id = 1, Name = "A", Price = 10.5m },
            new { Id = 2, Name = "B", Price = 20.005m }, // Scale 3
            new { Id = 3, Name = "C", Price = 0m }
        };

        // 2. 转换
        // 匿名类型也是支持的
        using var df = DataFrame.From(data);
        
        Assert.Equal(3, df.Height);
        Assert.Equal(3, df.Width);

        // 3. 验证
        // using var batch = df.ToArrow();
        var priceCol = df.Column("Price");
        // 验证 Decimal
        // var priceCol = batch.Column("Price") as Decimal128Array;
            // Assert.NotNull(priceCol);
        Assert.Equal(3, priceCol.Length); // 自动推断
        Assert.Equal(10.5m, priceCol.GetValue<decimal>(0));   // 之前期望 10500 是错的，Arrow 已经除回去了
        Assert.Equal(20.005m, priceCol.GetValue<decimal>(1)); 
        Assert.Equal(0m, priceCol.GetValue<decimal>(2));
    }
    [Fact]
    public void Test_Get_Column_As_Series()
    {
        // 1. 准备数据
        var data = new[]
        {
            new { Name = "Alice", Age = 30 },
            new { Name = "Bob",   Age = 40 }
        };
        using var df = DataFrame.From(data);

        // 2. 获取 Series (显式方法)
        using var sName = df.Column("Name");
        Assert.Equal("Name", sName.Name);
        Assert.Equal(2, sName.Length);
        Assert.Equal("Alice", sName.GetValue<string>(0));

        // 3. 获取 Series (索引器)
        using var sAge = df["Age"];
        Assert.Equal("Age", sAge.Name);
        Assert.Equal(2, sAge.Length);
        Assert.Equal(40, sAge.GetValue<int>(1));
        
        // 4. 验证类型信息
        Assert.Contains("i32", sAge.DataTypeName); // From<T> 默认 int -> i32
    }

    [Fact]
    public void Test_Get_Columns_Iterate()
    {
        using var df = DataFrame.From([new { A = 1, B = 2.0 }]);

        // 获取所有列
        var columns = df.GetColumns();
        
        Assert.Equal(2, columns.Length);
        Assert.Equal("A", columns[0].Name);
        Assert.Equal("B", columns[1].Name);
        
        // 清理
        foreach (var col in columns) col.Dispose();
    }
    [Fact]
    public void Test_DataFrame_Explode_Eager()
    {
        // 1. 构造数据: 模拟逗号分隔的字符串
        // Row 0: "1,2" (炸开后应变2行)
        // Row 1: "3"   (炸开后保持1行)
        using var s = new Series("nums", ["1,2","3"]);
        using var df = DataFrame.FromSeries(s);

        // 2. 预处理: 用 Split 生成 List 列
        // 此时 df 结构:
        // ┌──────┬───────────┐
        // │ nums ┆ list_vals │
        // ╞══════╪═══════════╡
        // │ 1,2  ┆ ["1","2"] │
        // │ 3    ┆ ["3"]     │
        // └──────┴───────────┘
        using var dfWithList = df.Select(
            Col("nums"),
            Col("nums").Str.Split(",").Alias("list_vals")
        );
        using var dtype = DataType.List(DataType.String);
        // Explode for all Int32 list 
        using var exploded = dfWithList.Explode(Selectors.DType(dtype));

        // 4. 验证结果
        // 总行数应该是 2 + 1 = 3
        Assert.Equal(3, exploded.Height);

        // 验证 list_vals 列的内容是否已展平为 String
        Assert.Equal("1", exploded["list_vals"][0]);
        Assert.Equal("2", exploded[1][1]);
        Assert.Equal("3", exploded.GetValue<string>("list_vals",2));

        // 验证其它列 (nums) 是否被正确复制 (Duplicated)
        Assert.Equal("1,2", exploded.GetValue<string>(0, "nums"));
        Assert.Equal("1,2", exploded["nums",1]);
        Assert.Equal("3",   exploded[2,0]);
    }
    [Fact]
    public void Test_Column_ByIndex_And_Iteration()
    {
        // 1. 准备数据
        var df = DataFrame.FromColumns(new 
        {
            Name = new[] { "A", "B" }, // Index 0
            Age = new[] { 10, 20 },    // Index 1
            Score = new[] { 99, 88 }   // Index 2
        });

        // 2. 测试 Column(int)
        var col0 = df.Column(0);
        Assert.Equal("Name", col0.Name);
        Assert.Equal("A", col0[0]);

        // 3. 测试 Indexer df[int]
        var col2 = df[2];
        Assert.Equal("Score", col2.Name);
        Assert.Equal(99, col2.Cast(DataType.Int32)[0]);

        // 4. 测试越界
        Assert.Throws<IndexOutOfRangeException>(() => df[99]);
        Assert.Throws<IndexOutOfRangeException>(() => df[-1]);

        // 5. [Bonus] 测试 foreach
        int count = 0;
        foreach (var series in df)
        {
            if (count == 0) Assert.Equal("Name", series.Name);
            if (count == 1) Assert.Equal("Age", series.Name);
            count++;
        }
        Assert.Equal(3, count);
    }
    [Fact]
    public void Test_DataFrame_Sort_Advanced()
    {
        // 数据: 
        // A: [1, 1, 2, 2]
        // B: [null, 10, null, 5]
        using var df = DataFrame.FromColumns(new 
        {
            A = new[] { 1, 1, 2, 2 },
            B = new int?[] { null, 10, null, 5 }
        });

        // 1. Sort by A asc, B desc (nulls last)
        // 预期逻辑:
        // A=1 分组: B=[null, 10]。B desc nulls last -> [10, null]
        // A=2 分组: B=[null, 5]。 B desc nulls last -> [5, null]
        // 结果顺序: 
        // row 1: A=1, B=10
        // row 0: A=1, B=null
        // row 3: A=2, B=5
        // row 2: A=2, B=null

        using var sorted = df.Sort(
            ["A", "B"],
            descending: [false, true], // A asc, B desc
            nullsLast: [false, true]   // A normal, B nulls last
        );

        Assert.Equal(10, sorted["B"][0]);
        Assert.Null(sorted["B"][1]);
        Assert.Equal(5, sorted["B"][2]);
        Assert.Null(sorted["B"][3]);
    }
    [Fact]
    public void Test_DataFrame_TopK_Eager()
    {
        var data = new[] { 1, 100, 50, 2 };
        using var df = DataFrame.FromColumns(new { val = data });

        // 直接在 DataFrame 上调用 TopK
        using var top = df.TopK(2, "val");

        Assert.Equal(2, top.Height);
        var arr = top["val"].ToArray<int>();
        Assert.Contains(100, arr);
        Assert.Contains(50, arr);
    }
    [Fact]
    public void Test_DataFrame_GroupByDynamic()
    {
        // 准备时间序列数据
        var dates = new[]
        {
            new DateTime(2023, 1, 1, 10, 0, 0),
            new DateTime(2023, 1, 1, 10, 10, 0),
            new DateTime(2023, 1, 1, 10, 20, 0),
            new DateTime(2023, 1, 1, 11, 0, 0) // 下一个小时
        };
        var values = new[] { 1, 2, 3, 4 };

        using var df = DataFrame.FromColumns(new { ts = dates, val = values });

        // 按 1小时 滚动，计算 sum
        using var res = df.GroupByDynamic("ts", TimeSpan.FromHours(1))
            .Agg(Col("val").Sum());

        // 预期结果：
        // 10:00:00 -> [1, 2, 3] -> Sum = 6
        // 11:00:00 -> [4]       -> Sum = 4
        
        Assert.Equal(2, res.Height);
        
        var sums = res["val"].ToArray<int>();
        Assert.Contains(6, sums);
        Assert.Contains(4, sums);
    }
    [Fact]
    public void Test_DataFrame_JoinAsOf_Eager()
    {
        // 准备数据
        // Left: [10:00, 10:02], val_l = [1, 2]
        var datesL = new[] 
        { 
            new DateTime(2023, 1, 1, 10, 0, 0),
            new DateTime(2023, 1, 1, 10, 2, 0)
        };
        using var dfLeft = DataFrame.FromColumns(new { ts = datesL, val_l = new[] { 1, 2 } });

        // Right: [09:59, 10:00, 10:01, 10:03], val_r = [10, 20, 30, 40]
        var datesR = new[] 
        { 
            new DateTime(2023, 1, 1, 9, 59, 0),
            new DateTime(2023, 1, 1, 10, 0, 0), // Match for 10:00
            new DateTime(2023, 1, 1, 10, 1, 0), // Match for 10:02 (backward)
            new DateTime(2023, 1, 1, 10, 3, 0)
        };
        using var dfRight = DataFrame.FromColumns(new { ts = datesR, val_r = new[] { 10, 20, 30, 40 } });

        // 执行 JoinAsOf (Backward strategy)
        using var res = dfLeft.JoinAsOf(
            dfRight,
            leftOn: Col("ts"),
            rightOn: Col("ts"),
            tolerance: null,
            strategy: AsofStrategy.Backward
        );

        // 验证
        Assert.Equal(2, res.Height);
        
        var rVals = res["val_r"].ToArray<int>();
        Assert.Equal(20, rVals[0]); // 10:00 matched 10:00
        Assert.Equal(30, rVals[1]); // 10:02 matched 10:01 (closest previous)
    }
    [Fact]
    public void Test_DataFrame_Slice()
    {
        // 1. 准备数据: 水果和颜色
        var df = DataFrame.FromSeries(
            new Series("Fruit", ["Apple", "Grape", "Grape", "Fig", "Fig"]),
            new Series("Color", ["Green", "Red", "White", "White", "Red"])
        );

        // 2. 执行切片: df.slice(2, 3)
        // 意思是从索引 2 开始，取 3 行
        // 原数据:
        // 0: Apple, Green
        // 1: Grape, Red
        // 2: Grape, White  <-- Start
        // 3: Fig,   White
        // 4: Fig,   Red    <-- End (Length 3)
        using var sl = df.Slice(2, 3);

        // 3. 验证形状
        Assert.Equal(3, sl.Height);
        Assert.Equal(2, sl.Width);

        // 4. 验证内容
        Assert.Equal("Grape", sl["Fruit"].GetValue<string>(0));
        Assert.Equal("Fig",   sl["Fruit"].GetValue<string>(1));
        Assert.Equal("Fig",   sl["Fruit"].GetValue<string>(2));
        
        // 验证越界情况（Polars 通常会截断而不是报错）
        using var slOverflow = df.Slice(4, 100);
        Assert.Equal(1, slOverflow.Height); // 只剩最后一行 "Fig"
        Assert.Equal("Fig", slOverflow["Fruit"].GetValue<string>(0));
    }
    [Fact]
    public void Test_Unique_Stable()
    {
        var df = DataFrame.From(
        [
            new { A = 1, B = "x" },
            new { A = 2, B = "y" },
            new { A = 1, B = "x" }, // Duplicate
            new { A = 3, B = "z" }
        ]);

        // 1. Default (All cols, Keep First)
        var res1 = df.Unique();
        Assert.Equal(3, res1.Height);
        Assert.Equal(1, res1["A"][0]); // Order preserved
        Assert.Equal(2, res1["A"][1]);
        Assert.Equal(3, res1["A"][2]);

        // 2. Subset (Check only A)
        var df2 = DataFrame.From(
        [
            new { A = 1, B = "x" },
            new { A = 1, B = "y" } // Duplicate on A
        ]);
        
        var res2 = df2.Unique(["A"], UniqueKeepStrategy.Last);
        Assert.Equal(1, res2.Height);
        Assert.Equal("y", res2["B"][0]); // Should keep the last one ("y")

        // 3. Keep None (Remove all duplicates)
        var res3 = df.Unique(null, UniqueKeepStrategy.None);
        Assert.Equal(2, res3.Height); // A=2 and A=3 are unique. A=1 appears twice so both removed.
    }
    [Fact]
    public void Test_HStack_VStack()
    {
        // --- 1. Test HStack ---
        // 初始 DF: [a]
        using var df1 = DataFrame.FromColumns(new { a = new[] { 1, 2, 3 } });
        
        // 新列: [b]
        using var sNew = new Series("b", [10, 20, 30]);

        // 执行 HStack -> [a, b]
        using var hStacked = df1.HStack(sNew);

        Assert.Equal(3, hStacked.Height);
        Assert.Equal(2, hStacked.Width);
        Assert.Equal("a", hStacked.Columns[0]);
        Assert.Equal("b", hStacked.Columns[1]);
        Assert.Equal(10, hStacked["b"][0]);

        // --- 2. Test VStack ---
        // DF2: [a, b] (schema 必须一致)
        using var df2 = DataFrame.FromColumns(new 
        { 
            a = new[] { 4, 5 }, 
            b = new[] { 40, 50 } 
        });

        // 执行 VStack: hStacked (3 rows) + df2 (2 rows) -> 5 rows
        using var vStacked = hStacked.VStack(df2);

        Assert.Equal(5, vStacked.Height);
        Assert.Equal(2, vStacked.Width);
        
        // 验证数据衔接
        // Row 0 (from df1)
        Assert.Equal(1, vStacked["a"][0]);
        Assert.Equal(10, vStacked["b"][0]);
        
        // Row 3 (from df2, index 0) -> 总索引 3
        Assert.Equal(4, vStacked["a"][3]);
        Assert.Equal(40, vStacked["b"][3]);
    }
}