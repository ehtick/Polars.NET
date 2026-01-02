namespace Polars.FSharp.Tests

open System
open Xunit
open Polars.FSharp

type ``Expression Logic Tests`` () =
    [<Fact>]
        member _.``Select inline style (Pythonic)`` () =
            use csv = new TempCsv "name,birthdate,weight,height\nQinglei,2025-11-25,70,1.80"
            let df = DataFrame.ReadCsv csv.Path

            // 像 Python 一样写在 list 里面！
            let res = 
                df
                |> pl.select [
                    col "name"
                    
                    // Inline 1: 简单的 alias
                    col "birthdate" |> alias "b_date"
                    
                    // Inline 2: 链式调用
                    (col "birthdate").Dt.Year().Alias "year"
                    
                    // Inline 3: 算术表达式
                    col "weight" / (col "height" * col "height")
                    |> alias "bmi"
                ]

            // 验证
            Assert.Equal(4L, res.Width) // name, b_date, year, bmi
            
            // 使用新的 Option 取值 API 验证
            // Qinglei
            Assert.Equal("Qinglei", res.String("name", 0).Value) 
            // BMI ≈ 21.6
            Assert.True(res.Float("bmi", 0).Value > 21.6)
    [<Fact>]
    member _.``Filter by numeric value (> operator)`` () =
        use csv = new TempCsv "val\n10\n20\n30"
        let df = DataFrame.ReadCsv csv.Path
        
        let res = df |> pl.filter (col "val" .> lit 15)
        
        Assert.Equal(2L, res.Rows)
    [<Fact>]
    member _.``Filter by numeric value (< operator)`` () =
        use csv = new TempCsv "name,birthdate,weight,height\nBen Brown,1985-02-15,72.5,1.77\nQinglei,2025-11-25,70.0,1.80\nZhang,2025-10-31,55,1.75"
        let df = DataFrame.ReadCsv (path=csv.Path,tryParseDates=true)

        let res = df |> pl.filter ((col "birthdate").Dt.Year() .< lit 1990 )

        Assert.Equal(1L,res.Rows)

    [<Fact>]
    member _.``Filter by string value (== operator)`` () =
        use csv = new TempCsv "name\nAlice\nBob\nAlice"
        let df = DataFrame.ReadCsv csv.Path
        
        // SRTP 魔法测试
        let res = df |> pl.filter (pl.col "name" .== pl.lit "Alice")
        
        Assert.Equal(2L, res.Rows)

    [<Fact>]
    member _.``Filter by double value (== operator)`` () =
        use csv = new TempCsv "value\n3.36\n4.2\n5\n3.36"
        let df = DataFrame.ReadCsv csv.Path
        
        // SRTP 魔法测试
        let res = df |> pl.filter (col "value" .== lit 3.36)
        
        Assert.Equal(2L, res.Rows)

    [<Fact>]
    member _.``Null handling works`` () =
        // 造一个带 null 的 CSV
        // age: 10, null, 30
        use csv = new TempCsv "age\n10\n\n30" 
        let lf = LazyFrame.ScanCsv csv.Path

        // 测试 1: fill_null
        // 把 null 填成 0，然后筛选 age > 0
        // 结果应该是 3 行 (10, 0, 30)
        let res = 
            lf 
            |> pl.withColumnLazy (
                col "age" 
                |> pl.fillNull (pl.lit 0) 
                |> pl.alias "age_filled"
            )
            |> pl.filterLazy (col "age_filled" .>= lit 0)
            |> pl.collect
        Assert.Equal(3L, res.Rows)
        
        // 测试 2: is_null
        // 筛选出 null 的行
        let df= DataFrame.ReadCsv csv.Path 
        let nulls = df |> pl.filter (pl.col "age" |> pl.isNull)
        Assert.Equal(1L, nulls.Rows)
    [<Fact>]
    member _.``IsBetween with DateTime Literals`` () =
        // 构造数据: Qinglei 的生日
        use csv = new TempCsv "name,birthdate,height\nQinglei,1990-05-20,1.80\nTooOld,1980-01-01,1.80\nTooShort,1990-05-20,1.60"
        
        // 必须开启日期解析
        let df = DataFrame.ReadCsv (path=csv.Path,tryParseDates=true)

        // Python logic translation:
        // filter(
        //    col("birthdate").is_between(date(1982,12,31), date(1996,1,1)),
        //    col("height") > 1.7
        // )
        
        // 定义边界
        let startDt = DateTime(1982, 12, 31)
        let endDt = DateTime(1996, 1, 1)

        let res = 
            df 
            |> pl.filter (
                // 条件 1: 生日区间
                (pl.col "birthdate").IsBetween(pl.lit startDt, pl.lit endDt)
                .&& // 条件 2: AND
                // 条件 3: 身高
                (pl.col "height" .> pl.lit 1.7)
            )

        // 验证: 只有 Qinglei 符合
        Assert.Equal(1L, res.Rows)
        Assert.Equal("Qinglei", res.String("name", 0).Value)
    [<Fact>]
    member _.``Expr: DateTime Ops (Truncate, Offset, Timestamp)`` () =
        // 数据: ["2023-01-01 10:15:00", "2023-01-01 10:45:00"]
        let s = Series.create("ts", ["2023-01-01 10:15:00"; "2023-01-01 10:45:00"])
        // 先解析成 Datetime
        use df_origin = DataFrame.create [s]
        let df =
            df_origin 
            |> pl.select([
            pl.col("ts").Str.ToDatetime("%Y-%m-%d %H:%M:%S").Alias "ts"
            ])

        let res = 
            df
            |> pl.select([
                pl.col "ts"

                // 1. Truncate to 1 hour (10:15 -> 10:00)
                pl.col("ts").Dt.Truncate("1h").Alias "truncated"

                // 2. Round to 1 hour (10:45 -> 11:00)
                pl.col("ts").Dt.Round("1h").Alias "rounded"

                // 3. Offset by 30m (10:15 -> 10:45)
                pl.col("ts").Dt.OffsetBy("30m").Alias "offset"

                // 4. Timestamp (Micros)
                pl.col("ts").Dt.TimestampMicros().Alias "micros"
            ])
            |> pl.show
        // 验证 Row 0: 10:15
        // let row0 = res.Row(0) // 假设你以后会实现 Row 访问，或者用 .Date(..).Value
        // 这里用原来的列式访问
        
        // Truncate: 10:15 -> 10:00
        let t0 = res.Datetime("truncated", 0).Value
        Assert.Equal(10, t0.Hour)
        Assert.Equal(0, t0.Minute)

        // Round: 10:45 (Row 1) -> 11:00
        let r1 = res.Datetime("rounded", 1).Value
        Assert.Equal(11, r1.Hour)
        Assert.Equal(0, r1.Minute)

        // Offset: 10:15 -> 10:45
        let o0 = res.Datetime("offset", 0).Value
        Assert.Equal(10, o0.Hour)
        Assert.Equal(45, o0.Minute)
        
        // Timestamp should be > 0
        Assert.True(res.Int("micros", 0).Value > 0L)

type ``String Logic Tests`` () =

    [<Fact>]
    member _.``Expr: String Cleaning & Parsing (Strip, Anchor, Date)`` () =
        // 1. 准备测试数据，覆盖多种场景
        // Row 0: 空格脏数据 -> 测试 Strip
        // Row 1: URL -> 测试 Prefix/StartsWith
        // Row 2: 文件名 -> 测试 Suffix/EndsWith
        // Row 3: 自定义字符脏数据 -> 测试 Strip(matches)
        // Row 4: 标准日期 -> 测试 ToDate
        // Row 5: 脏日期 -> 测试 链式调用 Strip().ToDate()
        let s = Series.create("raw", [
            "  abc  "           // 0
            "https://pl.rs" // 1
            "data.csv"          // 2
            "__key__"           // 3
            "20250101"          // 4
            "  2025-12-31  "    // 5
        ])
        
        use df = DataFrame.create [s]

        let res = 
            df
            |> pl.select([
                pl.col "raw"

                // 1. Strip 测试 (默认去空格)
                // "  abc  " -> "abc"
                pl.col("raw").Str.Strip().Alias "strip_default"
                
                // 2. LStrip / RStrip 测试
                // "  abc  " -> "abc  " / "  abc"
                pl.col("raw").Str.LStrip().Alias "lstrip"
                pl.col("raw").Str.RStrip().Alias "rstrip"

                // 3. Strip Matches 测试 (去自定义字符)
                // "__key__" -> "key"
                pl.col("raw").Str.Strip(matches="_").Alias "strip_custom"

                // 4. Prefix / Suffix 测试
                // "https://pl.rs" -> "pl.rs"
                // "data.csv" -> "data"
                pl.col("raw").Str.StripPrefix("https://").Alias "strip_prefix"
                pl.col("raw").Str.StripSuffix(".csv").Alias "strip_suffix"

                // 5. Anchors 测试 (StartsWith / EndsWith) -> Boolean
                pl.col("raw").Str.StartsWith("https").Alias "is_url"
                pl.col("raw").Str.EndsWith(".csv").Alias "is_csv"

                // 6. ToDate 测试 (解析)
                // "20250101" -> Date
                pl.col("raw").Str.ToDate("%Y%m%d").Alias "parsed_date"

                // 7. 链式调用测试 (清洗 + 解析)
                // "  2025-12-31  " -> "2025-12-31" -> Date
                pl.col("raw").Str.Strip().Str.ToDate("%Y-%m-%d").Alias "chain_date"
            ])

        // --- 验证结果 ---

        // 1. Strip
        Assert.Equal("abc", res.String("strip_default", 0).Value)
        Assert.Equal("abc  ", res.String("lstrip", 0).Value)
        Assert.Equal("  abc", res.String("rstrip", 0).Value)

        // 2. Custom Strip
        Assert.Equal("key", res.String("strip_custom", 3).Value) // __key__ -> key

        // 3. Prefix / Suffix
        Assert.Equal("pl.rs", res.String("strip_prefix", 1).Value)
        Assert.Equal("data", res.String("strip_suffix", 2).Value) // data.csv -> data

        // 4. Anchors (Boolean)
        Assert.Equal(Some true, res.Bool("is_url", 1)) // https://...
        Assert.Equal(Some false, res.Bool("is_url", 0))
        Assert.Equal(Some true, res.Bool("is_csv", 2)) // ...csv

        // 5. ToDate (解析成功)
        // Row 4: "20250101"
        let d1 = res.Date("parsed_date", 4).Value
        Assert.Equal(2025, d1.Year)
        Assert.Equal(1, d1.Month)
        Assert.Equal(1, d1.Day)

        // 6. ToDate (解析失败 - Strict=false 默认返回 Null)
        // Row 0: "  abc  " 无法解析为日期
        Assert.True(res.IsNullAt("parsed_date", 0))

        // 7. 链式调用 (Strip + ToDate)
        // Row 5: "  2025-12-31  "
        let d2 = res.Date("chain_date", 5).Value
        Assert.Equal(2025, d2.Year)
        Assert.Equal(12, d2.Month)
        Assert.Equal(31, d2.Day)

        // 打印 Schema 确认类型正确
        // parsed_date 应该是 Date 类型
        Assert.Equal(DataType.Date, res.Schema.["parsed_date"])
    [<Fact>]
    member _.``Math Ops (BMI Calculation with Pow)`` () =
        // 构造数据: 身高(m), 体重(kg)
        use csv = new TempCsv "name,height,weight\nAlice,1.65,60\nBob,1.80,80"
        let df = DataFrame.ReadCsv csv.Path

        // 目标逻辑: weight / (height ^ 2)
        let bmiExpr = 
            pl.col "weight" / pl.col "height" .** pl.lit 2
            |> pl.alias "bmi"

        let res = 
            df 
            |> pl.select [
                pl.col "name"
                bmiExpr
                // 顺便测一下 sqrt: sqrt(height)
                (pl.col "height").Sqrt().Alias "sqrt_h"
            ]

        // 验证 Bob 的 BMI: 80 / 1.8^2 = 24.691358...
        let bobBmi = res.Float("bmi", 1).Value
        Assert.True(bobBmi > 24.69 && bobBmi < 24.70)

        // 验证 Alice 的 Sqrt: sqrt(1.65) = 1.2845...
        let aliceSqrt = res.Float("sqrt_h", 0).Value
        Assert.True(aliceSqrt > 1.28 && aliceSqrt < 1.29)

    [<Fact>]
    member _.``Temporal Ops (Components, Format, Cast)`` () =
        // 构造数据: 包含日期和时间的字符串
        // Row 0: 2023年圣诞节下午3点半 (周一)
        // Row 1: 2024年元旦零点 (周一)
        let csvContent = "ts\n2023-12-25 15:30:00\n2024-01-01 00:00:00"
        use csv = new TempCsv(csvContent)
        
        // [关键] 开启 tryParseDates=true，让 Polars 自动解析为 Datetime 类型
        let df = DataFrame.ReadCsv (path=csv.Path,tryParseDates=true)

        let res =
            df
            |> pl.select [
                pl.col "ts"

                // 1. 提取组件 (Components)
                (pl.col "ts").Dt.Year().Alias "y"
                (pl.col "ts").Dt.Month().Alias "m"
                (pl.col "ts").Dt.Day().Alias "d"
                (pl.col "ts").Dt.Hour().Alias "h"
                
                // Polars 定义: Monday=1, Sunday=7
                (pl.col "ts").Dt.Weekday().Alias "w_day"
                
                // 2. 格式化 (Format to String)
                // 测试自定义格式: "2023/12/25"
                (pl.col "ts").Dt.ToString("%Y/%m/%d").Alias "fmt_custom"
                
                // 3. 类型转换 (Cast to Date)
                // Datetime (含时分秒) -> Date (只含日期)
                (pl.col "ts").Dt.Date().Alias "date_only"
            ]
        // --- 验证 Row 0: 2023-12-25 15:30:00 ---
        
        // 年月日
        Assert.Equal(2023L, res.Int("y", 0).Value)
        Assert.Equal(12L, res.Int("m", 0).Value)
        Assert.Equal(25L, res.Int("d", 0).Value)
        
        // 小时
        Assert.Equal(15L, res.Int("h", 0).Value)
        
        // 星期 (2023-12-25 是周一)
        Assert.Equal(1L, res.Int("w_day", 0).Value)

        // 格式化字符串验证
        Assert.Equal("2023/12/25", res.String("fmt_custom", 0).Value)

        // Date 类型验证
        // 我们的 formatValue 辅助函数会将 Date32 渲染为 "yyyy-MM-dd"
        // 如果转换成功，时分秒应该消失
        Assert.Equal("2023-12-25", res.String("date_only", 0).Value)

        // --- 验证 Row 1: 2024-01-01 00:00:00 ---
        Assert.Equal(2024L, res.Int("y", 1).Value)
        Assert.Equal(1L, res.Int("m", 1).Value)
        Assert.Equal(0L, res.Int("h", 1).Value) // 零点

    [<Fact>]
    member _.``Cast Ops: Int to Float, String to Int`` () =
        // [修改] 使用更大的数字，避免 Polars 推断为 UInt8
        // 同时给 val_str 加引号，确保它像个 String
        use csv = new TempCsv "val_str,val_int\n\"100\",1000\n\"200\",2000"
        
        // [修改] 显式指定 Schema，确保 val_str 是 String，val_int 是 Int64
        // 这样测试的就是纯粹的 Cast 逻辑，而不是 CSV 推断逻辑
        // (由于 ReadCsv 还没有 Schema 参数，我们依赖数据本身让推断正确)
        // 1000 肯定超过了 UInt8 (max 255)，会被推断为 Int64
        
        let df = DataFrame.ReadCsv csv.Path

        let res = 
            df 
            |> pl.select [
                // 1. String -> Int64
                (pl.col "val_str").Cast(DataType.Int64).Alias "str_to_int"
                
                // 2. Int64 -> Float64
                (pl.col "val_int").Cast(DataType.Float64).Alias "int_to_float"
            ]

        // 验证
        // "100" -> 100
        let v1 = res.Int("str_to_int", 0).Value
        Assert.Equal(100L, v1)

        // 1000 -> 1000.0
        let v2 = res.Float("int_to_float", 0).Value
        Assert.Equal(1000.0, v2)
    [<Fact>]
    member _.``Control Flow: IfElse (When/Then/Otherwise)`` () =
        // 构造成绩数据
        use csv = new TempCsv "student,score\nAlice,95\nBob,70\nCharlie,50"
        let df = DataFrame.ReadCsv csv.Path

        // 逻辑:
        // if score >= 90 then "A"
        // else if score >= 60 then "Pass"
        // else "Fail"
        
        let gradeExpr = 
            pl.ifElse 
                (pl.col "score" .>= pl.lit 90) 
                (pl.lit "A") 
                (
                    // 嵌套 IfElse
                    pl.ifElse 
                        (pl.col "score" .>= pl.lit 60)
                        (pl.lit "Pass")
                        (pl.lit "Fail")
                )
            |> pl.alias "grade"

        let res = 
            df 
            |> pl.withColumn gradeExpr
            |> pl.sort (pl.col "score", true) // 降序

        // 验证
        // Alice (95) -> A
        Assert.Equal("A", res.String("grade", 0).Value)
        // Bob (70) -> Pass
        Assert.Equal("Pass", res.String("grade", 1).Value)
        // Charlie (50) -> Fail
        Assert.Equal("Fail", res.String("grade", 2).Value)

    [<Fact>]
    member _.``String Regex: Replace and Extract`` () =
        use csv = new TempCsv "text\nUser: 12345\nID: 999"
        let df = DataFrame.ReadCsv csv.Path

        let res = 
            df 
            |> pl.select [
                // 1. Regex Replace: 把数字换成 #
                // \d+ 是正则
                (pl.col "text").Str.ReplaceAll("\d+", "#", useRegex=true).Alias "masked"
                
                // 2. Regex Extract: 提取数字部分
                // (\d+) 是第 1 组
                (pl.col "text").Str.Extract("(\d+)", 1).Alias "extracted_id"
            ]

        // 验证 Replace
        // "User: 12345" -> "User: #"
        Assert.Equal("User: #", res.String("masked", 0).Value)
        
        // 验证 Extract
        // "User: 12345" -> "12345"
        Assert.Equal("12345", res.String("extracted_id", 0).Value)
        Assert.Equal("999", res.String("extracted_id", 1).Value)
    [<Fact>]
    member _.``Dt: Add Business Days (Standard Week)`` () =
        // 2023-01-01 是周日
        // 2023-01-02 是周一 (Business Day)
        let start = DateOnly(2023, 1, 1)
        let df = DataFrame.ofRecords [ {| Date = start |} ]

        // 场景 1: 周日 + 1 工作日 (Roll=Forward) -> 应该是周一 + 1 = 周二 (2023-01-03)?
        // Polars 逻辑: 如果 start 不是工作日且 roll=forward，它会先滚动到下一个工作日(周一)，然后加 n。
        // 所以: Sun(Roll->Mon) + 1 BD = Tue.
        
        let res = 
            df.Select([
                pl.col("Date").Dt.AddBusinessDays(1, roll=Roll.Forward).Alias "Next"
            ])
            
        // 2023-01-03
        Assert.Equal(DateOnly(2023, 1, 3), res.Cell<DateOnly>("Next",0))

    [<Fact>]
    member _.``Dt: Add Business Days (With Holidays)`` () =
        // 2023-01-04 (周三)
        // 2023-01-05 (周四) -> 设为假期
        // 2023-01-06 (周五)
        // 2023-01-07 (周六)
        // 2023-01-08 (周日)
        // 2023-01-09 (周一)
        
        let start = DateOnly(2023, 1, 4) // Wed
        let holidays = [ DateOnly(2023, 1, 5) ] // Thu is holiday
        
        let df = DataFrame.ofRecords [ {| Date = start |} ]

        // Wed + 2 Business Days
        // Day 1: Thu (Skip/Holiday) -> Fri
        // Day 2: Sat (Skip) -> Sun (Skip) -> Mon
        // Result should be Mon Jan 09
        
        let res = 
            df.Select([
                pl.col("Date").Dt
                    .AddBusinessDays(2, holidays=holidays)
                    .Alias "Result"
            ])
            
        Assert.Equal(DateOnly(2023, 1, 9), res.Cell<DateOnly>("Result", 0))

    [<Fact>]
    member _.``Dt: Custom Week Mask (Weekend is Fri/Sat)`` () =
        // 模拟中东工作周 (周日-周四工作，周五周六休息)
        // Mask 顺序: Mon, Tue, Wed, Thu, Fri, Sat, Sun
        let customWeek = [| true; true; true; true; false; false; true |]
        
        // 2023-01-05 (周四)
        // + 1 BD -> Fri(Skip), Sat(Skip) -> Sun (2023-01-08)
        let start = DateOnly(2023, 1, 5) 
        let df = DataFrame.ofRecords [ {| Date = start |} ]
        
        let res = 
            df.Select([
                pl.col("Date").Dt
                    .AddBusinessDays(1, weekMask=customWeek)
                    .Alias "Result"
            ])
            
        Assert.Equal(DateOnly(2023, 1, 8), res.Cell<DateOnly>("Result",0))

    [<Fact>]
    member _.``Dt: Is Business Day`` () =
        let dates = [
            DateOnly(2023, 1, 6) // Fri
            DateOnly(2023, 1, 7) // Sat
            DateOnly(2023, 1, 8) // Sun
            DateOnly(2023, 1, 9) // Mon
        ]
        
        let df = 
            DataFrame.ofRecords [ 
                for d in dates do yield {| Date = d |} 
            ]

        // 默认: Sat/Sun 是非工作日
        let res = 
            df.WithColumns([
                pl.col("Date").Dt.IsBusinessDay().Alias "IsBiz"
            ])
        
        // Fri -> True
        Assert.True(res.Cell<bool>("IsBiz",0))
        // Sat -> False
        Assert.False(res.Cell<bool>("IsBiz",1))
        // Sun -> False
        Assert.False(res.Cell<bool>("IsBiz",2))
        // Mon -> True
        Assert.True(res.Cell<bool>("IsBiz",3))

    [<Fact>]
    member _.``Dt: Is Business Day (With Holidays)`` () =
        let df = DataFrame.ofRecords [ {| Date = DateOnly(2023, 1, 2) |} ] // Mon
        let hols = [ DateOnly(2023, 1, 2) ] // Monday is holiday
        
        let res = df.Select([
            pl.col("Date").Dt.IsBusinessDay(holidays=hols)
        ])
        
        Assert.False(res.Cell<bool>("Date", 0))
    [<Fact>]
    member _.``Array: Basic Aggregation & Operations`` () =
        // 1. 准备数据: List<Int>
        // Row 0: [1, 2, 3]
        // Row 1: [4, 5, 6]
        let data = [
            {| Vals = [1; 2; 3] |}
            {| Vals = [4; 5; 6] |}
        ]
        let df = DataFrame.ofRecords data
        
        // 2. 转换为 Array(width=3)
        // 必须先 Cast 成 FixedSizeList 才能使用 .Arr 命名空间
        // 注意：Array 在 Polars 中对应 DataType.Array(inner, width)
        let lf = 
            df.Lazy()
              .WithColumns([
                  pl.col("Vals").Cast(DataType.Array(DataType.Int32, 3UL))
              ])

        // 3. 测试 Arr.Sum, Min, Max
        let res = 
            lf.Select([
                pl.col("Vals").Array.Sum().Alias "Sum"
                pl.col("Vals").Array.Min().Alias "Min"
                pl.col("Vals").Array.Max().Alias "Max"
                pl.col("Vals").Array.Mean().Alias "Mean"
            ]).Collect()

        // Row 0: Sum=6, Min=1, Max=3, Mean=2.0
        Assert.Equal(6, res.Cell<int>("Sum",0))
        Assert.Equal(1, res.Cell<int>("Min",0))
        Assert.Equal(2.0, res.Cell<double>("Mean",0))
        
        // Row 1: Sum=15
        Assert.Equal(15, res.Cell<int>("Sum",1))

    [<Fact>]
    member _.``Array: Set Operations & Sort`` () =
        let data = [
            {| Vals = ["3"; "1"; "2"] |}
            {| Vals = ["1"; "1"; "2"] |}
        ]
        let lf = 
            DataFrame.ofRecords(data).Lazy()
                .WithColumns([
                    pl.col("Vals").Cast(Array(String, 3UL))
                ])

        let res = 
            lf.Select([
                // Sort Descending
                pl.col("Vals").Array.Sort(descending=true).Alias "Sorted_Str"
                // Unique
                pl.col("Vals").Array.Unique().List.Sort().Alias "Unique_Str"
                // Join
                pl.col("Vals").Array.Join("-").Alias "Joined"
            ]).Collect()

        // --- 验证 Row 0 ---
        // 原始: ["3", "1", "2"]
        
        // 1. Sort Descending -> "3,2,1"
        let sorted = res.CellList<string>("Sorted_Str",0)
        Assert.Equal<string list>(["3"; "2"; "1"], sorted)
        
        // 3. Join -> "3-1-2"
        Assert.Equal("3-1-2", res.Cell<string>("Joined",0))

        // --- 验证 Row 1 ---
        // 原始: ["1", "1", "2"]
        
        // 2. Unique -> "1,2"
        let unique = res.CellList<string>("Unique_Str",1)
        Assert.Equal<string list>(["1"; "2"],unique)

    [<Fact>]
    member _.``Array: Search & Get`` () =
        let data = [
            {| Vals = [10; 20; 30] |}
        ]
        let lf = 
            DataFrame.ofRecords(data).Lazy()
                .WithColumns([
                    pl.col("Vals").Cast(DataType.Array(DataType.Int32, 3UL))
                ])

        let res = 
            lf.Select([
                // Get by Index
                pl.col("Vals").Array.Get(1).Alias "Get_1"
                // Contains
                pl.col("Vals").Array.Contains(20).Alias "Has_20"
                pl.col("Vals").Array.Contains(99).Alias "Has_99"
                // ArgMax
                pl.col("Vals").Array.ArgMax().Alias "ArgMax"
            ]).Collect()

        // Get(1) -> 20
        Assert.Equal(20, res.Cell<int>("Get_1",0))
        
        // Contains
        Assert.True(res.Cell<bool>("Has_20",0))
        Assert.False(res.Cell<bool>("Has_99",0))
        
        // ArgMax -> 2 (index of 30)
        // ArgMax 返回的是 UInt32? 还是 Int? 通常是 UInt32 或 Int64，视 Polars 版本
        // 我们用 Convert 宽容处理
        let argMax = res.Cell<int>("ArgMax",0)
        Assert.Equal(2, argMax)
    [<Fact>]
    member _.``Math: Trig and Rounding`` () =
        // 准备数据
        // 0.0, PI/2, PI
        let data = [
            {| Val = 0.0 |}
            {| Val = Math.PI / 2.0 |}
            {| Val = Math.PI |}
            {| Val = -1.5 |} // 用于测试 Sign, Ceil, Floor
        ]
        
        let lf = DataFrame.ofRecords(data).Lazy()

        let res = 
            lf.Select([
                // Trig
                pl.col("Val").Sin().Alias "Sin"
                pl.col("Val").Cos().Alias "Cos"
                
                // Rounding
                pl.col("Val").Ceil().Alias "Ceil"
                pl.col("Val").Floor().Alias "Floor"
                
                // Sign
                pl.col("Val").Sign().Alias "Sign"
                
                // Cbrt (Cube Root of 8 = 2)
                pl.lit(8.0).Cbrt().Alias "Cbrt_8"
            ]).Collect()

        // 1. 验证 Sin(PI/2) = 1.0
        // Row 1 is PI/2
        Assert.Equal(1.0, res.Cell<double>("Sin",1), 5) // 精度 5 位小数

        // 2. 验证 Cos(PI) = -1.0
        // Row 2 is PI
        Assert.Equal(-1.0, res.Cell<double>("Cos",2), 5)

        // 3. 验证 Rounding (-1.5)
        // Row 3
        Assert.Equal(-1.0, res.Cell<double>("Ceil",3))  // Ceil(-1.5) -> -1.0
        Assert.Equal(-2.0, res.Cell<double>("Floor",3)) // Floor(-1.5) -> -2.0
        
        // 4. 验证 Sign
        // Row 0 (0.0) -> 0
        Assert.Equal(0.0, res.Cell<double>("Sign",0))
        // Row 1 (Positive) -> 1
        Assert.Equal(1.0, res.Cell<double>("Sign",1))
        // Row 3 (Negative) -> -1
        Assert.Equal(-1.0, res.Cell<double>("Sign",3))

        // 5. 验证 Cbrt
        Assert.Equal(2.0, res.Cell<double>("Cbrt_8",0))
    [<Fact>]
    member _.``List: ConcatList (Explicit Columns)`` () =
        // Data:
        // A: 1, 2
        // B: 3, 4
        let data = [
            {| A = 1; B = 3 |}
            {| A = 2; B = 4 |}
        ]
        let df = DataFrame.ofRecords data

        // 合并 A 和 B -> List
        let res = 
            df.Select([
                pl.concatList([ pl.col "A"; pl.col "B" ]).Alias "Merged"
            ])
        
        // Row 0: [1, 3]
        let l0 = res.CellList<int>("Merged", 0)
        Assert.Equal<int list>([1; 3], l0)

        // Row 1: [2, 4]
        let l1 = res.CellList<int>("Merged", 1)
        Assert.Equal<int list>([2; 4], l1)

    [<Fact>]
    member _.``List: ConcatList with Selector (The Magic)`` () =
        // Data:
        // A(int): 1
        // B(int): 10
        // C(str): "ignore"
        let data = [
            {| A = 1; B = 10; C = "ignore" |}
            {| A = 2; B = 20; C = "skip" |}
        ]
        let df = DataFrame.ofRecords data

        // 需求：把所有数值类型的列合并成一个 List 列
        // 这在特征工程中非常常见（把特征列打包送进模型）
        let res = 
            df.Select([
                // !> 是之前定义的转换符，或者如果 concatList 接受 seq<#IColumnExpr>，
                // 且 cs.numeric() 实现了它，直接传列表即可。
                // 注意：F# 列表是同质的，如果只传 Selector，直接写 list 即可。
                pl.concatList([ pl.cs.numeric() ]).Alias "Features"
            ])
        
        // Row 0: A=1, B=10 -> [1, 10]
        let l0 = res.CellList<int>("Features", 0)
        Assert.Equal<int list>([1; 10], l0)
        
        // Row 1: A=2, B=20 -> [2, 20]
        let l1 = res.CellList<int>("Features", 1)
        Assert.Equal<int list>([2; 20], l1)
    [<Fact>]
    member _.``List: Concat (Fluent API)`` () =
        // Data: A=1, B=2, C=3
        let data = [
            {| A = 1; B = 2; C = 3 |}
        ]
        let df = DataFrame.ofRecords data

        let res = 
            df.Select([
                // 方式 1: pl.concatList (函数式)
                pl.concatList([ pl.col "A"; pl.col "B"; pl.col "C" ]).Alias "Func"

                // 方式 2: col("A").List.Concat (链式)
                // 语意：以 A 为头，拼接 B 和 C
                pl.col("A").List.Concat([ pl.col "B"; pl.col "C" ]).Alias "Fluent_List"
                
                // 方式 3: 链式单列
                pl.col("A").List.Concat(pl.col "B").Alias "Fluent_Single"
            ])
        
        // 验证 1 & 2 结果一致: [1, 2, 3]
        let lFunc = res.CellList<int>("Func", 0)
        let lFluent = res.CellList<int>("Fluent_List", 0)
        
        Assert.Equal<int list>([1; 2; 3], lFunc)
        Assert.Equal<int list>([1; 2; 3], lFluent)

        // 验证 3: [1, 2]
        let lSingle = res.CellList<int>("Fluent_Single", 0)
        Assert.Equal<int list>([1; 2], lSingle)