module Polars.FSharp.Tests.DeltaTests

open System
open System.IO
open Xunit
open Polars.FSharp

[<Fact>]
let ``Scenario 1: Delta Lake Base Read Write and Append`` () =
    // 生成一个随机的本地临时目录作为我们的 "本地数据湖"
    let testPath = Path.Combine(Path.GetTempPath(), $"delta_test_lake_{Guid.NewGuid()}")
    
    try
        // ==========================================
        // 1. 造一个简单的 DataFrame (建仓)
        // ==========================================
        let sId1 = Series.create("id", [1; 2; 3])
        let sName1 = Series.create("name", ["Alice"; "Bob"; "Charlie"])
        let sPrice1 = Series.create("price", [10.5; 20.0; 15.75])
        let df1 = DataFrame.create [| sId1; sName1; sPrice1 |]

        // ==========================================
        // 2. Overwrite 模式写入本地 Delta 湖
        // ==========================================
        // 这里验证了 SinkDelta 的扩展方法以及默认参数的隐式转换是否顺畅
        df1.WriteDelta(testPath, mode = DeltaSaveMode.Overwrite, mkdir = true, syncOnClose=SyncOnClose.All)

        // ==========================================
        // 3. ScanDelta 读回来并验证
        // ==========================================
        let readDf1 = DataFrame.ReadDelta testPath
        
        // 断言行数和列数完全一致
        Assert.Equal(3L, readDf1.Height)
        Assert.Equal(3L, readDf1.Width)
        
        // ==========================================
        // 4. 造一批新数据准备 Append
        // ==========================================
        let sId2 = Series.create("id", [4; 5])
        let sName2 = Series.create("name", ["Dave"; "Eve"])
        let sPrice2 = Series.create("price", [9.99; 99.9])
        let df2 = DataFrame.create [| sId2; sName2; sPrice2 |]

        // ==========================================
        // 5. Append 模式追加进去
        // ==========================================
        // 验证 Delta 湖的追加语义
        df2.WriteDelta(testPath, mode = DeltaSaveMode.Append)

        // ==========================================
        // 6. 再次读回来，验证数据合并
        // ==========================================
        let readDf2 = LazyFrame.ScanDelta(testPath).Collect()
        
        // 3 + 2 = 5 行，验证 Append 成功
        Assert.Equal(5L, readDf2.Height)
        
        // 验证追加的数据确实进去了 (排个序取最后一行)
        let sortedDf = readDf2.Sort "id"
        // 取出 name 列，第 4 行 (0-indexed) 应该是 "Eve"
        let lastRowNameOpt = sortedDf.Select(pl.col "name").Row(4).[0]
        
        Assert.Equal("Some(Eve)", lastRowNameOpt.ToString())

    finally
        // ==========================================
        // 7. 打扫战场，清理本地数据湖文件夹
        // ==========================================
        // 保证测试的幂等性
        if Directory.Exists testPath then
            Directory.Delete(testPath, true)
[<Fact>]
let ``Scenario 2: Delta Lake Time Travel (History & Restore)`` () =
    // 新建一个专属于时空穿梭测试的临时数据湖目录
    let testPath = Path.Combine(Path.GetTempPath(), $"delta_time_travel_{Guid.NewGuid()}")
    
    try
        // ==========================================
        // 1. Version 0+1: 初始建仓 (Overwrite)
        // ==========================================
        let sId1 = Series.create("id", [1; 2; 3])
        let sName1 = Series.create("name", ["Alice"; "Bob"; "Charlie"])
        // 使用 DataFrame.create 工厂方法
        let df1 = DataFrame.create [| sId1; sName1 |] 
        
        df1.WriteDelta(testPath, mode = DeltaSaveMode.Overwrite, mkdir = true)
        
        // ==========================================
        // 2. Version 2: 追加数据 (Append)
        // ==========================================
        let sId2 = Series.create("id", [4; 5])
        let sName2 = Series.create("name", ["Dave"; "Eve"])
        let df2 = DataFrame.create [| sId2; sName2 |]
        
        df2.WriteDelta(testPath, mode = DeltaSaveMode.Append)
        
        // ==========================================
        // 3. 查阅历史 (History)
        // ==========================================
        let historyDf = Delta.History testPath
        
        // History 应该至少包含两行记录: Version 2 (WRITE/APPEND) 和 Version 1 (WRITE/OVERWRITE)
        Assert.True(historyDf.Height >= 2L, "History should contain at least 2 versions.")
        
        // ==========================================
        // 4. Time Travel 读时穿梭 (Scan Version 1)
        // ==========================================
        // 当前默认读取应该是 5 行数据
        let currentDf = LazyFrame.ScanDelta(testPath).Collect()
        Assert.Equal(5L, currentDf.Height)
        
        // 强行指定读取 Version 1 的快照
        let v0Df = LazyFrame.ScanDelta(testPath, version = 1L).Collect()
        Assert.Equal(3L, v0Df.Height) // Version 1 的时候明明只有 3 行！
        
        // ==========================================
        // 5. 时光倒流 (Restore to Version 1)
        // ==========================================
        // 哎呀，发现 Version 1 的数据追加错了，赶紧回滚！
        // 注意：Restore 实际上会创建一个新的事务 (Version 2) 来反映回滚后的状态
        let newVersion = Delta.Restore(testPath, version = 1L)
        
        // ==========================================
        // 6. 验证回滚结果
        // ==========================================
        // 再次以默认方式（读取最新版）读取数据
        let restoredDf = LazyFrame.ScanDelta(testPath).Collect()
        
        // 行数神奇地变回了 3 行，就像 Version 2 从来没发生过一样！
        Assert.Equal(3L, restoredDf.Height)
        
        // 严谨点：此时再去查 History，应该能看到刚刚的 Restore 操作也上榜了
        let newHistoryDf = Delta.History testPath
        Assert.True(newHistoryDf.Height >= 3L, "History should now contain the Restore operation.")

    finally
        // ==========================================
        // 7. 打扫战场
        // ==========================================
        if Directory.Exists testPath then
            Directory.Delete(testPath, true)

[<Fact>]
let ``Scenario 3: Delta Lake Advanced MERGE Semantics`` () =
    let testPath = Path.Combine(Path.GetTempPath(), $"delta_merge_{Guid.NewGuid()}")

    try
        // ==========================================
        // 1. 初始化目标表 (Target)
        // ==========================================
        let sId = Series.create("id", [1; 2; 3])
        let sName = Series.create("name", ["Alice"; "Bob"; "Charlie"])
        let sPrice = Series.create("price", [10.0; 20.0; 30.0])
        let targetDf = DataFrame.create [| sId; sName; sPrice |]
        
        // 建表并写入初始数据
        targetDf.WriteDelta(testPath, mode = DeltaSaveMode.Overwrite, mkdir = true)
        
        // ==========================================
        // 2. 准备源表 (Source 增量数据)
        // ==========================================
        // ID=2: 更新 (价格变高，应该被接受)
        // ID=3: 更新 (价格变低，测试条件拦截，应该被忽略)
        // ID=4: 新增 (应该直接插入)
        let sIdSrc = Series.create("id", [2; 3; 4])
        let sNameSrc = Series.create("name", ["Bob_Updated"; "Charlie_Downgraded"; "Dave"])
        let sPriceSrc = Series.create("price", [25.0; 15.0; 40.0])
        let sourceDf = DataFrame.create [| sIdSrc; sNameSrc; sPriceSrc |]

        // ==========================================
        // 3. 执行 MERGE (Upsert)
        // ==========================================
        // 业务逻辑：当匹配时，只有当 Source 价格大于 Target 价格才更新
        // 这里验证了我们之前手写的 Delta.Source 和 Delta.Target 辅助方法！
        let updateCond = Delta.Source "price" .> Delta.Target "price"
        
        sourceDf.MergeDeltaOrdered(
            testPath,
            mergeKeys = ["id"]
            // matchedUpdateCond = updateCond,
            // notMatchedInsertCond = pl.lit true // 显式传一个 lit 进去测试多句柄解包
        ).WhenMatchedUpdate(updateCond).WhenNotMatchedInsert(pl.lit true).Execute()

        // ==========================================
        // 4. 验证合并结果
        // ==========================================
        let finalDf = LazyFrame.ScanDelta(testPath).Collect().Sort("id")
        
        // 总共应该有 4 行记录 (1, 2, 3, 4)
        Assert.Equal(4L, finalDf.Height)

        // 提取 Name 列进行逐个验证
        let nameCol = finalDf.Select(pl.col "name")

        // ID=2 价格更高 (25 > 20)，应该被成功更新
        let bobName = nameCol.Row(1).[0].ToString()
        Assert.Equal("Some(Bob_Updated)", bobName)

        // ID=3 价格更低 (15 < 30)，不满足 updateCond，应该保持原样！
        let charlieName = nameCol.Row(2).[0].ToString()
        Assert.Equal("Some(Charlie)", charlieName)

        // ID=4 不在 Target 中，应该被成功插入
        let daveName = nameCol.Row(3).[0].ToString()
        Assert.Equal("Some(Dave)", daveName)

    finally
        // ==========================================
        // 5. 打扫战场
        // ==========================================
        if Directory.Exists testPath then
            Directory.Delete(testPath, true)

[<Fact>]
let ``Scenario 4: Delta Lake Maintenance (Delete, Optimize, Vacuum)`` () =
    let testPath = Path.Combine(Path.GetTempPath(), $"delta_maint_{Guid.NewGuid()}")

    try
        // ==========================================
        // 1. 建仓并写入 10 条测试数据
        // ==========================================
        let sId = Series.create("id", [1..10])
        let sVal = Series.create("val", ["A"; "B"; "C"; "D"; "E"; "F"; "G"; "H"; "I"; "J"])
        let df = DataFrame.create [| sId; sVal |]
        
        df.WriteDelta(testPath, mode = DeltaSaveMode.Overwrite, mkdir = true)

        // ==========================================
        // 2. 开启超级特性：Deletion Vectors (软删除)
        // ==========================================
        // 验证 DeltaTableFeatures 模块里的 Literal 常量是否完美生效
        Delta.AddFeature(testPath, DeltaTableFeatures.DeletionVectors)

        // ==========================================
        // 3. 执行 Delete (此时将触发 Merge-on-Read 行为)
        // ==========================================
        // 删除 ID < 5 的行 (即删掉 1, 2, 3, 4)
        let deleteCond = pl.col "id" .< pl.lit 5
        Delta.Delete(testPath, deleteCond)

        // 验证：此时表里应该只剩下 6 行数据
        let afterDeleteDf = LazyFrame.ScanDelta(testPath).Collect()
        Assert.Equal(6L, afterDeleteDf.Height)

        // ==========================================
        // 4. 执行 Optimize (碎片整理 + Z-Order 聚簇 + 物化 DV)
        // ==========================================
        // Optimize 会将刚才通过 Deletion Vectors 软删除的数据物理移除，并按 "id" 重新聚簇
        let optimizedFilesCount = 
            Delta.Optimize(
                testPath, 
                targetSizeMb = 128L, 
                zOrderColumns = ["id"] // 验证 seq<string> 参数传递
            )
        
        // ==========================================
        // 5. 执行 Vacuum (强制清空历史垃圾文件)
        // ==========================================
        // 警告：retentionHours = 0 且 enforceRetention = false 是极其激进的清理
        // 这里为了测试把刚才被 Optimize 淘汰的旧碎片彻底从本地磁盘抹除
        let deletedFilesCount = 
            Delta.Vacuum(
                testPath, 
                retentionHours = 0, 
                enforceRetention = false
            )
        
        // ==========================================
        // 6. 最终校验
        // ==========================================
        let finalDf = LazyFrame.ScanDelta(testPath).Collect()
        Assert.Equal(6L, finalDf.Height)
        
        // 我们还可以看看 History，这时候应该是一部壮丽的史诗了
        let historyDf = Delta.History testPath
        historyDf.Show()
        // 应该包含 WRITE, ADD FEATURE, DELETE, OPTIMIZE 等多个版本记录
        Assert.True(historyDf.Height >= 4L)

    finally
        // ==========================================
        // 7. 打扫战场
        // ==========================================
        if Directory.Exists testPath then
            Directory.Delete(testPath, true)