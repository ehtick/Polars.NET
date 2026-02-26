using Polars.CSharp;

namespace Polars.Integration.Tests;

public class HuggingFaceTests
{
    [Fact]
    public void Test_Scan_Real_HuggingFace_Public()
    {

        var hfUrl = "https://huggingface.co/datasets/scikit-learn/iris/resolve/refs%2Fconvert%2Fparquet/default/train/0000.parquet";

        Console.WriteLine($"Connecting to Hugging Face via HTTPS: {hfUrl} ...");

        var options = CloudOptions.Http(new Dictionary<string, string>
        {
            { "User-Agent", "Polars.NET-Test" }
        });

        // 3. Scan & Collect
        using var lf = LazyFrame.ScanParquet(hfUrl, cloudOptions: options);
        using var df = lf.Collect(useStreaming:true);

        // 4. 验证
        Assert.True(df.Height > 0);
        df.Show();
        
        var columns = df.Columns;
        Assert.Contains("Id", columns); 
        Assert.Contains("SepalLengthCm", columns);

        Console.WriteLine($"Hugging Face HTTPS Test Passed! Loaded {df.Height} rows.");
    }
}