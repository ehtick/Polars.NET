using Xunit;

namespace Polars.CSharp.Tests
{
    public class IntegrationFact : FactAttribute
    {
        public IntegrationFact()
        {
            // 检查环境变量，如果未设置为 "true"，则跳过测试
            var runIntegration = Environment.GetEnvironmentVariable("RUN_INTEGRATION_TESTS");
            if (string.IsNullOrEmpty(runIntegration) || !runIntegration.Equals("true", StringComparison.OrdinalIgnoreCase))
            {
                Skip = "Skipping integration tests. Set environment variable RUN_INTEGRATION_TESTS=true to enable.";
            }
        }
    }
}