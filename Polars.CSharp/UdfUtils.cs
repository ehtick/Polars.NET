using Apache.Arrow;
using Polars.NET.Core.Arrow; 
using Polars.NET.Core.Data;

namespace Polars.CSharp;

internal static class UdfUtils
{
    public static Func<IArrowArray, IArrowArray> Wrap<TIn, TOut>(Func<TIn, TOut> userFunc)
    {
        return inputArray =>
        {
            int length = inputArray.Length;

            // 1. Reader: 复用 ArrowReader 的强大读取能力
            // 它能自动处理 Int32 -> int, Int64 -> int (checked), StringView -> string 等转换
            var rawGetter = ArrowReader.CreateAccessor(inputArray, typeof(TIn));

            // 2. Writer: 复用 DbToArrowStream 中的缓冲池逻辑
            // 它底层使用 ArrowConverter，支持所有我们已支持的类型 (包括复杂类型)
            var buffer = ColumnBufferFactory.Create(typeof(TOut), length);

            // 3. Null 处理策略准备
            // 如果 TIn 是非空值类型 (如 int)，遇到 null 输入时我们无法调用 userFunc
            bool inputIsValueType = typeof(TIn).IsValueType && Nullable.GetUnderlyingType(typeof(TIn)) == null;

            for (int i = 0; i < length; i++)
            {
                // check null logic
                bool isNull = inputArray.IsNull(i);

                if (isNull && inputIsValueType)
                {
                    // 输入是 null，但函数要求 int (非空)。
                    // 策略：直接输出 null (Skip UDF execution)
                    // ColumnBuffer.Add(null) 会自动处理 AppendNull
                    buffer.Add(null!); 
                }
                else
                {
                    // 正常执行
                    // getter(i) 会处理好类型转换
                    TIn input = (TIn)rawGetter(i)!;
                    
                    // 执行用户逻辑
                    TOut output = userFunc(input);
                    
                    // 写入 Buffer
                    buffer.Add(output!);
                }
            }

            // 4. 构建输出 Array
            return buffer.BuildArray();
        };
    }
}