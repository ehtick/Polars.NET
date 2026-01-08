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

            // 1. Reader: ArrowReader
            var rawGetter = ArrowReader.CreateAccessor(inputArray, typeof(TIn));

            // 2. Writer: DbToArrowStream
            var buffer = ColumnBufferFactory.Create(typeof(TOut), length);

            // 3. Null 
            bool inputIsValueType = typeof(TIn).IsValueType && Nullable.GetUnderlyingType(typeof(TIn)) == null;

            for (int i = 0; i < length; i++)
            {
                // check null logic
                bool isNull = inputArray.IsNull(i);

                if (isNull && inputIsValueType)
                {
                    buffer.Add(null!); 
                }
                else
                {
                    TIn input = (TIn)rawGetter(i)!;
                    
                    TOut output = userFunc(input);
                    
                    buffer.Add(output!);
                }
            }

            return buffer.BuildArray();
        };
    }
}