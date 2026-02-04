using System.Runtime.CompilerServices;
using System.Text;

namespace Polars.NET.Core.Helpers;

public static unsafe class StringPacker
{
    /// <summary>
    /// Convert C# string[] To Arrow LargeUtf8 (Values + Offsets + Validity)。
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static (byte[] values, long[] offsets, byte[]? validity) Pack(string?[] data)
    {
        int len = data.Length;
        
        // Offsets Array
        // Arrow Protocol：Offsets[i] is the start for the i index string, [len] is total length
        var offsets = new long[len + 1];

        // Pre calculate total size and fill offsetes
        long currentOffset = 0;
        bool hasNull = false;

        // Pass 1: Calculate Length & Offsets
        fixed (long* pOffsets = offsets)
        {
            for (int i = 0; i < len; i++)
            {
                pOffsets[i] = currentOffset;
                string? s = data[i];
                if (s != null)
                {
                    currentOffset += Encoding.UTF8.GetByteCount(s);
                }
                else
                {
                    hasNull = true;
                }
            }
            pOffsets[len] = currentOffset; 
        }

        // Allocate Values
        var values = GC.AllocateUninitializedArray<byte>((int)currentOffset);

        // 4. Validity Bitmap 
        byte[]? validity = null;
        if (hasNull)
        {
            int validLen = (len + 7) >> 3;
            validity = new byte[validLen];
            Array.Fill(validity, (byte)0xFF);
            
        }

        // Pass 2: Convert Values & set Validity
        fixed (long* pOffsets = offsets) 
        fixed (byte* pValues = values)
        fixed (byte* pValid = validity) 
        {
            byte* pCurrentVal = pValues;
            
            for (int i = 0; i < len; i++)
            {
                string? s = data[i];
                if (s != null)
                {
                    // Write memory directly
                    fixed (char* pChar = s)
                    {
                        // GetBytes(char*, charCount, byte*, byteCount)
                        int written = Encoding.UTF8.GetBytes(pChar, s.Length, pCurrentVal, (int)(pOffsets[i+1] - pOffsets[i]));
                        pCurrentVal += written;
                    }
                }
                else
                {
                    // Set Validity as 0 (Null)
                    // Bitwise Ops: byteIndex = i / 8, bitIndex = i % 8
                    // target &= ~(1 << bit)
                    if (pValid != null)
                    {
                        pValid[i >> 3] &= (byte)~(1 << (i & 7));
                    }
                }
            }
        }

        return (values, offsets, validity);
    }
}