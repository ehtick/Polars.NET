using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace Polars.NET.Core.Helpers;

  public static unsafe class DecimalPacker
    {
        // C# decimal mem layout(Sequential): flags, hi, lo, mid (4 int)
        internal static readonly Int128[] PowersOf10Int128;
        static DecimalPacker() // Static Constructor
        {
            PowersOf10Int128 = new Int128[30]; // decimal max scale is 28
            PowersOf10Int128[0] = 1;
            for (int i = 1; i < PowersOf10Int128.Length; i++)
            {
                PowersOf10Int128[i] = PowersOf10Int128[i - 1] * 10;
            }
        }
        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        public static (Int128[] values, int scale) Pack(decimal[] data)
        {
            int len = data.Length;
            if (len == 0) return (Array.Empty<Int128>(), 0);

            byte maxScale = 0;

            // Pass 1: Scan Max Scale
            fixed (decimal* pSrc = data)
            {
                // decimal is 16 bytes (4 int)
                int* pInt = (int*)pSrc;
                
                // Unroll 
                for (int i = 0; i < len; i++)
                {
                    int flags = pInt[i * 4]; 
                    byte s = (byte)((flags >> 16) & 0xFF);
                    if (s > maxScale) maxScale = s;
                }
            }

            var values = GC.AllocateUninitializedArray<Int128>(len);

            // Pass 2: Convert
            fixed (decimal* pSrc = data)
            fixed (Int128* pDst = values)
            {
                // pSrc -> decimal[] Array
                // treat it as int array
                int* pRawDec = (int*)pSrc;
                
                for (int i = 0; i < len; i++)
                {
                    // Calc decimal int* start position
                    int baseIdx = i * 4;
                    
                    int flags = pRawDec[baseIdx];     // [0] Flags
                    int hi    = pRawDec[baseIdx + 1]; // [1] Hi
                    int lo    = pRawDec[baseIdx + 2]; // [2] Lo
                    int mid   = pRawDec[baseIdx + 3]; // [3] Mid

                    // Assemble 96-bit Mantissa -> Int128
                    Int128 mantissa = ((Int128)(uint)hi << 64) | ((Int128)(uint)mid << 32) | (Int128)(uint)lo;

                    // Handle +- (Flags & highest bit)
                    if ((flags & 0x80000000) != 0)
                    {
                        mantissa = -mantissa;
                    }

                    // Rescale
                    int currentScale = (flags >> 16) & 0xFF;
                    int diff = maxScale - currentScale;
                    
                    if (diff > 0)
                    {
                        mantissa *= PowersOf10Int128[diff];
                    }

                    pDst[i] = mantissa;
                }
            }

            return (values, maxScale);
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        public static (Int128[] values, byte[]? validity, int scale) Pack(decimal?[] data)
        {
            int len = data.Length;
            
            // Pass 1: Scan max Scale
            byte maxScale = 0;
            
            fixed (decimal?* pSrc = data)
            {
                for (int i = 0; i < len; i++)
                {
                    if (data[i].HasValue)
                    {
                        byte s = data[i].GetValueOrDefault().Scale;
                        if (s > maxScale) maxScale = s;
                    }
                }
            }
            
            // Pass 2: Convert
            var values = GC.AllocateUninitializedArray<Int128>(len);
            byte[]? validity = null;
            ref byte validRef = ref Unsafe.NullRef<byte>();
            
            fixed (decimal?* pSrc = data)
            fixed (Int128* pDst = values)
            {
                // Ref ptr scan
                ref decimal? srcRef = ref MemoryMarshal.GetArrayDataReference(data);
                ref Int128 dstRef = ref MemoryMarshal.GetArrayDataReference(values);

                for (int i = 0; i < len; i++)
                {
                    ref decimal? item = ref Unsafe.Add(ref srcRef, i);
                    
                    if (item.HasValue)
                    {
                        decimal d = item.GetValueOrDefault();
                        
                        // Treat decimal as 4 int struct
                        int* pDec = (int*)Unsafe.AsPointer(ref d);
                        
                        int flags = pDec[0];
                        int hi    = pDec[1];
                        int lo    = pDec[2];
                        int mid   = pDec[3];

                        // Convert 96-bit Int to Int128
                        // Int128 = (Hi << 64) | (Mid << 32) | Lo
                        Int128 mantissa = ((Int128)(uint)hi << 64) | ((Int128)(uint)mid << 32) | (Int128)(uint)lo;

                        // deal +-
                        if ((flags & 0x80000000) != 0)
                        {
                            mantissa = -mantissa;
                        }
                        
                        // Get current Scale
                        int scale = (flags >> 16) & 0xFF;
                        
                        // Rescale
                        // Target = Val * 10^(MaxScale - CurScale)
                        int diff = maxScale - scale;
                        if (diff > 0)
                        {
                            mantissa *= PowersOf10Int128[diff];
                        }
                        
                        Unsafe.Add(ref dstRef, i) = mantissa;
                        
                        // Validity
                        if (validity != null)
                        {
                            if (Unsafe.IsNullRef(ref validRef)) validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                            ref byte target = ref Unsafe.Add(ref validRef, i >> 3);
                            target |= (byte)(1 << (i & 7));
                        }
                    }
                    else
                    {
                        if (validity == null)
                        {
                            validity = new byte[(len + 7) >> 3];
                            validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                            int bytesToFill = i >> 3;
                            if (bytesToFill > 0) Unsafe.InitBlock(ref validRef, 0xFF, (uint)bytesToFill);
                            int remainingBits = i & 7;
                            if (remainingBits > 0) Unsafe.Add(ref validRef, bytesToFill) = (byte)((1 << remainingBits) - 1);
                        }
                        Unsafe.Add(ref dstRef, i) = Int128.Zero;
                    }
                }
            }

            return (values, validity, maxScale);
        }
    }