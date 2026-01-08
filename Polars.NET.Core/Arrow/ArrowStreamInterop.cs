using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;
using Apache.Arrow.C;
using Apache.Arrow;

namespace Polars.NET.Core.Arrow
{
    /// <summary>
    /// Arrow Stream FFI InterOp Logic
    /// </summary>
    public static unsafe class ArrowStreamInterop
    {
        // Context Object
        private class ScanContext
        {
            public Func<IEnumerator<Apache.Arrow.RecordBatch>> Factory = default!;
            public Apache.Arrow.Schema Schema = default!;
        }

        /// <summary>
        /// Perpare lazy scan context，then return GCHandle pointed to context.
        /// </summary>
        public static void* CreateScanContext<T>(IEnumerable<T> data, int batchSize, Apache.Arrow.Schema schema)
        {
            var context = new ScanContext
            {
                Factory = () => data.ToArrowBatches(batchSize).GetEnumerator(),
                Schema = schema
            };

            var gcHandle = GCHandle.Alloc(context);
            return (void*)GCHandle.ToIntPtr(gcHandle);
        }

        // ---------------------------------------------------------
        // Static Callback Delegate (for Rust callback)
        // ---------------------------------------------------------

        // Get steramfactory callback function pointer
        public static delegate* unmanaged[Cdecl]<void*, CArrowArrayStream*> GetFactoryCallback()
        {
            return &StreamFactoryCallbackStatic;
        }

        // Get destroy context callback function pointer
        public static delegate* unmanaged[Cdecl]<void*, void> GetDestroyCallback()
        {
            return &DestroyScanContextStatic;
        }

        [UnmanagedCallersOnly(CallConvs = new[] { typeof(CallConvCdecl) })]
        private static CArrowArrayStream* StreamFactoryCallbackStatic(void* userData)
        {
            try
            {
                // Resume Context
                var handle = GCHandle.FromIntPtr((IntPtr)userData);
                var context = (ScanContext)handle.Target!;
                
                // Create
                var enumerator = context.Factory();
                
                // Alloc C Struct at Heap
                var ptr = (CArrowArrayStream*)Marshal.AllocHGlobal(sizeof(CArrowArrayStream));
                
                // Init Exporter and export
                var exporter = new ArrowStreamExporter(enumerator, context.Schema);
                exporter.Export(ptr);
                
                return ptr;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Polars.NET Critical] Error in Stream Factory Callback: {ex}");
                return null;
            }
        }

        [UnmanagedCallersOnly(CallConvs = new[] { typeof(CallConvCdecl) })]
        private static void DestroyScanContextStatic(void* userData)
        {
            try
            {
                var ptr = (IntPtr)userData;
                if (ptr != IntPtr.Zero)
                {
                    var handle = GCHandle.FromIntPtr(ptr);
                    if (handle.IsAllocated)
                    {
                        handle.Free(); 
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Polars.NET Critical] Error in Destroy Callback: {ex}");
            }
        }

        
        // ---------------------------------------------------------
        // Eager Mode
        // ---------------------------------------------------------

        /// <summary>
        /// Eager Mode: Alloc C struct at current stack frame and call Rust to consume
        /// </summary>
        public static DataFrameHandle ImportEager(IEnumerator<Apache.Arrow.RecordBatch> stream, Apache.Arrow.Schema schema)
        {
            // Alloc Struct at Stack
            var cStream = new CArrowArrayStream();

            // Init Exporter
            using var exporter = new ArrowStreamExporter(stream, schema);
            
            exporter.Export(&cStream);

            // Call Rust
            return PolarsWrapper.DataFrameNewFromStream(&cStream);
        }

        public static void* CreateDirectScanContext(
            Func<IEnumerator<Apache.Arrow.RecordBatch>> factory, 
            Apache.Arrow.Schema schema)
        {
            var context = new ScanContext
            {
                Factory = factory,
                Schema = schema
            };

            var gcHandle = GCHandle.Alloc(context);
            return (void*)GCHandle.ToIntPtr(gcHandle);
        }

        public static DataFrameHandle ImportEager(IEnumerable<RecordBatch> stream)
        {
            var enumerator = stream.GetEnumerator();

            // Peek first frame
            if (!enumerator.MoveNext())
            {
                // null stream
                enumerator.Dispose();
                return new DataFrameHandle(); // Return invalid handle (IsInvalid == true)
            }

            // Get Schema
            var firstBatch = enumerator.Current;
            var schema = firstBatch.Schema;

            // Use PrependEnumerator to create enumerator
            var combinedEnumerator = new PrependEnumerator(firstBatch, enumerator);

            // Call Rust
            return ImportEager(combinedEnumerator, schema);
        }
        /// <summary>
        /// Logic For Lazy Scan
        /// </summary>
        public static LazyFrameHandle ScanStream(
            Func<IEnumerator<RecordBatch>> streamFactory, 
            Schema schema)
        {
            var userData = CreateDirectScanContext(streamFactory, schema);

            var cSchema = CArrowSchema.Create();
            CArrowSchemaExporter.ExportSchema(schema, cSchema);

            try
            {
                return PolarsWrapper.LazyFrameScanStream(
                    cSchema,
                    GetFactoryCallback(),
                    GetDestroyCallback(),
                    userData
                );
            }
            finally
            {
                CArrowSchema.Free(cSchema);
            }
        }
        // ------------------------------------------------------------
        // Sink to DataBase
        // ------------------------------------------------------------
        // ------------------------------------------------------------
        // Delegates
        // ------------------------------------------------------------

        // Rust: fn(*mut ArrowArray, *mut ArrowSchema, *mut char) -> i32
        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        public delegate int SinkCallback(
            CArrowArray* array, 
            CArrowSchema* schema, 
            byte* errorMsg
        );

        // Rust: fn(*mut c_void)
        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        public delegate void CleanupCallback(void* userData);

        // ------------------------------------------------------------
        // Context
        // ------------------------------------------------------------

        /// <summary>
        /// For Delegate lifecycle and delivery User Action
        /// </summary>
        private class SinkContext
        {
            public Action<RecordBatch> UserAction = null!;
            public SinkCallback KeepAliveCallback = null!; 
        }

        // ------------------------------------------------------------
        // Static Factory
        // ------------------------------------------------------------

        /// <summary>
        /// Prepare sink native resource
        /// </summary>
        public static (SinkCallback, CleanupCallback, IntPtr) PrepareSink(Action<RecordBatch> onBatchReceived)
        {
            // Build Context
            var ctx = new SinkContext
            {
                UserAction = onBatchReceived
            };

            // Define Native Callbacl (Pointer -> C# Object)
            ctx.KeepAliveCallback = (arrPtr, schemaPtr, errPtr) =>
            {
                try
                {
                    var schema = CArrowSchemaImporter.ImportSchema(schemaPtr);
                    var batch = CArrowArrayImporter.ImportRecordBatch(arrPtr, schema);
                    ctx.UserAction(batch);
                    return 0; // Success
                }
                catch (Exception ex)
                {
                    var msgBytes = System.Text.Encoding.UTF8.GetBytes(ex.Message);
                    int len = Math.Min(msgBytes.Length, 1023);
                    Marshal.Copy(msgBytes, 0, (IntPtr)errPtr, len);
                    errPtr[len] = 0;
                    return 1; // Error
                }
            };

            // Pack UserData (GCHandle)
            var handle = GCHandle.Alloc(ctx);
            IntPtr userDataPtr = GCHandle.ToIntPtr(handle);

            // Define Cleanup Callback
            CleanupCallback cleanup = (ptr) =>
            {
                var h = GCHandle.FromIntPtr((IntPtr)ptr);
                if (h.IsAllocated) h.Free();
            };

            return (ctx.KeepAliveCallback, cleanup, userDataPtr);
        }
    }
}