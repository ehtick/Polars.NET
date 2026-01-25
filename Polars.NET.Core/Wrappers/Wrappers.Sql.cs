using Polars.NET.Core.Native;

namespace Polars.NET.Core;

public static partial class PolarsWrapper
{
    public static SqlContextHandle SqlContextNew() 
        => ErrorHelper.Check(NativeBindings.pl_sql_context_new());

    public static void SqlRegister(SqlContextHandle ctx, string name, LazyFrameHandle lf)
    {
        UseUtf8String(name, namePtr => 
        {
            NativeBindings.pl_sql_context_register(ctx, namePtr, lf);
            
            lf.TransferOwnership();
            
            ErrorHelper.CheckVoid();
        });
    }

    public static LazyFrameHandle SqlExecute(SqlContextHandle ctx, string query)
    {
        return UseUtf8String(query, queryPtr => 
        {
            return ErrorHelper.Check(
                NativeBindings.pl_sql_context_execute(ctx, queryPtr)
            );
        });
    }
}