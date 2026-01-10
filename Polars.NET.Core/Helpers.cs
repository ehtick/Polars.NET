using System.Text;

namespace Polars.NET.Core
{
    public static class DurationFormatter
    {
        /// <summary>
        /// Format C# Duration to Polars duration string
        /// </summary>
        public static string ToPolarsString(TimeSpan ts)
        {
            if (ts == TimeSpan.Zero) return "0s";

            var sb = new StringBuilder();

            // Polars suffixs: ns, us, ms, s, m, h, d, w
            
            // 1. Days (d)
            if (ts.Days > 0) sb.Append($"{ts.Days}d");

            // 2. Hours (h)
            if (ts.Hours > 0) sb.Append($"{ts.Hours}h");

            // 3. Minutes (m)
            if (ts.Minutes > 0) sb.Append($"{ts.Minutes}m");

            // 4. Seconds (s)
            if (ts.Seconds > 0) sb.Append($"{ts.Seconds}s");

            // 5. Milliseconds (ms)
            if (ts.Milliseconds > 0) sb.Append($"{ts.Milliseconds}ms");

            // 6. [New in .NET 7+] Microseconds (us)
            if (ts.Microseconds > 0) sb.Append($"{ts.Microseconds}us");

            // 7. [New in .NET 7+] Nanoseconds (ns)
            if (ts.Nanoseconds > 0) sb.Append($"{ts.Nanoseconds}ns");

            return sb.ToString();
        }
        [return: System.Diagnostics.CodeAnalysis.NotNullIfNotNull(nameof(ts))]
        public static string? ToPolarsString(TimeSpan? ts)
        {
            return ts.HasValue ? ToPolarsString(ts.Value) : null;
        }
    }
    public static class PolarsExtensions
    {
        // ts.ToPolarsDuration()
        public static string ToPolarsDuration(this TimeSpan ts) 
            => DurationFormatter.ToPolarsString(ts);

        // tsNullable.ToPolarsDuration()
        public static string? ToPolarsDuration(this TimeSpan? ts) 
            => DurationFormatter.ToPolarsString(ts);
    }
    public static class ReflectionHelper
    {
        public static Type GetEnumerableElementType(Type collectionType)
        {
            if (collectionType.IsArray)
            {
                return collectionType.GetElementType()!;
            }

            if (collectionType.IsGenericType && collectionType.GetGenericTypeDefinition() == typeof(IEnumerable<>))
            {
                return collectionType.GetGenericArguments()[0];
            }

            foreach (var iface in collectionType.GetInterfaces())
            {
                if (iface.IsGenericType && iface.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                {
                    return iface.GetGenericArguments()[0];
                }
            }

            throw new ArgumentException($"Type {collectionType.Name} is not an IEnumerable<T>");
        }
    }
}