#r "System.Xml.Linq"
open System
open System.IO
open System.Reflection
open System.Xml.Linq
open System.Text.RegularExpressions
open System.Collections.Generic

// ================= 配置区 =================
let dllPath = "Polars.FSharp/bin/Release/net8.0/Polars.FSharp.dll"
let xmlPath = "Polars.FSharp/bin/Release/net8.0/Polars.FSharp.xml"
let outputDir = "docs/api/fsharp_generated"

// 目标模块
let targetTypes = ["pl"; "Series"; "DataFrame"; "Expr";"LazyFrame"] 
// =========================================

// 1. 依赖解析
let binDir = Path.GetDirectoryName(Path.GetFullPath(dllPath))
AppDomain.CurrentDomain.add_AssemblyResolve(fun _ args ->
    let assemblyName = (new AssemblyName(args.Name)).Name
    let depPath = Path.Combine(binDir, assemblyName + ".dll")
    if File.Exists(depPath) then Assembly.LoadFrom(depPath) else null
)

// ---------------------------------------------------------
// 2. XML 解析逻辑 (增强版)
// ---------------------------------------------------------
type DocMetadata = {
    Summary: string
    Params: Dictionary<string, string> 
    Returns: string
    Remarks: string
    Example: string
}

// 辅助：清洗 XML 内容转 Markdown
let cleanXml (raw: string) =
    if String.IsNullOrWhiteSpace(raw) then ""
    else
        raw
        |> fun s -> Regex.Replace(s, @"\s+", " ") // 压缩连续空格
        |> fun s -> Regex.Replace(s, @"<c>(.*?)</c>", "`$1`") 
        |> fun s -> Regex.Replace(s, @"<code>(.*?)</code>", "\n```\n$1\n```\n")
        |> fun s -> Regex.Replace(s, @"<para>(.*?)</para>", "\n\n$1\n\n")
        |> fun s -> Regex.Replace(s, @"<see cref=""(?:.*?\.)?([^""]+)""\s*/>", "`$1`")
        |> fun s -> Regex.Replace(s, @"<[^>]+>", "") // 移除剩余 HTML 标签
        |> fun s -> s.Trim()

// 【新增】专门清洗表格内容，防止表格炸裂
let cleanForTable (raw: string) =
    if String.IsNullOrWhiteSpace(raw) then "-"
    else
        // F# 链式调用时，点号必须对齐或缩进，且尽量不要在中间插注释
        raw.Replace("|", "&#124;")
           .Replace("\n", "<br>")
           .Replace("\r", "")

let parseDocContent (xmlContent: string) =
    let extract tag = 
        let m = Regex.Match(xmlContent, sprintf @"<%s>(.*?)</%s>" tag tag, RegexOptions.Singleline)
        if m.Success then cleanXml m.Groups.[1].Value else ""

    let paramDict = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) // 参数名忽略大小写
    let paramMatches = Regex.Matches(xmlContent, @"<param name=""([^""]+)"">(.*?)</param>", RegexOptions.Singleline)
    for pm in paramMatches do
        let name = pm.Groups.[1].Value
        let desc = cleanXml pm.Groups.[2].Value // 这里保留原始格式，生成表格时再 cleanForTable
        if not (paramDict.ContainsKey(name)) then paramDict.Add(name, desc)

    {
        Summary = extract "summary"
        Params = paramDict
        Returns = extract "returns"
        Remarks = extract "remarks"
        Example = extract "example"
    }

// 建立索引
if Directory.Exists(outputDir) then Directory.Delete(outputDir, true)
Directory.CreateDirectory(outputDir) |> ignore

if not (File.Exists(xmlPath)) then
    printfn "❌ XML File not found: %s" xmlPath
    exit 1

let rawXmlContent = File.ReadAllText(xmlPath)
let memberRegex = Regex(@"<member name=""([^""]+)"">(.*?)</member>", RegexOptions.Singleline)
let matches = memberRegex.Matches(rawXmlContent)

// 索引 Key 改为【小写】以忽略大小写差异
let xmlLookup = new Dictionary<string, ResizeArray<string * DocMetadata>>()

printfn "🔍 Indexing XML..."

for m in matches do
    let fullId = m.Groups.[1].Value
    let content = m.Groups.[2].Value
    
    // 提取方法名
    let noParams = match fullId.IndexOf('(') with | -1 -> fullId | i -> fullId.Substring(0, i)
    let noGeneric = Regex.Replace(noParams, @"``\d+", "")
    let methodName = 
        let lastDot = noGeneric.LastIndexOf('.')
        if lastDot >= 0 && lastDot < noGeneric.Length - 1 then 
            noGeneric.Substring(lastDot + 1)
        else noGeneric

    // 【关键修改】Key 转小写
    let lowerKey = methodName.ToLowerInvariant()

    let docData = parseDocContent content

    if not (xmlLookup.ContainsKey(lowerKey)) then
        xmlLookup.Add(lowerKey, new ResizeArray<string * DocMetadata>())
    xmlLookup.[lowerKey].Add((fullId, docData))

printfn "✅ Indexed %d unique method names." xmlLookup.Count

// ---------------------------------------------------------
// 3. 生成 Markdown
// ---------------------------------------------------------
let loadAssembly (path: string) =
    try Assembly.LoadFrom(path)
    with ex -> printfn "Warning: %s" ex.Message; null

let assembly = loadAssembly dllPath
if assembly = null then exit 1

let rec formatType (t: Type) =
    if t.IsGenericType then
        let name = t.Name.Substring(0, t.Name.IndexOf('`'))
        let args = t.GetGenericArguments() |> Array.map formatType |> String.concat ", "
        // HTML 转义，防止在表格里被当成标签
        sprintf "%s&lt;%s&gt;" name args 
    elif t.Name = "FSharpFunc`2" then
        let args = t.GetGenericArguments()
        sprintf "%s -> %s" (formatType args.[0]) (formatType args.[1])
    else
        t.Name

let generateDocForType (typeName: string) =
    try
        let typeInfo = assembly.GetTypes() |> Seq.tryFind (fun t -> t.Name = typeName)
        match typeInfo with
        | None -> printfn "⚠️ Type '%s' not found in assembly" typeName
        | Some t ->
            printfn "📝 Generating rich docs for: %s" typeName
            let sb = System.Text.StringBuilder()
            
            sb.AppendLine("---") |> ignore
            sb.AppendLine(sprintf "uid: Polars.FSharp.%s" typeName) |> ignore
            sb.AppendLine(sprintf "title: %s" typeName) |> ignore
            sb.AppendLine("---") |> ignore
            sb.AppendLine() |> ignore
            sb.AppendLine(sprintf "# %s" typeName) |> ignore
            sb.AppendLine(sprintf "Namespace: `%s`" t.Namespace) |> ignore
            sb.AppendLine() |> ignore
            
            let methods = 
                t.GetMethods(BindingFlags.Public ||| BindingFlags.Static ||| BindingFlags.Instance)
                |> Seq.filter (fun m -> 
                    not (m.Name.StartsWith("get_") || m.Name.StartsWith("set_")) &&
                    m.DeclaringType.Name = typeName)
                |> Seq.sortBy (fun m -> m.Name)

            for m in methods do
                sb.AppendLine(sprintf "## %s" m.Name) |> ignore
                
                let mutable doc = { Summary = "*No documentation found.*"; Params = new Dictionary<string,string>(); Returns=""; Remarks=""; Example="" }
                
                // 【关键修改】使用小写 Key 进行查找
                let searchKey = m.Name.ToLowerInvariant()

                if xmlLookup.ContainsKey(searchKey) then
                    let candidates = xmlLookup.[searchKey]
                    // 模糊匹配：尽量找包含当前类名的 XML 记录 (忽略大小写)
                    let bestMatch = 
                        candidates 
                        |> Seq.tryFind (fun (id, _) -> 
                            let idLower = id.ToLowerInvariant()
                            let typeLower = typeName.ToLowerInvariant()
                            idLower.Contains("." + typeLower + ".") || idLower.Contains("." + typeLower + "`"))
                    
                    match bestMatch with
                    | Some (_, d) -> doc <- d
                    | None -> if candidates.Count > 0 then doc <- snd candidates.[0]

                // 1. Summary
                sb.AppendLine() |> ignore
                sb.AppendLine(doc.Summary) |> ignore
                sb.AppendLine() |> ignore

                // 2. Signature
                sb.AppendLine("```fsharp") |> ignore
                let paramsInfo = m.GetParameters()
                let paramsStr = 
                    try
                        paramsInfo
                        |> Array.map (fun p -> sprintf "%s: %s" p.Name (formatType p.ParameterType))
                        |> String.concat " -> "
                    with _ -> "..."
                let returnType = formatType m.ReturnType
                let prefix = if m.IsStatic then "" else "(instance) "
                sb.AppendLine(sprintf "// Signature:") |> ignore
                sb.AppendLine(sprintf "%s%s -> %s" prefix paramsStr returnType) |> ignore
                sb.AppendLine("```") |> ignore

                // 3. Parameters Table
                if paramsInfo.Length > 0 then
                    sb.AppendLine() |> ignore
                    sb.AppendLine("**Parameters**") |> ignore
                    sb.AppendLine() |> ignore
                    sb.AppendLine("| Name | Type | Description |") |> ignore
                    sb.AppendLine("| :--- | :--- | :--- |") |> ignore
                    
                    for p in paramsInfo do
                        let pName = p.Name
                        let pType = formatType p.ParameterType
                        
                        // 查找描述 (参数名也忽略大小写)
                        let rawDesc = 
                            if doc.Params.ContainsKey(pName) then doc.Params.[pName]
                            else "-"
                        
                        // 【关键修改】清洗描述，防止表格炸裂
                        let tableDesc = cleanForTable rawDesc
                        
                        sb.AppendLine(sprintf "| `%s` | `%s` | %s |" pName pType tableDesc) |> ignore
                
                // 4. Returns
                if not (String.IsNullOrWhiteSpace(doc.Returns)) then
                    sb.AppendLine() |> ignore
                    sb.AppendLine("**Returns**") |> ignore
                    sb.AppendLine() |> ignore
                    sb.AppendLine(doc.Returns) |> ignore

                // 5. Examples
                if not (String.IsNullOrWhiteSpace(doc.Example)) then
                    sb.AppendLine() |> ignore
                    sb.AppendLine("**Example**") |> ignore
                    sb.AppendLine() |> ignore
                    sb.AppendLine(doc.Example) |> ignore

                // 6. Remarks
                if not (String.IsNullOrWhiteSpace(doc.Remarks)) then
                    sb.AppendLine() |> ignore
                    sb.AppendLine("> [!NOTE]") |> ignore
                    sb.AppendLine("> " + doc.Remarks.Replace("\n", "\n> ")) |> ignore

                sb.AppendLine() |> ignore
                sb.AppendLine("---") |> ignore

            File.WriteAllText(Path.Combine(outputDir, sprintf "%s.md" typeName), sb.ToString())
    with ex ->
        printfn "❌ Error processing %s: %s" typeName ex.Message

for t in targetTypes do
    generateDocForType t

let sbToc = System.Text.StringBuilder()
for t in targetTypes do
    sbToc.AppendLine(sprintf "- name: %s" t) |> ignore
    sbToc.AppendLine(sprintf "  href: %s.md" t) |> ignore
File.WriteAllText(Path.Combine(outputDir, "toc.yml"), sbToc.ToString())

printfn "🎉 Done! Rich documentation generated."