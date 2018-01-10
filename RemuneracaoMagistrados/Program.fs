open System
open System.IO
open System.Text
open FSharp.Data
open NPOI.SS.UserModel
open NPOI.HSSF.UserModel

let extractExcelLinksFromUrl url = 

    printfn "Get Links From %s" url

    let results = HtmlDocument.Load(url)

    results.Descendants ["a"]
    |> Seq.choose (fun x -> 
           x.TryGetAttribute("href")
           |> Option.map (fun a -> a.Value())
    )
    |> Seq.choose (fun x ->
                    match x with
                    | x when x.EndsWith(".xls") -> Some(x)
                    | _ -> None
                    )
    |> Seq.choose (fun x ->
                    match x with
                    | x when x.Contains("http://") -> Some(x)
                    | x -> Some("http://www.cnj.jus.br/" + x)
                    )

let downloadExcelFileFromUrl url = 
    
    printfn "Download Exel File From Url %s" url

    try

        let filename = ".data/download/" + Path.GetFileName url

        if not (File.Exists filename) then

            let request = Http.AsyncRequestStream(url, timeout=1000*60*2)
                          |> Async.RunSynchronously
        
            use outputFile = new FileStream(filename, FileMode.Create)
            do request.ResponseStream.CopyTo(outputFile)
        
        Some(filename)

    with e ->
        printfn "Erro: %A" e.Message
        None
    
let formatCell (x:CellValue) =

    if isNull x then
        ""
    else
        match x.CellType with
        | CellType.Numeric when x.NumberValue = float 0 -> ""
        | CellType.Numeric -> x.NumberValue.ToString()
        | CellType.Boolean -> x.BooleanValue.ToString()
        | CellType.String  -> x.StringValue
        | _ -> ""

let validRow row = 

    let part = row |> Seq.filter (fun x-> x <> "***.***.***-**") 
    
    match row with
    | row when Seq.head row = "***.***.***-**" && 
               Seq.length row >= 17 &&
               Seq.exists (fun n -> n <> String.Empty) part -> true
    | _ -> false

let extractFileData filePath =

    printfn "Extracting data from file %s" filePath

    let fs = new FileStream(filePath, FileMode.Open, FileAccess.Read)
    let workbook = HSSFWorkbook fs
    let formulaEvaluator = workbook.GetCreationHelper().CreateFormulaEvaluator()

    try

        formulaEvaluator.EvaluateAll()
        let sheet = workbook.GetSheet "Contracheque"

        let orgao = (sheet.GetRow 15).Cells.Item 3 |> formulaEvaluator.Evaluate |> formatCell
        let anoReferencia = ((sheet.GetRow 16).Cells.Item 3).DateCellValue.ToShortDateString() 
        let anoPublicacao = ((sheet.GetRow 17).Cells.Item 3).DateCellValue.ToShortDateString() 
        let metadata = [|orgao; anoReferencia; anoPublicacao|] :> seq<string>

        let result = seq{ for i in 1..sheet.LastRowNum do

                            let row = sheet.GetRow(i-1)

                            let cellsAsString = Seq.map (formulaEvaluator.Evaluate >> 
                                                        (fun x -> match x with
                                                                  | null -> ""
                                                                  | _ -> formatCell(x))) row.Cells
                                                              
                            if Seq.length cellsAsString >= 17 then
                                yield Seq.take 17 cellsAsString
                        }
                        |> Seq.filter validRow
                        |> Seq.map (fun x -> Seq.concat [x;metadata])

        Some(result)

    with e ->
        printfn "%s" e.Message
        None

let saveData (data: seq<seq<string>>) =
    
    printfn "Save data as CSV"
    let outFile = new StreamWriter(".data/data.csv", false, Encoding.UTF8)

    let firstRow = "CPF,Nome,Cargo,Lotacao,Subsidio,DireitosPessoais,Indenizacoes,DireitosEventuais,TotaldeRendimentos,PrevidenciaPublica,ImpostodeRenda,DescontosDiversos,RetençãoporTetoConstitucional,TotaldeDescontos,RendimentoLiquido,Remuneraçãodoórgãodeorigem,Diarias,Orgao,AnoReferencia,AnoPublicacao"
    try
        outFile.WriteLine(sprintf "%s," firstRow)
        data 
        |> Seq.iter(fun x -> 
                        let row = System.String.Join(",", x)
                        outFile.WriteLine(sprintf "%s," row)
                    )
    with e ->
        printfn "%s" e.Message
    
    outFile.Close()

[<EntryPoint>]
let main argv =

    printfn "Start!"

    if not (Directory.Exists ".data") then
        Directory.CreateDirectory ".data" |> ignore
    if not (Directory.Exists ".data/download") then
        Directory.CreateDirectory ".data/download" |> ignore

    extractExcelLinksFromUrl  "http://www.cnj.jus.br/transparencia/remuneracao-dos-magistrados"
    |> Seq.choose downloadExcelFileFromUrl
    |> Seq.choose extractFileData
    |> Seq.concat
    |> saveData
    
    printfn "!End"
    0