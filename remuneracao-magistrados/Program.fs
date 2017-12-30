open System
open System.IO
open System.Text
open FSharp.Data
open FSharp.Collections.ParallelSeq
open NPOI.SS.UserModel
open NPOI.HSSF.UserModel  
open Newtonsoft.Json

type JsonItem = { 
    CPF : string;
    Nome : string;
    Cargo : string;
    Lotacao : string;
    Subsidio : string;
    DireitosPessoais : string;
    Indenizacoes : string;
    DireitosEventuais : string;
    TotaldeRendimentos : string;
    PrevidenciaPublica : string;
    ImpostodeRenda : string;
    DescontosDiversos : string;
    RetençãoporTetoConstitucional : string;
    TotaldeDescontos : string;
    RendimentoLiquido : string;
    Remuneraçãodoórgãodeorigem : string;
    Diarias : string;
    Orgao: string;
    AnoReferencia: string;
    AnoPublicacao: string
 }

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

let downloadFile url = 
    
    try
        printfn "Download From Url %s" url

        let filename = ".data/download/" + Path.GetFileName url
        let request = Http.AsyncRequestStream(url, timeout=1000*60*2)
                      |> Async.RunSynchronously
        
        use outputFile = new FileStream(filename, FileMode.Create)
        do request.ResponseStream.CopyTo(outputFile)
        
        Some(filename)

    with e ->
        printfn "Erro: %A" e.Message
        None
    
let formatCell (x:CellValue) =

    if isNull x  then
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

    let validateSeq x = Seq.exists (fun n -> n <> String.Empty) x
    match row with
    | row when Seq.head row = "***.***.***-**" && 
               Seq.length row >= 17 &&
               validateSeq part -> true
    | _ -> false

let readFile filePath =

    printfn "Map File %s" filePath
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

let saveFile data =
    
    printfn "Print File"

    let outFile = new StreamWriter(".data/data.json", false, Encoding.UTF8)

    try
        let jsonString = JsonConvert.SerializeObject(data, Formatting.Indented)
        outFile.WriteLine(sprintf "%A," jsonString)
    with e ->
        printfn "%s" e.Message
    
    outFile.Close()

let transformToJson (data: seq<seq<string>>) =
    
    seq { 
        for item in data do
            yield { 
                CPF = Seq.item(0) item;
                Nome = Seq.item(1) item;
                Cargo = Seq.item(2) item;
                Lotacao = Seq.item(3) item;
                Subsidio = Seq.item(4) item;
                DireitosPessoais = Seq.item(5) item;
                Indenizacoes = Seq.item(6) item;
                DireitosEventuais = Seq.item(7) item;
                TotaldeRendimentos = Seq.item(8) item;
                PrevidenciaPublica = Seq.item(9) item;
                ImpostodeRenda = Seq.item(10) item;
                DescontosDiversos = Seq.item(11) item;
                RetençãoporTetoConstitucional = Seq.item(12) item;
                TotaldeDescontos = Seq.item(13) item;
                RendimentoLiquido = Seq.item(14) item;
                Remuneraçãodoórgãodeorigem = Seq.item(15) item;
                Diarias = Seq.item(16) item;
                Orgao = Seq.item(17) item;
                AnoReferencia = Seq.item(18) item;
                AnoPublicacao = Seq.item(19) item;
            }
        }


[<EntryPoint>]
let main argv =
    printfn "Start!"

    if not (Directory.Exists ".data") then
        Directory.CreateDirectory ".data" |> ignore
    if not (Directory.Exists ".data/download") then
        Directory.CreateDirectory ".data/download" |> ignore

    extractExcelLinksFromUrl  "http://www.cnj.jus.br/transparencia/remuneracao-dos-magistrados"
    |> PSeq.choose downloadFile
    |> PSeq.choose readFile
    |> PSeq.map transformToJson
    |> Seq.concat
    |> saveFile
    
    printfn "!End"
    0 