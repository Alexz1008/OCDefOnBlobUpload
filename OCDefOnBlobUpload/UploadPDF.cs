using Azure;
using Azure.AI.DocumentIntelligence;
using Azure.Core;
using Azure.Identity;
using Azure.Search.Documents.Indexes;
using Azure.Storage.Blobs;
using HttpMultipartParser;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using PdfSharp.Pdf;
using PdfSharp.Pdf.IO;
using System.Net;

namespace OCDefOnBlobUpload;

// NOTE: Although chunking and OCR can be done in here, Azure AI search has it built in so it's disabled.
// Without them enabled all this function does is upload the PDF and call the AI Search indexer.
public class UploadPDF
{
    private readonly ILogger<UploadPDF> _logger;
    private readonly string filesContainer = Environment.GetEnvironmentVariable("FILES_CONTAINER")!;
    private readonly string? managedIdentity = Environment.GetEnvironmentVariable("MANAGED_IDENTITY_CLIENT_ID");
    private readonly string accountName = Environment.GetEnvironmentVariable("STORAGE_ACCOUNT_NAME")!;
    private readonly string searchEndpoint = Environment.GetEnvironmentVariable("AZURE_SEARCH_ENDPOINT")!;
    private readonly string adminSearchKey = Environment.GetEnvironmentVariable("AZURE_SEARCH_ADMIN_KEY")!;
    private readonly string blobIndexerName = Environment.GetEnvironmentVariable("AZURE_BLOB_INDEXER_NAME")!;

    public UploadPDF(ILogger<UploadPDF> logger)
    {
        _logger = logger;
    }

    [Function(nameof(UploadPDF))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Function, "get", "post")] HttpRequestData req)
    {
        _logger.LogInformation("HTTP trigger function processed a request.");
        TokenCredential cred = managedIdentity != null ? new ManagedIdentityCredential(clientId: managedIdentity) : new VisualStudioCredential();
        var pdfUri = new Uri($"https://{accountName}.blob.core.windows.net/{filesContainer}");
        _logger.LogInformation("Successfully authenticated function");

        // Validate request
        if (!req.Method.Equals("POST", StringComparison.OrdinalIgnoreCase) ||
            !req.Headers.TryGetValues("Content-Type", out var contentTypes) ||
            !contentTypes.First().StartsWith("multipart/form-data", StringComparison.OrdinalIgnoreCase))
        {
            var badResponse = req.CreateResponse(HttpStatusCode.BadRequest);
            await badResponse.WriteStringAsync("Request must be multipart/form-data POST with a file.");
            return badResponse;
        }


        // Make sure all files are valid before proceeding
        var parser = await MultipartFormDataParser.ParseAsync(req.Body);
        foreach (var file in parser.Files)
        {
            if (file == null || file.Data == null || file.Data.Length == 0)
            {
                var badResponse = req.CreateResponse(HttpStatusCode.BadRequest);
                if (file != null && file.FileName != null)
                    await badResponse.WriteStringAsync($"File {file.FileName} is empty.");
                else
                    await badResponse.WriteStringAsync("One or more files are null.");
                return badResponse;
            }

            // May be removed later, checks if a file is a pdf
            else if (!file.FileName.EndsWith("pdf", StringComparison.OrdinalIgnoreCase))
            {
                var badResponse = req.CreateResponse(HttpStatusCode.BadRequest);
                await badResponse.WriteStringAsync($"File {file.FileName} is not a PDF.");
                return badResponse;
            }
        }

        // Extract case number from form data
        var caseNumberParam = parser.Parameters.FirstOrDefault(p => p.Name.Equals("caseNumber", StringComparison.OrdinalIgnoreCase));
        if (caseNumberParam == null || string.IsNullOrWhiteSpace(caseNumberParam.Data))
        {
            var badResponse = req.CreateResponse(HttpStatusCode.BadRequest);
            await badResponse.WriteStringAsync("Case number is required.");
            return badResponse;
        }
        string caseNumber = caseNumberParam.Data;
        _logger.LogInformation($"Processing upload for case number: {caseNumber}");

        const int maxPagesPerChunk = 1000;

        BlobContainerClient container = new BlobContainerClient(pdfUri, cred);
        _logger.LogInformation("Successfully connected to blob container " + container.AccountName + "/" + container.Name);
        foreach (var file in parser.Files)
        {
            var originalFileName = file.FileName ?? "uploaded_file.pdf";
            var fileExtension = Path.GetExtension(originalFileName);
            var fileNameWithoutExtension = Path.GetFileNameWithoutExtension(originalFileName);

            using var memoryStream = new MemoryStream();
            await file.Data.CopyToAsync(memoryStream);
            memoryStream.Position = 0;

            using var inputDoc = PdfReader.Open(memoryStream, PdfDocumentOpenMode.Import);
            int totalPages = inputDoc.PageCount;
            _logger.LogInformation($"PDF {originalFileName} has {totalPages} pages.");

            if (totalPages <= maxPagesPerChunk)
            {
                // Small enough — upload as a single file
                var fileName = $"{fileNameWithoutExtension}_CN_{caseNumber}{fileExtension}";
                await UploadPdfToBlob(container, fileName, inputDoc, 0, totalPages, caseNumber);
            }
            else
            {
                // Split into chunks of maxPagesPerChunk
                int totalChunks = (int)Math.Ceiling((double)totalPages / maxPagesPerChunk);
                _logger.LogInformation($"Splitting {originalFileName} into {totalChunks} chunks of up to {maxPagesPerChunk} pages.");
                for (int chunk = 0; chunk < totalChunks; chunk++)
                {
                    int startPage = chunk * maxPagesPerChunk;
                    int endPage = Math.Min(startPage + maxPagesPerChunk, totalPages);
                    var fileName = $"{fileNameWithoutExtension}_CN_{caseNumber}_part{chunk + 1}{fileExtension}";
                    await UploadPdfToBlob(container, fileName, inputDoc, startPage, endPage, caseNumber);
                }
            }
        }

        _logger.LogInformation("Completed upload, calling Azure AI Search indexer...");
        SearchIndexerClient indexerClient = new SearchIndexerClient(new Uri(searchEndpoint), new AzureKeyCredential(adminSearchKey));
        try
        {
            await indexerClient.RunIndexerAsync(blobIndexerName);
        }
        catch (RequestFailedException ex) when (ex.Status == 429)
        {
            _logger.LogWarning("Indexer already running: {0}", ex.Message);
        }

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteStringAsync("Upload and indexing complete!");
        return response;
    }

    private async Task UploadPdfToBlob(BlobContainerClient container, string fileName, PdfDocument sourceDoc, int startPage, int endPage, string caseNumber)
    {
        using var chunkDoc = new PdfDocument();
        for (int i = startPage; i < endPage; i++)
        {
            chunkDoc.AddPage(sourceDoc.Pages[i]);
        }

        using var uploadStream = new MemoryStream();
        chunkDoc.Save(uploadStream);
        uploadStream.Position = 0;

        BlobClient blob = container.GetBlobClient(fileName);
        _logger.LogInformation($"Uploading {fileName} ({endPage - startPage} pages) for archiving...");
        await blob.UploadAsync(uploadStream, overwrite: true);
        var tags = new Dictionary<string, string>
        {
            { "CaseNumber", caseNumber }
        };
        await blob.SetTagsAsync(tags);
        _logger.LogInformation($"Upload of {fileName} complete.");
    }
}
