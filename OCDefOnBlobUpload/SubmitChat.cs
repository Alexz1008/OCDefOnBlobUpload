using Azure;
using Azure.AI.OpenAI;
using Azure.Core;
using Azure.Identity;
using Azure.Search.Documents;
using Azure.Search.Documents.Models;
using Azure.Storage.Blobs;
using Markdig;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using OpenAI.Chat;
using System.ClientModel;
using System.Net;
using System.Text.Json;


namespace OCDefOnBlobUpload;

// NOTE: Although search embedding can be done here, Azure AI search does this automatically.
// Without that enabled all this function does is connect to gpt-4o and feed the results of the vector search into it.
public class SubmitChat
{
    private readonly ILogger<SubmitChat> _logger;
    private readonly MarkdownPipeline _markdownPipeline;

    private readonly string? managedIdentity = Environment.GetEnvironmentVariable("MANAGED_IDENTITY_CLIENT_ID");
    private readonly string chatHistoryContainer = Environment.GetEnvironmentVariable("CHAT_HISTORY_CONTAINER")!;
    private readonly string openAiKey = Environment.GetEnvironmentVariable("AZURE_OPENAI_KEY")!;
    private readonly string openAiEndpoint = Environment.GetEnvironmentVariable("AZURE_OPENAI_ENDPOINT")!;
    private readonly string accountName = Environment.GetEnvironmentVariable("STORAGE_ACCOUNT_NAME")!;
    private readonly string searchEndpoint = Environment.GetEnvironmentVariable("AZURE_SEARCH_ENDPOINT")!;
    private readonly string searchIndex = Environment.GetEnvironmentVariable("AZURE_SEARCH_INDEX")!;
    private readonly string searchKey = Environment.GetEnvironmentVariable("AZURE_SEARCH_KEY")!;
    private readonly string blobSuffix = string.Equals(Environment.GetEnvironmentVariable("IS_GCC"), "true", StringComparison.OrdinalIgnoreCase)
        ? "blob.core.usgovcloudapi.net" : "blob.core.windows.net";
    private readonly string systemPrompt = @"##  W&A Assistant Guidelines
        > You are the “W&A Assistant”. You are **NOT** a lawyer and must **never** draft legal documents or create legal arguments.

        ----------

        ### SCOPE

        **Only permitted to:**

        -   (a) Summarize provided transcripts and case files
        -   (b) Answer questions using those same files
        -   (c) Extract entities, dates, events, and citations to the exact passages
        -   (d) Assess strengths/weaknesses, potential appeal grounds (e.g., judicial errors, overlooked defenses) from those files

        **Never permitted to:**

        -   Generate original legal content
        -   Draft appeals, motions, briefs, letters, or emails
        -   Write recommendations
        -   Speculate about law or case strategy

        ----------

        ### GROUNDING

        -   Answer **only** with information grounded in the retrieved documents and official government documents.
        -   For every non-trivial answer, include **inline citations**:  
            `[DocName, page/section]`
        -   If the answer **cannot be found** in the provided materials, reply:
    
            > I can’t find that in the case materials. Please upload a source or point me to the relevant document.

        ----------

        ### STYLE & NAMING

        -   Refer to yourself **only** as “Assistant”
        -   Do **not** use the words AI, Copilot, Agent, or model
        -   Be **neutral, concise, and factual**
        -   No creative rewriting or embellishment

        ----------

        ### SAFETY

        If asked to create, rewrite, or improve any legal content (e.g., appeals, motions, briefs, letters, emails), respond with:

        > I can’t do that. I’m limited to summarizing and answering questions directly from the uploaded case materials. I can help you find the relevant passages or produce a factual summary with citations.

        If asked for legal interpretation or advice beyond the documents, **refuse** and suggest consulting counsel.

        Follow content safety:

        -   Do **not** output hateful, sexual, or self-harm content
        -   Handle violent content **factually and neutrally** when it appears in source materials

        ----------

        ### OUTPUT LIMITS

        -   Temperature ≤ 0.2
        -   Max 512 output tokens unless summarizing multi-document bundles
        -   No links to the open web
        -   No tools other than court‑approved indexes";


    public SubmitChat(ILogger<SubmitChat> logger)
    {
        _logger = logger;
        _markdownPipeline = new MarkdownPipelineBuilder().UseAdvancedExtensions().Build();
    }

    [Microsoft.Azure.Functions.Worker.Function(nameof(SubmitChat))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Function, "get", "post")] HttpRequestData req)
    {
        // Read in JSON request
        string body = await new StreamReader(req.Body).ReadToEndAsync();
        ChatRequest chatRequest;
        try
        {
            chatRequest = JsonSerializer.Deserialize<ChatRequest>(body, new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            })!;
            _logger.LogInformation($"Received chat request: {chatRequest.Message}");
        }
        catch
        {
            _logger.LogError($"Failed to parse chat request.");
            var errorResponse = req.CreateResponse(HttpStatusCode.InternalServerError);
            await errorResponse.WriteStringAsync("Failed to parse chat request. Submissions require a session ID, message, and case number.");
            return errorResponse;
        }

        // Search for user query with AI Search
        _logger.LogInformation("Connecting to Azure OpenAI...");
        var client = new AzureOpenAIClient(new Uri(openAiEndpoint), new AzureKeyCredential(openAiKey));
        var searchOptions = new SearchOptions
        {
            Size = 5,
            Select = { "chunk" },
            Filter = $"CaseNumber eq '{chatRequest.CaseNumber}'"
        };


        _logger.LogInformation("Connecting to Azure AI Search and sending query...");
        var searchClient = new SearchClient(new Uri(searchEndpoint), searchIndex, new AzureKeyCredential(searchKey));
        var searchResults = await searchClient.SearchAsync<SearchDocument>(chatRequest.Message, searchOptions);
        var relevantChunks = searchResults.Value.GetResults()
            .Select(r => r.Document["chunk"]?.ToString())
            .Where(s => !string.IsNullOrEmpty(s))
            .ToList();

        // Search for previous chat history
        _logger.LogInformation("Checking blob storage for existing chat history...");
        var historyUri = new Uri($"https://{accountName}.{blobSuffix}/{chatHistoryContainer}");
        TokenCredential cred = managedIdentity != null ? new ManagedIdentityCredential(clientId: managedIdentity) : new VisualStudioCredential();
        BlobContainerClient container = new BlobContainerClient(historyUri, cred);
        BlobClient blobClient = container.GetBlobClient(chatRequest.SessionId + ".json");
        ChatMessage[] messages = GetFreshMessages();
        ChatHistory chatHistory = GetFreshHistory();
        if (blobClient.Exists())
        {
            _logger.LogInformation("Found existing chat history for session " + chatRequest.SessionId);
            // Add previous chat history as context
            blobClient.DownloadContentAsync().ContinueWith(task =>
            {
                if (task.IsCompletedSuccessfully)
                {
                    // Read in JSON chat history
                    try
                    {
                        chatHistory = JsonSerializer.Deserialize<ChatHistory>(task.Result.Value.Content.ToString(), new JsonSerializerOptions
                        {
                            PropertyNameCaseInsensitive = true
                        })!;
                        messages = chatHistory.GetMessages();
                        _logger.LogInformation($"Successfully received chat history for session: {chatRequest.SessionId}");
                        foreach (var msg in chatHistory.Messages)
                        {
                            _logger.LogInformation($"History message - Role: {msg.Role}, Message: {msg.Message}");
                        }
                    }
                    catch
                    {
                        _logger.LogWarning($"Failed to parse chat history for session: {chatRequest.SessionId}");
                    }
                }
                else
                {
                    _logger.LogWarning("Failed to download chat history, starting fresh.");
                }
            }).Wait();
        }
        else
        {
            _logger.LogInformation("No previous chat history found, starting fresh.");
        }

        // Add AI search results as context, then add user query
        foreach (var relevantChunk in relevantChunks)
        {
            messages = messages.Append(new AssistantChatMessage($"Context: {relevantChunk}")).ToArray();
        }
        chatHistory.AddMessage(ChatHistory.Role.USER, chatRequest.Message);
        messages = messages.Append(new UserChatMessage($"User Question: {chatRequest.Message}")).ToArray();


        // Send in messages
        string answer;
        try
        {
            ChatClient chatClient = client.GetChatClient("gpt-4o");
            ClientResult<ChatCompletion> chatResult = chatClient.CompleteChatAsync(messages).Result;
            var raw = chatResult.Value.Content.FirstOrDefault()?.Text ?? "I'm sorry, I couldn't find an answer to your question.";
            answer = Markdown.ToHtml(raw, _markdownPipeline);

            // Add to chat history and save to blob
            chatHistory.AddMessage(ChatHistory.Role.BOT, answer);
            blobClient.UploadAsync(BinaryData.FromString(JsonSerializer.Serialize(chatHistory)), overwrite: true).Wait();
        }
        catch (Exception ex)
        {
            _logger.LogError($"Unexpected error when sending messages to openAI: {ex.Message}");
            var errorResponse = req.CreateResponse(HttpStatusCode.InternalServerError);
            await errorResponse.WriteStringAsync("Failed to send messages to openAI.");
            return errorResponse;
        }
        _logger.LogInformation("Completed and returned response.");
        // Return the answer to the frontend
        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteStringAsync(answer); // Replace with answer
        return response;
    }

    private ChatMessage[] GetFreshMessages() => new ChatMessage[]
    {
        new SystemChatMessage(systemPrompt)
    };

    private ChatHistory GetFreshHistory()
    {

        return new ChatHistory
        {
            Messages = new ChatHistory.HistoryMessage[]
            {
                new ChatHistory.HistoryMessage
                {
                    Role = ChatHistory.Role.SYSTEM,
                    Message = systemPrompt
                },
            }
        };
    }
}
