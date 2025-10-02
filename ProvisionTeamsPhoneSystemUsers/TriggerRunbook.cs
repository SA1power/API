using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace ProvisionTeamsPhoneSystemUsers;

public class TriggerRunbook
{
    private readonly ILogger<TriggerRunbook> _logger;

    public TriggerRunbook(ILogger<TriggerRunbook> logger)
    {
        _logger = logger;
    }

    [Function("TriggerRunbook")]
    public IActionResult Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequest req)
    {
        _logger.LogInformation("C# HTTP trigger function processed a request.");
        return new OkObjectResult("Welcome to Azure Functions!");
    }
}
