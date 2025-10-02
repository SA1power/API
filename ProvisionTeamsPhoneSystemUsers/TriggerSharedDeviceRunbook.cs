using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace ProvisionTeamsPhoneSystemUsers;

public class TriggerSharedDeviceRunbook
{
    private readonly ILogger<TriggerSharedDeviceRunbook> _logger;

    public TriggerSharedDeviceRunbook(ILogger<TriggerSharedDeviceRunbook> logger)
    {
        _logger = logger;
    }

    [Function("TriggerSharedDeviceRunbook")]
    public IActionResult Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequest req)
    {
        _logger.LogInformation("C# HTTP trigger function processed a request.");
        return new OkObjectResult("Welcome to Azure Functions!");
    }
}
