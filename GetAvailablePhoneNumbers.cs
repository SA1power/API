using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace AvailablePhoneNumberAPI;

public class GetAvailablePhoneNumbers
{
    private readonly ILogger<GetAvailablePhoneNumbers> _logger;

    public GetAvailablePhoneNumbers(ILogger<GetAvailablePhoneNumbers> logger)
    {
        _logger = logger;
    }

    [Function("GetAvailablePhoneNumbers")]
    public IActionResult Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequest req)
    {
        _logger.LogInformation("C# HTTP trigger function processed a request.");
        return new OkObjectResult("Welcome to Azure Functions!");
    }
}
