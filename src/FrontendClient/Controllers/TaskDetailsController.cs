using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using FrontendClient.Models;
using Grpc.Net.Client;
using BackendApi;

namespace FrontendClient.Controllers
{
    public class TaskDetailsController : Controller
    {
        private readonly ILogger<HomeController> _logger;

        public TaskDetailsController(ILogger<HomeController> logger)
        {
            _logger = logger;
            AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
        }

        public async Task<IActionResult> Index(string JobId)
        {
            using var channel = GrpcChannel.ForAddress("http://" + Environment.GetEnvironmentVariable("BACKEND_API_HOST") + ":5000");
            var client = new Job.JobClient(channel);
            var reply = await client.GetProcessingResultAsync(new GetProcessingResultRequest { Id = JobId });
            return View("Task", new TaskViewModel { Id = JobId, Rank = reply.Rank, Status = reply.Status });
        }
    }
}
