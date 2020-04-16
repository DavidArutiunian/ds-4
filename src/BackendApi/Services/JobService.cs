using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using NATS.Client;
using System.Text;
using StackExchange.Redis;
using BackendApi.Models;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace BackendApi.Services
{
    public class JobService : Job.JobBase, IDisposable
    {
        private static int MAX_RETRIES = 10;
        private static int SLEEP_TIMEOUT = 1000;
        private readonly static Dictionary<string, string> _jobs = new Dictionary<string, string>();
        private readonly ILogger<JobService> _logger;
        private readonly IConnection _nats;
        private readonly ConnectionMultiplexer _redis;

        public JobService(ILogger<JobService> logger)
        {
            _logger = logger;
            _nats = new ConnectionFactory().CreateConnection("nats://" + Environment.GetEnvironmentVariable("NATS_HOST"));
            _redis = ConnectionMultiplexer.Connect(Environment.GetEnvironmentVariable("REDIS_HOST"));
        }

        public override Task<RegisterResponse> Register(RegisterRequest request, ServerCallContext context)
        {
            string id = Guid.NewGuid().ToString();
            var resp = new RegisterResponse { Id = id };
            _jobs[id] = request.Description;
            PublishRedisMessage(id, request.Description, request.Data);
            PublishNatsMessage(id);
            return Task.FromResult(resp);
        }

        public override Task<GetProcessingResultResponse> GetProcessingResult(GetProcessingResultRequest request, ServerCallContext context)
        {
            string id = request.Id;
            double rank = -1;
            IDatabase db = _redis.GetDatabase();
            int index = 0;
            while (index++ < MAX_RETRIES)
            {
                string JSON = db.StringGet(id);
                var model = JsonSerializer.Deserialize<RedisPayloadModel>(JSON);
                if (model.Rank != -1)
                {
                    rank = model.Rank;
                    break;
                }
                Thread.Sleep(SLEEP_TIMEOUT);
            }
            var resp = new GetProcessingResultResponse { Rank = rank };
            if (rank == -1)
            {
                resp.Status = "in_progress";
            }
            else
            {
                resp.Status = "done";
            }
            return Task.FromResult(resp);
        }

        private void PublishNatsMessage(string id)
        {
            string message = $"JobCreated|{id}";
            byte[] payload = Encoding.Default.GetBytes(message);
            _nats.Publish("events", payload);
        }

        private void PublishRedisMessage(string id, string description, string data)
        {
            var model = new RedisPayloadModel { Description = description, Data = data };
            string result = JsonSerializer.Serialize(model);
            IDatabase db = _redis.GetDatabase();
            db.StringSet(id, result);
        }

        public void Dispose()
        {
            _nats.Dispose();
            _redis.Dispose();
        }
    }
}
