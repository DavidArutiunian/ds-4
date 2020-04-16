using System;
using System.Text;
using NATS.Client;
using NATS.Client.Rx;
using NATS.Client.Rx.Ops;
using System.Linq;
using StackExchange.Redis;
using JobLogger.Models;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Subscriber
{
    public class SubscriberService
    {
        public void Run(IConnection nats, ConnectionMultiplexer redis)
        {
            var events = nats.Observe("events")
                    .Where(m => m.Data?.Any() == true)
                    .Select(m => Encoding.Default.GetString(m.Data));

            events.Subscribe(msg =>
            {
                IDatabase db = redis.GetDatabase();
                string id = msg.Split('|').Last();
                string JSON = db.StringGet(id);
                var model = JsonSerializer.Deserialize<RedisPayloadModel>(JSON);
                Console.WriteLine(id);
                Console.WriteLine(model.Description);
                Console.WriteLine(model.Data);
            });
        }
    }
}
