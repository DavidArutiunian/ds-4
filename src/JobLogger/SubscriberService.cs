using System;
using System.Text;
using NATS.Client;
using NATS.Client.Rx;
using NATS.Client.Rx.Ops;
using System.Linq;
using StackExchange.Redis;

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
                string description = db.StringGet(id);
                Console.WriteLine(id);
                Console.WriteLine(description);
            });
        }
    }
}
