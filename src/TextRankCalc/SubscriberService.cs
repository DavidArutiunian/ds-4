using System;
using System.Text;
using System.Collections.Generic;
using NATS.Client;
using NATS.Client.Rx;
using NATS.Client.Rx.Ops;
using System.Linq;
using StackExchange.Redis;
using TextRankCalc.Models;
using System.Text.Json;
using System.Text.RegularExpressions;

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
                Console.WriteLine($"Successfully got message {id}");
                RedisPayloadModel model = null;
                try
                {
                    model = JsonSerializer.Deserialize<RedisPayloadModel>(JSON);
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine($"Deserialize error: {ex.Message}");
                    return;
                }
                Console.WriteLine("Starting calculating rank...");
                double rank = GetTextRank(model.Data);
                Console.WriteLine($"Calculation has been successfull! Rank is {rank}");
                model.Rank = rank;
                var payload = JsonSerializer.Serialize(model);
                db.StringSet(id, payload);
            });
        }

        public double GetTextRank(string text)
        {
            double vowels = Regex.Matches(text, @"[AEIOUaeiou]").Count;
            double consonants = Regex.Matches(text, @"[QWRTYPSDFGHJKLZXCVBNMqwrtypsdfghjklzxcvbnm]").Count;
            return vowels / Math.Max(consonants, 1);
        }
    }
}
