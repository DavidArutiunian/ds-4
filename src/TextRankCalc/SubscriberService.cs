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
using System.Text.Json.Serialization;

namespace Subscriber
{
    public class SubscriberService
    {
        private HashSet<char> vowels = new HashSet<char> { 'a', 'e', 'i', 'o', 'u' };
        private HashSet<char> consonants = new HashSet<char> { 'q', 'w', 'r', 't', 'y', 'p', 's', 'd', 'f', 'g', 'h', 'j', 'k', 'l', 'z', 'x', 'c', 'v', 'b', 'n', 'm' };

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
            string loweredText = text.ToLower();
            double totalVowels = loweredText.Count(c => vowels.Contains(c));
            double totalConsonants = loweredText.Count(c => consonants.Contains(c));
            return totalVowels / Math.Max(totalConsonants, 1);
        }
    }
}
