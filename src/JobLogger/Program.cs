using System;
using NATS.Client;
using Subscriber;
using StackExchange.Redis;

namespace JobLogger
{
    class Program
    {
        private static bool running = true;

        static void Main(string[] args)
        {
            var subscriber = new SubscriberService();

            using (IConnection nats = new ConnectionFactory().CreateConnection("nats://" + Environment.GetEnvironmentVariable("NATS_HOST")))
            using (ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(Environment.GetEnvironmentVariable("REDIS_HOST")))
            {
                subscriber.Run(nats, redis);

                Console.WriteLine("JobLogger service is started");

                Console.CancelKeyPress += delegate (object sender, ConsoleCancelEventArgs e)
                {
                    e.Cancel = true;
                    Program.running = false;
                };

                while (running) { }

                Console.WriteLine("JobLogger service is shut down");
            }
        }
    }
}
