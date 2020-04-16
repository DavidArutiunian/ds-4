using System;
using NATS.Client;

namespace Publisher
{
    class Program
    {
        static void Main(string[] args)
        {
            string natsConnectionString = "nats://127.0.0.1";

            var greeterService = new GreeterService(() => DateTime.UtcNow);
            using (IConnection connection = new ConnectionFactory().CreateConnection(natsConnectionString))
            {
                greeterService.RunAsync(connection, 5).Wait();
            }
        }

    }
}
