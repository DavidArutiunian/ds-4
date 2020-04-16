using System;
using NATS.Client;

namespace Subscriber
{
    class Program
    {
        static void Main(string[] args)
        {
            var subscriber = new SubscriberService();

            using (IConnection connection = new ConnectionFactory().CreateConnection())
            {
                subscriber.Run(connection);
                Console.WriteLine("Events listening started. Press any key to exit");
                Console.ReadKey();
            }
        }
    }
}
