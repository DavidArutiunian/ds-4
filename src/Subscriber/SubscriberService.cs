using System;
using System.Text;
using NATS.Client;
using NATS.Client.Rx;
using NATS.Client.Rx.Ops;
using System.Linq;

namespace Subscriber
{
    public class SubscriberService
    {
        public void Run(IConnection connection)
        {
            var greetings = connection.Observe("greeter")
                    .Where(m => m.Data?.Any() == true)
                    .Select(m => Encoding.Default.GetString(m.Data));

            greetings.Subscribe(msg => Console.WriteLine(msg));
        }    
    }
}