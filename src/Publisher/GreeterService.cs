using System;
using System.Text;
using System.Threading.Tasks;
using NATS.Client;

namespace Publisher
{
    public class GreeterService
    {
        private readonly Func<DateTime> _utcNow;

        public GreeterService(Func<DateTime> utcNow)
        {
            _utcNow = utcNow;
        }

        public async Task RunAsync(IConnection connection, int n)
        {
            for (int i = 0; i < n; ++i)
            {
                DateTime currTime = _utcNow();
                string message = $"EveryoneGreeted|Hello everyone at {currTime}!";

                Console.WriteLine("Sending message: " + message);

                byte[] payload = Encoding.Default.GetBytes(message);
                connection.Publish("greeter", payload);

                await Task.Delay(1000);
            }
        }
    }
}
