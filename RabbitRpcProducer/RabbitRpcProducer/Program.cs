using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Concurrent;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RabbitRpcProducer;

namespace RabbitRpcProducer
{
    public class Rpc
    {
        public static void Main(string[] args)
        {
            Console.WriteLine("RPC Client");
            string n = args.Length > 0 ? args[0] : "30";

            /*Task t = InvokeAsync(n);
            t.Wait();*/

            Task[] tasks =
            {
                InvokeAsync("50"),
                InvokeAsync("10"),
                InvokeAsync("25"),
                InvokeAsync("75"),
                InvokeAsync("100"),
            };

            //Task.WaitAll(tasks);

            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
        }

        private static async Task InvokeAsync(string n)
        {
            var rnd = new Random(Guid.NewGuid().GetHashCode());
            var rpcClient = new RpcClient();

            Console.WriteLine(" [x] Requesting fib({0})", n);
            var response = await rpcClient.CallAsync(n.ToString());
            Console.WriteLine($" [.] Was '{n}', Got '{response}'");

            rpcClient.Close();
        }
    }
}