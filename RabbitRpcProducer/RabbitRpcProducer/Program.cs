using System;
using System.Threading.Tasks;

namespace RabbitRpcProducer
{
    public class Rpc
    {
        public static void Main(string[] args)
        {
            Console.WriteLine("RPC Client");

            /*Task t = InvokeAsync("30");
            t.Wait();*/

            InvokeArrayOfFunctions();

            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
        }

        private static async Task InvokeAsync(string n)
        {
            var rpcClient = new RpcClient();

            Console.WriteLine(" [x] Requesting fib({0})", n);
            var response = await rpcClient.CallAsync(n);
            Console.WriteLine($" [.] Was '{n}', Got '{response}'");

            rpcClient.Close();
        }

        private static void InvokeArrayOfFunctions()
        {
            var rpcClient = new RpcClient();

            Console.WriteLine(" [x] Requesting fib(50)");
            var cont4 = rpcClient.CallAsync("50").ContinueWith(task => Console.WriteLine($" [.] Was '50', Got '{task.Result}'"));

            Console.WriteLine(" [x] Requesting fib(10)");
            var cont1 = rpcClient.CallAsync("10").ContinueWith(task => Console.WriteLine($" [.] Was '10', Got '{task.Result}'"));

            Console.WriteLine(" [x] Requesting fib(25)");
            var cont2 = rpcClient.CallAsync("25").ContinueWith(task => Console.WriteLine($" [.] Was '25', Got '{task.Result}'"));

            Console.WriteLine(" [x] Requesting fib(75)");
            var cont3 = rpcClient.CallAsync("75").ContinueWith(task => Console.WriteLine($" [.] Was '75', Got '{task.Result}'"));

            Console.WriteLine(" [x] Requesting fib(100)");
            var cont5 = rpcClient.CallAsync("100").ContinueWith(task => Console.WriteLine($" [.] Was '100', Got '{task.Result}'"));

            cont1.Wait();
            cont2.Wait();
            cont3.Wait();
            cont4.Wait();
            cont5.Wait();

            rpcClient.Close();
        }
    }
}