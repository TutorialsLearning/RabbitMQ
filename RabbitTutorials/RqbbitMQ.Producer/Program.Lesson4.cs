using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace RqbbitMQ.Producer
{
    public partial class Program
    {
        static void Lesson4()
        {
            // https://www.rabbitmq.com/tutorials/tutorial-four-dotnet.html 

            string connectionString = "amqp://cl-rozetka:LKNJeAliDq68YMhB1ugv@10.10.7.13:5672/RozetkaProcessing_Prod";
            var factory = new ConnectionFactory() { Uri = connectionString };
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    // The routing algorithm behind a direct exchange is simple
                    // - a message goes to the queues whose binding key exactly matches
                    // the routing key of the message.
                    channel.ExchangeDeclare("direct_logs", "direct");

                    string[] severities = new[]
                    {
                        "info",
                        "warning",
                        "error",
                    };
                    foreach (string severity in severities)
                    {
                        var message = $"Hello, world | {severity}";
                        var body = Encoding.UTF8.GetBytes(message);
                        var properties = channel.CreateBasicProperties();
                        properties.Persistent = true;

                        channel.BasicPublish(
                            exchange: "direct_logs",
                            routingKey: severity,
                            basicProperties: null,
                            body: body);

                        Console.WriteLine(" [x] Sent {0}", message);
                    }
                }
            }

            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
        }
    }
}
