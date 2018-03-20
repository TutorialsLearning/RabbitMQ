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
        static void Lesson5()
        {
            // https://www.rabbitmq.com/tutorials/tutorial-five-dotnet.html

            string connectionString = "amqp://cl-rozetka:LKNJeAliDq68YMhB1ugv@10.10.7.13:5672/RozetkaProcessing_Prod";
            var factory = new ConnectionFactory() { Uri = connectionString };
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    channel.ExchangeDeclare("topic_logs", "topic"); // use 'topic' exchange

                    string[] messages = new[]
                    {
                        "info",
                        "kern.warning",
                        "error.critical",
                        "kern.something.critical"
                    };
                    foreach (string routingKey in messages)
                    {
                        var message = $"Hello, world | {routingKey}";
                        var body = Encoding.UTF8.GetBytes(message);
                        var properties = channel.CreateBasicProperties();
                        properties.Persistent = true;

                        channel.BasicPublish(
                            exchange: "topic_logs",
                            routingKey: routingKey,
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
