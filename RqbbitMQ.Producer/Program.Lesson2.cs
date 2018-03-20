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
        static void Lesson2()
        {
            // https://www.rabbitmq.com/tutorials/tutorial-two-dotnet.html

            string connectionString = "amqp://cl-rozetka:LKNJeAliDq68YMhB1ugv@10.10.7.13:5672/RozetkaProcessing_Prod";
            var factory = new ConnectionFactory() { Uri = connectionString };
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    // When RabbitMQ quits or crashes it will forget the queues and 
                    // messages unless you tell it not to. Two things are required to make sure that 
                    // messages aren't lost: we need to mark both the queue and messages as durable.
                    channel.QueueDeclare(queue: "task_queue",
                        durable: true, // mark queue as durable
                        exclusive: false,
                        autoDelete: false,
                        arguments: null);

                    string[] messages = new[]
                    {
                        "first",
                        "second",
                        "third",
                    };

                    foreach (string message in messages)
                    {
                        var body = Encoding.UTF8.GetBytes(message);

                        var properties = channel.CreateBasicProperties();
                        properties.Persistent = true; // mark message as durable

                        channel.BasicPublish(
                            exchange: "", // using a default exchange: messages are routed to the queue with the name specified by routingKey, if it exists
                            routingKey: "task_queue",
                            basicProperties: properties,
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
