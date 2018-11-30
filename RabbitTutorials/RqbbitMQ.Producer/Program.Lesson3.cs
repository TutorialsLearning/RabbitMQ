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
        static void Lesson3()
        {
            // https://www.rabbitmq.com/tutorials/tutorial-three-dotnet.html

            // ! At first, run Consumer to create temp-queue and bind it to "logs"-exchange,
            // then run this method to publish messages to that exchange

            string connectionString = "amqp://cl-rozetka:LKNJeAliDq68YMhB1ugv@10.10.7.13:5672/RozetkaProcessing_Prod";
            var factory = new ConnectionFactory() { Uri = connectionString };
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    // The core idea in the messaging model in RabbitMQ is 
                    // that the producer never sends any messages directly to a queue.
                    // Actually, quite often the producer doesn't even know if a 
                    // message will be delivered to any queue at all.
                    // Instead, the producer can only send messages to an exchange.
                    // An exchange is a very simple thing. On one side it receives messages
                    // from producers and the other side it pushes them to queues. 
                    // The exchange must know exactly what to do with a message it 
                    // receives. Should it be appended to a particular queue? 
                    // Should it be appended to many queues? Or should it get discarded. 
                    // The rules for that are defined by the exchange type.
                    // There are a few exchange types available: direct, topic, headers and fanout.
                    channel.ExchangeDeclare("logs", "fanout"); // Create an exchange of 'fanout' type, and call it logs                                                              
                    // The fanout exchange is very simple. As you can probably guess from the name,
                    // it just broadcasts all the messages it receives to all the queues it knows.

                    string[] messages = new[]
                    {
                        "first.",
                        "second..",
                        "third...",
                        "fourth....",
                        "fifth.....",
                        "sixth......",
                        "seventh.......",
                        "eigth........",
                        "ninth.........",
                        "tenth..........",
                    };

                    foreach (string message in messages)
                    {
                        var body = Encoding.UTF8.GetBytes(message);

                        var properties = channel.CreateBasicProperties();
                        properties.Persistent = true; // mark message as durable

                        channel.BasicPublish(
                            exchange: "logs", // use named exchange
                            routingKey: "", // We need to supply a routingKey when sending, but its value is ignored for fanout exchanges.
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
