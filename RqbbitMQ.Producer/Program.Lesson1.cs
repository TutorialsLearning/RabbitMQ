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
        static void Lesson1()
        {
            //https://www.rabbitmq.com/tutorials/tutorial-one-dotnet.html

            // The connection abstracts the socket connection, and takes care of protocol version negotiation and authentication and so on for us.
            string connectionString = "amqp://cl-rozetka:LKNJeAliDq68YMhB1ugv@10.10.7.13:5672/RozetkaProcessing_Prod";
            var factory = new ConnectionFactory() { Uri = connectionString };
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    // To send, we must declare a queue for us to send to; 
                    channel.QueueDeclare(queue: "hello",
                        durable: true,
                        exclusive: false,
                        autoDelete: false,
                        arguments: null);
                    // Declaring a queue is idempotent - it will only be created if it doesn't exist already.

                    // then we can publish a message to the queue:
                    string message = "Hello World!";
                    var body = Encoding.UTF8.GetBytes(message);
                    channel.BasicPublish(
                        exchange: "", // using a default exchange: messages are routed to the queue with the name specified by routingKey, if it exists
                        routingKey: "hello",
                        basicProperties: null,
                        body: body);
                    // The message content is a byte array, so you can encode whatever you like there.
                    Console.WriteLine(" [x] Sent {0}", message);
                }
            }

            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
        }
    }
}
