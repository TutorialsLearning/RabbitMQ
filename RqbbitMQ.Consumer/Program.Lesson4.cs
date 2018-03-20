using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RqbbitMQ.Consumer
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
                var channel = connection.CreateModel();
                channel.ExchangeDeclare(exchange: "direct_logs", type: "direct");
                var queueName = channel.QueueDeclare().QueueName;

                string[] severities =
                {
                    "info",
                    "warning",
                    "error"
                };
                // A binding is a relationship between an exchange and a queue. 
                // This can be simply read as: the queue is interested in messages from this exchange.
                // Bindings can take an extra routingKey parameter(to avoid the confusion 
                // with a BasicPublish parameter we're going to call it a binding key). 
                // This is how we could create a binding with a key:
                foreach (var severity in severities) // We're going to create a new binding for each severity we're interested in.
                {
                    channel.QueueBind(
                        queue: queueName,
                        exchange: "direct_logs",
                        routingKey: severity); // The meaning of a binding key depends on the exchange type.
                }

                channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);

                Console.WriteLine(" [*] Waiting for messages.");

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body;
                    var message = Encoding.UTF8.GetString(body);
                    var routingKey = ea.RoutingKey;
                    Console.WriteLine(" [x] Received '{0}':'{1}'", routingKey, message);

                    channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);

                };

                channel.BasicConsume(
                    queue: queueName,
                    noAck: false,
                    consumer: consumer);

                Console.WriteLine(" Press [enter] to exit.");
                Console.ReadLine();
            }
        }
    }
}
