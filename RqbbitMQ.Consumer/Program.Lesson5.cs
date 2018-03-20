using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RqbbitMQ.Consumer
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
                var channel = connection.CreateModel();
                channel.ExchangeDeclare(exchange: "topic_logs", type: "topic");
                var queueName = channel.QueueDeclare().QueueName;

                string[] patterns =
                {
                    "#",
                    "kern.*",
                    "*.critical"
                };

                foreach (var bindingKey in patterns)
                {
                    channel.QueueBind(
                        queue: queueName,
                        exchange: "topic_logs",
                        routingKey: bindingKey);
                }

                channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);

                Console.WriteLine(" [*] Waiting for messages.");

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body;
                    var message = Encoding.UTF8.GetString(body);
                    var routingKey = ea.RoutingKey;

                    Console.WriteLine("----------------------------------------");
                    foreach (PropertyDescriptor descriptor in TypeDescriptor.GetProperties(ea))
                    {
                        string name = descriptor.Name;
                        object value = descriptor.GetValue(ea);
                        Console.WriteLine("{0}={1}", name, value);
                    }
                    Console.WriteLine("----------------------------------------");

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
