using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RqbbitMQ.Consumer
{
    public partial class Program
    {
        static void Lesson3()
        {
            // https://www.rabbitmq.com/tutorials/tutorial-three-dotnet.html

            string connectionString = "amqp://cl-rozetka:LKNJeAliDq68YMhB1ugv@10.10.7.13:5672/RozetkaProcessing_Prod";
            var factory = new ConnectionFactory() { Uri = connectionString };
            using (var connection = factory.CreateConnection())
            {
                var channel = connection.CreateModel();

                // When we supply no parameters to queueDeclare() we create a non-durable, 
                // exclusive, autodelete queue with a generated name:
                var queueName = channel.QueueDeclare().QueueName;

                // We've already created a fanout exchange and a queue. 
                // Now we need to tell the exchange to send messages to our queue. 
                // That relationship between exchange and a queue is called a binding.
                channel.QueueBind(
                    queue: queueName,
                    exchange: "logs",
                    routingKey: ""); // fanout exchange ignore this value

                channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);

                Console.WriteLine(" [*] Waiting for messages.");

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body;
                    var message = Encoding.UTF8.GetString(body);
                    Console.WriteLine(" [x] Received {0}", message);

                    int dots = message.Split('.').Length - 1;
                    Thread.Sleep(dots * 1000);

                    Console.WriteLine(" [x] Done");


                    channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);

                };

                channel.BasicConsume(
                    queue: queueName,
                    noAck: false, // turn on message acknowledgments (by default)
                    consumer: consumer);

                Console.WriteLine(" Press [enter] to exit.");
                Console.ReadLine();
            }
        }
    }
}
