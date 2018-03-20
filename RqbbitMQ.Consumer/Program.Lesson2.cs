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
        static void Lesson2()
        {
            // https://www.rabbitmq.com/tutorials/tutorial-two-dotnet.html

            /*
             By default, RabbitMQ will send each message to the next consumer, 
             in sequence. On average every consumer will get the same number of messages. 
             This way of distributing messages is called round-robin. 
             Try this out with three or more workers. 
             */

            string connectionString = "amqp://cl-rozetka:LKNJeAliDq68YMhB1ugv@10.10.7.13:5672/RozetkaProcessing_Prod";
            var factory = new ConnectionFactory() { Uri = connectionString };
            using (var connection = factory.CreateConnection())
            {
                var channel = connection.CreateModel();
                channel.QueueDeclare(queue: "task_queue",
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: null);

                // RabbitMQ dispatches a message when the message enters the queue.
                // It doesn't look at the number of unacknowledged messages for a consumer. 
                // It just blindly dispatches every n-th message to the n-th consumer.
                // In order to change this behavior we can use the basicQos method with 
                // the prefetchCount = 1 setting.This tells RabbitMQ not to give more than one 
                // message to a worker at a time. Or, in other words, don't dispatch a new message
                // to a worker until it has processed and acknowledged the previous one. 
                // Instead, it will dispatch it to the next worker that is not still busy.
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

                    // In order to make sure a message is never lost, 
                    // RabbitMQ supports message acknowledgments. 
                    // An ack(nowledgement) is sent back by the consumer to tell 
                    // RabbitMQ that a particular message has been received, 
                    // processed and that RabbitMQ is free to delete it.
                    //
                    channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                    //
                    // If a consumer dies (its channel is closed, connection is closed, 
                    // or TCP connection is lost) without sending an ack, RabbitMQ 
                    // will understand that a message wasn't processed fully and will
                    // re -queue it. If there are other consumers online at the same time,
                    // it will then quickly redeliver it to another consumer. That way you 
                    // can be sure that no message is lost, even if the workers occasionally die.
                    //
                    // There aren't any message timeouts; RabbitMQ will redeliver the message when the consumer dies.
                    // It's fine even if processing a message takes a very, very long time.
                    //
                    // !
                    // Forgotten acknowledgment
                    // It's a common mistake to miss the BasicAck. It's an easy error, but the 
                    // consequences are serious. Messages will be redelivered when your 
                    // client quits(which may look like random redelivery), but RabbitMQ will eat
                    // more and more memory as it won't be able to release any unacked messages.
                };

                // RabbitMQ dispatches a message when the message enters the queue. 
                // It doesn't look at the number of unacknowledged messages for a consumer.
                // It just blindly dispatches every n-th message to the n-th consumer.
                // In order to change this behavior we can use the basicQos method with 
                // the prefetchCount = 1 setting. This tells RabbitMQ not to give more 
                // than one message to a worker at a time. Or, in other words, don't dispatch 
                // a new message to a worker until it has processed and acknowledged the
                // previous one. Instead, it will dispatch it to the next worker that is not still busy.
                channel.BasicConsume(queue: "task_queue",
                    noAck: false, // turn on message acknowledgments (by default)
                    consumer: consumer);

                Console.WriteLine(" Press [enter] to exit.");
                Console.ReadLine();
            }
        }
    }
}
