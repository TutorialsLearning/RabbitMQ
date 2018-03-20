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
    class RabbitTutorialConsumer
    {

        static void Consumer()
        {
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
                using (var channel = connection.CreateModel())
                {
                    // To send, we must declare a queue for us to send to: 
                    channel.QueueDeclare(
                        queue: "hello",
                        // When RabbitMQ quits or crashes it will forget the queues and 
                        // messages unless you tell it not to. Two things are required to make sure that 
                        // messages aren't lost: we need to mark both the queue and messages as durable.
                        durable: true, // mark queue as durable
                        exclusive: false,
                        autoDelete: false,
                        arguments: null);
                    // Declaring a queue is idempotent - it will only be created if it doesn't exist already.

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


                    string[] severities = new[]
                    {
                        "info",
                        "warning",
                        "error",
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

                    // RabbitMQ dispatches a message when the message enters the queue.
                    // It doesn't look at the number of unacknowledged messages for a consumer. 
                    // It just blindly dispatches every n-th message to the n-th consumer.
                    // In order to change this behavior we can use the basicQos method with 
                    // the prefetchCount = 1 setting.This tells RabbitMQ not to give more than one 
                    // message to a worker at a time. Or, in other words, don't dispatch a new message
                    // to a worker until it has processed and acknowledged the previous one. 
                    // Instead, it will dispatch it to the next worker that is not still busy.
                    channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);

                    // We'll keep the consumer running continuously to listen for messages and print them out.
                    var consumer = new EventingBasicConsumer(channel);
                    consumer.Received += (model, ea) =>
                    {
                        var body = ea.Body;
                        var message = Encoding.UTF8.GetString(body);
                        var routingKey = ea.RoutingKey;
                        Console.WriteLine(" [x] Received '{0}':'{1}'", routingKey, message);

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
}
