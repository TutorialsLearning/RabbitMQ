using System;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

class RPCServer
{
    public static void Main()
    {
        var connectionString = "amqp://processing-test:LKNJeAliDq68YMhB1ugv@10.10.7.22:5672/Processing_Test";
        var factory = new ConnectionFactory() { Uri = connectionString };
        var connection = factory.CreateConnection();
        var channel = connection.CreateModel();

        channel.QueueDeclare(queue: "rpc_queue", durable: false, exclusive: false, autoDelete: false, arguments: null);
        channel.BasicQos(0, 1, false);
        var consumer = new EventingBasicConsumer(channel);
        channel.BasicConsume(queue: "rpc_queue", noAck: false, consumer: consumer);
        Console.WriteLine(" [x] Awaiting RPC requests");

        consumer.Received += (model, ea) =>
        {
            string response = null;

            var body = ea.Body;
            var props = ea.BasicProperties;
            var replyProps = channel.CreateBasicProperties();
            replyProps.CorrelationId = props.CorrelationId;

            try
            {
                var message = Encoding.UTF8.GetString(body);
                int n = int.Parse(message);
                Console.WriteLine(" [.] fib({0})", message);

                Task.Run(() =>
                {
                    Thread.Sleep(n * 100);

                    var res = Work(n).ToString();

                    var responseBytes = Encoding.UTF8.GetBytes(res);
                    channel.BasicPublish(
                        exchange: "", 
                        routingKey: props.ReplyTo, 
                        basicProperties: replyProps, 
                        body: responseBytes);
                    channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                });

            }
            catch (Exception e)
            {
                Console.WriteLine(" [.] " + e.Message);
                //response = "";
            }
            finally
            {
                
            }
        };

        Console.WriteLine(" Press [enter] to exit.");
        Console.ReadLine();

    }


    private static int Work(int n)
    {
        return n;
    }
}