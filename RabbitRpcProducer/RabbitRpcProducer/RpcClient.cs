using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RabbitRpcProducer
{
    /// <remarks>
    /// Having received a response in that queue it's not clear to which request the response belongs.
    /// That's when the CorrelationId property is used.
    /// We're going to set it to a unique value for every request.
    /// Later, when we receive a message in the callback queue we'll look at this property,
    /// and based on that we'll be able to match a response with a request.
    /// If we see an unknown CorrelationId value, we may safely discard the message -
    /// it doesn't belong to our requests.
    /// 
    /// You may ask, why should we ignore unknown messages in the callback queue, rather than
    /// failing with an error? It's due to a possibility of a race condition on the server side.
    /// Although unlikely, it is possible that the RPC server will die just after sending us
    /// the answer, but before sending an acknowledgment message for the request.
    /// If that happens, the restarted RPC server will process the request again.
    /// That's why on the client we must handle the duplicate responses gracefully,
    /// and the RPC should ideally be idempotent.
    /// </remarks>
    public class RpcClient
    {
        private const string QUEUE_NAME = "rpc_queue";

        private readonly IConnection connection;
        private readonly IModel channel;
        private readonly string replyQueueName;
        private readonly EventingBasicConsumer consumer;
        private readonly ConcurrentDictionary<string, TaskCompletionSource<string>> callbackMapper =
                    new ConcurrentDictionary<string, TaskCompletionSource<string>>();

        public RpcClient()
        {
            var connectionString = "amqp://processing-test:LKNJeAliDq68YMhB1ugv@10.10.7.22:5672/Processing_Test";
            var factory = new ConnectionFactory() { Uri = connectionString };

            connection = factory.CreateConnection();
            channel = connection.CreateModel();
            replyQueueName = channel.QueueDeclare().QueueName;
            consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                if (!callbackMapper.TryRemove(ea.BasicProperties.CorrelationId, out TaskCompletionSource<string> tcs))
                    return;
                var body = ea.Body;
                var response = Encoding.UTF8.GetString(body);
                tcs.TrySetResult(response);
            };

            channel.BasicConsume(
                consumer: consumer,
                queue: replyQueueName,
                noAck: true);
        }

        public Task<string> CallAsync(string message, CancellationToken cancellationToken = default(CancellationToken))
        {
            IBasicProperties props = channel.CreateBasicProperties();
            var correlationId = Guid.NewGuid().ToString();
            props.CorrelationId = correlationId;
            props.ReplyTo = replyQueueName;
            var messageBytes = Encoding.UTF8.GetBytes(message);
            var tcs = new TaskCompletionSource<string>();
            callbackMapper.TryAdd(correlationId, tcs);

            channel.BasicPublish(
                exchange: "",
                routingKey: QUEUE_NAME,
                basicProperties: props,
                body: messageBytes);

            cancellationToken.Register(() =>
            {
                if (callbackMapper.TryRemove(correlationId, out var taskCompletionSource))
                {
                    taskCompletionSource.SetCanceled(); // taskCompletionSource == tcs
                }
            });
            return tcs.Task;
        }

        /// <summary>
        /// Обертка над CallAsync(...) для упрощенного синхронного вызова, с таймаутом отмены
        /// </summary>
        /// <param name="message">Сообщение, которое будет отправлено в очередь</param>
        /// <param name="timeout">Таймаут по истечении которого задача будет отменена</param>
        /// <returns>Ответ сервера или исключение, если завершено по таймауту</returns>
        public string SendSingleMessage(string message, int timeout)
        {
            using (var tokenSource = new CancellationTokenSource())
            {
                CancellationToken token = tokenSource.Token;

                tokenSource.CancelAfter(timeout);

                var task = CallAsync(message, token);
                task.Wait(token);

                return task.Result;
            }
        }

        public void Close()
        {
            connection.Close();
        }
    }
}
