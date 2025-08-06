using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RabbitConsistentHash;

public class Subscriber
{
    public static async Task Subscribe(IChannel channel, int queueIndex)
    {
        var queueName = $"queue.account.{queueIndex}";

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += (model, ea) =>
        {
            var body = ea.Body.ToArray();
            var message = Encoding.UTF8.GetString(body);
            var accountId = ea.BasicProperties.Headers?["accountId"] != null
                ? Encoding.UTF8.GetString((byte[])ea.BasicProperties.Headers["accountId"])
                : "unknown";
            Console.WriteLine($"[Queue {queueIndex}] Received: {message} for accountId: {accountId}");
            return Task.CompletedTask;
        };

        _ = Task.Run(() =>
        {
            channel.BasicConsumeAsync(queue: queueName, autoAck: true, consumer: consumer);
        });
    }
}