using System.Text;
using RabbitMQ.Client;

namespace RabbitConsistentHash;

public class Publisher
{
    public static async Task Publish(IChannel channel, string accountId, string message)
    {
        string exchangeName = "account-hash-exchange";
        var body = Encoding.UTF8.GetBytes(message);


        var props = new BasicProperties();
        props.Headers = new Dictionary<string, object>();
        props.Headers.Add(new("accountId", accountId));
        
        // Use accountId as routing key for consistent hashing
        await channel.BasicPublishAsync(
            exchange: exchangeName,
            routingKey: "",
            basicProperties: props,
            body: body, mandatory:false
        );

        Console.WriteLine($"Published: '{message}' for accountId: {accountId}");
    }
}