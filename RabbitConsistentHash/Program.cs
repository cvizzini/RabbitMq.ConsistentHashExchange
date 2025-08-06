// See https://aka.ms/new-console-template for more information

using RabbitConsistentHash;
using RabbitMQ.Client;

var factory = new ConnectionFactory() { HostName = "localhost", UserName = "guest", Password = "guest" };
await using var connection = await factory.CreateConnectionAsync();
await using var channel = await connection.CreateChannelAsync();
var exchangeName = "account-hash-exchange";
var queueCount = 10;

// Declare consistent hash exchange
await channel.ExchangeDeclareAsync(exchange: exchangeName, type: "x-consistent-hash",
    arguments: new Dictionary<string, object>()
    {
        {"hash-header", "accountId"}
    });

// Declare and bind queues
for (var i = 0; i < queueCount; i++)
{
    var queueName = $"queue.account.{i}";
    await channel.QueueDeclareAsync(queue: queueName, durable: true, exclusive: false, autoDelete: false);
    await channel.QueueBindAsync(queue: queueName, exchange: exchangeName, routingKey: "1");
}

for (var i = 0; i < queueCount; i++)
{
    await Subscriber.Subscribe(channel, i);
}


while (true)
{
    //var accountId = guid.ToString();
    var accountId = Guid.NewGuid().ToString();
    await Publisher.Publish(channel, accountId, $"This is a test message for account {accountId}");

    await Task.Delay(TimeSpan.FromSeconds(1));
}