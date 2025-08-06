namespace RabbitConsistentHash.Configuration;

public class RabbitMqSettings
{
    public const string SectionName = "RabbitMQ";
    
    public string HostName { get; set; } = "localhost";
    public string UserName { get; set; } = "guest";
    public string Password { get; set; } = "guest";
    public int Port { get; set; } = 5672;
    public string VirtualHost { get; set; } = "/";
    public ExchangeSettings Exchange { get; set; } = new();
    public QueueSettings Queue { get; set; } = new();
}

public class ExchangeSettings
{
    public string Name { get; set; } = "account-hash-exchange";
    public string Type { get; set; } = "x-consistent-hash";
    public string HashHeader { get; set; } = "accountId";
}

public class QueueSettings
{
    public string Prefix { get; set; } = "queue.account";
    public int Count { get; set; } = 10;
    public bool Durable { get; set; } = true;
    public bool AutoDelete { get; set; } = false;
    public bool Exclusive { get; set; } = false;
}

public class PublisherSettings
{
    public const string SectionName = "Publisher";
    
    public int MessageIntervalSeconds { get; set; } = 1;
    public int MessageCount { get; set; } = -1; // -1 for infinite
}