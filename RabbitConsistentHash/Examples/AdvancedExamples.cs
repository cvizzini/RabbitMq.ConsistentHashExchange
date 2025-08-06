using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitConsistentHash.Configuration;
using RabbitConsistentHash.Services;
using RabbitMQ.Client;
using System.Text;

namespace RabbitConsistentHash.Examples;

/// <summary>
/// Advanced examples showing different patterns and use cases for RabbitMQ Consistent Hash Exchange
/// </summary>
public class AdvancedExamples
{
    private readonly IRabbitMqConnectionService _connectionService;
    private readonly RabbitMqSettings _settings;
    private readonly ILogger<AdvancedExamples> _logger;

    public AdvancedExamples(
        IRabbitMqConnectionService connectionService,
        IOptions<RabbitMqSettings> settings,
        ILogger<AdvancedExamples> logger)
    {
        _connectionService = connectionService;
        _settings = settings.Value;
        _logger = logger;
    }

    /// <summary>
    /// Demonstrates publishing different message types with custom headers
    /// </summary>
    public async Task PublishDifferentMessageTypes()
    {
        using var channel = await _connectionService.CreateChannelAsync();
        
        // Example 1: Order processing messages
        await PublishOrderMessage(channel, "customer-123", new
        {
            OrderId = "order-456",
            CustomerId = "customer-123",
            Amount = 99.99m,
            Items = new[] { "item1", "item2" }
        });

        // Example 2: User activity messages
        await PublishUserActivityMessage(channel, "user-789", new
        {
            UserId = "user-789",
            Action = "login",
            Timestamp = DateTime.UtcNow,
            IpAddress = "192.168.1.1"
        });

        // Example 3: Inventory update messages
        await PublishInventoryMessage(channel, "warehouse-001", new
        {
            WarehouseId = "warehouse-001",
            ProductId = "product-555",
            Quantity = 100,
            Operation = "restock"
        });
    }

    /// <summary>
    /// Shows how to use weighted routing for different queue capacities
    /// </summary>
    public async Task DemonstrateWeightedRouting()
    {
        using var channel = await _connectionService.CreateChannelAsync();
        
        // Create queues with different weights
        var queues = new[]
        {
            new { Name = "high-capacity-queue", Weight = "3" },
            new { Name = "medium-capacity-queue", Weight = "2" },
            new { Name = "low-capacity-queue", Weight = "1" }
        };

        foreach (var queue in queues)
        {
            await channel.QueueDeclareAsync(queue.Name, durable: true, exclusive: false, autoDelete: false);
            await channel.QueueBindAsync(queue.Name, _settings.Exchange.Name, queue.Weight);
            
            _logger.LogInformation("Created queue {QueueName} with weight {Weight}", 
                queue.Name, queue.Weight);
        }
    }

    /// <summary>
    /// Demonstrates handling message routing based on different hash headers
    /// </summary>
    public async Task DemonstrateMultiHeaderRouting()
    {
        // Create separate exchanges for different routing strategies
        using var channel = await _connectionService.CreateChannelAsync();
        
        var exchanges = new[]
        {
            new { Name = "customer-routing", Header = "customerId" },
            new { Name = "region-routing", Header = "region" },
            new { Name = "priority-routing", Header = "priority" }
        };

        foreach (var exchange in exchanges)
        {
            var arguments = new Dictionary<string, object?> 
            { 
                { "hash-header", exchange.Header } 
            };

            await channel.ExchangeDeclareAsync(
                exchange.Name, 
                "x-consistent-hash", 
                durable: true, 
                autoDelete: false, 
                arguments: arguments);
            
            _logger.LogInformation("Created exchange {ExchangeName} with hash header {HashHeader}", 
                exchange.Name, exchange.Header);
        }
    }

    /// <summary>
    /// Shows error handling and dead letter queue setup
    /// </summary>
    public async Task SetupErrorHandling()
    {
        using var channel = await _connectionService.CreateChannelAsync();
        
        // Create dead letter exchange
        await channel.ExchangeDeclareAsync("dlx", "direct", durable: true, autoDelete: false);
        
        // Create dead letter queue
        await channel.QueueDeclareAsync("dead-letters", durable: true, exclusive: false, autoDelete: false);
        await channel.QueueBindAsync("dead-letters", "dlx", "failed");
        
        // Create main queue with dead letter configuration
        var dlxArguments = new Dictionary<string, object?>
        {
            { "x-dead-letter-exchange", "dlx" },
            { "x-dead-letter-routing-key", "failed" },
            { "x-message-ttl", 30000 }, // 30 seconds TTL
            { "x-max-retries", 3 }
        };

        await channel.QueueDeclareAsync("main-queue-with-dlx", 
            durable: true, exclusive: false, autoDelete: false, arguments: dlxArguments);
        
        _logger.LogInformation("Set up error handling with dead letter exchange");
    }

    private async Task PublishOrderMessage(IChannel channel, string customerId, object orderData)
    {
        var messageBody = System.Text.Json.JsonSerializer.Serialize(orderData);
        var body = Encoding.UTF8.GetBytes(messageBody);

        var properties = new BasicProperties
        {
            Headers = new Dictionary<string, object?>
            {
                { _settings.Exchange.HashHeader, customerId },
                { "message-type", "order" },
                { "version", "1.0" }
            },
            MessageId = Guid.NewGuid().ToString(),
            Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
            ContentType = "application/json",
            DeliveryMode = DeliveryModes.Persistent
        };

        await channel.BasicPublishAsync(_settings.Exchange.Name, "", false, properties, body);
        _logger.LogInformation("Published order message for customer {CustomerId}", customerId);
    }

    private async Task PublishUserActivityMessage(IChannel channel, string userId, object activityData)
    {
        var messageBody = System.Text.Json.JsonSerializer.Serialize(activityData);
        var body = Encoding.UTF8.GetBytes(messageBody);

        var properties = new BasicProperties
        {
            Headers = new Dictionary<string, object?>
            {
                { _settings.Exchange.HashHeader, userId },
                { "message-type", "user-activity" },
                { "version", "1.0" }
            },
            MessageId = Guid.NewGuid().ToString(),
            Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
            ContentType = "application/json",
            DeliveryMode = DeliveryModes.Persistent
        };

        await channel.BasicPublishAsync(_settings.Exchange.Name, "", false, properties, body);
        _logger.LogInformation("Published user activity message for user {UserId}", userId);
    }

    private async Task PublishInventoryMessage(IChannel channel, string warehouseId, object inventoryData)
    {
        var messageBody = System.Text.Json.JsonSerializer.Serialize(inventoryData);
        var body = Encoding.UTF8.GetBytes(messageBody);

        var properties = new BasicProperties
        {
            Headers = new Dictionary<string, object?>
            {
                { _settings.Exchange.HashHeader, warehouseId },
                { "message-type", "inventory" },
                { "version", "1.0" }
            },
            MessageId = Guid.NewGuid().ToString(),
            Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
            ContentType = "application/json",
            DeliveryMode = DeliveryModes.Persistent
        };

        await channel.BasicPublishAsync(_settings.Exchange.Name, "", false, properties, body);
        _logger.LogInformation("Published inventory message for warehouse {WarehouseId}", warehouseId);
    }
}