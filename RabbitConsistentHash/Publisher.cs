using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitConsistentHash.Configuration;
using RabbitConsistentHash.Services;
using RabbitMQ.Client;

namespace RabbitConsistentHash.Services;

/// <summary>
/// Service for publishing messages to RabbitMQ consistent hash exchange with proper error handling and logging
/// </summary>
public class MessagePublisher : IMessagePublisher
{
    private readonly IRabbitMqConnectionService _connectionService;
    private readonly RabbitMqSettings _settings;
    private readonly ILogger<MessagePublisher> _logger;

    public MessagePublisher(
        IRabbitMqConnectionService connectionService,
        IOptions<RabbitMqSettings> settings,
        ILogger<MessagePublisher> logger)
    {
        _connectionService = connectionService ?? throw new ArgumentNullException(nameof(connectionService));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _settings = settings?.Value ?? throw new ArgumentNullException(nameof(settings));
    }

    /// <summary>
    /// Publishes a message to the consistent hash exchange using the account ID for routing
    /// </summary>
    /// <param name="accountId">The account ID used for consistent hashing routing</param>
    /// <param name="message">The message content to publish</param>
    /// <param name="cancellationToken">Cancellation token for async operations</param>
    public async Task PublishAsync(string accountId, string message, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(accountId))
        {
            throw new ArgumentException("Account ID cannot be null or empty", nameof(accountId));
        }

        if (string.IsNullOrWhiteSpace(message))
        {
            throw new ArgumentException("Message cannot be null or empty", nameof(message));
        }

        try
        {
            using var channel = await _connectionService.CreateChannelAsync(cancellationToken);
            
            // Convert message to bytes
            var body = Encoding.UTF8.GetBytes(message);

            // Create message properties with the account ID header for consistent hashing
            var properties = new BasicProperties
            {
                Headers = new Dictionary<string, object?>
                {
                    { _settings.Exchange.HashHeader, accountId }
                },
                // Add additional useful properties
                MessageId = Guid.NewGuid().ToString(),
                Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
                ContentType = "text/plain",
                DeliveryMode = DeliveryModes.Persistent // Make message persistent
            };

            // Publish the message to the consistent hash exchange
            await channel.BasicPublishAsync(
                exchange: _settings.Exchange.Name,
                routingKey: "", // Routing key not used for consistent hash exchange
                basicProperties: properties,
                body: body,
                mandatory: false, // Don't require queue to exist
                cancellationToken: cancellationToken
            );

            _logger.LogInformation("Published message for accountId: {AccountId}, messageId: {MessageId}", 
                accountId, properties.MessageId);
            
            _logger.LogDebug("Message content: {Message}", message);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to publish message for accountId: {AccountId}", accountId);
            throw;
        }
    }
}