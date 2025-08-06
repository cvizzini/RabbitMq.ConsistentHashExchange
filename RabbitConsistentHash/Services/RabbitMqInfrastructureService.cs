using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitConsistentHash.Configuration;

namespace RabbitConsistentHash.Services;

/// <summary>
/// Service for setting up RabbitMQ infrastructure including exchanges, queues, and bindings
/// </summary>
public class RabbitMqInfrastructureService : IRabbitMqInfrastructureService
{
    private readonly IRabbitMqConnectionService _connectionService;
    private readonly RabbitMqSettings _settings;
    private readonly ILogger<RabbitMqInfrastructureService> _logger;

    public RabbitMqInfrastructureService(
        IRabbitMqConnectionService connectionService,
        IOptions<RabbitMqSettings> settings,
        ILogger<RabbitMqInfrastructureService> logger)
    {
        _connectionService = connectionService ?? throw new ArgumentNullException(nameof(connectionService));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _settings = settings?.Value ?? throw new ArgumentNullException(nameof(settings));
    }

    /// <summary>
    /// Sets up the complete RabbitMQ infrastructure required for consistent hash exchange
    /// </summary>
    public async Task SetupInfrastructureAsync(CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Setting up RabbitMQ infrastructure...");
        
        using var channel = await _connectionService.CreateChannelAsync(cancellationToken);
        
        // Declare the consistent hash exchange
        await SetupExchangeAsync(channel, cancellationToken);
        
        // Declare and bind queues
        await SetupQueuesAsync(channel, cancellationToken);
        
        _logger.LogInformation("RabbitMQ infrastructure setup completed successfully");
    }

    /// <summary>
    /// Creates the consistent hash exchange with appropriate configuration
    /// </summary>
    private async Task SetupExchangeAsync(RabbitMQ.Client.IChannel channel, CancellationToken cancellationToken)
    {
        try
        {
            _logger.LogInformation("Declaring consistent hash exchange: {ExchangeName}", _settings.Exchange.Name);
            
            // Configure exchange arguments for consistent hashing
            var exchangeArguments = new Dictionary<string, object?>
            {
                { "hash-header", _settings.Exchange.HashHeader }
            };

            await channel.ExchangeDeclareAsync(
                exchange: _settings.Exchange.Name,
                type: _settings.Exchange.Type,
                durable: true, // Make exchange persistent
                autoDelete: false,
                arguments: exchangeArguments,
                cancellationToken: cancellationToken
            );
            
            _logger.LogInformation("Successfully declared exchange {ExchangeName} of type {ExchangeType} with hash header {HashHeader}",
                _settings.Exchange.Name, _settings.Exchange.Type, _settings.Exchange.HashHeader);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to declare exchange {ExchangeName}", _settings.Exchange.Name);
            throw;
        }
    }

    /// <summary>
    /// Creates all queues and binds them to the consistent hash exchange
    /// </summary>
    private async Task SetupQueuesAsync(RabbitMQ.Client.IChannel channel, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Setting up {QueueCount} queues for consistent hashing", _settings.Queue.Count);
        
        for (var i = 0; i < _settings.Queue.Count; i++)
        {
            var queueName = $"{_settings.Queue.Prefix}.{i}";
            
            try
            {
                // Declare queue with appropriate settings
                await channel.QueueDeclareAsync(
                    queue: queueName,
                    durable: _settings.Queue.Durable,
                    exclusive: _settings.Queue.Exclusive,
                    autoDelete: _settings.Queue.AutoDelete,
                    arguments: null,
                    cancellationToken: cancellationToken
                );

                // Bind queue to the consistent hash exchange
                // The routing key "1" represents the weight for this queue in the hash ring
                await channel.QueueBindAsync(
                    queue: queueName,
                    exchange: _settings.Exchange.Name,
                    routingKey: "1", // Weight of 1 for even distribution
                    cancellationToken: cancellationToken
                );

                _logger.LogDebug("Queue {QueueName} declared and bound to exchange {ExchangeName}", 
                    queueName, _settings.Exchange.Name);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to setup queue {QueueName}", queueName);
                throw;
            }
        }
        
        _logger.LogInformation("Successfully set up all {QueueCount} queues", _settings.Queue.Count);
    }
}