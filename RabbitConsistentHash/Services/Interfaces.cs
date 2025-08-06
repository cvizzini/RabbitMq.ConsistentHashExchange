using RabbitMQ.Client;

namespace RabbitConsistentHash.Services;

/// <summary>
/// Service interface for managing RabbitMQ connections with resilience and error handling
/// </summary>
public interface IRabbitMqConnectionService : IDisposable
{
    /// <summary>
    /// Gets or creates a connection to RabbitMQ with automatic retry and error handling
    /// </summary>
    /// <param name="cancellationToken">Cancellation token for async operations</param>
    /// <returns>Active RabbitMQ connection</returns>
    Task<IConnection> GetConnectionAsync(CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Creates a new channel from the managed connection
    /// </summary>
    /// <param name="cancellationToken">Cancellation token for async operations</param>
    /// <returns>New RabbitMQ channel</returns>
    Task<IChannel> CreateChannelAsync(CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Checks if the connection is healthy
    /// </summary>
    bool IsConnected { get; }
}

/// <summary>
/// Service interface for publishing messages to RabbitMQ consistent hash exchange
/// </summary>
public interface IMessagePublisher
{
    /// <summary>
    /// Publishes a message to the consistent hash exchange using the specified account ID for routing
    /// </summary>
    /// <param name="accountId">Account ID used for consistent hashing</param>
    /// <param name="message">Message content to publish</param>
    /// <param name="cancellationToken">Cancellation token for async operations</param>
    Task PublishAsync(string accountId, string message, CancellationToken cancellationToken = default);
}

/// <summary>
/// Service interface for subscribing to messages from RabbitMQ queues
/// </summary>
public interface IMessageSubscriber
{
    /// <summary>
    /// Starts subscribing to messages from all configured queues
    /// </summary>
    /// <param name="cancellationToken">Cancellation token to stop subscription</param>
    Task StartAsync(CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Stops message subscription
    /// </summary>
    Task StopAsync();
}

/// <summary>
/// Service interface for managing RabbitMQ infrastructure (exchanges, queues, bindings)
/// </summary>
public interface IRabbitMqInfrastructureService
{
    /// <summary>
    /// Sets up the required RabbitMQ infrastructure including exchanges, queues, and bindings
    /// </summary>
    /// <param name="cancellationToken">Cancellation token for async operations</param>
    Task SetupInfrastructureAsync(CancellationToken cancellationToken = default);
}