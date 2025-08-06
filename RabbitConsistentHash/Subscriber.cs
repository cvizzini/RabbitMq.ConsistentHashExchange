using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitConsistentHash.Configuration;
using RabbitConsistentHash.Services;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RabbitConsistentHash.Services;

/// <summary>
/// Service for subscribing to messages from RabbitMQ queues with proper error handling and logging
/// </summary>
public class MessageSubscriber : IMessageSubscriber, IDisposable
{
    private readonly IRabbitMqConnectionService _connectionService;
    private readonly RabbitMqSettings _settings;
    private readonly ILogger<MessageSubscriber> _logger;
    private readonly List<IChannel> _channels = new();
    private readonly List<AsyncEventingBasicConsumer> _consumers = new();
    private readonly CancellationTokenSource _cancellationTokenSource = new();

    public MessageSubscriber(
        IRabbitMqConnectionService connectionService,
        IOptions<RabbitMqSettings> settings,
        ILogger<MessageSubscriber> logger)
    {
        _connectionService = connectionService ?? throw new ArgumentNullException(nameof(connectionService));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _settings = settings?.Value ?? throw new ArgumentNullException(nameof(settings));
    }

    /// <summary>
    /// Starts consuming messages from all configured queues
    /// </summary>
    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Starting message subscribers for {QueueCount} queues", _settings.Queue.Count);

        var combinedCancellationToken = CancellationTokenSource
            .CreateLinkedTokenSource(cancellationToken, _cancellationTokenSource.Token).Token;

        // Start consumers for each queue
        var subscriptionTasks = new List<Task>();
        
        for (var queueIndex = 0; queueIndex < _settings.Queue.Count; queueIndex++)
        {
            var task = StartQueueSubscriptionAsync(queueIndex, combinedCancellationToken);
            subscriptionTasks.Add(task);
        }

        // Wait for all subscriptions to be established
        await Task.WhenAll(subscriptionTasks);
        
        _logger.LogInformation("All message subscribers started successfully");
    }

    /// <summary>
    /// Stops all message subscriptions and cleanly disposes resources
    /// </summary>
    public async Task StopAsync()
    {
        _logger.LogInformation("Stopping message subscribers...");
        
        _cancellationTokenSource.Cancel();
        
        try
        {
            // Dispose all channels
            foreach (var channel in _channels)
            {
                await channel.DisposeAsync();
            }
            
            _channels.Clear();
            _consumers.Clear();
            
            _logger.LogInformation("All message subscribers stopped");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error stopping message subscribers");
            throw;
        }
    }

    /// <summary>
    /// Sets up subscription for a specific queue index
    /// </summary>
    private async Task StartQueueSubscriptionAsync(int queueIndex, CancellationToken cancellationToken)
    {
        var queueName = $"{_settings.Queue.Prefix}.{queueIndex}";
        
        try
        {
            var channel = await _connectionService.CreateChannelAsync(cancellationToken);
            _channels.Add(channel);

            // Configure quality of service - process one message at a time per consumer
            await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false, cancellationToken);

            var consumer = new AsyncEventingBasicConsumer(channel);
            _consumers.Add(consumer);

            // Set up message handler with proper error handling
            consumer.ReceivedAsync += async (sender, eventArgs) =>
            {
                await HandleMessageAsync(queueIndex, eventArgs, channel, cancellationToken);
            };

            // Start consuming messages
            var consumerTag = await channel.BasicConsumeAsync(
                queue: queueName,
                autoAck: false, // Manual acknowledgment for reliability
                consumer: consumer,
                cancellationToken: cancellationToken
            );

            _logger.LogInformation("Started consuming from queue {QueueName} with consumer tag {ConsumerTag}", 
                queueName, consumerTag);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to start subscription for queue {QueueName}", queueName);
            throw;
        }
    }

    /// <summary>
    /// Handles incoming messages with proper error handling and acknowledgment
    /// </summary>
    private async Task HandleMessageAsync(int queueIndex, BasicDeliverEventArgs eventArgs, 
        IChannel channel, CancellationToken cancellationToken)
    {
        var queueName = $"{_settings.Queue.Prefix}.{queueIndex}";
        
        try
        {
            // Extract message content
            var messageBody = eventArgs.Body.ToArray();
            var messageContent = Encoding.UTF8.GetString(messageBody);

            // Extract account ID from headers
            var accountId = "unknown";
            if (eventArgs.BasicProperties.Headers != null && 
                eventArgs.BasicProperties.Headers.TryGetValue(_settings.Exchange.HashHeader, out var headerValue))
            {
                accountId = headerValue switch
                {
                    byte[] bytes => Encoding.UTF8.GetString(bytes),
                    string str => str,
                    _ => headerValue?.ToString() ?? "unknown"
                };
            }

            // Log the received message with structured logging
            _logger.LogInformation(
                "[Queue {QueueIndex}] Received message for accountId: {AccountId}, messageId: {MessageId}",
                queueIndex, accountId, eventArgs.BasicProperties.MessageId ?? "unknown");
            
            _logger.LogDebug(
                "[Queue {QueueIndex}] Message content: {MessageContent}", 
                queueIndex, messageContent);

            // Simulate message processing (in a real application, this would be your business logic)
            await ProcessMessageAsync(queueIndex, accountId, messageContent, eventArgs, cancellationToken);

            // Acknowledge successful processing
            await channel.BasicAckAsync(eventArgs.DeliveryTag, multiple: false, cancellationToken);
            
            _logger.LogDebug("Message acknowledged for queue {QueueName}, deliveryTag: {DeliveryTag}", 
                queueName, eventArgs.DeliveryTag);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing message from queue {QueueName}, deliveryTag: {DeliveryTag}", 
                queueName, eventArgs.DeliveryTag);
            
            try
            {
                // Reject message and requeue for retry (you might want to implement dead letter queue logic)
                await channel.BasicRejectAsync(eventArgs.DeliveryTag, requeue: true, cancellationToken);
            }
            catch (Exception rejectEx)
            {
                _logger.LogError(rejectEx, "Failed to reject message for queue {QueueName}", queueName);
            }
        }
    }

    /// <summary>
    /// Process the received message (placeholder for business logic)
    /// </summary>
    private async Task ProcessMessageAsync(int queueIndex, string accountId, string messageContent, 
        BasicDeliverEventArgs eventArgs, CancellationToken cancellationToken)
    {
        // This is where your business logic would go
        // For demonstration purposes, we'll just simulate some processing time
        await Task.Delay(Random.Shared.Next(10, 100), cancellationToken);
        
        // In a real application, you might:
        // - Validate the message
        // - Call external services
        // - Update databases
        // - Transform the message
        // - Forward to other systems
    }

    public void Dispose()
    {
        _cancellationTokenSource.Cancel();
        _cancellationTokenSource.Dispose();
        
        foreach (var channel in _channels)
        {
            channel.Dispose();
        }
        
        _channels.Clear();
        _consumers.Clear();
    }
}