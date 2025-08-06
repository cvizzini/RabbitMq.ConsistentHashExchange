using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitConsistentHash.Configuration;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace RabbitConsistentHash.Services;

/// <summary>
/// Robust RabbitMQ connection service with automatic retry, error handling, and connection management
/// </summary>
public class RabbitMqConnectionService : IRabbitMqConnectionService
{
    private readonly RabbitMqSettings _settings;
    private readonly ILogger<RabbitMqConnectionService> _logger;
    private readonly ConnectionFactory _connectionFactory;
    private IConnection? _connection;
    private readonly object _connectionLock = new();
    private bool _disposed = false;

    public RabbitMqConnectionService(IOptions<RabbitMqSettings> settings, ILogger<RabbitMqConnectionService> logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _settings = settings?.Value ?? throw new ArgumentNullException(nameof(settings));
        
        _connectionFactory = new ConnectionFactory()
        {
            HostName = _settings.HostName,
            UserName = _settings.UserName,
            Password = _settings.Password,
            Port = _settings.Port,
            VirtualHost = _settings.VirtualHost,
            // Enable automatic recovery for connection resilience
            AutomaticRecoveryEnabled = true,
            NetworkRecoveryInterval = TimeSpan.FromSeconds(5),
            // Configure connection timeout and heartbeat
            RequestedConnectionTimeout = TimeSpan.FromSeconds(30),
            RequestedHeartbeat = TimeSpan.FromSeconds(60)
        };
        
        _logger.LogInformation("RabbitMQ connection service initialized for {HostName}:{Port}", 
            _settings.HostName, _settings.Port);
    }

    public bool IsConnected => _connection?.IsOpen ?? false;

    public Task<IConnection> GetConnectionAsync(CancellationToken cancellationToken = default)
    {
        if (_connection?.IsOpen == true)
        {
            return Task.FromResult(_connection);
        }

        lock (_connectionLock)
        {
            // Double-check pattern - connection might have been created by another thread
            if (_connection?.IsOpen == true)
            {
                return Task.FromResult(_connection);
            }

            return Task.FromResult(CreateConnection());
        }
    }

    public async Task<IChannel> CreateChannelAsync(CancellationToken cancellationToken = default)
    {
        var connection = await GetConnectionAsync(cancellationToken);
        var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);
        
        _logger.LogDebug("Created new RabbitMQ channel");
        return channel;
    }

    private IConnection CreateConnection()
    {
        const int maxRetries = 5;
        var retryCount = 0;
        
        while (retryCount < maxRetries)
        {
            try
            {
                _logger.LogInformation("Attempting to connect to RabbitMQ at {HostName}:{Port} (attempt {Attempt}/{MaxRetries})", 
                    _settings.HostName, _settings.Port, retryCount + 1, maxRetries);
                
                _connection?.Dispose();
                _connection = _connectionFactory.CreateConnectionAsync().GetAwaiter().GetResult();
                
                _logger.LogInformation("Successfully connected to RabbitMQ");
                
                return _connection;
            }
            catch (BrokerUnreachableException ex)
            {
                retryCount++;
                _logger.LogWarning(ex, "Failed to connect to RabbitMQ (attempt {Attempt}/{MaxRetries}): {Message}", 
                    retryCount, maxRetries, ex.Message);
                
                if (retryCount >= maxRetries)
                {
                    _logger.LogError("Failed to connect to RabbitMQ after {MaxRetries} attempts", maxRetries);
                    throw new InvalidOperationException(
                        $"Unable to connect to RabbitMQ after {maxRetries} attempts. Please check your RabbitMQ server is running and connection settings are correct.", ex);
                }
                
                // Exponential backoff with jitter
                var delay = TimeSpan.FromSeconds(Math.Pow(2, retryCount) + Random.Shared.NextDouble());
                Thread.Sleep(delay);
            }
        }
        
        throw new InvalidOperationException("Should not reach this point");
    }

    public void Dispose()
    {
        if (_disposed) return;
        
        try
        {
            _connection?.Dispose();
            _logger.LogInformation("RabbitMQ connection disposed");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error disposing RabbitMQ connection");
        }
        finally
        {
            _disposed = true;
        }
    }
}