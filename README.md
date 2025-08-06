# RabbitMQ Consistent Hash Exchange - C# Implementation Guide

![.NET 8](https://img.shields.io/badge/.NET-8.0-purple)
![RabbitMQ](https://img.shields.io/badge/RabbitMQ-3.12-orange)
![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)

This project demonstrates **professional-grade C# implementation** of RabbitMQ's Consistent Hash Exchange, showcasing modern .NET development patterns, best practices, and real-world usage scenarios. Perfect for C# developers learning advanced message routing and distributed system patterns.

## 🎯 What Makes This Project Special

This is **more than just a basic example** - it's a comprehensive learning resource that demonstrates:

- ✅ **Modern .NET 8 patterns** with dependency injection, configuration, and hosted services
- ✅ **Production-ready code** with error handling, connection resilience, and structured logging
- ✅ **Clean architecture** with service interfaces, separation of concerns, and testability
- ✅ **Comprehensive testing** with unit tests using xUnit and Moq
- ✅ **Docker integration** for easy RabbitMQ setup
- ✅ **Real-world examples** showing different message routing patterns
- ✅ **Detailed documentation** explaining concepts and implementation decisions

## 📋 Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)  
- [Quick Start](#quick-start)
- [Architecture & Design Patterns](#architecture--design-patterns)
- [Configuration Management](#configuration-management)
- [Error Handling & Resilience](#error-handling--resilience)
- [Testing](#testing)
- [Advanced Examples](#advanced-examples)
- [Docker Support](#docker-support)
- [Performance Considerations](#performance-considerations)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)

## Overview

### What is Consistent Hash Exchange?

The **Consistent Hash Exchange** is a RabbitMQ exchange type that routes messages to queues based on a consistent hash algorithm applied to a specified message header. This ensures:

- **Message Ordering**: Messages with the same hash key always go to the same queue
- **Load Distribution**: Even distribution of messages across available queues
- **Scalability**: Easy to add/remove queues with minimal message redistribution
- **Session Affinity**: Maintains stateful processing for related messages

### Key Benefits for C# Developers

1. **Predictable Routing**: Unlike round-robin, messages with the same identifier always route to the same consumer
2. **Horizontal Scaling**: Add more consumers without breaking message ordering
3. **Fault Tolerance**: Isolated failures - only messages for specific hash keys are affected
4. **Performance**: Better throughput than single-threaded ordered processing

## Prerequisites

- **.NET 8.0 SDK** - [Download here](https://dotnet.microsoft.com/download/dotnet/8.0)
- **RabbitMQ Server 3.12+** - [Installation Guide](https://www.rabbitmq.com/download.html)
  - Or use the included Docker setup (recommended)
- **Docker & Docker Compose** (optional, for easy RabbitMQ setup)

## Quick Start

### Option 1: Using Docker (Recommended)

```bash
# Clone the repository
git clone <repository-url>
cd RabbitMq.ConsistentHashExchange

# Start RabbitMQ with Docker
docker-compose up -d

# Wait for RabbitMQ to be ready (about 30 seconds)
# Check status: docker-compose logs rabbitmq

# Build and run the application
dotnet build
dotnet run --project RabbitConsistentHash
```

### Option 2: Local RabbitMQ Installation

```bash
# Ensure RabbitMQ is running with the consistent hash exchange plugin
rabbitmq-plugins enable rabbitmq_consistent_hash_exchange

# Build and run
dotnet build
dotnet run --project RabbitConsistentHash
```

### Expected Output

```
info: RabbitConsistentHash.Services.RabbitMqConnectionService[0]
      RabbitMQ connection service initialized for localhost:5672

info: RabbitConsistentHash.Services.RabbitMqInfrastructureService[0]
      Setting up RabbitMQ infrastructure...

info: RabbitConsistentHash.Services.RabbitMqInfrastructureService[0]
      Successfully declared exchange account-hash-exchange of type x-consistent-hash with hash header accountId

info: RabbitConsistentHash.Services.MessageSubscriber[0]
      Starting message subscribers for 10 queues

info: RabbitConsistentHash.Services.MessagePublisher[0]
      Published message for accountId: 12345678-1234-1234-1234-123456789abc, messageId: a1b2c3d4-...

info: RabbitConsistentHash.Services.MessageSubscriber[0]
      [Queue 3] Received message for accountId: 12345678-1234-1234-1234-123456789abc, messageId: a1b2c3d4-...
```

Notice how messages with the same `accountId` consistently route to the same queue number!

## Architecture & Design Patterns

### Project Structure

```
RabbitConsistentHash/
├── Configuration/          # Strongly-typed configuration classes
│   └── Settings.cs        # RabbitMQ and application settings
├── Services/              # Core business services with interfaces
│   ├── Interfaces.cs      # Service contracts
│   ├── RabbitMqConnectionService.cs
│   ├── RabbitMqInfrastructureService.cs
│   ├── MessagePublisher.cs
│   └── MessageSubscriber.cs
├── Examples/              # Advanced usage patterns
│   └── AdvancedExamples.cs
├── Program.cs             # Application entry point with DI setup
├── Publisher.cs           # Legacy publisher (for comparison)
├── Subscriber.cs          # Legacy subscriber (for comparison)
└── appsettings.json       # Configuration file
```

### Design Patterns Used

- **Dependency Injection**: Full DI container with service lifetime management
- **Options Pattern**: Strongly-typed configuration with `IOptions<T>`
- **Service Layer Pattern**: Clear separation between infrastructure and business logic
- **Repository Pattern**: Abstracted data access through interfaces
- **Background Service Pattern**: Long-running services with proper lifecycle management
- **Factory Pattern**: Connection factory with retry and resilience logic

### Key Services

#### `IRabbitMqConnectionService`
- Manages RabbitMQ connections with automatic retry and error recovery
- Provides connection pooling and channel creation
- Handles connection lifecycle and monitoring

#### `IRabbitMqInfrastructureService`
- Sets up exchanges, queues, and bindings declaratively  
- Configurable infrastructure based on settings
- Idempotent operations for reliable deployment

#### `IMessagePublisher`
- Type-safe message publishing with proper serialization
- Automatic header management for consistent hashing
- Message persistence and delivery guarantees

#### `IMessageSubscriber`
- Scalable message consumption across multiple queues
- Error handling with acknowledgments and retries
- Structured logging for monitoring and debugging

## Configuration Management

### appsettings.json Structure

```json
{
  "RabbitMQ": {
    "HostName": "localhost",
    "UserName": "guest", 
    "Password": "guest",
    "Port": 5672,
    "VirtualHost": "/",
    "Exchange": {
      "Name": "account-hash-exchange",
      "Type": "x-consistent-hash",
      "HashHeader": "accountId"
    },
    "Queue": {
      "Prefix": "queue.account",
      "Count": 10,
      "Durable": true,
      "AutoDelete": false,
      "Exclusive": false
    }
  },
  "Publisher": {
    "MessageIntervalSeconds": 1,
    "MessageCount": -1
  }
}
```

### Environment-Specific Configuration

The application supports multiple environments through configuration:

```bash
# Development
dotnet run --environment Development

# Production  
dotnet run --environment Production

# Custom environment variables
export RabbitMQ__HostName=my-rabbitmq-server
export RabbitMQ__Port=5673
dotnet run
```

### Configuration Classes

```csharp
public class RabbitMqSettings
{
    public const string SectionName = "RabbitMQ";
    
    public string HostName { get; set; } = "localhost";
    public string UserName { get; set; } = "guest";
    public string Password { get; set; } = "guest";
    // ... other properties
}
```

## Error Handling & Resilience

### Connection Resilience

The connection service implements several resilience patterns:

```csharp
// Automatic retry with exponential backoff
private IConnection CreateConnection()
{
    const int maxRetries = 5;
    var retryCount = 0;
    
    while (retryCount < maxRetries)
    {
        try
        {
            // Connection attempt with timeout
            _connection = _connectionFactory.CreateConnectionAsync().GetAwaiter().GetResult();
            return _connection;
        }
        catch (BrokerUnreachableException ex)
        {
            // Exponential backoff with jitter
            var delay = TimeSpan.FromSeconds(Math.Pow(2, retryCount) + Random.Shared.NextDouble());
            Thread.Sleep(delay);
        }
    }
}
```

### Message Processing Resilience

```csharp
// Manual acknowledgments with error handling
try
{
    await ProcessMessageAsync(queueIndex, accountId, messageContent, eventArgs, cancellationToken);
    await channel.BasicAckAsync(eventArgs.DeliveryTag, multiple: false, cancellationToken);
}
catch (Exception ex)
{
    _logger.LogError(ex, "Error processing message");
    // Reject and requeue for retry
    await channel.BasicRejectAsync(eventArgs.DeliveryTag, requeue: true, cancellationToken);
}
```

### Graceful Shutdown

The application handles shutdown signals gracefully:

```csharp
public override async Task StopAsync(CancellationToken cancellationToken)
{
    _logger.LogInformation("Stopping demonstration service...");
    await _subscriber.StopAsync();
    await base.StopAsync(cancellationToken);
}
```

## Testing

### Running Tests

```bash
# Run all tests
dotnet test

# Run with detailed output
dotnet test --logger "console;verbosity=detailed"

# Run with coverage (requires coverlet)
dotnet test --collect:"XPlat Code Coverage"
```

### Test Categories

#### Unit Tests
- **Configuration Tests**: Validate settings classes and default values
- **Service Tests**: Test service contracts and error conditions
- **Publisher Tests**: Verify message publishing logic
- **Infrastructure Tests**: Test RabbitMQ setup procedures

#### Integration Tests
- **End-to-End Tests**: Full message flow testing
- **Connection Tests**: RabbitMQ connectivity and resilience
- **Performance Tests**: Throughput and latency measurements

### Example Test

```csharp
[Fact]
public async Task PublishAsync_WithInvalidAccountId_ShouldThrowArgumentException()
{
    // Arrange, Act & Assert
    await Assert.ThrowsAsync<ArgumentException>(() => 
        _publisher.PublishAsync("", "test message"));
}
```

## Advanced Examples

The project includes advanced examples in the `Examples/` directory:

### Different Message Types

```csharp
// Order processing
await PublishOrderMessage(channel, "customer-123", new
{
    OrderId = "order-456",
    CustomerId = "customer-123", 
    Amount = 99.99m,
    Items = new[] { "item1", "item2" }
});

// User activity tracking
await PublishUserActivityMessage(channel, "user-789", new
{
    UserId = "user-789",
    Action = "login", 
    Timestamp = DateTime.UtcNow
});
```

### Weighted Routing

```csharp
// High-capacity queue gets 3x more messages
await channel.QueueBindAsync("high-capacity-queue", exchangeName, "3");
await channel.QueueBindAsync("standard-queue", exchangeName, "1");
```

### Multiple Hash Headers

```csharp
// Route by customer ID
var customerExchange = new Dictionary<string, object?> { { "hash-header", "customerId" } };

// Route by geographic region  
var regionExchange = new Dictionary<string, object?> { { "hash-header", "region" } };
```

## Docker Support

### Docker Compose Setup

The included `docker-compose.yml` provides:

- **RabbitMQ 3.12** with management plugin
- **Consistent hash exchange plugin** pre-enabled
- **Health checks** and **persistent storage**
- **Management UI** accessible at http://localhost:15672

```bash
# Start RabbitMQ
docker-compose up -d

# View logs
docker-compose logs -f rabbitmq

# Stop and cleanup
docker-compose down -v
```

### Custom RabbitMQ Configuration

Modify the `docker-compose.yml` to customize:

```yaml
environment:
  RABBITMQ_DEFAULT_USER: myuser
  RABBITMQ_DEFAULT_PASS: mypassword  
  RABBITMQ_DEFAULT_VHOST: /production
```

## Performance Considerations

### Scaling Guidelines

- **Queue Count**: Start with 2x CPU cores, adjust based on load testing
- **Consumer Count**: One consumer per queue for optimal performance
- **Connection Pooling**: Use the provided connection service for efficient resource usage
- **Message Size**: Keep messages under 1MB for best performance

### Monitoring Metrics

Monitor these key metrics:

```csharp
// Queue depth
var queueInfo = await channel.QueueDeclarePassiveAsync("queue.account.0");
_logger.LogInformation("Queue depth: {MessageCount}", queueInfo.MessageCount);

// Consumer performance
var processingTime = stopwatch.ElapsedMilliseconds;
_logger.LogInformation("Message processed in {ProcessingTime}ms", processingTime);
```

### Performance Tips

1. **Batch Processing**: Process multiple messages in batches when possible
2. **Async Processing**: Use async/await throughout the processing pipeline  
3. **Connection Reuse**: Share connections across publishers/consumers
4. **Prefetch Settings**: Configure `BasicQos` based on processing capacity

## Troubleshooting

### Common Issues

#### Connection Refused
```
BrokerUnreachableException: None of the specified endpoints were reachable
```
**Solution**: Ensure RabbitMQ is running and accessible on the configured port.

#### Plugin Not Enabled
```
NOT_FOUND - no exchange 'account-hash-exchange' of type 'x-consistent-hash'
```
**Solution**: Enable the consistent hash exchange plugin:
```bash
rabbitmq-plugins enable rabbitmq_consistent_hash_exchange
```

#### Permission Denied
```
ACCESS_REFUSED - access to vhost '/' refused for user 'guest'
```
**Solution**: Configure proper user credentials or use default guest/guest for localhost.

### Debug Logging

Enable detailed logging in `appsettings.json`:

```json
{
  "Logging": {
    "LogLevel": {
      "Default": "Debug",
      "RabbitConsistentHash": "Trace"
    }
  }
}
```

### Health Checks

The application includes health monitoring:

```csharp
public bool IsConnected => _connection?.IsOpen ?? false;
```

Monitor application health through structured logs and connection status.

## Real-World Use Cases

### E-Commerce Order Processing
- Route orders by customer ID to maintain order sequence
- Scale processing based on customer tier (VIP vs regular)
- Maintain shopping cart state across multiple interactions

### User Session Management  
- Route user activities by session ID
- Maintain session state in specific consumers
- Implement sticky sessions for stateful processing

### Financial Transaction Processing
- Route transactions by account number
- Ensure transaction ordering per account
- Implement account-level rate limiting

### IoT Data Processing
- Route sensor data by device ID
- Maintain device state in specific processors
- Implement device-specific processing logic

## Best Practices Demonstrated

### Code Quality
- ✅ **Comprehensive error handling** with specific exception types
- ✅ **Structured logging** with correlation IDs and context
- ✅ **Input validation** with meaningful error messages  
- ✅ **Resource disposal** with proper using statements
- ✅ **Async/await patterns** throughout the codebase

### Architecture 
- ✅ **Dependency injection** with service lifetime management
- ✅ **Configuration management** with strongly-typed settings
- ✅ **Service interfaces** for testability and flexibility
- ✅ **Separation of concerns** between infrastructure and business logic

### Testing & Quality
- ✅ **Unit tests** with high code coverage
- ✅ **Integration tests** for end-to-end validation
- ✅ **Mocking** external dependencies
- ✅ **Test data builders** for maintainable tests

## Contributing

We welcome contributions! Please see our [Contributing Guidelines](CONTRIBUTING.md) for details.

### Development Setup

```bash
# Clone and setup
git clone <repository-url>
cd RabbitMq.ConsistentHashExchange

# Start dependencies
docker-compose up -d

# Run tests
dotnet test

# Run application
dotnet run --project RabbitConsistentHash
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🎓 Learning Resources

- [RabbitMQ Consistent Hash Exchange Documentation](https://github.com/rabbitmq/rabbitmq-consistent-hash-exchange)
- [.NET Generic Host Documentation](https://docs.microsoft.com/en-us/aspnet/core/fundamentals/host/generic-host)
- [RabbitMQ .NET Client Documentation](https://www.rabbitmq.com/dotnet-api-guide.html)
- [Dependency Injection in .NET](https://docs.microsoft.com/en-us/dotnet/core/extensions/dependency-injection)

**Made with ❤️ for the C# developer community**