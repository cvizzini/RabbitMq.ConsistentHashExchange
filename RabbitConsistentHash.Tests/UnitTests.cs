using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using RabbitConsistentHash.Configuration;
using RabbitConsistentHash.Services;
using Xunit;

namespace RabbitConsistentHash.Tests.Configuration;

public class RabbitMqSettingsTests
{
    [Fact]
    public void RabbitMqSettings_ShouldHaveCorrectDefaultValues()
    {
        // Arrange & Act
        var settings = new RabbitMqSettings();
        
        // Assert
        Assert.Equal("localhost", settings.HostName);
        Assert.Equal("guest", settings.UserName);
        Assert.Equal("guest", settings.Password);
        Assert.Equal(5672, settings.Port);
        Assert.Equal("/", settings.VirtualHost);
        Assert.NotNull(settings.Exchange);
        Assert.NotNull(settings.Queue);
    }
    
    [Fact]
    public void ExchangeSettings_ShouldHaveCorrectDefaultValues()
    {
        // Arrange & Act
        var settings = new ExchangeSettings();
        
        // Assert
        Assert.Equal("account-hash-exchange", settings.Name);
        Assert.Equal("x-consistent-hash", settings.Type);
        Assert.Equal("accountId", settings.HashHeader);
    }
    
    [Fact]
    public void QueueSettings_ShouldHaveCorrectDefaultValues()
    {
        // Arrange & Act
        var settings = new QueueSettings();
        
        // Assert
        Assert.Equal("queue.account", settings.Prefix);
        Assert.Equal(10, settings.Count);
        Assert.True(settings.Durable);
        Assert.False(settings.AutoDelete);
        Assert.False(settings.Exclusive);
    }
    
    [Fact]
    public void PublisherSettings_ShouldHaveCorrectDefaultValues()
    {
        // Arrange & Act
        var settings = new PublisherSettings();
        
        // Assert
        Assert.Equal(1, settings.MessageIntervalSeconds);
        Assert.Equal(-1, settings.MessageCount);
    }
}

public class MessagePublisherTests
{
    private readonly Mock<IRabbitMqConnectionService> _mockConnectionService;
    private readonly Mock<ILogger<MessagePublisher>> _mockLogger;
    private readonly IOptions<RabbitMqSettings> _options;
    private readonly MessagePublisher _publisher;

    public MessagePublisherTests()
    {
        _mockConnectionService = new Mock<IRabbitMqConnectionService>();
        _mockLogger = new Mock<ILogger<MessagePublisher>>();
        
        var settings = new RabbitMqSettings();
        _options = Options.Create(settings);
        
        _publisher = new MessagePublisher(_mockConnectionService.Object, _options, _mockLogger.Object);
    }

    [Fact]
    public void Constructor_WithNullConnectionService_ShouldThrowArgumentNullException()
    {
        // Arrange, Act & Assert
        Assert.Throws<ArgumentNullException>(() => 
            new MessagePublisher(null!, _options, _mockLogger.Object));
    }

    [Fact]
    public void Constructor_WithNullSettings_ShouldThrowArgumentNullException()
    {
        // Arrange, Act & Assert
        Assert.Throws<ArgumentNullException>(() => 
            new MessagePublisher(_mockConnectionService.Object, null!, _mockLogger.Object));
    }

    [Fact]
    public void Constructor_WithNullLogger_ShouldThrowArgumentNullException()
    {
        // Arrange, Act & Assert
        Assert.Throws<ArgumentNullException>(() => 
            new MessagePublisher(_mockConnectionService.Object, _options, null!));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public async Task PublishAsync_WithInvalidAccountId_ShouldThrowArgumentException(string? accountId)
    {
        // Arrange, Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() => 
            _publisher.PublishAsync(accountId!, "test message"));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public async Task PublishAsync_WithInvalidMessage_ShouldThrowArgumentException(string? message)
    {
        // Arrange, Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() => 
            _publisher.PublishAsync("account123", message!));
    }
}

public class RabbitMqInfrastructureServiceTests
{
    private readonly Mock<IRabbitMqConnectionService> _mockConnectionService;
    private readonly Mock<ILogger<RabbitMqInfrastructureService>> _mockLogger;
    private readonly IOptions<RabbitMqSettings> _options;
    private readonly RabbitMqInfrastructureService _service;

    public RabbitMqInfrastructureServiceTests()
    {
        _mockConnectionService = new Mock<IRabbitMqConnectionService>();
        _mockLogger = new Mock<ILogger<RabbitMqInfrastructureService>>();
        
        var settings = new RabbitMqSettings();
        _options = Options.Create(settings);
        
        _service = new RabbitMqInfrastructureService(_mockConnectionService.Object, _options, _mockLogger.Object);
    }

    [Fact]
    public void Constructor_WithNullConnectionService_ShouldThrowArgumentNullException()
    {
        // Arrange, Act & Assert
        Assert.Throws<ArgumentNullException>(() => 
            new RabbitMqInfrastructureService(null!, _options, _mockLogger.Object));
    }

    [Fact]
    public void Constructor_WithNullSettings_ShouldThrowArgumentNullException()
    {
        // Arrange, Act & Assert
        Assert.Throws<ArgumentNullException>(() => 
            new RabbitMqInfrastructureService(_mockConnectionService.Object, null!, _mockLogger.Object));
    }

    [Fact]
    public void Constructor_WithNullLogger_ShouldThrowArgumentNullException()
    {
        // Arrange, Act & Assert
        Assert.Throws<ArgumentNullException>(() => 
            new RabbitMqInfrastructureService(_mockConnectionService.Object, _options, null!));
    }
}