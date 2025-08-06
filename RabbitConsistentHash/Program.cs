using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitConsistentHash.Configuration;
using RabbitConsistentHash.Services;

namespace RabbitConsistentHash;

/// <summary>
/// Main application demonstrating RabbitMQ Consistent Hash Exchange with modern .NET patterns
/// </summary>
public class Program
{
    public static async Task Main(string[] args)
    {
        // Build configuration
        var configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            .AddEnvironmentVariables()
            .AddCommandLine(args)
            .Build();

        // Create host with dependency injection
        var host = Host.CreateDefaultBuilder(args)
            .ConfigureServices((context, services) =>
            {
                // Configuration
                services.Configure<RabbitMqSettings>(configuration.GetSection(RabbitMqSettings.SectionName));
                services.Configure<PublisherSettings>(configuration.GetSection(PublisherSettings.SectionName));

                // Services
                services.AddSingleton<IRabbitMqConnectionService, RabbitMqConnectionService>();
                services.AddSingleton<IRabbitMqInfrastructureService, RabbitMqInfrastructureService>();
                services.AddSingleton<IMessagePublisher, MessagePublisher>();
                services.AddSingleton<IMessageSubscriber, MessageSubscriber>();

                // Background service for running the demo
                services.AddHostedService<ConsistentHashDemoService>();
            })
            .ConfigureLogging(logging =>
            {
                logging.ClearProviders();
                logging.AddConsole();
            })
            .UseConsoleLifetime()
            .Build();

        try
        {
            await host.RunAsync();
        }
        catch (Exception ex)
        {
            var logger = host.Services.GetService<ILogger<Program>>();
            logger?.LogCritical(ex, "Application terminated unexpectedly");
            throw;
        }
    }
}

/// <summary>
/// Background service that orchestrates the consistent hash exchange demonstration
/// </summary>
public class ConsistentHashDemoService : BackgroundService
{
    private readonly IRabbitMqInfrastructureService _infrastructureService;
    private readonly IMessagePublisher _publisher;
    private readonly IMessageSubscriber _subscriber;
    private readonly PublisherSettings _publisherSettings;
    private readonly ILogger<ConsistentHashDemoService> _logger;
    private readonly IHostApplicationLifetime _applicationLifetime;

    public ConsistentHashDemoService(
        IRabbitMqInfrastructureService infrastructureService,
        IMessagePublisher publisher,
        IMessageSubscriber subscriber,
        IOptions<PublisherSettings> publisherSettings,
        ILogger<ConsistentHashDemoService> logger,
        IHostApplicationLifetime applicationLifetime)
    {
        _infrastructureService = infrastructureService ?? throw new ArgumentNullException(nameof(infrastructureService));
        _publisher = publisher ?? throw new ArgumentNullException(nameof(publisher));
        _subscriber = subscriber ?? throw new ArgumentNullException(nameof(subscriber));
        _publisherSettings = publisherSettings.Value ?? throw new ArgumentNullException(nameof(publisherSettings));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _applicationLifetime = applicationLifetime ?? throw new ArgumentNullException(nameof(applicationLifetime));
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            _logger.LogInformation("Starting RabbitMQ Consistent Hash Exchange demonstration...");

            // Step 1: Setup RabbitMQ infrastructure
            _logger.LogInformation("Setting up RabbitMQ infrastructure...");
            await _infrastructureService.SetupInfrastructureAsync(stoppingToken);

            // Step 2: Start message subscribers
            _logger.LogInformation("Starting message subscribers...");
            await _subscriber.StartAsync(stoppingToken);

            // Allow some time for subscribers to initialize
            await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);

            // Step 3: Start publishing messages
            _logger.LogInformation("Starting message publication...");
            await PublishMessagesAsync(stoppingToken);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Application shutdown requested");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Critical error in demonstration service");
            _applicationLifetime.StopApplication();
            throw;
        }
        finally
        {
            await _subscriber.StopAsync();
        }
    }

    /// <summary>
    /// Publishes demo messages to demonstrate consistent hash routing
    /// </summary>
    private async Task PublishMessagesAsync(CancellationToken stoppingToken)
    {
        var messageCount = 0;
        var maxMessages = _publisherSettings.MessageCount;
        var intervalSeconds = _publisherSettings.MessageIntervalSeconds;

        _logger.LogInformation("Publishing messages every {IntervalSeconds} seconds. Max messages: {MaxMessages} (-1 = infinite)",
            intervalSeconds, maxMessages == -1 ? "infinite" : maxMessages.ToString());

        // Pre-generate some account IDs to show consistent routing
        var accountIds = new[]
        {
            "12345678-1234-1234-1234-123456789abc",
            "87654321-4321-4321-4321-cba987654321",
            "11111111-2222-3333-4444-555555555555",
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "ffffffff-0000-1111-2222-333333333333"
        };

        while (!stoppingToken.IsCancellationRequested && (maxMessages == -1 || messageCount < maxMessages))
        {
            try
            {
                // Alternate between predefined account IDs and random ones to demonstrate consistency
                var accountId = messageCount % 3 == 0 
                    ? accountIds[messageCount % accountIds.Length] 
                    : Guid.NewGuid().ToString();

                var messageContent = $"Demo message #{messageCount + 1} for account {accountId} - {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC";

                await _publisher.PublishAsync(accountId, messageContent, stoppingToken);

                messageCount++;

                if (intervalSeconds > 0)
                {
                    await Task.Delay(TimeSpan.FromSeconds(intervalSeconds), stoppingToken);
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error publishing message #{MessageCount}", messageCount + 1);
                // Continue publishing other messages
            }
        }

        _logger.LogInformation("Completed publishing {MessageCount} messages", messageCount);
        
        // Keep the application running to show continued message processing
        _logger.LogInformation("Application will continue running to show message processing. Press Ctrl+C to stop.");
        
        try
        {
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Shutdown requested, stopping application...");
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Stopping demonstration service...");
        await _subscriber.StopAsync();
        await base.StopAsync(cancellationToken);
        _logger.LogInformation("Demonstration service stopped");
    }
}