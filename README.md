# RabbitMQ Consistent Hash Exchange Example

This project demonstrates how to use RabbitMQ's consistent hash exchange to distribute messages across multiple queues based on a hash of a message property, ensuring that messages with the same hash key always go to the same queue.

## Overview

The consistent hash exchange is a RabbitMQ exchange type that routes messages to queues based on a consistent hash algorithm. This is particularly useful when you need to:

- Ensure message ordering for related messages (e.g., all messages for a specific account)
- Distribute load evenly across multiple consumer queues
- Maintain session affinity or stateful processing

In this example, messages are routed based on an `accountId` header, ensuring that all messages for the same account are always processed by the same queue and consumer.

## Prerequisites

- **.NET 8.0 SDK** - [Download here](https://dotnet.microsoft.com/download/dotnet/8.0)
- **RabbitMQ Server** - [Installation Guide](https://www.rabbitmq.com/download.html)
  - Default configuration (localhost, guest/guest credentials)
  - Consistent hash exchange plugin must be enabled (usually enabled by default)

## Getting Started

### 1. Start RabbitMQ Server

Make sure RabbitMQ is running on your local machine with default settings:
- Host: `localhost`
- Username: `guest`
- Password: `guest`
- Port: `5672` (AMQP)

### 2. Build the Project

```bash
dotnet build
```

### 3. Run the Application

```bash
dotnet run --project RabbitConsistentHash
```

## How It Works

### Consistent Hash Exchange Setup

The application creates:
- **1 Exchange**: `account-hash-exchange` (type: `x-consistent-hash`)
- **10 Queues**: `queue.account.0` through `queue.account.9`
- **Hash Header**: Messages are routed based on the `accountId` header

### Message Flow

1. **Publisher** (`Publisher.cs`):
   - Generates random account IDs
   - Publishes messages to the consistent hash exchange
   - Includes the `accountId` in the message headers

2. **Consistent Hash Exchange**:
   - Calculates a hash of the `accountId` header value
   - Routes the message to one of the 10 bound queues
   - Ensures messages with the same `accountId` always go to the same queue

3. **Subscribers** (`Subscriber.cs`):
   - Each queue has a dedicated consumer
   - Processes messages and logs which queue received each message
   - Shows the account ID for traceability

### Code Structure

```
RabbitConsistentHash/
├── Program.cs          # Main application entry point
├── Publisher.cs        # Message publishing logic
├── Subscriber.cs       # Message consuming logic
└── RabbitConsistentHash.csproj
```

#### Key Components

- **Program.cs**: Sets up RabbitMQ connection, declares the exchange and queues, starts consumers, and runs the publishing loop
- **Publisher.cs**: Contains the `Publish` method that sends messages with `accountId` headers
- **Subscriber.cs**: Contains the `Subscribe` method that consumes messages from a specific queue

## Example Output

When running the application, you'll see output similar to:

```
Published: 'This is a test message for account 12345678-1234-1234-1234-123456789abc' for accountId: 12345678-1234-1234-1234-123456789abc
[Queue 3] Received: This is a test message for account 12345678-1234-1234-1234-123456789abc for accountId: 12345678-1234-1234-1234-123456789abc

Published: 'This is a test message for account 87654321-4321-4321-4321-cba987654321' for accountId: 87654321-4321-4321-4321-cba987654321
[Queue 7] Received: This is a test message for account 87654321-4321-4321-4321-cba987654321 for accountId: 87654321-4321-4321-4321-cba987654321
```

Notice how messages with the same `accountId` consistently go to the same queue number.

## Configuration

You can modify the following parameters in `Program.cs`:

- **Exchange Name**: Change `exchangeName` variable
- **Queue Count**: Modify `queueCount` to create more or fewer queues
- **Hash Header**: Update the exchange arguments to use a different header for hashing
- **Message Interval**: Adjust the `Task.Delay` value to change publishing frequency

## Benefits of Consistent Hashing

1. **Message Ordering**: Messages for the same hash key (accountId) maintain order
2. **Load Distribution**: Messages are distributed evenly across available queues
3. **Scalability**: Easy to add or remove queues without major redistribution
4. **Fault Tolerance**: If a queue becomes unavailable, only messages for specific hash keys are affected

## Troubleshooting

### RabbitMQ Connection Issues
- Ensure RabbitMQ server is running
- Check that the default guest/guest credentials are enabled
- Verify the management plugin is installed for web UI access

### Exchange Plugin Issues
- Ensure the consistent hash exchange plugin is enabled:
  ```bash
  rabbitmq-plugins enable rabbitmq_consistent_hash_exchange
  ```

## Dependencies

- **RabbitMQ.Client**: Version 7.1.2 - Official RabbitMQ .NET client library

## License

This project is provided as an educational example for demonstrating RabbitMQ consistent hash exchange functionality.