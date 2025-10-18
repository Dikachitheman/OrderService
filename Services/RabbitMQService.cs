using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Text.Json;

namespace Services
{
    public class RabbitMQService : IDisposable
    {
        private readonly IConnection _connection;
        private readonly IChannel _channel;

        public RabbitMQService(string hostName = "localhost")
        {
            try
            {
                Console.WriteLine($"[RabbitMQ] Initializing connection to {hostName}");
                var factory = new ConnectionFactory() { HostName = hostName };
                _connection = factory.CreateConnectionAsync().GetAwaiter().GetResult();
                _channel = _connection.CreateChannelAsync().GetAwaiter().GetResult();
                Console.WriteLine("[RabbitMQ] Connection established successfully");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[RabbitMQ ERROR] Failed to connect: {ex.Message}");
                throw;
            }
        }

        public void DeclareQueue(string queueName)
        {
            try
            {
                Console.WriteLine($"[RabbitMQ] Declaring queue: {queueName}");
                _channel.QueueDeclareAsync(
                    queue: queueName,
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: null).GetAwaiter().GetResult();
                Console.WriteLine($"[RabbitMQ] Queue '{queueName}' declared successfully");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[RabbitMQ ERROR] Failed to declare queue '{queueName}': {ex.Message}");
                throw;
            }
        }

        public void PublishMessage<T>(string queueName, T message)
        {
            try
            {
                var json = JsonSerializer.Serialize(message);
                var body = Encoding.UTF8.GetBytes(json);

                var properties = new BasicProperties
                {
                    Persistent = true
                };

                _channel.BasicPublishAsync(
                    exchange: "",
                    routingKey: queueName,
                    mandatory: false,
                    basicProperties: properties,
                    body: body).GetAwaiter().GetResult();

                Console.WriteLine($"[RabbitMQ] Published to {queueName}: {json}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[RabbitMQ ERROR] Failed to publish to '{queueName}': {ex.Message}");
                throw;
            }
        }

        public void ConsumeMessages<T>(string queueName, Action<T> onMessageReceived)
        {
            try
            {
                Console.WriteLine($"[RabbitMQ] Starting consumer for queue: {queueName}");
                var consumer = new AsyncEventingBasicConsumer(_channel);
                
                consumer.ReceivedAsync += async (model, ea) =>
                {
                    var body = ea.Body.ToArray();
                    var json = Encoding.UTF8.GetString(body);
                    
                    try
                    {
                        Console.WriteLine($"[RabbitMQ] Received from {queueName} (Tag: {ea.DeliveryTag}): {json}");
                        
                        var message = JsonSerializer.Deserialize<T>(json);
                        onMessageReceived(message);
                        
                        await _channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);
                        Console.WriteLine($"[RabbitMQ] Successfully processed message (Tag: {ea.DeliveryTag})");
                    }
                    catch (JsonException ex)
                    {
                        Console.WriteLine($"[RabbitMQ ERROR] Failed to deserialize message from {queueName}: {ex.Message}");
                        await _channel.BasicNackAsync(deliveryTag: ea.DeliveryTag, multiple: false, requeue: false);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[RabbitMQ ERROR] Error processing message from {queueName}: {ex.Message}");
                        await _channel.BasicNackAsync(deliveryTag: ea.DeliveryTag, multiple: false, requeue: true);
                    }
                };

                _channel.BasicConsumeAsync(queue: queueName, autoAck: false, consumer: consumer).GetAwaiter().GetResult();
                Console.WriteLine($"[RabbitMQ] Consumer started for queue: {queueName}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[RabbitMQ ERROR] Failed to start consumer for '{queueName}': {ex.Message}");
                throw;
            }
        }

        public void Dispose()
        {
            try
            {
                Console.WriteLine("[RabbitMQ] Disposing service...");
                _channel?.CloseAsync().GetAwaiter().GetResult();
                _connection?.CloseAsync().GetAwaiter().GetResult();
                Console.WriteLine("[RabbitMQ] Service disposed successfully");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[RabbitMQ ERROR] Error during disposal: {ex.Message}");
            }
        }
    }
}