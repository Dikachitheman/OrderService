using Amazon.SQS;
using Amazon.SQS.Model;
using System.Text.Json;

namespace Services;

public class AWSSQSService
{
    private readonly IAmazonSQS _sqsClient;
    private readonly Dictionary<string, string> _queueUrls;

    public AWSSQSService(IAmazonSQS sqsClient)
    {
        _sqsClient = sqsClient;
        _queueUrls = new Dictionary<string, string>();
    }

    public async Task<string> CreateQueueAsync(string queueName)
    {
        try
        {
            var request = new CreateQueueRequest
            {
                QueueName = queueName,
                Attributes = new Dictionary<string, string>
                {
                    { "MessageRetentionPeriod", "345600" }, // 4 days
                    { "VisibilityTimeout", "30" }
                }
            };

            var response = await _sqsClient.CreateQueueAsync(request);
            _queueUrls[queueName] = response.QueueUrl;
            Console.WriteLine($"✅ Queue created/retrieved: {queueName}");
            return response.QueueUrl;
        }
        catch (QueueNameExistsException)
        {
            var urlResponse = await _sqsClient.GetQueueUrlAsync(queueName);
            _queueUrls[queueName] = urlResponse.QueueUrl;
            Console.WriteLine($"✅ Queue already exists: {queueName}");
            return urlResponse.QueueUrl;
        }
    }

    public async Task PublishMessageAsync<T>(string queueName, T message)
    {
        if (!_queueUrls.ContainsKey(queueName))
        {
            await CreateQueueAsync(queueName);
        }

        var messageBody = JsonSerializer.Serialize(message);
        var request = new SendMessageRequest
        {
            QueueUrl = _queueUrls[queueName],
            MessageBody = messageBody
        };

        await _sqsClient.SendMessageAsync(request);
    }

    public async Task ConsumeMessagesAsync<T>(string queueName, Func<T, Task> onMessageReceived, CancellationToken cancellationToken = default)
    {
        if (!_queueUrls.ContainsKey(queueName))
        {
            await CreateQueueAsync(queueName);
        }

        _ = Task.Run(async () =>
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var request = new ReceiveMessageRequest
                    {
                        QueueUrl = _queueUrls[queueName],
                        MaxNumberOfMessages = 10,
                        WaitTimeSeconds = 20, // Long polling
                        AttributeNames = new List<string> { "All" },
                        MessageAttributeNames = new List<string> { "All" }
                    };

                    var response = await _sqsClient.ReceiveMessageAsync(request, cancellationToken);

                    foreach (var message in response.Messages)
                    {
                        try
                        {
                            var deserializedMessage = JsonSerializer.Deserialize<T>(message.Body);
                            if (deserializedMessage != null)
                            {
                                await onMessageReceived(deserializedMessage);
                            }

                            // Delete message after successful processing
                            await _sqsClient.DeleteMessageAsync(new DeleteMessageRequest
                            {
                                QueueUrl = _queueUrls[queueName],
                                ReceiptHandle = message.ReceiptHandle
                            }, cancellationToken);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"❌ Error processing message: {ex.Message}");
                            // Message will become visible again after visibility timeout
                        }
                    }
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    Console.WriteLine($"❌ Error receiving messages: {ex.Message}");
                    await Task.Delay(5000, cancellationToken); // Wait before retrying
                }
            }
        }, cancellationToken);
    }
}