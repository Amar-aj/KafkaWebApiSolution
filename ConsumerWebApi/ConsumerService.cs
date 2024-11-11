
using Confluent.Kafka;

namespace ConsumerWebApi;

public class ConsumerService : BackgroundService
{
    private readonly IConfiguration _configuration;
    private readonly IConsumer<Ignore, string> _consumer;

    private readonly ILogger<ConsumerService> _logger;

    readonly string _topic = "ProduceChat";

    public ConsumerService(IConfiguration configuration, ILogger<ConsumerService> logger)
    {
        _logger = logger;
        _configuration = configuration;

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _configuration["Kafka:BootstrapServers"],
            AutoOffsetReset = AutoOffsetReset.Earliest,
            GroupId = "my-group",
        };
        _consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build();
    }
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _consumer.Subscribe(_topic);

        while (!stoppingToken.IsCancellationRequested)
        {
            ProcessKafkaMessage(stoppingToken);

            Task.Delay(TimeSpan.FromMinutes(1), stoppingToken);
        }

        _consumer.Close();
    }

    public void ProcessKafkaMessage(CancellationToken stoppingToken)
    {
        try
        {
            var consumeResult = _consumer.Consume(stoppingToken);

            _logger.LogInformation($"Received chat update: topic - {consumeResult.Topic}  message - {consumeResult.Message.Value}");
        }
        catch (Exception ex)
        {
            _logger.LogError($"Error processing Kafka message: {ex.Message}");
        }
    }
}
