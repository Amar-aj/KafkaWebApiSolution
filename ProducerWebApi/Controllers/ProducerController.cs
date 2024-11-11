using Confluent.Kafka;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace ProducerWebApi.Controllers;

[Route("api/[controller]")]
[ApiController]
public class ProducerController : ControllerBase
{
    private readonly IConfiguration _configuration;
    private readonly IProducer<Null, string> _producer;
    readonly string _topic = "ProduceChat";
    readonly string _clientId = "my-app";

    public ProducerController(IConfiguration configuration)
    {
        _configuration = configuration;

        var producerconfig = new ProducerConfig
        {
            BootstrapServers = _configuration["Kafka:BootstrapServers"],
            ClientId = _clientId,
            BrokerAddressFamily = BrokerAddressFamily.V4,
        };
        _producer = new ProducerBuilder<Null, string>(producerconfig).Build();
    }

    [HttpPost]
    public async Task<IActionResult> PostAsync(string message)
    {
        var kafkamessage = new Message<Null, string> { Value = message, };
        await _producer.ProduceAsync(_topic, kafkamessage);
        return Ok("Updated Successfully...");
    }
}
