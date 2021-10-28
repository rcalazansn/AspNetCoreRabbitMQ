using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Consumer.Worker
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly RabbitMq _rabbitMq;

        private IModel _model;
        private IConnection _connection;

        private string _consumerTag;

        private const string QueueName = "pedido_cadastrar_queue";
        private const string ExchangeName = "pedido_exchange";
        public Worker(ILogger<Worker> logger, RabbitMq rabbitMq)
        {
            _logger = logger;
            _rabbitMq = rabbitMq ?? throw new ArgumentNullException(nameof(rabbitMq));

            InitializeRabbitMqListener().Wait();
        }
        private async Task InitializeRabbitMqListener()
        {
            var factory = new ConnectionFactory
            {
                HostName = _rabbitMq.Hostname,
                UserName = _rabbitMq.UserName,
                Password = _rabbitMq.Password
            };

            _connection = factory.CreateConnection();
            _model = _connection.CreateModel();

            _model.BasicQos(0, _rabbitMq.PrefetchCount, false); // False: Por consumer True: Por channel

            BuildConsumer();

            await Task.CompletedTask;
        }
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            Consumer();

            await Task.CompletedTask;
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            _model.BasicCancelNoWait(_consumerTag);

            await Task.CompletedTask;
        }

        private void BuildConsumer()
        {
            //Deadletter
            _model.ExchangeDeclare("pedido_deadletter_exchange", "fanout", true, false, null);
            _model.QueueDeclare("pedido_deadletter_queue", true, false, false, null);
            _model.QueueBind("pedido_deadletter_queue", "pedido_deadletter_exchange", string.Empty, null);

            //Retry
            _model.ExchangeDeclare("retry_exchange", "fanout", true, false, null);
            _model.QueueDeclare("retry_queue", true, false, false, new Dictionary<string, object>() {
                { "x-dead-letter-exchange", "pedido_deadletter_exchange" },
                { "x-dead-letter-routing-key", "" }
            });
            _model.QueueBind("retry_queue", "retry_exchange", string.Empty, null);

            //Fila Principal
            _model.ExchangeDeclare(ExchangeName, "topic", true, false, null);
            _model.QueueDeclare(QueueName, true, false, false, new Dictionary<string, object>() {
                    { "x-dead-letter-exchange", "retry_exchange" }
            });
            _model.QueueBind(QueueName, ExchangeName, "", null);
        }
        private void Consumer()
        {
            var consumer = new EventingBasicConsumer(_model);
            var options = new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase, WriteIndented = true };

            consumer.Received += (ch, ea) =>
            {
                var content = Encoding.UTF8.GetString(ea.Body.ToArray());

                try
                {
                    if (content != "retry")
                    {
                        _logger.LogInformation("Processado com sucesso");
                        _model.BasicAck(ea.DeliveryTag, false);
                    }
                    else
                    {
                        _logger.LogWarning("Encaminhando para fila de retentativa..");
                        _model.BasicNack(ea.DeliveryTag, false, false);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "{Erro}", ex.Message);

                    _model.BasicNack(ea.DeliveryTag, false, false);
                }
            };

            _consumerTag =  _model.BasicConsume(queue: QueueName, autoAck: false, consumer: consumer);
        }
    }
}
