using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using RabbitMQ.Client;

namespace Producer.Worker
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly RabbitMq _rabbitMq;

        private IModel _model;
        private IConnection _connection;

        public const string QueueName = "pedido_cadastrar_queue";
        public const string ExchangeName = "pedido_exchange";

        private readonly IList<string> Messages = new List<string> { "Mensagem 1", "Mensagem 2", "Mensagem 3", "retry" };
        private readonly Random random = new Random();
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
            while (!stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation($"Publicando mensagem: {DateTime.Now}");
                Producer();

                await Task.Delay(1000, stoppingToken);
            }
        }

        private void BuildConsumer()
        {
            //Deadletter
            this._model.ExchangeDeclare("pedido_deadletter_exchange", "fanout", true, false, null);
            this._model.QueueDeclare("pedido_deadletter_queue", true, false, false, null);
            this._model.QueueBind("pedido_deadletter_queue", "pedido_deadletter_exchange", string.Empty, null);

            //Retry
            this._model.ExchangeDeclare("retry_exchange", "fanout", true, false, null);
            this._model.QueueDeclare("retry_queue", true, false, false, new Dictionary<string, object>() {
                { "x-dead-letter-exchange", "pedido_deadletter_exchange" },
                { "x-dead-letter-routing-key", "" }
            });
            this._model.QueueBind("retry_queue", "retry_exchange", string.Empty, null);

            //Fila Principal
            this._model.ExchangeDeclare(ExchangeName, "topic", true, false, null);
            this._model.QueueDeclare(QueueName, true, false, false, new Dictionary<string, object>() {
                    { "x-dead-letter-exchange", "retry_exchange" }
            });
            this._model.QueueBind(QueueName, ExchangeName, "", null);
        }
        private void Producer()
        {
            var body = Encoding.UTF8.GetBytes(Messages[random.Next(Messages.Count)]);

            _model.BasicPublish(ExchangeName,
                                 routingKey: "",
                                 basicProperties: null,
                                 body: body);
        }
    }
}
