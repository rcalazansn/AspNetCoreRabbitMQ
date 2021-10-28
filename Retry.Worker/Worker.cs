using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Retry.Worker
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly RabbitMq _rabbitMq;
        private readonly IConnection _connection;
        public string[] QueueNames { get; set; }
        public Worker(ILogger<Worker> logger, IConnection connection, RabbitMq rabbitMq)
        {
            _logger = logger;
            _rabbitMq = rabbitMq ?? throw new ArgumentNullException(nameof(rabbitMq));

            _connection = connection;
        }
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            using IModel model = _connection.CreateModel();
            model.BasicQos(0, _rabbitMq.PrefetchCount, false);

            EventingBasicConsumer consumer = BuildConsumer(model);

            WaitQueueCreation();

            string[] consumerTags = StartConsume(model, consumer);

            while (!stoppingToken.IsCancellationRequested)
            {
                await Task.Delay(1000, stoppingToken);
            }

            StopConsume(model, consumerTags);
        }
        private EventingBasicConsumer BuildConsumer(IModel model)
        {
            var consumer = new EventingBasicConsumer(model);

            consumer.Received += (sender, ea) =>
            {
                List<object> xdeaths = (List<object>)ea.BasicProperties.Headers["x-death"];
                foreach (Dictionary<string, object> xdeath in xdeaths)
                {
                    string queueName = Encoding.UTF8.GetString((byte[])xdeath["queue"]);
                    if (QueueNames.Contains(queueName))
                        continue;

                    long count = (long)xdeath["count"];

                    string exchangeName = Encoding.UTF8.GetString((byte[])xdeath["exchange"]);

                    List<object> routingKeyList = ((List<object>)xdeath["routing-keys"]);

                    List<string> keys = routingKeyList.ConvertAll(key => Encoding.UTF8.GetString((byte[])key));

                    if (count <= 3)
                    {
                        _logger.LogWarning($"{DateTime.Now.ToString("hh:mm:ss")} - Encaminhando para a fila: {keys.FirstOrDefault()} Qtd:({count})");

                       // Thread.Sleep(TimeSpan.FromSeconds(6));

                        model.BasicPublish(exchangeName, keys.FirstOrDefault() ?? string.Empty, ea.BasicProperties, ea.Body);
                        model.BasicAck(ea.DeliveryTag, false);
                    }
                    else
                    {
                        _logger.LogWarning($"{DateTime.Now.ToString("hh:mm:ss")} - Enviando para dead letter");
                        model.BasicNack(ea.DeliveryTag, false, false);
                    }
                }
            };
            return consumer;
        }
        private void WaitQueueCreation()
        {
            using var model = _connection.CreateModel();
            foreach (var queueName in QueueNames)
                model.QueueDeclarePassive(queueName);
        }
        private string[] StartConsume(IModel model, EventingBasicConsumer consumer)
        {
            return QueueNames
                      .Select(queueName =>
                          model.BasicConsume(queue: queueName, autoAck: false, consumer: consumer)
                      )
                      .ToArray();
        }
        private void StopConsume(IModel model, string[] consumerTags)
        {
            foreach (var consumerTag in consumerTags)
                model.BasicCancelNoWait(consumerTag);
        }
    }
}
