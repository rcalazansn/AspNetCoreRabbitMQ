using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;

using RabbitMQ.Client;

namespace Retry.Worker
{
    public class Program
    {
        public static void Main(string[] args)
        {
            CreateHostBuilder(args).Build().Run();
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureServices((hostContext, services) =>
                {
                    IConfiguration configuration = hostContext.Configuration;

                    RabbitMq rabbitMq = configuration.GetSection("RabbitMq").Get<RabbitMq>();
                    services.AddSingleton(rabbitMq);

                    services.AddSingleton(sp =>
                    {
                        ConnectionFactory factory = new ConnectionFactory();
                        hostContext.Configuration.Bind("RABBITMQ", factory);
                        return factory;
                    });

                    services.AddSingleton(sp =>
                        sp.GetRequiredService<ConnectionFactory>().CreateConnection());

                    services.AddHostedService(sp =>
                    {
                        var consumer = new Worker(
                            sp.GetRequiredService<ILogger<Worker>>(),
                            sp.GetRequiredService<IConnection>(),
                            rabbitMq
                        );
                        hostContext.Configuration.Bind("RABBITMQ", consumer);
                        consumer.QueueNames = (rabbitMq.RetryQueue ?? "").Split(';', StringSplitOptions.RemoveEmptyEntries);

                        return consumer;
                    });
                });
    }
}
