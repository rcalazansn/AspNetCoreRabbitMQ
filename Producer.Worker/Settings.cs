namespace Producer.Worker
{
    public class RabbitMq
    {
        public string Hostname { get; set; }
        public string UserName { get; set; }
        public string Password { get; set; }
        public ushort PrefetchCount => 10;
    }
}
