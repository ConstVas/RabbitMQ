using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Collections.Concurrent;

namespace DeclareRpcClient
{
    public class RpcClient
    {
        protected readonly IConnection connection;
        protected readonly IModel channel;
        protected readonly string replyQueueName;
        protected readonly AsyncEventingBasicConsumer consumer;
        protected readonly IBasicProperties props;

        public RpcClient(string name)
        {
            var factory = new ConnectionFactory() { HostName = "localhost", DispatchConsumersAsync = true };

            connection = factory.CreateConnection(name);
            channel = connection.CreateModel();
            replyQueueName = channel.QueueDeclare().QueueName;
            consumer = new AsyncEventingBasicConsumer(channel);

            props = channel.CreateBasicProperties();
            DeclareAMQPObjects();
            InitReceived();
        }

        public virtual void InitReceived() { }
        public virtual void DeclareAMQPObjects() {
            DeclareExchange();
        }

        public void DeclareExchange()
        {
            channel.ExchangeDeclare(exchange: "user_model", type: "topic");
            channel.ExchangeDeclare(exchange: "user_logs", type: "topic");
            channel.ExchangeDeclare(exchange: "claims", type: "topic");

            channel.ExchangeDeclare(exchange: "topic_logs", type: "topic");
            channel.ExchangeDeclare(exchange: "change_model", type: "topic");

            channel.QueueDeclare(queue: "ReceiveUserChange", durable: false,
                         exclusive: false, autoDelete: true, arguments: null);

            channel.QueueDeclare(queue: "claim_queue", durable: false,
                         exclusive: false, autoDelete: false, arguments: null);

        }

        public void Close()
        {
            connection.Close();
        }
    }
}
