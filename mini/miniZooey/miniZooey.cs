using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Concurrent;
using System.Text;

namespace miniZooey
{
    class User
    {
        public Guid Id { get; set; }
        public string Name { get; set; }
    }
    class miniZooey
    {
        public static void Main()
        {

            var rpcClient = new RpcClient();
            var unser = "";
            while (unser != "q")
            {
                Console.WriteLine("1:Получить список Департаментов из miniSophie");
                Console.WriteLine("2:Запросить Клеймы у miniAuth");
                Console.WriteLine("q:Выйти");

                unser = Console.ReadLine();

                switch (unser)
                {
                    case "1":
                        Console.WriteLine(" [x] Получить департаменты для пользователя: Zooey");
                        var departments = rpcClient.GetDepartments();
                        Console.WriteLine(" [.] Полученые департаменты '{0}'", departments);
                        break;
                    case "2":
                        var UserId = Guid.NewGuid().ToString();
                        Console.WriteLine(" [x] Получить клеймы для пользователя: {0}", UserId);
                        var claims = rpcClient.GetClaims(UserId);
                        Console.WriteLine(" [.] Полученые клеймы '{0}'", claims);
                        break;
                    case "q":
                        break;
                    default:
                        break;
                }
                Console.ReadLine();
            }
            rpcClient.Close();
        }

        public static Department GetDepartments()
        {
            return new Department();
        }

        public static string[] GetClaims()
        {
            return new string[1];
        }

        public static void ChangeDB()
        {

        }
    }
    public class Department
    {

    }

    public class RpcClient
    {
        private readonly IConnection connection;
        private readonly IModel channel;
        private readonly string replyQueueName;
        private readonly EventingBasicConsumer consumer;
        private readonly BlockingCollection<string> respQueue = new BlockingCollection<string>();
        private readonly IBasicProperties props;

        public RpcClient()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };

            connection = factory.CreateConnection();
            channel = connection.CreateModel();
            replyQueueName = channel.QueueDeclare().QueueName;
            consumer = new EventingBasicConsumer(channel);

            props = channel.CreateBasicProperties();
            // декларация обьектов  
            DeclareExchange();

            ReceiveDepartmentChange();
            ReceiveUserChange();

        }
        public void DeclareExchange()
        {
            channel.ExchangeDeclare(exchange: "change_model", type: "topic");
            channel.ExchangeDeclare(exchange: "topic_logs", type: "topic");
        }
        public void ReceiveUserChange()
        {
            var queueName = channel.QueueDeclare().QueueName;

            channel.QueueBind(queue: queueName,
                                  exchange: "user_logs",
                                  routingKey: "user.#");

            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                var body = ea.Body;
                var jsonified = Encoding.UTF8.GetString(body);
                User user = JsonConvert.DeserializeObject<User>(jsonified);
                var routingKey = ea.RoutingKey;
                Console.WriteLine(" [.] Получено сообщение по ключу '{0}':Пользователь Id='{1}', Name ='{2}'",
                                  routingKey,
                                  user.Id, user.Name);
            };
            channel.BasicConsume(queue: queueName,
                                 autoAck: true,
                                 consumer: consumer);
        }


        public void ReceiveDepartmentChange()
        {
            var queueName = channel.QueueDeclare().QueueName;

            channel.QueueBind(queue: queueName,
                                  exchange: "topic_logs",
                                  routingKey: "models.#");

            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                var body = ea.Body;
                var message = Encoding.UTF8.GetString(body);
                var routingKey = ea.RoutingKey;
                Console.WriteLine(" [.] Получено сообщение по ключу '{0}':'{1}'",
                                  routingKey,
                                  message);
            };
            channel.BasicConsume(queue: queueName,
                                 autoAck: true,
                                 consumer: consumer);
        }

        public string GetClaims(string UserId)
        {
            var correlationId = Guid.NewGuid().ToString();
            props.CorrelationId = correlationId;
            props.ReplyTo = replyQueueName;

            consumer.Received += (model, ea) =>
            {
                var body = ea.Body;
                var response = Encoding.UTF8.GetString(body);
                if (ea.BasicProperties.CorrelationId == correlationId)
                {
                    respQueue.Add(response);
                }
            };

            var messageBytes = Encoding.UTF8.GetBytes(UserId);
            channel.BasicPublish(
                exchange: "",
                routingKey: "claim_queue",
                basicProperties: props,
                body: messageBytes);

            channel.BasicConsume(
                consumer: consumer,
                queue: replyQueueName,
                autoAck: true);

            return respQueue.Take();
        }


        public string GetDepartments()
        {
            var correlationId = Guid.NewGuid().ToString();
            props.CorrelationId = correlationId;
            props.ReplyTo = replyQueueName;

            consumer.Received += (model, ea) =>
            {
                var body = ea.Body;
                var response = Encoding.UTF8.GetString(body);
                if (ea.BasicProperties.CorrelationId == correlationId)
                {
                    respQueue.Add(response);
                }
            };

            channel.BasicPublish(
                exchange: "",
                routingKey: "get_data_queue",
                basicProperties: props,
                body: Encoding.UTF8.GetBytes("Zooey"));

            channel.BasicConsume(
                consumer: consumer,
                queue: replyQueueName,
                autoAck: true);

            return respQueue.Take();
        }


        public void Close()
        {
            connection.Close();
        }
    }
}
