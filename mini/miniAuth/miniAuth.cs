using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Concurrent;
using System.Text;
using System.Threading;

namespace miniAuth
{
    class User
    {
        public Guid Id { get; set; }
        public string Name { get; set; }
    }
    class miniAuth
    {
        public static void Main()
        {
            var rpcClient = new RpcClient();

            Console.WriteLine(" [x] Ожидание запросов");
            var unser = "";

            while (unser != "q")
            {
                Console.WriteLine("1:Пользователь создан");
                Console.WriteLine("2:Заблокировать пользователя");
                Console.WriteLine("q:Выйти");

                unser = Console.ReadLine();

                switch (unser)
                {
                    case "1":
                        rpcClient.EmitUserAdd();
                        break;
                    case "2":
                        rpcClient.EmitUserBlock();
                        break;
                    case "q":
                        break;
                    default:
                        break;
                }
                Console.ReadLine();
            }

            Console.WriteLine(" Нажмите [Any key] чтобы выйти.");
            Console.ReadLine();
            rpcClient.Close();
        }

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

            connection = factory.CreateConnection("Auth");
            channel = connection.CreateModel();
            replyQueueName = channel.QueueDeclare().QueueName;
            consumer = new EventingBasicConsumer(channel);

            props = channel.CreateBasicProperties();
            DeclareExchange();
            ReceivedSendClaims();
        }

        public void DeclareExchange()
        {
            channel.ExchangeDeclare(exchange: "user_model", type: "topic");
            channel.ExchangeDeclare(exchange: "user_logs", type: "topic");


            channel.QueueDeclare(queue: "claim_queue", durable: false,
                         exclusive: true, autoDelete: true, arguments: null);
        }

        public void EmitUserBlock()
        {
            var routingKey = "user.block";
            var UserId = Guid.NewGuid();
            var user = new User()
            {
                Id = UserId,
                Name = "Иван"
            };
            var jsonUser = JsonConvert.SerializeObject(user);
            var userBuffer = Encoding.UTF8.GetBytes(jsonUser);
            var message = "Пользователь " + UserId + " заблокирован";
            var body = Encoding.UTF8.GetBytes(message);
            channel.BasicPublish(exchange: "user_logs",
                                 routingKey: routingKey,
                                 basicProperties: null,
                                 body: userBuffer);
            Console.WriteLine(" [x] Отправлено сообщение с ключем '{0}':'{1}'", routingKey, message);
        }

        public void EmitUserAdd()
        {

            var routingKey = "user.add";
            var UserId = Guid.NewGuid();
            var user = new User()
            {
                Id = UserId,
                Name = "Иван"
            };
            var jsonUser = JsonConvert.SerializeObject(user);
            var userBuffer = Encoding.UTF8.GetBytes(jsonUser);
            var message = "Пользователь " + UserId + " создан";
            var body = Encoding.UTF8.GetBytes(message);
            channel.BasicPublish(exchange: "user_logs",
                                 routingKey: routingKey,
                                 basicProperties: null,
                                 body: userBuffer);
            Console.WriteLine(" [x] Отправлено сообщение с ключем '{0}':'{1}'", routingKey, message);
        }
        public void ReceivedSendClaims()
        {
            channel.BasicQos(0, 1, false);
            var consumer = new EventingBasicConsumer(channel);
            channel.BasicConsume(queue: "claim_queue",
              autoAck: false, consumer: consumer);

            consumer.Received += (model, ea) =>
            {
                string response = null;

                var body = ea.Body;
                var props = ea.BasicProperties;
                var replyProps = channel.CreateBasicProperties();
                replyProps.CorrelationId = props.CorrelationId;

                try
                {
                    var message = Encoding.UTF8.GetString(body);
                    Console.WriteLine(" [.] Запрос клеймов для пользователя - {0}", message);
                    response = GetClaims(message).ToString();
                    Console.WriteLine(" [x] Отправлены клеймы - {0}", response);
                }
                catch (Exception e)
                {
                    Console.WriteLine(" [.] " + e.Message);
                    response = "";
                }
                finally
                {
                    var responseBytes = Encoding.UTF8.GetBytes(response);
                    channel.BasicPublish(exchange: "", routingKey: props.ReplyTo,
                      basicProperties: replyProps, body: responseBytes);
                    channel.BasicAck(deliveryTag: ea.DeliveryTag,
                      multiple: false);
                }
            };
        }
        string GetClaims(string UserId)
        {
            return "Claims_" + UserId;
        }

        public void Close()
        {
            connection.Close();
        }
    }

}
