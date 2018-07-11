using System;
using System.Collections.Concurrent;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Newtonsoft.Json;

namespace miniSophie
{
    class User
    {
        public Guid Id { get; set; }
        public string Name { get; set; }
    }
    class miniSophie
    {
        public static void Main()
        {
            var rpcClient = new RpcClient();
            var unser = "";
            while (unser != "q")
            {
                Console.WriteLine("1:Запросить Клеймы у miniAuth");
                Console.WriteLine("2:Изменить данные в БД о Department");
                Console.WriteLine("q:Выйти");

                unser = Console.ReadLine();

                switch (unser)
                {
                    case "1":
                        var UserId = Guid.NewGuid().ToString();
                        Console.WriteLine(" [x] Получить клеймы для пользователя: {0}", UserId);
                        var claims = rpcClient.GetClaims(UserId);
                        Console.WriteLine(" [.] Полученые клеймы '{0}'", claims);
                        break;
                    case "2":
                        rpcClient.EmitDepartmentChange();
                        Console.WriteLine(" [x] Данные Department изменены");
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

            connection = factory.CreateConnection("Sophie");
            channel = connection.CreateModel();
            replyQueueName = channel.QueueDeclare().QueueName;
            consumer = new EventingBasicConsumer(channel);

            props = channel.CreateBasicProperties();
            // декларация обьектов  
            DeclareExchange();

            ReceiveUserChange();
            ReceivedSendDepartments();

        }

        public void DeclareExchange()
        {
            channel.ExchangeDeclare(exchange: "user_model", type: "topic");
            channel.ExchangeDeclare(exchange: "user_logs", type: "topic");
            channel.ExchangeDeclare(exchange: "topic_logs", type: "topic");
            channel.ExchangeDeclare(exchange: "change_model", type: "topic");


            channel.QueueDeclare(queue: "get_data_queue", durable: false,
                         exclusive: false, autoDelete: true, arguments: null);
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

        public void EmitDepartmentChange()
        {
            var routingKey = "models.department";
            var message = "В Sophie изменилась сущность Department!";
            var body = Encoding.UTF8.GetBytes(message);
            channel.BasicPublish(exchange: "topic_logs",
                                 routingKey: routingKey,
                                 basicProperties: null,
                                 body: body);
            Console.WriteLine(" [x] Отправлено сообщение с ключем '{0}':'{1}'", routingKey, message);
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
        public void ReceivedSendDepartments()
        {
            channel.BasicQos(0, 1, false);
            var consumer = new EventingBasicConsumer(channel);
            channel.BasicConsume(queue: "get_data_queue",
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
                    Console.WriteLine(" [.] Запрос Департаментов для - {0}", message);
                    response = GetDepartments(message).ToString();
                    Console.WriteLine(" [x] Отправлен ответ Департаментов - {0}", response);
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
        string GetDepartments(string name)
        {
            return "Список департаментов";
        }

        public void Close()
        {
            connection.Close();
        }
    }
}
