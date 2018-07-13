using DeclareRpcClient;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Concurrent;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

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
            var rpcClient = new AuthRpcClient();

            Console.WriteLine(" [x] Ожидание запросов");
            var unser = "";

            while (unser != "q")
            {
                Console.WriteLine("1:Пользователь создан");
                Console.WriteLine("2:Заблокировать пользователя");
                Console.WriteLine("3:Вызвать рекурсию");
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
                    case "3":
                        var req = rpcClient.CallRecursion("Recursion");

                        var newTaswk = req.ContinueWith(
                            (x) =>
                            {
                                Console.WriteLine(x.Result);
                            });
                        break;
                    case "q":
                        break;
                    default:
                        break;
                }
            }

            Console.WriteLine(" Нажмите [Any key] чтобы выйти.");
            Console.ReadLine();
            rpcClient.Close();
        }

    }


    public class AuthRpcClient : RpcClient
    {
        protected readonly BufferBlock<string> respQueue = new BufferBlock<string>();
        public AuthRpcClient() : base("Auth")
        {
        }

        public override void InitReceived()
        {
            ReceivedSendClaims();
            //ReceivedRecursion();
        }

        public override void DeclareAMQPObjects()
        {
           base.DeclareAMQPObjects();

            channel.QueueDeclare(queue: "AuthRecursion", durable: false,
                         exclusive: false, autoDelete: false, arguments: null);
            channel.BasicConsume(queue: "claim_queue",
             autoAck: false, consumer: consumer);

            channel.ExchangeDeclare(exchange: "AuthRecursion", type: "topic");
            channel.BasicConsume(queue: "AuthRecursion",
              autoAck: false, consumer: consumer);
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
            
            consumer.Received += async (model, ea) =>
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
                    await Task.Delay(0);
                }
            };
        }
        string GetClaims(string UserId)
        {
            return "Claims_" + UserId;
        }

        public async Task<string> CallRecursion(string name)
        {
            var correlationId = Guid.NewGuid().ToString();
            props.CorrelationId = correlationId;
            props.ReplyTo = replyQueueName;

            consumer.Received += async (model, ea) =>
            {
                var body = ea.Body;
                var response = Encoding.UTF8.GetString(body);
                if (ea.BasicProperties.CorrelationId == correlationId)
                {
                    respQueue.Post(response);
                }
                await Task.Delay(0);
            };

            var messageBytes = Encoding.UTF8.GetBytes(name);
            channel.BasicPublish(
                exchange: "AuthRecursion",
                routingKey: "Recursion",
                basicProperties: props,
                body: messageBytes);

            channel.BasicConsume(
                consumer: consumer,
                queue: replyQueueName,
                autoAck: true);

            var result = await respQueue.ReceiveAsync();

            return result;
        }
        public void ReceivedRecursion()
        {
            channel.BasicQos(0, 1, false);

            var queueName = "AuthRecursion";
            channel.QueueBind(queue: queueName,
                                  exchange: "AuthRecursion",
                                  routingKey: "#");
            consumer.Received += async (model, ea) =>
            {
                string response = null;

                var body = ea.Body;
                var props = ea.BasicProperties;
                var replyProps = channel.CreateBasicProperties();
                replyProps.CorrelationId = props.CorrelationId;

                try
                {
                    var message = Encoding.UTF8.GetString(body);
                    response = CallRecursion(message).ToString();
                }
                finally
                {
                    var responseBytes = Encoding.UTF8.GetBytes(response);
                    channel.BasicPublish(exchange: "SophieRecursion", routingKey: props.ReplyTo,
                      basicProperties: replyProps, body: responseBytes);
                    channel.BasicAck(deliveryTag: ea.DeliveryTag,
                      multiple: false);
                    await Task.Delay(0);
                }
            };
        }
    }

}
