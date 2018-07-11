using System;
using System.Collections.Concurrent;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Newtonsoft.Json;
using DeclareRpcClient;
using System.Threading.Tasks.Dataflow;
using System.Threading.Tasks;

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
            var rpcClient = new SophieRpcClient();
            var unser = "";
            while (unser != "q")
            {
                Console.WriteLine("1:Запросить Клеймы у miniAuth");
                Console.WriteLine("q:Выйти");

                unser = Console.ReadLine();

                switch (unser)
                {
                    case "1":
                        var UserId = Guid.NewGuid().ToString();
                        Console.WriteLine(" [x] Получить клеймы для пользователя: {0}", UserId);



                        var claims = rpcClient.GetClaims(UserId);

                        var newTaswk = claims.ContinueWith(
                            (x) =>
                            {
                                Console.WriteLine(" [.] Полученые клеймы '{0}'", x.Result);
                            });
                        break;
                    case "q":
                        break;
                    default:
                        break;
                }
            }

            rpcClient.Close();
        }
    }
    public class SophieRpcClient : RpcClient
    {
        protected readonly BufferBlock<string> respQueue = new BufferBlock<string>();
        public SophieRpcClient() : base("Sophie")
        { }

        public override void InitReceived()
        {
            ReceiveUserChange();
        }

        public override void DeclareAMQPObjects()
        {
            base.DeclareAMQPObjects();

            //channel.BasicConsume(queue: "claim_queue",
            //  autoAck: false, consumer: consumer);
        }

        public void ReceiveUserChange()
        {
            var queueName = "ReceiveUserChange";
            channel.QueueBind(queue: queueName,
                                  exchange: "user_logs",
                                  routingKey: "user.#");

            consumer.Received += async (model, ea) =>
            {
                var body = ea.Body;
                var jsonified = Encoding.UTF8.GetString(body);
                User user = JsonConvert.DeserializeObject<User>(jsonified);
                var routingKey = ea.RoutingKey;
                Console.WriteLine(" [.] Получено сообщение по ключу '{0}':Пользователь Id='{1}', Name ='{2}'",
                                  routingKey,
                                  user.Id, user.Name);
                await Task.Delay(250);
            };
            channel.BasicConsume(queue: queueName,
                                 autoAck: true,
                                 consumer: consumer);
        }
        public async Task<string> GetClaims(string UserId)
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

            var result = await respQueue.ReceiveAsync();

            return result;
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
    }
}
