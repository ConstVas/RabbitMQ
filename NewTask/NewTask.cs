using RabbitMQ.Client;
using System;
using System.Text;

namespace NewTask
{
    class NewTask
    {
         public static void Main(string[] args)
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: "task_queue",
                 durable: true, 
                 exclusive: false, 
                 autoDelete: false, 
                 arguments: null);
                for (int i = 0; i < 10; i++)
                {
                var message = GetMessage(new string[]{ i.ToString() });
                var body = Encoding.UTF8.GetBytes(message);

                var properties = channel.CreateBasicProperties();
                properties.Persistent = true;

                channel.BasicPublish(exchange: "", 
                routingKey: "task_queue", 
                basicProperties: properties, 
                body: body);
                Console.WriteLine(" [x] Sent {0}", message);
                }
                
            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
            }
        }

        private static string GetMessage(string[] args)
        {
            return ((args.Length > 0) ? string.Join(" ", args) : "Hello World!");
        }
    }
}
