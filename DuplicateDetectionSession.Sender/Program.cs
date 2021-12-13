//https://docs.microsoft.com/en-us/azure/service-bus-messaging/duplicate-detection


using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using System;
using System.Linq;

namespace DuplicateDetectionSession.Sender
{
    class Program
    {
        private static string _connectionString = "Endpoint=sb://sbnewsdk.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=p6mCbuu78yoosaxoFhkSlCq2yXwRAh2VBsmMBWphGTY=";
        private static string _queueName = "order-queue-session";
        private static ServiceBusSender _serviceBusSender;


        private static void CreateQueueIfNotExist()
        {
            ServiceBusAdministrationClient serviceBusAdministrationClient = new ServiceBusAdministrationClient(_connectionString);
            if (!serviceBusAdministrationClient.QueueExistsAsync(_queueName).Result)
            {
                Console.WriteLine($"Queue '{_queueName}' is not found. Will be created...");
                serviceBusAdministrationClient.CreateQueueAsync(new CreateQueueOptions(_queueName)
                {
                    RequiresDuplicateDetection = true,
                    DuplicateDetectionHistoryTimeWindow = TimeSpan.FromMinutes(5),
                    RequiresSession = true,
                    AutoDeleteOnIdle = TimeSpan.FromHours(1)
                }).Wait();
                Console.WriteLine($"Queue '{_queueName}' has been created");
            }
            else
            {
                Console.WriteLine($"Queue '{_queueName}' already exists");
            }
        }

        private static void InitConnection()
        {
            ServiceBusClient serviceBusClient = new ServiceBusClient(_connectionString);
            _serviceBusSender = serviceBusClient.CreateSender(_queueName);
        }

        private static void DoLoop()
        {
            Console.WriteLine($"Hit CTRL+C to quit..");
            string data = string.Empty;
            while (true)
            {
                Console.Write("[<SessionId:OrderId>]>  ");
                data = Console.ReadLine().Trim();
                if (string.IsNullOrEmpty(data))
                {
                    Console.WriteLine("Value cannot be empty!");
                }
                else
                {
                    var dataArr = data.Split(":", StringSplitOptions.RemoveEmptyEntries);
                    string sessionId = dataArr.FirstOrDefault();
                    string orderId = dataArr.LastOrDefault();
                    _serviceBusSender.SendMessageAsync(new ServiceBusMessage(orderId)
                    {
                        SessionId = sessionId,
                        MessageId = sessionId + orderId
                    }).Wait();
                }
            }
        }

        static void Main(string[] args)
        {
            CreateQueueIfNotExist();
            InitConnection();
            DoLoop();

        }
    }
}
