
//https://docs.microsoft.com/en-us/azure/service-bus-messaging/duplicate-detection

using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using System;

namespace DuplicateDetectionDefault.Sender
{
    class Program
    {
        private static string _connectionString = "Endpoint=sb://sbnewsdk.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=p6mCbuu78yoosaxoFhkSlCq2yXwRAh2VBsmMBWphGTY=";
        private static string _queueName = "order-queue-def";
        private static ServiceBusSender _serviceBusSender;


        private static void CreateQueueIfNotExist()
        {
            ServiceBusAdministrationClient serviceBusAdministrationClient = new ServiceBusAdministrationClient(_connectionString);
            if (!serviceBusAdministrationClient.QueueExistsAsync(_queueName).Result)
            {
                Console.WriteLine($"Queue '{_queueName}' is not found. Will be created...");
                serviceBusAdministrationClient.CreateQueueAsync(new CreateQueueOptions(_queueName)
                {
                    RequiresDuplicateDetection = true
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
            string orderId = string.Empty;
            while (true)
            {
                Console.Write("Enter order-id: ");
                orderId = Console.ReadLine().Trim();
                if (string.IsNullOrEmpty(orderId))
                {
                    Console.WriteLine("Order id cannot be empty!");
                }
                else
                {
                    _serviceBusSender.SendMessageAsync(new ServiceBusMessage(orderId)
                    {
                        MessageId = orderId
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
