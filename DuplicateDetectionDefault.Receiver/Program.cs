//https://docs.microsoft.com/en-us/azure/service-bus-messaging/duplicate-detection

using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using System;
using System.Threading.Tasks;

namespace DuplicateDetectionDefault.Receiver
{
    class Program
    {
        private static string _connectionString = "Endpoint=sb://sbnewsdk.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=p6mCbuu78yoosaxoFhkSlCq2yXwRAh2VBsmMBWphGTY=";
        private static string _queueName = "order-queue-def";
        private static ServiceBusProcessor _serviceBusProcessor;
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
            _serviceBusProcessor = serviceBusClient.CreateProcessor(_queueName, new ServiceBusProcessorOptions
            {
                AutoCompleteMessages = false,
               MaxConcurrentCalls = 10
            });
            _serviceBusProcessor.ProcessMessageAsync += (args) =>
            {
                string orderId = args.Message.Body.ToString();
                Console.WriteLine($"Order id: {orderId} has been processed!");
                args.CompleteMessageAsync(args.Message).Wait();
                return Task.CompletedTask;
            };
            _serviceBusProcessor.ProcessErrorAsync += (err) =>
            {
                Console.WriteLine(err.Exception.ToString());
                return Task.CompletedTask;
            };
            _serviceBusProcessor.StartProcessingAsync();
            Console.WriteLine("Hit ENTER to quit!");
            Console.ReadLine();
        }
        static void Main(string[] args)
        {
            CreateQueueIfNotExist();
            InitConnection();

        }
    }
}
