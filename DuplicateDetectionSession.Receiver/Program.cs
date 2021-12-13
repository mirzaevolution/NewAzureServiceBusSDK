//https://docs.microsoft.com/en-us/azure/service-bus-messaging/duplicate-detection

using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using System;
using System.Threading.Tasks;
namespace DuplicateDetectionSession.Receiver
{
    class Program
    {
        private static string _connectionString = "Endpoint=sb://sbnewsdk.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=p6mCbuu78yoosaxoFhkSlCq2yXwRAh2VBsmMBWphGTY=";
        private static string _queueName = "order-queue-session";

        private static ServiceBusSessionProcessor _serviceBusSessionProcessor;
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
            _serviceBusSessionProcessor = serviceBusClient.CreateSessionProcessor(_queueName, new ServiceBusSessionProcessorOptions
            {
                AutoCompleteMessages = false,
                MaxConcurrentSessions = 10
            });
            _serviceBusSessionProcessor.ProcessMessageAsync += (args) =>
            {
                string sessionId = args.SessionId;
                string orderId = args.Message.Body.ToString();
                Console.WriteLine($"[{sessionId}] Order id: {orderId} has been processed!");
                args.CompleteMessageAsync(args.Message).Wait();
                return Task.CompletedTask;
            };
            _serviceBusSessionProcessor.ProcessErrorAsync += (err) =>
            {
                Console.WriteLine(err.Exception.ToString());
                return Task.CompletedTask;
            };
            _serviceBusSessionProcessor.StartProcessingAsync();
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
