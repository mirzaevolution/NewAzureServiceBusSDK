using System;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;

namespace BasicQueue.Receiver
{
    class Program
    {
        static string _queueName = "BasicQueue";
        static string _connectionString = "Endpoint=sb://sbnewsdk.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=p6mCbuu78yoosaxoFhkSlCq2yXwRAh2VBsmMBWphGTY=";
        static bool _isError = false;
        static ServiceBusProcessor _serviceBusProcessor;
        static async Task CreateQueueIfNotExists()
        {
            try
            {
                ServiceBusAdministrationClient sbAdminClient = new ServiceBusAdministrationClient(_connectionString);
                if (!await sbAdminClient.QueueExistsAsync(_queueName))
                {
                    Console.WriteLine($"Queue '{_queueName}' doesn't exist. Will be creating new one");
                    await sbAdminClient.CreateQueueAsync(_queueName);
                    Console.WriteLine($"Queue '{_queueName}' has been created");
                }
                else
                {
                    Console.WriteLine($"Queue '{_queueName}' already exists");

                }
            }
            catch (Exception ex)
            {
                _isError = true;
                Console.WriteLine(ex);
            }
        }
        static async Task InitiateConnection()
        {
            if (!_isError)
            {

                try
                {
                    ServiceBusClient serviceBusClient = new ServiceBusClient(_connectionString);
                    _serviceBusProcessor = serviceBusClient.CreateProcessor(_queueName, new ServiceBusProcessorOptions
                    {
                        AutoCompleteMessages = false
                    });
                    _serviceBusProcessor.ProcessMessageAsync += OnMessagesReceived;
                    _serviceBusProcessor.ProcessErrorAsync += OnErrorOccured;
                    await _serviceBusProcessor.StartProcessingAsync();
                    Console.WriteLine("Connection started...");
                    Console.ReadLine();

                }
                catch (Exception ex)
                {
                    _isError = true;
                    Console.WriteLine(ex);
                }
                finally
                {
                    await _serviceBusProcessor.StopProcessingAsync();
                    await _serviceBusProcessor.DisposeAsync();
                }
            }
        }

        private static Task OnErrorOccured(ProcessErrorEventArgs arg)
        {

            try
            {
                Console.WriteLine("An error occured while processing incoming message. Detail:");
                Console.WriteLine($"Error message: {arg.Exception.Message}");
                Console.WriteLine($"Source operation: {arg.ErrorSource.ToString()}");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
            return Task.CompletedTask;
        }

        private static async Task OnMessagesReceived(ProcessMessageEventArgs arg)
        {
            try
            {
                if (arg.Message != null)
                {
                    string messageString = arg.Message.Body.ToString();
                    string sourceClient = arg.Message.Subject;
                    Console.WriteLine($"[{sourceClient}@{DateTime.Now}]:  {messageString}");
                    await arg.CompleteMessageAsync(arg.Message);
                    
                }
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex);
            }
        }

        static void Main(string[] args)
        {
            CreateQueueIfNotExists().Wait();
            InitiateConnection().Wait();
            
        }
    }
}
