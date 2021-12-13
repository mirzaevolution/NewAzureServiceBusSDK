using Azure.Messaging.ServiceBus;
using System;
using System.Threading.Tasks;
namespace BasicTopicSub.Receiver
{
    class Program
    {
        private static string _connectionString = "Endpoint=sb://sbnewsdk.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=p6mCbuu78yoosaxoFhkSlCq2yXwRAh2VBsmMBWphGTY=";
        private static ServiceBusProcessor _serviceBusProcessor1;
        private static ServiceBusProcessor _serviceBusProcessor2;
        private static string _topicName = "basic-topic";
        private static string _subscription1Name = "sub1";
        private static string _subscription2Name = "sub2";
        private static async Task InitConnection()
        {
            try
            {
                ServiceBusClient serviceBusClient = new ServiceBusClient(_connectionString);
                _serviceBusProcessor1 = serviceBusClient.CreateProcessor(_topicName, _subscription1Name, new ServiceBusProcessorOptions
                {
                    AutoCompleteMessages  = false,
                    MaxConcurrentCalls = 10
                    
                });
                _serviceBusProcessor2 = serviceBusClient.CreateProcessor(_topicName, _subscription2Name, new ServiceBusProcessorOptions
                {
                    AutoCompleteMessages = false,
                    MaxConcurrentCalls = 10

                });

                _serviceBusProcessor1.ProcessMessageAsync += async (arg) =>
                {
                    Console.WriteLine($"[{_topicName}/{_subscription1Name}@{DateTime.Now}]: {arg.Message.Body.ToString()}");
                    await arg.CompleteMessageAsync(arg.Message);
                };
                _serviceBusProcessor1.ProcessErrorAsync += (arg) =>
                {
                    Console.WriteLine($"[ERROR - {_topicName}/{_subscription1Name}@{DateTime.Now}]: {arg.Exception.Message}");
                    return Task.CompletedTask;
                };
                _serviceBusProcessor2.ProcessMessageAsync += async (arg) =>
                {
                    Console.WriteLine($"[{_topicName}/{_subscription2Name}@{DateTime.Now}]: {arg.Message.Body.ToString()}");
                    await arg.CompleteMessageAsync(arg.Message);
                };
                _serviceBusProcessor2.ProcessErrorAsync += (arg) =>
                {
                    Console.WriteLine($"[ERROR - {_topicName}/{_subscription2Name}@{DateTime.Now}]: {arg.Exception.Message}");
                    return Task.CompletedTask;
                };



                await _serviceBusProcessor1.StartProcessingAsync();
                await _serviceBusProcessor2.StartProcessingAsync();
                Console.WriteLine("Connection started. Press CTRL+C/Enter to quit");
                Console.ReadLine();

            }
            catch(Exception ex)
            {
                Console.WriteLine(ex);
            }
            finally
            {
                await _serviceBusProcessor1.CloseAsync();
                await _serviceBusProcessor1.DisposeAsync();
                await _serviceBusProcessor2.CloseAsync();
                await _serviceBusProcessor2.DisposeAsync();
            }
        }


        static void Main(string[] args)
        {
            InitConnection().Wait();
        }
    }
}
