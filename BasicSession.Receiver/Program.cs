using System;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;

namespace BasicSession.Receiver
{
    class Program
    {
        private static readonly string _queueName = "msg-session";
        private static string _connectionString = "Endpoint=sb://sbnewsdk.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=p6mCbuu78yoosaxoFhkSlCq2yXwRAh2VBsmMBWphGTY=";
        private static ServiceBusSessionProcessor _sessionProcessor;
        
        private static async Task InitConnection()
        {
            try
            {
                ServiceBusClient serviceBusClient = new ServiceBusClient(_connectionString);
                _sessionProcessor = serviceBusClient.CreateSessionProcessor(_queueName, new ServiceBusSessionProcessorOptions
                {
                    AutoCompleteMessages = false,
                    MaxConcurrentSessions = 2 //because we only handle 'alpha' and 'beta' sessions
                });
                _sessionProcessor.ProcessMessageAsync += async (handler) =>
                {
                    try
                    {
                        Console.WriteLine($"[{handler.SessionId}@{DateTime.Now}]: {handler.Message.Body.ToString()}");
                        await handler.CompleteMessageAsync(handler.Message);

                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[{handler.SessionId}@{DateTime.Now} - ERROR]: {ex.Message}");

                        await handler.AbandonMessageAsync(handler.Message);
                    }
                };
                _sessionProcessor.ProcessErrorAsync += (errorInfo) =>
                {
                    Console.WriteLine($"ERROR: {errorInfo.Exception.ToString()}");
                    return Task.CompletedTask;
                };
                await _sessionProcessor.StartProcessingAsync();
                Console.WriteLine("Session Receiver started...");
                Console.ReadLine();
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex);
                return;
            }
        }

        static void Main(string[] args)
        {
            InitConnection().Wait();
        }
    }
}
