using Azure.Messaging.ServiceBus;
using System;
using System.Threading.Tasks;

namespace BasicTopicSub.Sender
{
    class Program
    {
        private static string _connectionString = "Endpoint=sb://sbnewsdk.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=p6mCbuu78yoosaxoFhkSlCq2yXwRAh2VBsmMBWphGTY=";
        private static ServiceBusSender _serviceBusSender;
        private static string _topicName = "basic-topic";
        private static bool _isError = false;
        static void InitConnection()
        {
            try
            {
                Console.WriteLine("Starting connection");
                ServiceBusClient serviceBusClient = new ServiceBusClient(_connectionString);
                _serviceBusSender = serviceBusClient.CreateSender(_topicName);
                Console.WriteLine("Connection started");
            }
            catch(Exception ex)
            {
                _isError = true;
                Console.WriteLine(ex);
            }
        }
        static async Task MessageLoop()
        {
            if (_isError)
            {
                return;
            }
            string message = string.Empty;
            do
            {
                Console.Write("> ");
                message = Console.ReadLine().Trim();
                if (!string.IsNullOrEmpty(message))
                {
                    try
                    {
                        await _serviceBusSender.SendMessageAsync(new ServiceBusMessage(message));
                    }
                    catch(Exception ex)
                    {

                        _isError = true;
                        Console.WriteLine(ex);
                    }
                }
            } while (!_isError);
            try
            {
                await _serviceBusSender.CloseAsync();
                await _serviceBusSender.DisposeAsync();
            }
            catch { }
        }
        static void Main(string[] args)
        {
            InitConnection();
            MessageLoop().Wait();
        }
    }
}
