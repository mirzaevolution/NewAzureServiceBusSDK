using System;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
namespace BasicQueue.Sender
{
    class Program
    {
        static string _queueName = "BasicQueue";
        static string _connectionString = "Endpoint=sb://sbnewsdk.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=p6mCbuu78yoosaxoFhkSlCq2yXwRAh2VBsmMBWphGTY=";
        static bool _isError = false;
        static Guid _clientId = Guid.NewGuid(); 
        static ServiceBusSender _serviceBusSender;

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
            catch(Exception ex)
            {
                _isError = true;
                Console.WriteLine(ex);
            }
        }

        static void ConnectToQueue()
        {
            if (!_isError)
            {
                try
                {
                    ServiceBusClient serviceBusClient = new ServiceBusClient(_connectionString);
                    _serviceBusSender = serviceBusClient.CreateSender(_queueName);
                    _isError = false;
                    Console.WriteLine("Connection started...");
                    Console.WriteLine($"Client id: {_clientId}");
                    Console.WriteLine("Press CTRL+C to quit");
                }
                catch (Exception ex)
                {
                    _isError = true;
                    Console.WriteLine(ex);
                }
            }
        }
        static async Task SendMessageLoop()
        {
            if (_isError)
                return;
            string message = string.Empty;
            do
            {
                Console.Write("> ");
                message = Console.ReadLine().Trim();
                if (!string.IsNullOrEmpty(message))
                {
                    try
                    {
                        ServiceBusMessage serviceBusMessage = new ServiceBusMessage(message);
                        serviceBusMessage.Subject = _clientId.ToString();
                        await _serviceBusSender.SendMessageAsync(serviceBusMessage);

                    }
                    catch (Exception ex)
                    {
                        _isError = true;
                        Console.WriteLine(ex);
                    }

                }
            } while (!_isError);
        }

        static void Main(string[] args)
        {
            CreateQueueIfNotExists().Wait();
            ConnectToQueue();
            SendMessageLoop().Wait();
        }
    }
}
