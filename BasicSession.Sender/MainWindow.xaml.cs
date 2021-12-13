using System;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using Azure.Messaging.ServiceBus;
namespace BasicSession.Sender
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        private static ServiceBusSender _senderClient;
        private readonly string _queueName = "msg-session";
        private readonly string _sessionAlpha = "alpha";
        private readonly string _sessionBeta = "beta";
        private string _connectionString = "Endpoint=sb://sbnewsdk.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=p6mCbuu78yoosaxoFhkSlCq2yXwRAh2VBsmMBWphGTY=";


        public MainWindow()
        {
            InitializeComponent();
            InitConnection();
        }

        private void DisableAllControls()
        {
            foreach(Control control in new Control[]
            {
                TextBoxMessage, ButtonAlphaSession, ButtonBetaSession
            })
            {
                control.IsEnabled = false;
            }
        }

        private void InitConnection()
        {
            try
            {
                ServiceBusClient serviceBusClient = new ServiceBusClient(_connectionString);
                _senderClient = serviceBusClient.CreateSender(_queueName);
            }
            catch(Exception ex)
            {
                ShowErrorMessage($"Error: {ex.Message}. Please restart the app!");
                DisableAllControls();
            }
        }

        private void ShowErrorMessage(string content, string title = "Error")
        {
            MessageBox.Show(content, title, MessageBoxButton.OK, MessageBoxImage.Error);
        }

        private async void ButtonAlphaSessionClick(object sender, RoutedEventArgs e)
        {
            await SendMessage(_sessionAlpha);
        }

        private async void ButtonBetaSessionClick(object sender, RoutedEventArgs e)
        {
            await SendMessage(_sessionBeta);
        }

        private async Task SendMessage(string sessionId)
        {
            string message = TextBoxMessage.Text.Trim();
            if (!string.IsNullOrEmpty(message))
            {
                try
                {

                    await _senderClient.SendMessageAsync(new ServiceBusMessage(message)
                    {
                        SessionId = sessionId
                    });

                    MessageBox.Show("Message sent.", "Success", MessageBoxButton.OK, MessageBoxImage.Information);
                }
                catch(Exception ex)
                {
                    ShowErrorMessage(ex.Message);
                }
            }
            else
            {
                ShowErrorMessage("Empty message is not allowed", "Validation Error");
                return;
            }
        }
    }
}
