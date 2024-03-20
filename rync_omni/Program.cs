// See https://aka.ms/new-console-template for more information
using RabbitMQ.Client.Events;
using RabbitMQ.Client;
using rync_omni;
using Serilog;
using Serilog.Events;
using System.Text;
using rsync_omni.ScheduleService;
using OmniSyncHub;
using EasyNetQ;
using System.Net;

internal class Program
{
    public static IBus bus { get; set; }
    public static string HostIP { get; set; }
    public static config appSettings = new config();
    public static List<OmniSuSource> lsOmni { set; get; }
    private static async Task Main(string[] args)
    {
        Console.WriteLine("Hello, World!");

        Log.Logger = new LoggerConfiguration()
        .MinimumLevel.Information()
        .MinimumLevel.Override("Microsoft", LogEventLevel.Error)
        .Enrich.FromLogContext()
        .WriteTo.File(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Logs", "Log.txt"), rollingInterval: RollingInterval.Day, rollOnFileSizeLimit: true, fileSizeLimitBytes: 5000000, flushToDiskInterval: TimeSpan.FromSeconds(3))
        .CreateLogger();

        try
        {
            string txt = System.IO.File.ReadAllText("config.json");
            appSettings = Newtonsoft.Json.JsonConvert.DeserializeObject<config>(txt);

            Log.Information("Start!");

            string hostName = Dns.GetHostName();
            IPHostEntry hostEntry = Dns.GetHostEntry(hostName);

            var ipv4Addresses = hostEntry.AddressList
                .Where(ip => ip.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork &&
                             !ip.ToString().StartsWith("127.") &&
                             !ip.ToString().StartsWith("172.") &&
                             !ip.ToString().StartsWith("10.") &&
                             !ip.ToString().StartsWith("192."))
                .ToList();

            if (ipv4Addresses.Any())
            {
                foreach (var ip in ipv4Addresses)
                {
                    Log.Information("Địa chỉ IPv4 của server: " + ip.ToString());
                    HostIP = ip.ToString();
                }
            }
            else
            {
                Log.Information("Không tìm thấy địa chỉ IPv4 phù hợp.");
            }

            var zen_hostName = Program.appSettings.Zendesk.Hostname;
            var zen_virtualHost = Program.appSettings.Zendesk.VirtualHost;
            var zen_username = Program.appSettings.Zendesk.username;
            var zen_password = Program.appSettings.Zendesk.password;
            var connectStr = $"host={zen_hostName};virtualHost={zen_virtualHost};username={zen_username};password={zen_password}";

            bus = RabbitHutch.CreateBus(connectStr);

            Service sv = new Service();
            ServiceQueue ServiceQueue = new ServiceQueue();
            Thread receivedMsg = new Thread(() => ServiceQueue.receivedMsg());
            receivedMsg.Start();

            Thread sendMsb = new Thread(() => pullCRM(sv, ServiceQueue));
            sendMsb.Start();
            

        }
        catch (Exception ex)
        {
            Log.Error($"main {ex.ToString()}");
        }
        finally
        {
            while (true) { }
        }
    }
    
    public static void pullCRM(Service sv, ServiceQueue ServiceQueue)
    {
        string key = Guid.NewGuid().ToString();
        try
        {
            // Log.Information("pullRabbitMQ ");
            var HostName = appSettings.RabbitMQ.Hostname;
            var factory = new ConnectionFactory() { Port = appSettings.RabbitMQ.Port };
            if (!string.IsNullOrEmpty(appSettings.RabbitMQ.username) && !string.IsNullOrEmpty(appSettings.RabbitMQ.password))
            {
                factory.UserName = appSettings.RabbitMQ.username;
                factory.Password = appSettings.RabbitMQ.password;
            }
            using (var connection = factory.CreateConnection(HostName))
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: appSettings.RabbitMQ.Queue_RsyncData,
                                     durable: false,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);
                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += async (model, ea) =>
                {
                    var body = ea.Body;
                    var message = Encoding.UTF8.GetString(body.ToArray());
                    Log.Information($"pullCRM key={key}; " + message);
                    try
                    {
                        Log.Information($"Received Outbound: Data = {message.ToString()}");
                        await sv.runSendAsync(message, ServiceQueue);
                    }
                    catch (Exception ec)
                    {
                        Log.Error($"pullCRM: message key={key}; " + ec.ToString());
                    }
                    channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                };
                channel.BasicQos(0, 5, false);
                channel.BasicConsume(queue: appSettings.RabbitMQ.Queue_RsyncData,
                                     autoAck: false,
                                     consumer: consumer);
                Thread.Sleep(-1);
            }
        }
        catch (Exception ex)
        {
            Log.Error($"pullCRM: key={key}; " + ex.ToString());
        }
    }
}


