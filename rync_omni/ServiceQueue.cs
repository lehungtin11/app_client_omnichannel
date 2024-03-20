using EasyNetQ;
using Newtonsoft.Json;
using rync_omni;
using Serilog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OmniSyncHub
{
    internal class ServiceQueue
    {
        public void receivedMsg()
        {
            try
            {
                var hostName = Program.appSettings.Zendesk.Hostname;
                var virtualHost = Program.appSettings.Zendesk.VirtualHost;
                var username = Program.appSettings.Zendesk.username;
                var password = Program.appSettings.Zendesk.password;
                var connectStr = $"host={hostName};virtualHost={virtualHost};username={username};password={password}";
                Log.Information(connectStr);

                using (var bus = RabbitHutch.CreateBus(connectStr))
                {
                    var queueName = Program.appSettings.Zendesk.Queue_Inbound;
                    Service sv = new Service();

                    bus.SendReceive.Receive(queueName, x => x.Add<MsgInbound>(message =>
                    {
                        var msg = JsonConvert.SerializeObject(message);
                        Log.Information($"Received {queueName}: {msg}");
                        sv.runReceivedQueue(message);
                        // Thêm logic xử lý message ở đây
                    }));

                    bus.SendReceive.Receive($"{queueName}-log", x => x.Add<LogOmni>(message =>
                    {
                        var msg = JsonConvert.SerializeObject(message);
                        Log.Information($"Received {queueName}-log: {msg}");
                        sv.runReceivedLog(message);
                        // Thêm logic xử lý message ở đây
                    }));


                    while (true)
                    {
                    }
                }
            }
            catch (Exception err) {
                Log.Error($"receivedMsg: {err.ToString()}");
            }
        }
        public void sendMsg(string in_msg)
        {
            //var connectStr = "host=frozen-lion.lmq.cloudamqp.com;virtualHost=Master-OmniChannel;username=master_omni;password=6UQH1JKsYl2L5Ywyp2DGJOQ5Awu10J8L";
            //var queueName = "outbound-msg";

            try
            {
                
            }
            catch (Exception e)
            {
                Log.Error($"Service queue-outbound-msg sendMsg: Exception= {e.ToString()}");
            }
        }
    }

    public class MsgInbound
    {
        public string fk { get; set; }
        public string ip { get; set; }
        public string id { get; set; }
        public string type { get; set; }
        public string integrationId { get; set; }
        public string channelId { get; set; }
        public string fkService { get; set; }
        public bool isStart { get; set; }
        public DateTime createdAt { get; set; }
        public MsgUser user { get; set; }
        public MsgConversation conversation { get; set; }
        public Content content { get; set; }
    }
    public class MsgConversation
    {
        public string id { get; set; }
        public string type { get; set; }
        public string subject { get; set; }
        public string brandId { get; set; }
        public string source { get; set; }
        public EmailUsers email { get; set; }

    }
    public class MsgUser
    {
        public string name { get; set; }
        public string type { get; set; }
        public string userId { get; set; }
        public string externalId { get; set; }
        public string avatarUrl { get; set; }
    }

    public class Content
    {
        public string type { get; set; }
        public string text { get; set; }
        public string plain_body { get; set; }
        public string mediaUrl { get; set; }
        public string mediaType { get; set; }
        public string mediaSize { get; set; }
        public string altText { get; set; }
        public string comment { get; set; }
        public List<ZaloAttachment> attachments { get; set; }
    }
    public class EmailUsers
    {
        public string to { get; set; }
        public UserZ from { get; set; }
        public List<UserZ> ccs { get; set; }
    }
    public class UserZ
    {
        public long id { get; set; }
        public string name { get; set; }
        public string email { get; set; }
    }
    public class ZaloAttachment
    {
        public ZaloPayload payload { get; set; }
        public string type { get; set; }
    }

    public class ZaloPayload
    {
        public string thumbnail { get; set; }
        public string url { get; set; }
    }
    public class LogOmni
    {
        public string fk { get; set; }
        public string type { get; set; }
        public string id { get; set; }
        public string status { get; set; }
        public string log { get; set; }

    }
}
