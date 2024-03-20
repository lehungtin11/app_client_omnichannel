using Org.BouncyCastle.Utilities.Collections;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace rync_omni
{

    public class config
    {
        public MySQL MySQL { get; set; }
        public Rabbitmq RabbitMQ { get; set; }
        public RabbitMQ_MSB RabbitMQ_MSB { get; set; }
        public CRM CRM { get; set; }
        public string cron { get; set; }
        public string PostgreSQLConnection { get; set; }
        public Zendesk Zendesk { get; set; }
    }
    public class Message
    {
        public string id { get; set; }
        public string type { get; set; }
        public string nxl { get; set; }
    }
    public class Rabbitmq
    {
        public string[] Hostname { get; set; }
        public int Port { get; set; }
        public string username { get; set; }
        public string password { get; set; }
        public string Queue_CIF { get; set; }
        public string Queue_RsyncData { get; set; }
        public string Queue_ntf { get; set; }
    }
    public class RabbitMQ_MSB
    {
        public string[] Hostname { get; set; }
        public int Port { get; set; }
        public string username { get; set; }
        public string password { get; set; }
        public string VirtualHost { get; set; }
        public string Queue_CIF_Inbound { get; set; }
        public string Queue_CIF_Outbound { get; set; }
        public string Queue_Inbound { get; set; }
        public string Queue_Oubound { get; set; }
    }

    public class Zendesk
    {
        public string Hostname { get; set; }
        public string VirtualHost { get; set; }
        public string username { get; set; }
        public string password { get; set; }
        public string Queue_Inbound { get; set; }
        public string Queue_Oubound { get; set; }
        public string localPath { get; set; }
    }
    public class OmniSuSource
    {
        public string id { get; set; }
        public string individual { get; set; }
        public string pollingStrategy { get; set; }
        public string name { get; set; }
        public string type { get; set; }
        public string sla { get; set; }
        public string enable { get; set; }
        public string loaiXL { get; set; }
    }
    public class Ticket_Get
    {
        public string id { get; set; }
        public string agent { get; set; }
        public string fkContact { get; set; }
        public string status { get; set; }
        public string subject { get; set; }
    }
    public class Ticket_Send
    {
        public string id { get; set; }
        public string agent { get; set; }
    }
    public class AgentApproval
    {
        public string username { get; set; }
        public string agent { get; set; }
    }
    public class CRMMsg
    {
        public string fkTicket { get; set; }
        public string source { get; set; }
        public string content { get; set; }
        public string syncId { get; set; }
        public string type { get; set; }
        public string name { get; set; }
        public string status { get; set; }
        public string createdBy { get; set; }
        public string dateCreated { get; set; }
        public string agent_ticket { get; set; }
        public string status_current { get; set; }
        public string content_type { get; set; }
        public string mediaUrl { get; set; }
        public string mediaType { get; set; }
        public string mediaSize { get; set; }
        public string altText { get; set; }
        public string comment { get; set; }
        public string id { get; set; }
        //t.c_conv_id, t.c_source as channelId, t.c_type as conv_source, t.c_syncId as user_userId, t.c_integrationId, t.c_fkService, t.c_brandId as conv_brand,
        // Ticket
        public string conv_id { get; set; }
        public string conv_source { get; set; }
        public string integrationId { get; set; }
        public string fkService { get; set; }
        public string brandId { get; set; }
        // Contact
        public string avatarUrl { get; set; }
        public string externalId { get; set; }
        // Email
        public string subject { get; set; }
        public string from { get; set; }
        public string to { get; set; }
        public string cc { get; set; }
        public string fkFile { get; set; }
        public string fkContact { get; set; }
        public bool isStart { get; set; }
        // Zalo
        public string attach_thumbnail { get; set; }
        public string attach_url { get; set; }
        public string attach_type { get; set; }


    }

    public class CRM
    {
        public string url { get; set; }
        public string api_id { get; set; }
        public string api_key { get; set; }
    }
    public class Interaction
    {
        public DateTime dateCreated { get; set; }
        public string type { get; set; }
        public string id { get; set; }
        public string createdBy { get; set; }
    }
    public class InteractionOUT
    {
        public int slgTuongTac { get; set; }
        public string thongb { get; set; }
        public bool isStatus { get; set; }
        public string enable { get; set; }
    }
    public class INCIF
    {
        public string channel_id { get; set; }
        public string user_id { get; set; }
        public string cif { get; set; }
    }
    public class ContactCIF
    {
        public string id { get; set; }
        public string fkKH { get; set; }
    }
    public class SettingClose
    {
        public string interaction { get; set; }
        public string action { get; set; }
        public string status { get; set; }
        public string note { get; set; }
        public string enable { get; set; }
        public string filter { get; set; }
        public string time { get; set; }
    }
    public class MySQL
    {
        public string connectstr { get; set; }
        public int maxPoolSize { get; set; }
    }
    /*
    public class MsgInbound
    {
        public string channel_id { get; set; } // in_individual
        public string user_id { get; set; } // in_messageId
        public string name { get; set; } // in_contact_name
        public string msg { get; set; } // in_content
        public string created_at { get; set; }

        // DS Moi
        public string contact_id { get; set; }
        public string contact_phone { get; set; }
        public string contact_email { get; set; }
        public string contact_facebook { get; set; }
        public string contact_zalo { get; set; }
        public string idFile { get; set; }
        public string fileName { get; set; }
        public string type { get; set; }
        public string subject { get; set; }
}
    */
    public class NTF_SLA
    {
        public string id { get; set; }
        public string ten { get; set; }
        public string fkKH { get; set; }
        public Class_HTML class_html { get; set; }
        public string isNew { get; set; }
        public string messageId { get; set; }
        public string codeTicket { get; set; }
        public string dateCreated { get; set; }
        public string fkNguoiXuLy { get; set; }
        public string sla_message { get; set; }
        public string kenh_tiep_nhan { get; set; }
        public string sla_conversation { get; set; }
        public string last_time { get; set; }
        public DateTime? reAssignDate { get; set; }
    }

    public class Class_HTML
    {
        public string type { get; set; }
        public string color { get; set; }
    }

    public class Agent_Polling
    {
        public string fullName { get; set; }
        public string id { get; set; }
        public string sort { get; set; }
        public string ext { get; set; }
    }

    public class Reasign_Time
    {
        public string times { get; set; }
    }

    public class Contact {
        public string id { get; set; }
        public string name { get; set; }
        public string fkLink { get; set; }
    }

    public class FileDetails {
        public string name { get; set; }
        public string url { get; set; }
        public string error { get; set; }
    
    }
}
