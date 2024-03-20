using EasyNetQ;
using Google.Protobuf.WellKnownTypes;
using Microsoft.VisualBasic;
using MySql.Data.MySqlClient;
using MySqlX.XDevAPI.Common;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Npgsql;
using OmniSyncHub;
using Org.BouncyCastle.Utilities.Collections;
using Quartz.Util;
using RabbitMQ.Client;
using Serilog;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics.Contracts;
using System.IO;
using System.Linq;
using System.Net.Mail;
using System.Net.NetworkInformation;
using System.Reflection.Metadata;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;


namespace rync_omni
{
    internal class Service
    {
        
        public void runReceivedQueue(MsgInbound msg)
        {
            string agent = "", idTk = "", idTK_data = "", thongb = "", _type = "rsync";
            Contact contact = new Contact();
            int slgTuongTac = 0;
            bool isNew = false, isStatus = false;
            InteractionOUT interactionOUT = null;
            var o_msg = JsonConvert.SerializeObject(msg);
            try
            {
                // var msg = JsonConvert.DeserializeObject<MsgInbound>(jsonString);
                var lsOmni = getOmniChannel();
                OmniSuSource omni = lsOmni.FirstOrDefault(x => x.id == msg.channelId);

                if (msg.createdAt == null || string.IsNullOrEmpty(msg.createdAt.ToString()))
                {
                    DateTime currentDateTime = DateTime.Now;
                    // msg.createdAt phải là kiểu DateTime
                    msg.createdAt = currentDateTime;
                }

                // kiểm tra ticket có tồn tại không
                var itk = getTicket(msg.user.userId, msg.channelId, msg.type, msg.conversation.id);

                // lấy id contact
                if (itk == null || string.IsNullOrEmpty(itk.fkContact))
                {
                    contact = getContact_tmp(msg.user.userId);

                    if (string.IsNullOrEmpty(contact.id))
                    {
                        contact = getContact(msg.user.userId, msg.channelId, msg.type);
                    }
                }
                else if (!string.IsNullOrEmpty(itk.fkContact))
                {
                    contact.id = itk.fkContact;
                }

                // lấy agent
                if (itk == null || string.IsNullOrEmpty(itk.agent))
                {
                    //string x = get_ds_ext_available();
                    string x = "";
                    if (!string.IsNullOrEmpty(x))
                    {
                        if (omni != null)
                        {
                            agent = getAgent(omni.id, x);
                        }
                        else
                        {
                            Log.Warning($"Không tìm thấy agent trong kênh: {msg.type}");
                        }
                    }
                }
                else if (!string.IsNullOrEmpty(itk.agent))
                {
                    agent = itk.agent;
                }

                // lấy id ticket
                if (itk == null || string.IsNullOrEmpty(itk.id))
                {
                    if (string.IsNullOrEmpty(contact.id))
                    {
                        contact.id = insertContact(msg, agent);
                        contact.fkLink = insertContactLink(msg, agent, contact.id);
                    }

                    if (msg.type == "email")
                    {
                        idTk = insertTicket(msg, "", contact.id, agent, "");
                    }
                    else
                    {
                        if (omni != null)
                        {
                            idTk = insertTicket(msg, omni.individual, contact.id, agent, "");
                        }
                        else
                        {
                            Log.Warning($"Không tìm thấy kênh \"{msg.type}\" trong list kênh!");
                        }
                    }   
                }
                else
                {
                    idTk = itk.id;
                    if (msg.type != "email")
                    {
                        bool isBot = checkBotMessage(msg);
                        if (isBot)
                        {
                            _type = "send";
                        }

                        isNew = updateTicket(idTk, msg.createdAt.ToString());

                        interactionOUT = kiemTraTuongTac(idTk, "Inbound", "Rsync");
                        if (interactionOUT != null)
                        {
                            slgTuongTac = interactionOUT.slgTuongTac;
                            thongb = interactionOUT.thongb;
                            isStatus = interactionOUT.isStatus;
                        }

                        if (itk.subject.Contains("Guest") && msg.user.name != "Guest")
                        {
                            var _contact = getContact(msg.user.userId, msg.channelId, msg.type);
                            if (_contact.name != msg.user.name)
                            {
                                updateContactName(itk, msg.user.name);
                                updateSubjectTicket(itk, msg.user.name, msg.channelId);
                            }
                        }
                    }
                }

                /// Nếu msg.type = startConversation và msg.isStart = true là lần đầu nhắn tin vào

                // insert ticket data
                if (!string.IsNullOrEmpty(idTk))
                {
                    if (msg.type != "email")
                    {
                        var attachmentDetails = new
                        {
                            Thumbnail = (string)null,
                            Url = (string)null,
                            Type = (string)null
                        };

                        if (msg?.content?.attachments != null)
                        {
                            var firstAttachment = msg.content.attachments[0];

                            attachmentDetails = new
                            {
                                Thumbnail = firstAttachment?.payload?.thumbnail,
                                Url = firstAttachment?.payload?.url,
                                Type = firstAttachment?.type
                            };

                            if (!string.IsNullOrEmpty(attachmentDetails.Url))
                            {
                                var fileDetails = handleDownload(attachmentDetails.Url,attachmentDetails.Type);
                                attachmentDetails = new
                                {
                                    Thumbnail = fileDetails.name,
                                    Url = fileDetails.url,
                                    Type = firstAttachment?.type
                                };

                                if (!string.IsNullOrEmpty(fileDetails.error)) {
                                    msg.content.comment = fileDetails.error;
                                };
                                msg.content.attachments[0].payload.thumbnail = fileDetails.name;
                                msg.content.attachments[0].payload.url = fileDetails.url;
                            };
                        };

                        

                        var intf = new
                        {
                            id = idTk,
                            fk = contact.id,
                            name = msg.user.name,
                            dateCreated = msg.createdAt,
                            content = msg.content.text,
                            messageId = msg.user.userId,
                            type = _type,
                            nxl = agent,
                            source = msg.channelId,
                            slgTT = slgTuongTac,
                            thongBao = thongb,
                            statusUpdate = isStatus,
                            social_type = msg.conversation.source,
                            avatar = msg.user.avatarUrl,
                            content_type = msg.content.type,
                            channel_name = "",
                            media = new
                            {
                                mediaUrl = msg.content.mediaUrl,
                                mediaType = msg.content.mediaType,
                                mediaSize = msg.content.mediaSize,
                                altText = msg.content.altText,
                            },
                            attachments = new {
                                thumbnail = attachmentDetails.Thumbnail,
                                url = attachmentDetails.Url,
                                type = attachmentDetails.Type,
                            },
                            comment = msg.content?.comment ?? ""
                        };
                        string json = Newtonsoft.Json.JsonConvert.SerializeObject(intf);
                        sendNtf(json);
                    }
                    idTK_data = insertTicketData(msg, idTk, _type, agent, contact.id);
                }
                else {
                    Log.Error($"Ticket ID rỗng: {o_msg}, type= {msg.type}");
                }
            }
            catch (Exception e)
            {
                Log.Error($"run: {e.ToString()}");
            }
            finally
            {
                Log.Information($"Finally runReceivedQueue: idKH = {contact.id}, agent = {agent}, idTk = {idTk}, idTK_data = {idTK_data}, json={o_msg}");
            }
        }
        public async Task runSendAsync(string jsonString, ServiceQueue ServiceQueue)
        {
            string id = "", status = "", user = "", content = "", status_current = "", syncId = "", thongb = "", status_send = "", type = "", user_type = "user", conv_type = "personal";
            int slgTuongTac = 0;
            bool isStatus = false;
            InteractionOUT interactionOUT = null;
            try
            {
                var data = JsonConvert.DeserializeObject<Ticket_Send>(jsonString);

                // Tách chuỗi bằng ký tự '+'
                string[] parts = data.id.Split('+');

                if (parts.Length > 1)
                {
                    // Nếu có hai phần tử sau khi tách, phần đầu tiên là id, phần thứ hai là type
                    id = parts[0];
                    type = parts[1];

                    if(type == "email")
                    {
                        user_type = "email";
                        conv_type = parts[2];
                    }
                }
                else if (parts.Length == 1)
                {
                    // Nếu chỉ có một phần tử, không thể xác định id và type một cách rõ ràng
                    id = parts[0];
                }
                else
                {
                    // Trường hợp chuỗi không đúng định dạng
                    Log.Error($"Chuỗi không đúng định dạng. {jsonString}");
                }


                // lấy agent mới
                var lsAgent = getAgent_Approval();

                var msg = getMsgCRM_ID(id, type);
                user = msg.createdBy;
                content = msg.content;
                syncId = msg.syncId;
                status_current = msg.status;
                // Kiểm tra agent có nằm trong danh sách cần phê duyệt không
                var isCheck = lsAgent.FirstOrDefault(x => x.username == msg.createdBy);

                if (isCheck == null || status_current == "Đã duyệt" ||  status_current == "Đang thực hiện gửi")
                {
                    status = "Đã gửi";
                    // Gửi ra KH
                    // updateMsg(id, status, type);

                    var isStaus = getStatus_Close(msg.status_current);

                    if (isStaus)
                    {
                        status_send = "inactive";
                    }
                    else
                        status_send = "active";

                    // Tạo một danh sách cho ccs
                    List<UserZ> ccsList = new List<UserZ>();

                    // Kiểm tra xem msg.cc có giá trị không
                    if (!string.IsNullOrEmpty(msg.cc))
                    {
                        // Tách chuỗi msg.cc thành một mảng các địa chỉ email
                        var ccArray = msg.cc.Split(";");

                        // Thêm mỗi địa chỉ email vào danh sách ccs
                        foreach (var cc in ccArray)
                        {
                            if (!string.IsNullOrEmpty(cc))
                            {
                                ccsList.Add(new UserZ { id = 0, name = "", email = cc.Trim() });
                            }
                        }
                    }

                    var msg_send = new MsgInbound
                    {
                        fk = id,
                        ip = Program.HostIP,
                        id = msg.id,
                        type = msg.conv_source,
                        integrationId = msg.integrationId,
                        channelId = msg.source,
                        fkService = msg.fkService,
                        isStart = false,
                        createdAt = DateTime.Now,
                        user = new MsgUser
                        {
                            name = msg.name,
                            type = user_type,
                            userId = msg.syncId,
                            externalId = msg.externalId,
                            avatarUrl = msg.avatarUrl
                        },
                        conversation = new MsgConversation
                        {
                            id = msg.conv_id,
                            type = conv_type,
                            subject = msg.subject,
                            brandId = msg.brandId,
                            source = msg.conv_source,
                            email = new EmailUsers
                            {
                                to = msg.to,
                                from = new UserZ
                                {
                                    id = 0,
                                    name = "",
                                    email = msg.from
                                },
                                ccs = ccsList
                            }
                        },
                        content = new Content
                        {
                            type = msg.content_type,
                            text = msg.content,
                            plain_body = msg.content,
                            mediaUrl = msg.mediaUrl,
                            mediaType = msg.mediaType,
                            mediaSize = msg.mediaSize,
                            altText = msg.altText,
                            comment = "",
                            attachments = new List<ZaloAttachment>()
                        }
                    };

                    if (msg_send.conversation.source != "email" && msg_send.content.type == "file") {

                        if (msg_send.content.text.Contains(';')) {
                            var fileArray = msg_send.content.text.Split(';');
                            var mediaUrl = msg_send.content.mediaUrl;
                            msg_send.content.text = "";

                            foreach (var file in fileArray)
                            {
                                if (!string.IsNullOrEmpty(file))
                                {
                                    msg_send.content.mediaUrl = mediaUrl + "/" + file;

                                    string json = Newtonsoft.Json.JsonConvert.SerializeObject(msg_send);
                                    var msg_ob = JsonConvert.DeserializeObject<MsgInbound>(json);
                                    Log.Warning($"SendReceive: {json}");
                                    Program.bus.SendReceive.Send(Program.appSettings.Zendesk.Queue_Oubound, msg_ob);
                                }
                            }
                        } else
                        {
                            msg_send.content.mediaUrl += "/" + msg_send.content.text;
                            msg_send.content.text = "";

                            string json = Newtonsoft.Json.JsonConvert.SerializeObject(msg_send);
                            var msg_ob = JsonConvert.DeserializeObject<MsgInbound>(json);
                            Log.Warning($"SendReceive: {json}");
                            Program.bus.SendReceive.Send(Program.appSettings.Zendesk.Queue_Oubound, msg_ob);
                        }

                    } else
                    {
                        string json = Newtonsoft.Json.JsonConvert.SerializeObject(msg_send);
                        var msg_ob = JsonConvert.DeserializeObject<MsgInbound>(json);
                        Log.Warning($"SendReceive: {json}");
                        Program.bus.SendReceive.Send(Program.appSettings.Zendesk.Queue_Oubound, msg_ob);
                    }

                    interactionOUT = kiemTraTuongTac(msg.fkTicket, "Outbound", user);
                    if (interactionOUT != null)
                    {
                        slgTuongTac = interactionOUT.slgTuongTac;
                        thongb = interactionOUT.thongb;
                        isStatus = interactionOUT.isStatus;
                    }
                }
                else if (status_current == "sending")
                {
                    // gọi API duyệt
                    bool rs = await omni_review_message_approver_processAsync(id, isCheck.agent);
                    if (rs)
                    {
                        status = "Đang duyệt";
                        updateMsg(id, status, type, "");
                    }
                    else
                    {
                        status = "Gửi không thành công";
                        updateMsg(id, status, type, "");
                    }
                }
                else if (status_current == "Không được duyệt")
                {
                    status = status_current;
                }

                if (type != "email")
                {
                    updateTicket(msg.fkTicket, "");
                }

            }
            catch (Exception e)
            {
                Log.Error($"runSend: {e.ToString()}");
            }
            finally
            {
                logStatusMsg(id, user, status, status_current, syncId, content, status_send);
                Log.Information($"runSendAsync id = {id}, user = {user}, status = {status}, content = {content}, syncId = {syncId}, status_send = {status_send}; json={jsonString}");
            }
        }
        void SetTimeout(Action action, int delayInMilliseconds)
        {
            var timer = new System.Threading.Timer(x =>
            {
                action.Invoke();
            }, null, delayInMilliseconds, Timeout.Infinite);
        }
        public void runReceivedLog(LogOmni message)
        {
            try
            {
                if (!string.IsNullOrEmpty(message.fk.ToString()))
                {
                    // Đoạn code để thực thi sau khoảng thời gian
                    string status = message.status.ToString(), type = message.type.ToString().ToLower(), fk = message.fk.ToString();

                    var msg = getMsgCRM_ID(fk, type);

                    switch (type)
                    {
                        case "messenger":
                        case "comment":
                        case "web":
                        case "zalo":
                            if (!string.IsNullOrEmpty(status) && status == "true")
                            {
                                status = "Đã gửi";
                            }
                            else
                            {
                                status = "Gửi không thành công";
                            }

                            updateMsg(fk, status, type, "");

                            var intf = new
                            {
                                id = msg.fkTicket,
                                fk = fk,
                                name = msg.name,
                                dateCreated = msg.dateCreated,
                                content = msg.content,
                                messageId = msg.syncId,
                                type = "send",
                                nxl = msg.agent_ticket,
                                source = msg.source,
                                status = status,
                                slgTT = "",
                                thongBao = "",
                                statusUpdate = "",
                                social_type = msg.conv_source,
                                avatar = msg.avatarUrl,
                                content_type = msg.content_type,
                                channel_name = "",
                                media = new
                                {
                                    mediaUrl = msg.mediaUrl,
                                    mediaType = msg.mediaType,
                                    mediaSize = msg.mediaSize,
                                    altText = msg.altText,
                                },
                                attachments = new
                                {
                                    thumbnail = msg.attach_thumbnail,
                                    url = msg.attach_url,
                                    type = msg.attach_type,
                                },
                                comment = msg?.comment ?? ""
                            };
                            string jso2n = Newtonsoft.Json.JsonConvert.SerializeObject(intf);
                            sendNtf(jso2n);

                            break;

                        // Email
                        case "create":
                        case "forward":
                        case "reply":
                            string new_ticket_id = "";

                            if (!string.IsNullOrEmpty(status) && status == "true")
                            {
                                status = "Đã gửi";
                            }
                            else
                            {
                                status = "Gửi không thành công";
                            }

                            if (type == "create" || type == "forward")
                            {

                                MsgInbound new_msg = new MsgInbound
                                {
                                    fk = "",
                                    ip = Program.HostIP,
                                    id = msg.id,
                                    type = "new_email",
                                    integrationId = msg.integrationId,
                                    channelId = msg.source,
                                    fkService = msg.fkService,
                                    isStart = false,
                                    createdAt = DateTime.Now,
                                    user = new MsgUser
                                    {
                                        name = msg.name,
                                        type = "email",
                                        userId = msg.syncId,
                                        externalId = msg.externalId,
                                        avatarUrl = msg.avatarUrl
                                    },
                                    conversation = new MsgConversation
                                    {
                                        id = message.id,
                                        type = message.type,
                                        subject = msg.subject,
                                        brandId = msg.brandId,
                                        source = msg.conv_source,
                                        email = new EmailUsers
                                        {
                                            to = msg.to,
                                            from = new UserZ
                                            {
                                                id = 0,
                                                name = "",
                                                email = msg.from
                                            },
                                            ccs = new List<UserZ>()
                                        }
                                    },
                                    content = new Content
                                    {
                                        type = "text",
                                        text = msg.content,
                                        plain_body = msg.content,
                                        mediaUrl = msg.mediaUrl,
                                        mediaType = msg.mediaType,
                                        mediaSize = msg.mediaSize,
                                        altText = msg.altText,
                                        comment = "",
                                        attachments = new List<ZaloAttachment>()
                                    }
                                };

                                string messLog = Newtonsoft.Json.JsonConvert.SerializeObject(new_msg);
                                string messLog2 = Newtonsoft.Json.JsonConvert.SerializeObject(msg);

                                new_ticket_id = insertTicket(new_msg, "", msg.fkContact, msg.createdBy, msg.cc);
                                insertTicketData(new_msg, new_ticket_id, "email", msg.createdBy, msg.fkContact);
                                updateMsg(fk, status, "email", new_ticket_id);
                            }
                            else
                            {
                                updateMsg(fk, status, "email", "");
                            }

                            var messageEmail = new
                            {
                                id = new_ticket_id,
                                fk = fk,
                                name = msg.name,
                                dateCreated = msg.dateCreated,
                                content = msg.content,
                                messageId = msg.syncId,
                                type = "email",
                                nxl = msg.createdBy,
                                source = msg.source,
                                status = status,
                                slgTT = "",
                                thongBao = "",
                                statusUpdate = "",
                                social_type = msg.conv_source,
                                avatar = msg.avatarUrl,
                                content_type = msg.content_type,
                                channel_name = "",
                                media = new
                                {
                                    mediaUrl = msg.mediaUrl,
                                    mediaType = msg.mediaType,
                                    mediaSize = msg.mediaSize,
                                    altText = msg.altText,
                                },
                                comment = msg.comment
                            };

                            string convert2Json = Newtonsoft.Json.JsonConvert.SerializeObject(messageEmail);

                            sendNtf(convert2Json);

                            break;
                    }
                }
            }
            catch (Exception error)
            {
                Log.Error($"runReceivedLog: {error.ToString()}");
            }
        }
        public List<OmniSuSource> getOmniChannel()
        {
            List<OmniSuSource> lsSource = new List<OmniSuSource>();
            try
            {
                string queryString = "select id, individual, pollingStrategy, name, type, sla, enable from omni_su_source";

                using (var connection = ConnectionManager.GetConnection())
                {
                    MySqlCommand command = connection.CreateCommand();
                    command.CommandText = queryString;
                    MySqlDataReader reader = command.ExecuteReader();

                    while (reader.Read())
                    {
                        OmniSuSource omniSuSource = new OmniSuSource()
                        {
                            id = reader[0].ToString(),
                            individual = reader[1].ToString(),
                            pollingStrategy = reader[2].ToString(),
                            name = reader[3].ToString(),
                            type = reader[4].ToString(),
                            sla = reader[5].ToString(),
                            enable = reader[6].ToString()
                        };
                        lsSource.Add(omniSuSource);
                    }
                }
            }
            catch (Exception e)
            {
                Log.Error($"getOmniChannel: {e.ToString()}");
            }

            return lsSource;
        }
        // Kiểm tra sự tồn tại của ticket
        private Ticket_Get getTicket(string user, string source, string type, string conv_id)
        {
            Ticket_Get tk = new Ticket_Get();
            string queryString = "";
            try
            {
                if (type == "email")
                {
                    queryString = "select id, fkNguoiXuLy, fkKH, trangThai, TieuDe from sp_tickets where conv_id = @conv_id and fkEmail = @user and source = @source and trangThai not in (\"Đóng\", \"Đã xử lý\") order by dateCreated desc limit 1";
                }
                else { 
                    queryString = "select id, agent, fkContact, status, subject from omni_tickets where syncId = @user and source = @source and status in (select id from omni_su_status where enable = 'true') order by dateCreated desc limit 1";
                }

                using (var connection = ConnectionManager.GetConnection())
                {
                    MySqlCommand command = connection.CreateCommand();

                    command.CommandText = queryString;
                    command.Parameters.AddWithValue("@user", user);
                    command.Parameters.AddWithValue("@source", source);
                    command.Parameters.AddWithValue("@conv_id", conv_id);
                    MySqlDataReader reader = command.ExecuteReader();

                    while (reader.Read())
                    {

                        tk = new Ticket_Get()
                        {
                            id = reader[0].ToString(),
                            agent = reader[1].ToString(),
                            fkContact = reader[2].ToString(),
                            status = reader[3].ToString(),
                            subject = reader[4].ToString()
                        };
                    }
                }
            }
            catch (Exception e)
            {
                Log.Error($"getTicket: {e.ToString()}");
            }
            return tk;
        }
        private bool getStatus_Close(string status)
        {
            bool kq = false;
            try
            {
                string queryString = "select id from omni_su_status where coalesce(inactive,'') = 'true' and id = @status limit 1";

                using (var connection = ConnectionManager.GetConnection())
                {
                    MySqlCommand command = connection.CreateCommand();
                    command.Parameters.AddWithValue("@status", status);
                    command.CommandText = queryString;
                    MySqlDataReader reader = command.ExecuteReader();

                    while (reader.Read())
                    {
                        kq = true;
                    }
                }
            }
            catch (Exception e)
            {
                Log.Error($"getStatus_Close: {e.ToString()}");
            }
            return kq;
        }
        // Kiểm tra sự tồn tại của ticket
        private Ticket_Get getTicketbyId(string id)
        {
            Ticket_Get tk = new Ticket_Get();
            try
            {
                string queryString = "select id, agent, fkContact, status from omni_tickets where id = @id limit 1";

                using (var connection = ConnectionManager.GetConnection())
                {
                    MySqlCommand command = connection.CreateCommand();

                    command.CommandText = queryString;
                    command.Parameters.AddWithValue("@id", id);
                    MySqlDataReader reader = command.ExecuteReader();

                    while (reader.Read())
                    {

                        tk = new Ticket_Get()
                        {
                            id = reader[0].ToString(),
                            agent = reader[1].ToString(),
                            fkContact = reader[2].ToString(),
                            status = reader[3].ToString()
                        };
                    }
                }
            }
            catch (Exception e)
            {
                Log.Error($"getTicketbyId: {e.ToString()}");
            }
            return tk;
        }
        // Kiểm tra KH
        private Contact getContact(string user, string source, string type)
        {
            Contact contact = new Contact();
            string queryString = "";
            try
            {
                if (type == "email")
                {
                    queryString = "select id, ten from sp_khachhang where email = @user and source = @source limit 1";
                }
                else {
                    queryString = "select id, name from omni_contacts where syncId = @user and source = @source limit 1";
                }

                using (var connection = ConnectionManager.GetConnection())
                {
                    MySqlCommand command = connection.CreateCommand();

                    command.CommandText = queryString;
                    command.Parameters.AddWithValue("@user", user);
                    command.Parameters.AddWithValue("@source", source);
                    MySqlDataReader reader = command.ExecuteReader();

                    while (reader.Read())
                    {
                        contact.id = reader[0].ToString();
                        contact.name = reader[1].ToString();
                    }
                }
            }
            catch (Exception e)
            {
                Log.Error($"getContact: {e.ToString()}");
            }
            return contact;
        }
        // Kiểm tra KH trong bảng tạm
        private Contact getContact_tmp(string user)
        {
            Contact contact = new Contact();
            try
            {
                string queryString = "select l.fkContact, c.name from jwdb.omni_contact_link l left join jwdb.omni_contacts c on c.id = l.fkContact where userId = @user limit 1";

                using (var connection = ConnectionManager.GetConnection())
                {
                    MySqlCommand command = connection.CreateCommand();

                    command.CommandText = queryString;
                    command.Parameters.AddWithValue("@user", user);
                    MySqlDataReader reader = command.ExecuteReader();

                    while (reader.Read())
                    {
                        contact.id = reader[0].ToString();
                        contact.name = reader[1].ToString();
                    }
                }
            }
            catch (Exception e)
            {
                Log.Error($"getContact_tmp: {e.ToString()}");
            }
            return contact;
        }
        // Lấy thông tin Agent phụ trách
        private string getAgent(string source, string agentAvailable)
        {
            string agent = "";
            try
            {
                string queryString = "select omni.getuser_pollingstrategy(@source,@agentAvailable)";

                using (var connection = ConnectionManager.GetConnection())
                {
                    MySqlCommand command = connection.CreateCommand();
                    command.CommandText = queryString;
                    command.Parameters.AddWithValue("@source", source);
                    command.Parameters.AddWithValue("@agentAvailable", agentAvailable);
                    MySqlDataReader reader = command.ExecuteReader();

                    while (reader.Read())
                    {
                        agent = reader[0].ToString();
                    }
                }
            }
            catch (Exception e)
            {
                Log.Error($"getAgent: {e.ToString()}");
            }
            return agent;
        }

        // Lấy danh sách thông tin Agent phụ trách
        public string getListAgent(string polling_id)
        {
            string agent = "";
            try
            {
                string queryString = "select Agent from omni_pollingstrategy where id = @polling_id";

                using (var connection = ConnectionManager.GetConnection())
                {
                    MySqlCommand command = connection.CreateCommand();
                    command.CommandText = queryString;
                    command.Parameters.AddWithValue("@polling_id", polling_id);
                    MySqlDataReader reader = command.ExecuteReader();

                    while (reader.Read())
                    {
                        agent = reader[0].ToString();
                    }
                }
            }
            catch (Exception e)
            {
                Log.Error($"getAgent: {e.ToString()}");
            }
            return agent;
        }

        // Lấy thông tin Agent phân công lại
        private string getReasignAgent(string source, string agentAvailable, string nguoiXuyLy)
        {
            string agent = "";
            try
            {
                string queryString = "select omni.getuser_reasign(@source,@agentAvailable,@currentUser)";

                using (var connection = ConnectionManager.GetConnection())
                {
                    MySqlCommand command = connection.CreateCommand();
                    command.CommandText = queryString;
                    command.Parameters.AddWithValue("@source", source);
                    command.Parameters.AddWithValue("@agentAvailable", agentAvailable);
                    command.Parameters.AddWithValue("@currentUser", nguoiXuyLy);
                    MySqlDataReader reader = command.ExecuteReader();

                    while (reader.Read())
                    {
                        agent = reader[0].ToString();
                    }
                }
            }
            catch (Exception e)
            {
                Log.Error($"getReasignAgent: {e.ToString()}");
            }
            return agent;
        }
        // Lấy thời gian phân công lại
        public Reasign_Time getReasignTime()
        {
            Reasign_Time time = new Reasign_Time();
            try
            {
                string queryString = "select times from jwdb.omni_su_reasign limit 1";

                using (var connection = ConnectionManager.GetConnection())
                {
                    MySqlCommand command = connection.CreateCommand();
                    command.CommandText = queryString;
                    MySqlDataReader reader = command.ExecuteReader();

                    while (reader.Read())
                    {
                        time.times = reader[0].ToString();
                    }
                }
            }
            catch (Exception e)
            {
                Log.Error($"getReasignTime: {e.ToString()}");
            }
            return time;
        }

        // update contact name
        private bool updateContactName(Ticket_Get ticket, string customer_name)
        {
            bool rs = false;
            try
            {
                if (ticket == null)
                {
                    return false;
                }

                string queryString = "update omni_contacts"
                + " set name = @name"
                + " where id = @id"
                ;
                using (var connection = ConnectionManager.GetConnection())
                {
                    MySqlCommand command = connection.CreateCommand();
                    command.CommandText = queryString;
                    command.Parameters.AddWithValue("@id", ticket.fkContact);
                    command.Parameters.AddWithValue("@name", customer_name);

                    command.ExecuteNonQuery();
                }
                Log.Information($"updateContactName: id={ticket.fkContact}, name={customer_name}");

                rs = true;
            }
            catch (Exception e)
            {
                Log.Error($"updateContactName: {e.ToString()}");
            }
            finally
            {
                if (!rs)
                {
                    Log.Error($"updateContactName id={ticket.fkContact}, name={customer_name} {rs}");
                }
            }
            return rs;
        }

        // Cập nhật subject ticket
        private bool updateSubjectTicket(Ticket_Get ticket, string customer_name, string channel_id)
        {
            bool rs = false;
            try
            {
                if (string.IsNullOrEmpty(ticket.id))
                {
                    return false;
                }

                string queryString = "update omni_tickets"
                + " set subject = @subject"
                + " where id = @id"
;
                using (var connection = ConnectionManager.GetConnection())
                {
                    MySqlCommand command = connection.CreateCommand();
                    command.CommandText = queryString;
                    command.Parameters.AddWithValue("@id", ticket.id);
                    command.Parameters.AddWithValue("@subject", $"[{channel_id}] {customer_name}");

                    command.ExecuteNonQuery();
                }
                // Log.Information($"updateSubjectTicket: id={ticket.id}, subject=[{channel_id}] {customer_name}");

                rs = true;
            }
            catch (Exception e)
            {
                Log.Error($"updateSubjectTicket: {e.ToString()}");
            }
            finally
            {
                if (!rs)
                {
                    Log.Error($"updateSubjectTicket id={ticket.id}, subject=[{channel_id}] {customer_name} {rs}");
                }
            }
            return rs;
        }

        // created ticket
        private string insertTicket(MsgInbound msg, string individual, string idKH, string agent, string in_ccs)
        {
            string id = Guid.NewGuid().ToString();
            string queryString = "";
            string subject = "", CCs = "", email_from = "", email_to = "";
            try
            {
                if (msg.type == "email" || msg.type == "new_email")
                {
                    msg.conversation.source = "EMAIL";
                    email_from = msg.conversation.email.from.email;
                    email_to = msg.conversation.email.to;

                    if (string.IsNullOrEmpty(msg.conversation.subject)) {
                        if(string.IsNullOrEmpty(msg.conversation.email.from.name)) {
                            subject = $"Email mới từ {msg.conversation.email.from.email}";
                        } else {
                            subject = $"Email mới từ {msg.conversation.email.from.name}";
                        }
                    } else {
                        subject = msg.conversation.subject;
                    }

                    if (string.IsNullOrEmpty(agent))
                    {
                        agent = "System";
                        // agent = msg.conversation.email.from.email;
                    }

                    if (msg.type == "new_email")
                    {
                        CCs = in_ccs;
                    }
                    else { 
                        // Kiểm tra xem "ccs" có giá trị không và tạo chuỗi email
                        if (msg.conversation.email.ccs != null && msg.conversation.email.ccs.Any())
                        {
                            CCs = string.Join(", ",msg.conversation.email.ccs.Select(cc => cc.email));
                        }
                    } 

                    queryString = "INSERT INTO sp_tickets"
                    + "(id,dateCreated,dateModified,createdBy,modifiedBy,"
                    + "fkNguoiXuLy,TieuDe,fkKH,source,fkEmail,trangThai,"
                    + "kenh_tiep_nhan, mail_subject, mail_from, mail_to, mail_cc, tenKH,"
                    + "conv_id, integrationId, isStart, fkService, brandId, msg_id"
                    + ")"
                    + "VALUES (@id,now(),now(),@email_from,@agent,"
                    + $"@agent,@subject,@fkContact,@source,@syncId,@status,"
                    + "@type, @subject, @email_from, @email_to, @email_cc, @name,"
                    + "@conv_id, @integrationId, @isStart, @fkService, @brandId, @msg_id"
                    + ")"
                    ;

                    // Sẽ dùng khóa fkEmail, source dùng để tìm ticket
                }
                else {
                    subject = $"[{msg.conversation.source}] {msg.user.name}";
                    queryString = "INSERT INTO omni_tickets"
                    + "(id,dateCreated,dateModified,createdBy,modifiedBy,"
                    + "agent,subject,fkContact,isNew,source,department,syncId,status,"
                    + "type,"
                    + "conv_id, integrationId, isStart, fkService, brandId"
                    + ")"
                    + "VALUES (@id,now(),now(),@agent,@agent,"
                    + $"@agent,@subject,@fkContact,@isNew,@source,@department,@syncId,@status,"
                    + "@type,"
                    + "@conv_id, @integrationId, @isStart, @fkService, @brandId"
                    + ")"
                    ;
                }
                using (var connection = ConnectionManager.GetConnection())
                {
                    MySqlCommand command = connection.CreateCommand();
                    command.CommandText = queryString;
                    command.Parameters.AddWithValue("@id", id);
                    command.Parameters.AddWithValue("@agent", agent);
                    command.Parameters.AddWithValue("@subject", subject);
                    command.Parameters.AddWithValue("@fkContact", idKH);
                    command.Parameters.AddWithValue("@isNew", msg.createdAt);
                    command.Parameters.AddWithValue("@source", msg.channelId);
                    command.Parameters.AddWithValue("@department", individual);
                    command.Parameters.AddWithValue("@syncId", msg.user.userId);
                    command.Parameters.AddWithValue("@status", "Chưa xử lý");
                    // DS Moi
                    command.Parameters.AddWithValue("@type", msg.conversation.source);
                    command.Parameters.AddWithValue("@conv_id", msg.conversation.id);
                    command.Parameters.AddWithValue("@integrationId", msg.integrationId);
                    command.Parameters.AddWithValue("@isStart", msg.isStart);
                    command.Parameters.AddWithValue("@fkService", msg.fkService);
                    command.Parameters.AddWithValue("@brandId", msg.conversation.brandId);
                    // Email
                    command.Parameters.AddWithValue("@email_from", email_from);
                    command.Parameters.AddWithValue("@email_to", email_to);
                    command.Parameters.AddWithValue("@email_cc", CCs);
                    command.Parameters.AddWithValue("@msg_id", msg.id);
                    command.Parameters.AddWithValue("@name", msg.user.name);

                    command.ExecuteNonQuery();
                }
            }
            catch (Exception e)
            {
                Log.Error($"insertTicket: {e.ToString()}");
                id = "";
            }
            return id;
        }
        // created ticket sp_synsocial_data
        private string insertTicketData(MsgInbound msg,string fkTicket, string type, string agent, string fkContact)
        {
            // string id = msg.id;
            string source = msg.channelId;
            string content = msg.content.text;
            string syncId = msg.user.userId;
            string name = msg.user.name;
            string created_at = msg.createdAt.ToString();
            string id = Guid.NewGuid().ToString();
            string queryString = "";
            string CCs = "", email_from = "", email_to = "";
            string status = "Đã nhận";
            try
            {
                var attachmentDetails = new
                {
                    Thumbnail = (string)null,
                    Url = (string)null,
                    Type = (string)null
                };

                if (msg?.content?.attachments != null)
                {
                    var firstAttachment = msg.content.attachments[0];

                    attachmentDetails = new
                    {
                        Thumbnail = firstAttachment?.payload?.thumbnail,
                        Url = firstAttachment?.payload?.url,
                        Type = firstAttachment?.type
                    };
                };

                if (msg.type == "email" || msg.type == "new_email")
                {
                    if (msg.type == "new_email")
                    {
                        type = msg.conversation.type;
                        status = "Đã gửi";
                    }
                    else {
                        type = "sync";
                    }
                    content = msg.content.text;
                    email_from = msg.conversation.email.from.email;
                    email_to = msg.conversation.email.to;
                    if (string.IsNullOrEmpty(agent))
                    {
                        agent = "System";
                        //agent = msg.conversation.email.from.email;
                    }

                    queryString = "INSERT INTO sp_tickets_data"
                    + "(id,dateCreated,dateModified,createdBy,modifiedBy,"
                    + "from,to,cc,type_email,TieuDe,fkKH,"
                    + "fkTicket,source,content,content_text,syncId,type,name,process_status"
                    + ")"
                    + "VALUES (@id,now(),now(),@email_from,@email_from,"
                    + $"@email_from,@email_to,@email_cc,@email_type,@subject,@fkContact,"
                    + $"@fkTicket,@email_source,@content,@content_text,@syncId,@type,@name,@email_status"
                    + $")";

                    // Kiểm tra xem "ccs" có giá trị không và tạo chuỗi email
                    if (msg.conversation.email.ccs != null && msg.conversation.email.ccs.Any())
                    {
                        CCs = string.Join(";", msg.conversation.email.ccs.Select(cc => cc.email));
                    }
                }
                else {
                    queryString = "INSERT INTO omni_ticket_data"
                    + "(id,dateCreated,dateModified,createdBy,modifiedBy,"
                    + "fkTicket,source,content,syncId,type,name,created_at,status,"
                    + "content_type,mediaUrl,mediaType,mediaSize,altText,comment,fkContact,"
                    + "attach_thumbnail,attach_url,attach_type"
                    + ")"
                    + "VALUES (@id,now(),now(),@agent,@agent,"
                    + $"@fkTicket,@source,@content,@syncId,@type,@name,@created_at,@status,"
                    + $"@content_type,@mediaUrl,@mediaType,@mediaSize,@altText,@comment,@fkContact,"
                    + $"@attach_thumbnail,@attach_url,@attach_type"
                    + $")";
                }
;
                using (var connection = ConnectionManager.GetConnection())
                {
                    MySqlCommand command = connection.CreateCommand();
                    command.CommandText = queryString;
                    command.Parameters.AddWithValue("@id", id);
                    command.Parameters.AddWithValue("@agent", agent);
                    command.Parameters.AddWithValue("@fkTicket", fkTicket);
                    command.Parameters.AddWithValue("@source", msg.user.userId);
                    command.Parameters.AddWithValue("@content", content);
                    command.Parameters.AddWithValue("@syncId", syncId);
                    command.Parameters.AddWithValue("@type", type);
                    command.Parameters.AddWithValue("@name", name);
                    command.Parameters.AddWithValue("@created_at", created_at);
                    command.Parameters.AddWithValue("@status", status);

                    command.Parameters.AddWithValue("@content_type", msg.content.type);
                    command.Parameters.AddWithValue("@mediaUrl", msg.content.mediaUrl);
                    command.Parameters.AddWithValue("@mediaType", msg.content.mediaType);
                    command.Parameters.AddWithValue("@mediaSize", msg.content.mediaSize);
                    command.Parameters.AddWithValue("@altText", msg.content.altText);
                    command.Parameters.AddWithValue("@comment", msg.content.comment);
                    command.Parameters.AddWithValue("@fkContact", fkContact);

                    // Email
                    command.Parameters.AddWithValue("@email_from", email_from);
                    command.Parameters.AddWithValue("@email_to", email_to);
                    command.Parameters.AddWithValue("@email_cc", CCs);
                    command.Parameters.AddWithValue("@subject", msg.conversation.subject);
                    command.Parameters.AddWithValue("@content_text", msg.content.plain_body);
                    command.Parameters.AddWithValue("@email_source", source);
                    command.Parameters.AddWithValue("@email_status", "Đã nhận");
                    command.Parameters.AddWithValue("@email_type", "email");

                    // Zalo
                    command.Parameters.AddWithValue("@attach_thumbnail", attachmentDetails.Thumbnail);
                    command.Parameters.AddWithValue("@attach_url", attachmentDetails.Url);
                    command.Parameters.AddWithValue("@attach_type", attachmentDetails.Type);
                    


                    command.ExecuteNonQuery();
                }
            }
            catch (Exception e)
            {
                Log.Error($"insertTicketData: {e.ToString()}");
                id = "";
            }
            return id;
        }
        // insert contact
        private string insertContact(MsgInbound msg, string agent)
        {
            string queryString = "";
            string id = Guid.NewGuid().ToString();
            Log.Information($"Type: {msg.type}");
            try
            {
                if (string.IsNullOrEmpty(agent))
                {
                    agent = "System";
                    //agent = msg.conversation.email.from.email;
                }

                if (msg.type == "email")
                {
                    if (string.IsNullOrEmpty(msg.user.name))
                    {
                        msg.user.name = $"Khách hàng mới từ {msg.user.userId}";
                    }

                    queryString = "INSERT INTO sp_khachhang"
                    + "(id,dateCreated,dateModified,createdBy,modifiedBy,"
                    + "ten,source,email,"
                    + "createdFrom"
                    + ")"
                    + "VALUES (@id,now(),now(),@agent,@agent,"
                    + $"@name,@source,@syncId,"
                    + "@_type"
                    + ")";
                } else
                {
                    if (string.IsNullOrEmpty(msg.user.name))
                    {
                        msg.user.name = $"Khách hàng mới";
                    }

                    queryString = "INSERT INTO omni_contacts"
                    + "(id,dateCreated,dateModified,createdBy,modifiedBy,"
                    + "name,source,syncId,"
                    + "zalo, facebook, phone, email,"
                    + "type"
                    + ")"
                    + "VALUES (@id,now(),now(),@agent,@agent,"
                    + $"@name,@source,@syncId,"
                    + "@zalo, @facebook, @in_phone, @in_email,"
                    + "@_type"
                    + ")";
                }
;
                using (var connection = ConnectionManager.GetConnection())
                {
                    MySqlCommand command = connection.CreateCommand();
                    command.CommandText = queryString;
                    command.Parameters.AddWithValue("@id", id);
                    command.Parameters.AddWithValue("@agent", agent);
                    command.Parameters.AddWithValue("@name", msg.user.name);
                    command.Parameters.AddWithValue("@source", msg.channelId);
                    command.Parameters.AddWithValue("@syncId", msg.user.userId);
                    // DS Moi
                    command.Parameters.AddWithValue("@zalo", "");
                    command.Parameters.AddWithValue("@facebook", "");
                    command.Parameters.AddWithValue("@in_phone", "");
                    command.Parameters.AddWithValue("@in_email", "");
                    command.Parameters.AddWithValue("@_type", msg.conversation.source);


                    command.ExecuteNonQuery();
                }
            }
            catch (Exception e)
            {
                Log.Error($"insertContact: {e.ToString()}");
                id = "";
            }
            return id;
        }
        // Insert contact link
        private string insertContactLink(MsgInbound msg, string agent, string fkContact)
        {
            if (string.IsNullOrEmpty(agent))
            {
                agent = "System";
            }
            string id = Guid.NewGuid().ToString();
            try
            {
                string queryString = "INSERT INTO omni_contact_link"
                + "(id,dateCreated,dateModified,createdBy,modifiedBy,"
                + "fkContact, avatarUrl, externalId, userId"
                + ")"
                + "VALUES (@id,now(),now(),@agent,@agent,"
                + "@fkContact, @avatarUrl, @externalId, @userId"
                + ")"
;
                using (var connection = ConnectionManager.GetConnection())
                {
                    MySqlCommand command = connection.CreateCommand();
                    command.CommandText = queryString;
                    command.Parameters.AddWithValue("@id", id);
                    command.Parameters.AddWithValue("@fkContact", fkContact);
                    command.Parameters.AddWithValue("@avatarUrl", msg.user.avatarUrl);
                    command.Parameters.AddWithValue("@externalId", msg.user.externalId);
                    command.Parameters.AddWithValue("@userId", msg.user.userId);
                    command.Parameters.AddWithValue("@agent", agent);


                    command.ExecuteNonQuery();
                }
            }
            catch (Exception e)
            {
                Log.Error($"insertContactLink: {e.ToString()}");
                id = "";
            }
            return id;
        }
        // Cập nhật trạng thái ticket
        private bool updateTicket(string id, string isNew)
        {
            bool rs = false;
            try
            {
                if (string.IsNullOrEmpty(id))
                {
                    return false;
                }

                string queryString = "update omni_tickets"
                + " set isNew = @isNew, dateModified = now() "
                + " where id = @id"
                ;
                using (var connection = ConnectionManager.GetConnection())
                {
                    MySqlCommand command = connection.CreateCommand();
                    command.CommandText = queryString;
                    command.Parameters.AddWithValue("@id", id);
                    command.Parameters.AddWithValue("@isNew", isNew);

                    command.ExecuteNonQuery();
                }

                rs = true;
            }
            catch (Exception e)
            {
                Log.Error($"updateTicket: {e.ToString()}");
            }
            finally
            {
                if (!rs)
                {
                    Log.Error($"updateTicket id={id}, isNew={isNew} {rs}");
                }
            }
            return rs;
        }
        // Cập nhật trạng thái ticket data
        private void updateTicketData(string id, string type, string status, string conv_id)
        {
            try
            {
                string queryString = "";
                if (type == "reply" || type == "forward" || type == "create")
                {
                    queryString = "update sp_tickets_data"
                    + " set process_status = @status, dateModified = now(), conv_id = @conv_id "
                    + " where id = @id"
                    ;
                }
                else
                {
                    queryString = "update omni_ticket_data"
                    + " set status = @status, dateModified = now() "
                    + " where id = @id"
                    ;
                }
                using (var connection = ConnectionManager.GetConnection())
                {
                    MySqlCommand command = connection.CreateCommand();
                    command.CommandText = queryString;
                    command.Parameters.AddWithValue("@id", id);
                    command.Parameters.AddWithValue("@status", status);
                    command.Parameters.AddWithValue("@conv_id", conv_id);

                    command.ExecuteNonQuery();
                }

            }
            catch (Exception e)
            {
                Log.Error($"updateTicketData: {e.ToString()}");
            }
        }
        // Phân công lại ticket
        public bool reAsignNewAgent(NTF_SLA ticketDuocGan)
        {
            bool saveStatus = false;
            try
            {
                if (string.IsNullOrEmpty(ticketDuocGan.id))
                {
                    return saveStatus;
                }

                if(string.IsNullOrEmpty(ticketDuocGan.fkNguoiXuLy))
                {
                    return saveStatus;
                }

                string queryString = "update omni_tickets"
                + " set agent = @nguoiXuLy, "
                + " reAssignDate = @reAssignDate"
                + " where id = @id"
                ;
                using (var connection = ConnectionManager.GetConnection())
                {
                    MySqlCommand command = connection.CreateCommand();
                    command.CommandText = queryString;
                    command.Parameters.AddWithValue("@id", ticketDuocGan.id);
                    command.Parameters.AddWithValue("@nguoiXuLy", ticketDuocGan.fkNguoiXuLy);
                    command.Parameters.AddWithValue("@reAssignDate", ticketDuocGan.reAssignDate);

                    command.ExecuteNonQuery();
                }
                Log.Information($"reAsignNewAgent success: id: {ticketDuocGan.id},agent: {ticketDuocGan.fkNguoiXuLy}");

                saveStatus = true;
            }
            catch (Exception e)
            {
                Log.Error($"reAsignNewAgent error: {e.ToString()}");
            }
            finally
            {
                if (!saveStatus)
                {
                    Log.Error($"reAsignNewAgent error: id={ticketDuocGan.id}, fkNguoiXuLy={ticketDuocGan.fkNguoiXuLy}");
                }
            }
            return saveStatus;
        }
        // Cập nhật trạng thái ticket
        private bool updateTicket_Status(string id, string agent, string status)
        {
            bool rs = false;
            try
            {
                if (string.IsNullOrEmpty(id) || string.IsNullOrEmpty(status))
                {
                    return false;
                }

                string queryString = "update omni_tickets"
                + " set status = @status "
                + " where id = @id"
                ;
                using (var connection = ConnectionManager.GetConnection())
                {
                    MySqlCommand command = connection.CreateCommand();
                    command.CommandText = queryString;
                    command.Parameters.AddWithValue("@id", id);
                    command.Parameters.AddWithValue("@status", status);

                    command.ExecuteNonQuery();
                }
                insertLogStatus(id, agent, status);
                rs = true;
            }
            catch (Exception e)
            {
                Log.Error($"updateTicket_Status: {e.ToString()}");
            }
            finally
            {
                if (!rs)
                {
                    Log.Error($"updateTicket_Status id={id}, isNew={status} {rs}");
                }
            }
            return rs;
        }
        // Lấy danh sách Agent mới
        public List<AgentApproval> getAgent_Approval()
        {
            List<AgentApproval> lsAgent = new List<AgentApproval>();
            try
            {
                string queryString = "select n.username, m.agentApproval from jwdb.omni_member_new n left join jwdb.omni_su_member m on n.fk = m.id group by n.username, m.agentApproval";

                using (var connection = ConnectionManager.GetConnection())
                {
                    MySqlCommand command = connection.CreateCommand();
                    command.CommandText = queryString;
                    MySqlDataReader reader = command.ExecuteReader();

                    while (reader.Read())
                    {
                        AgentApproval agent = new AgentApproval()
                        {
                            username = reader[0].ToString(),
                            agent = reader[1].ToString()
                        };
                        lsAgent.Add(agent);
                    }
                }
            }
            catch (Exception e)
            {
                Log.Error($"getAgent_Approval: {e.ToString()}");
            }

            return lsAgent;
        }
        // Lấy thông tin tin nhắn từ CRM
        public CRMMsg getMsgCRM_ID(string id, string type)
        {
            string queryString = "", subTitle = "";
            CRMMsg msg = new CRMMsg();
            try
            {
                if (type == "email" || type == "forward" || type == "create" || type == "reply")
                {
                    queryString = $"select "
                    + "d.fkTicket, d.content, d.type, d.name, d.process_status, d.createdBy, d.dateCreated, t.fkNguoiXuLy, d.process_status,"
                    + "\"text\", \"\", \"\", \"\", \"\", \"\", t.msg_id,"
                    + "t.conv_id, t.source, \"email\", t.fkEmail, t.integrationId, t.fkService, t.brandId,"
                    + "c.avatarUrl, c.externalId,"
                    + "t.TieuDe, d.TieuDe, d.from, d.to, d.cc, d.fkFile, t.fkKH, t.isStart,"
                    + "'', '', '' "
                    + "from jwdb.sp_tickets_data d "
                    + "left join jwdb.sp_tickets t on t.id = d.fkTicket "
                    + "left join jwdb.omni_contact_link c on c.fkContact = t.fkKH "
                    + "where d.id = @id";
                }
                else
                {
                    queryString = $"select "
                    + "d.fkTicket, d.content, d.type, d.name, d.status, d.createdBy, d.dateCreated, d.agent_ticket, d.status_current,"
                    + "d.content_type, d.mediaUrl, d.mediaType, d.mediaSize, d.altText, d.comment, d.id,"
                    + "t.conv_id, t.source, t.type, t.syncId, t.integrationId, t.fkService, t.brandId,"
                    + "c.avatarUrl,c.externalId,"
                    + "'', '', '', '', '', '', '', '',"
                    + "d.attach_thumbnail, d.attach_url, d.attach_type "
                    + "from jwdb.omni_ticket_data d "
                    + "left join jwdb.omni_tickets t on t.id = d.fkTicket "
                    + "left join jwdb.omni_contact_link c on c.fkContact = t.fkContact "
                    + "where d.id = @id";
                }

                using (var connection = ConnectionManager.GetConnection())
                {
                    MySqlCommand command = connection.CreateCommand();
                    command.CommandText = queryString;
                    command.Parameters.AddWithValue("@id", id);
                    MySqlDataReader reader = command.ExecuteReader();

                    while (reader.Read())
                    {
                        msg.fkTicket = reader[0].ToString();
                        msg.content = reader[1].ToString();
                        msg.type = reader[2].ToString();
                        msg.name = reader[3].ToString();
                        msg.status = reader[4].ToString();
                        msg.createdBy = reader[5].ToString();
                        msg.dateCreated = reader[6].ToString();
                        msg.agent_ticket = reader[7].ToString();
                        msg.status_current = reader[8].ToString();
                        msg.content_type = reader[9].ToString();
                        msg.mediaUrl = reader[10].ToString();
                        msg.mediaType = reader[11].ToString();
                        msg.mediaSize = reader[12].ToString();
                        msg.altText = reader[13].ToString();
                        msg.comment = reader[14].ToString();
                        msg.id = reader[15].ToString();
                        // Ticket
                        msg.conv_id = reader[16].ToString();
                        msg.source = reader[17].ToString();
                        msg.conv_source = reader[18].ToString();
                        msg.syncId = reader[19].ToString();
                        msg.integrationId = reader[20].ToString();
                        msg.fkService = reader[21].ToString();
                        msg.brandId = reader[22].ToString();
                        // Contact
                        msg.avatarUrl = reader[23].ToString();
                        msg.externalId = reader[24].ToString();
                        // Email
                        msg.subject = reader[25].ToString();
                        subTitle = reader[26].ToString();
                        msg.from = reader[27].ToString();
                        msg.to = reader[28].ToString();
                        msg.cc = reader[29].ToString();
                        msg.fkFile = reader[30].ToString();
                        msg.fkContact = reader[31].ToString();
                        try {
                            msg.isStart = bool.Parse(reader[32].ToString());
                        }
                        catch
                        {
                            msg.isStart = false;
                        }
                        if (!string.IsNullOrEmpty(subTitle)) {
                            msg.subject = subTitle;
                        }
                        msg.attach_thumbnail = reader[33].ToString();
                        msg.attach_url = reader[34].ToString();
                        msg.attach_type = reader[35].ToString();
                    }
                }
                return msg;
            }
            catch (Exception e)
            {
                Log.Error($"getMsgCRM_ID: {e.ToString()}");
            }

            return null;
        }
        public bool updateMsg(string id, string status, string type, string otherTicket)
        {
            string queryString = "";
            try
            {
                if (type == "email")
                {
                    queryString = "update sp_tickets_data set dateModified = now(), process_status = @status, otherTicket = @otherTicket where id = @id";
                } else
                {
                    queryString = "update omni_ticket_data set lasttime = now(), status = @status where id = @id";
                }

                using (var connection = ConnectionManager.GetConnection())
                {
                    MySqlCommand command = connection.CreateCommand();
                    command.CommandText = queryString;
                    command.Parameters.AddWithValue("@id", id);
                    command.Parameters.AddWithValue("@status", status);
                    command.Parameters.AddWithValue("@otherTicket", otherTicket);
                    command.ExecuteNonQuery();
                }
                return true;
            }
            catch (Exception e)
            {
                Log.Error($"updateMsg: {e.ToString()}");
            }

            return false;
        }
        public async Task<bool> omni_review_message_approver_processAsync(string id, string agent)
        {
            try
            {
                var client = new HttpClient();
                var request = new HttpRequestMessage(HttpMethod.Post, $"{Program.appSettings.CRM.url}/jw/api/process/omni_review_message_approver_process/startProcessByUser/admin");
                request.Headers.Add("accept", "application/json");
                request.Headers.Add("api_id", Program.appSettings.CRM.api_id);
                request.Headers.Add("api_key", Program.appSettings.CRM.api_key);
                var content = new StringContent("{\"idData\":\"" + id + "\",\"agent\":\"" + agent + "\"}", null, "application/json");
                request.Content = content;
                var response = await client.SendAsync(request);
                //response.EnsureSuccessStatusCode();

                return response.IsSuccessStatusCode;
            }
            catch (Exception e)
            {
                Log.Error($"omni_review_message_approver_processAsync: {e.ToString()}");
            }

            return false;
        }
        private void logStatusMsg(string fkId, string agent, string status, string status_current, string syncId, string content, string status_send)
        {
            try
            {

            }
            catch (Exception e)
            {
                Log.Error($"logStatusMsg: {e.ToString()}");
            }
        }
        public async Task SendMessage_MSB(string message, string queue, int solan = 0)
        {
            try
            {
                Log.Information($"SendMessage_MSB {queue} {message}");
                var HostName = Program.appSettings.RabbitMQ_MSB.Hostname;
                var factory = new ConnectionFactory() { Port = Program.appSettings.RabbitMQ_MSB.Port };
                if (!string.IsNullOrEmpty(Program.appSettings.RabbitMQ.username) && !string.IsNullOrEmpty(Program.appSettings.RabbitMQ_MSB.password))
                {
                    factory.UserName = Program.appSettings.RabbitMQ_MSB.username;
                    factory.Password = Program.appSettings.RabbitMQ_MSB.password;
                }
                if (!string.IsNullOrEmpty(Program.appSettings.RabbitMQ_MSB.VirtualHost))
                {
                    factory.VirtualHost = Program.appSettings.RabbitMQ_MSB.VirtualHost;
                }
                using (var connection = factory.CreateConnection(HostName))
                using (var channel = connection.CreateModel())
                {
                    channel.QueueDeclare(queue: queue,
                                         durable: false,
                                         exclusive: false,
                                         autoDelete: false,
                                         arguments: null);
                    var body = Encoding.UTF8.GetBytes(message);
                    channel.BasicPublish(exchange: "",
                                         routingKey: queue,
                                         basicProperties: null,
                                         body: body);
                    channel.BasicQos(0, 10, false);
                }
            }
            catch (Exception ex)
            {
                Log.Error("Loi SendMessage Queue " + queue + " --- mess : " + message + " -- lan " + solan + "--Error--" + ex.ToString());
                if (solan < 3)
                {
                    await SendMessage_MSB(message, queue, solan + 1);
                }
            }
        }
        public async Task SendMessageAsync(string message, string queue, int solan = 0)
        {
            try
            {
                var HostName = Program.appSettings.RabbitMQ.Hostname;
                var factory = new ConnectionFactory() { Port = Program.appSettings.RabbitMQ.Port };
                if (!string.IsNullOrEmpty(Program.appSettings.RabbitMQ.username) && !string.IsNullOrEmpty(Program.appSettings.RabbitMQ.password))
                {
                    factory.UserName = Program.appSettings.RabbitMQ.username;
                    factory.Password = Program.appSettings.RabbitMQ.password;
                }
                using (var connection = factory.CreateConnection(HostName))
                using (var channel = connection.CreateModel())
                {
                    channel.QueueDeclare(queue: queue,
                                         durable: false,
                                         exclusive: false,
                                         autoDelete: false,
                                         arguments: null);
                    var body = Encoding.UTF8.GetBytes(message);
                    channel.BasicPublish(exchange: "",
                                         routingKey: queue,
                                         basicProperties: null,
                                         body: body);
                    channel.BasicQos(0, 10, false);
                }
            }
            catch (Exception ex)
            {
                Log.Error("Loi SendMessage Queue " + queue + " --- mess : " + message + " -- lan " + solan + "--Error--" + ex.ToString());
                if (solan < 3)
                {
                    await SendMessageAsync(message, queue, solan + 1);
                }
            }
        }
        private List<string> getAgent_Admin()
        {

            try
            {
                List<string> lsSup = new List<string>();

                string queryString = "select u.id from dir_user u join dir_employment e on u.id = e.userId and u.active = 1 where e.departmentId in ('AllSupervisor','Supervisor','Admin') group by u.id";

                using (var connection = ConnectionManager.GetConnection())
                {
                    MySqlCommand command = connection.CreateCommand();

                    command.CommandText = queryString;
                    MySqlDataReader reader = command.ExecuteReader();

                    while (reader.Read())
                    {
                        lsSup.Add(reader[0].ToString());
                    }
                }

                return lsSup;
            }
            catch (Exception e)
            {
                Log.Error($"getAgent_Admin: {e.ToString()}");
            }
            return null;
        }
        private void sendNtf(string message)
        {
            try
            {
                if (!string.IsNullOrEmpty(message))
                {
                    Message message1 = JsonConvert.DeserializeObject<Message>(message);

                    if (message1.type == "email")
                    {
                        if (!string.IsNullOrEmpty(message1.nxl))
                            sendExchange($"agent-{message1.nxl}", message);
                    }
                    else {
                        // các kênh chat
                        if (!string.IsNullOrEmpty(message1.id))
                            sendExchange($"crm-{message1.id}", message);
                        // send người xử lý
                        if (!string.IsNullOrEmpty(message1.nxl))
                            sendExchange($"agent-{message1.nxl}", message);

                        // gửi cho tất cả các sup
                        List<string> lsSup = getAgent_Admin();

                        var lsSup_final = lsSup.Where(s => s != message1.nxl).ToList();

                        foreach (var item in lsSup_final)
                        {
                            sendExchange($"agent-{item}", message);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Log.Error($"sendNtf: {e.ToString()}");
            }
        }
        public void sendExchange(string exchange, string message)
        {
            try
            {
                var HostName = Program.appSettings.RabbitMQ.Hostname;
                var factory = new ConnectionFactory() { Port = Program.appSettings.RabbitMQ.Port };
                if (!string.IsNullOrEmpty(Program.appSettings.RabbitMQ.username) && !string.IsNullOrEmpty(Program.appSettings.RabbitMQ.password))
                {
                    factory.UserName = Program.appSettings.RabbitMQ.username;
                    factory.Password = Program.appSettings.RabbitMQ.password;
                }
                using (var connection = factory.CreateConnection(HostName))
                using (var channel = connection.CreateModel())
                {
                    channel.ExchangeDeclare(exchange: exchange,
                                         durable: true,
                                         type: "direct",
                                         autoDelete: false,
                                         arguments: null);
                    var body = Encoding.UTF8.GetBytes(message);
                    channel.BasicPublish(exchange: exchange,
                                         routingKey: "",
                                         basicProperties: null,
                                         body: body);
                    channel.BasicQos(0, 10, false);
                }
            }
            catch (Exception e)
            {
                Log.Error($"sendExchange: {e.ToString()}");
            }
        }
        private List<Interaction> getInteraction(string id)
        {
            List<Interaction> interactions = new List<Interaction>();
            try
            {
                string queryString = "select dateCreated, type, id, createdBy from omni_ticket_data where fkTicket = @id";

                using (var connection = ConnectionManager.GetConnection())
                {
                    MySqlCommand command = connection.CreateCommand();
                    command.CommandText = queryString;
                    command.Parameters.AddWithValue("@id", id);
                    MySqlDataReader reader = command.ExecuteReader();

                    while (reader.Read())
                    {
                        Interaction interaction = new Interaction();
                        interaction.dateCreated = reader.GetDateTime(0);
                        interaction.type = reader[1].ToString();
                        interaction.id = reader[2].ToString();
                        interaction.createdBy = reader[3].ToString();

                        interactions.Add(interaction);
                    }
                }

            }
            catch (Exception e)
            {
                Log.Error($"countInteraction: {e.ToString()}");
            }
            return interactions;
        }
        private SettingClose getSettingClose(string id)
        {
            SettingClose su = new SettingClose();
            try
            {
                string queryString = "select interaction, action, status, note, enable, filter from omni_setting_close where id = @id";

                using (var connection = ConnectionManager.GetConnection())
                {
                    MySqlCommand command = connection.CreateCommand();
                    command.CommandText = queryString;
                    command.Parameters.AddWithValue("@id", id);
                    MySqlDataReader reader = command.ExecuteReader();

                    while (reader.Read())
                    {

                        su.interaction = reader[0].ToString();
                        su.action = reader[1].ToString();
                        su.status = reader[2].ToString();
                        su.note = reader[3].ToString();
                        su.enable = reader[4].ToString();
                        su.filter = reader[5].ToString();
                    }
                }

            }
            catch (Exception e)
            {
                Log.Error($"getSettingClose: {e.ToString()}");
            }
            return su;
        }
        private SettingClose getSetting_Cancel()
        {
            SettingClose su = new SettingClose();
            try
            {
                string queryString = "select interaction, action, status, note, enable, time from omni_setting_cancel order by dateCreated desc limit 1";

                using (var connection = ConnectionManager.GetConnection())
                {
                    MySqlCommand command = connection.CreateCommand();
                    command.CommandText = queryString;
                    MySqlDataReader reader = command.ExecuteReader();

                    while (reader.Read())
                    {
                        su.interaction = reader[0].ToString();
                        su.action = reader[1].ToString();
                        su.status = reader[2].ToString();
                        su.note = reader[3].ToString();
                        su.enable = reader[4].ToString();
                        su.time = reader[5].ToString();
                    }
                }

            }
            catch (Exception e)
            {
                Log.Error($"getSetting_Cancel: {e.ToString()}");
            }
            return su;
        }
        private int countInteraction(List<Interaction> interactions, int max)
        {
            int count = 0;
            try
            {
                var ls = interactions.OrderBy(x => x.dateCreated).ToList();

                bool isSend = false, isRsync = false;
                foreach (var item in interactions)
                {
                    if (item.type.ToLower() == "send")
                        isSend = true;
                    else
                        isRsync = true;

                    if (isSend && isRsync)
                    {
                        count++;
                        isSend = false;
                        isRsync = false;
                    }

                    if (count >= max && max != 0)
                        break;
                }
            }
            catch (Exception e)
            {
                Log.Error($"countInteraction: {e.ToString()}");
            }
            return count;
        }
        private InteractionOUT kiemTraTuongTac(string idTk, string type, string agent)
        {
            InteractionOUT o = new InteractionOUT();
            try
            {
                var su = getSettingClose("1");
                if (su != null && su.enable == "Mở" && su.filter.Contains(type))
                {
                    var ls = getInteraction(idTk);
                    o.slgTuongTac = countInteraction(ls, Int32.Parse(su.interaction));

                    if (su.action.Contains("Thông báo") && o.slgTuongTac >= Int32.Parse(su.interaction))
                    {
                        o.thongb = su.note;
                    }
                    if (su.action.Contains("Chuyển trạng thái") && o.slgTuongTac >= Int32.Parse(su.interaction))
                    {
                        // update ticket
                        updateTicket_Status(idTk, agent, su.status);
                        o.isStatus = true;
                    }
                }
                else
                    return null;
            }
            catch (Exception e)
            {
                Log.Error($"kiemTraTuongTac: {e.ToString()}");
            }

            return o;
        }
        public InteractionOUT jobCancel_Ticket(List<NTF_SLA> lsz)
        {
            InteractionOUT o = new InteractionOUT();
            try
            {
                // xử lý dữ liệu từ get_sla

                var su = getSetting_Cancel();
                if (su != null && su.enable == "Mở" && (su.action.Contains("Thông báo") || su.action.Contains("Chuyển trạng thái")))
                {
                    //var z = getNTFCancel(su.time);
                    //if (!string.IsNullOrEmpty(z))
                    //{
                    //var lsz = Newtonsoft.Json.JsonConvert.DeserializeObject<List<NTF_SLA>>(z);
                    foreach (var item in lsz)
                    {
                        if (string.IsNullOrEmpty(item.last_time))
                            continue;
                        DateTime givenTime = DateTime.ParseExact(item.last_time, "yyyy-MM-dd HH:mm:ss.ffffff", null);
                        DateTime newTime = givenTime.AddMinutes(Int32.Parse(su.time));
                        DateTime currentTime = DateTime.Now;

                        int result = DateTime.Compare(newTime, currentTime);
                        if (result > 0)
                            continue;
                        var ls = getInteraction(item.id);
                        o.slgTuongTac = countInteraction(ls, Int32.Parse(su.interaction));

                        Log.Information($"slgTuongTac: {o.slgTuongTac}");
                        Log.Information($"interaction: {Int32.Parse(su.interaction)}");
                        Log.Information($"action: {su.action}");

                        if (su.action.Contains("Thông báo") && o.slgTuongTac >= Int32.Parse(su.interaction))
                        {
                            o.thongb = su.note;
                        }

                        if (su.action.Contains("Chuyển trạng thái") && o.slgTuongTac >= Int32.Parse(su.interaction))
                        {
                            // update ticket
                            updateTicket_Status(item.id, item.fkNguoiXuLy, su.status);
                            o.isStatus = true;

                            var isStaus = getStatus_Close(su.status);
                            if (isStaus)
                            {
                                string status_send = "inactive";
                                var msg_send = new
                                {
                                    user_id = item.messageId,
                                    status = status_send,
                                    msg = "Đóng đoạn chat."
                                };
                                string json = Newtonsoft.Json.JsonConvert.SerializeObject(msg_send);
                                //Send msg outbound
                                SendMessage_MSB(json, Program.appSettings.RabbitMQ_MSB.Queue_Oubound);
                            }
                        }

                        var intf = new
                        {
                            id = item.id,
                            fk = item.fkKH,
                            name = item.ten,
                            dateCreated = item.dateCreated,
                            content = su.status,
                            messageId = item.messageId,
                            type = "sla_cancel",
                            nxl = item.fkNguoiXuLy,
                            source = item.kenh_tiep_nhan,
                            status = su.status,
                            slgTT = o.slgTuongTac,
                            thongBao = su.note,
                            statusUpdate = o.isStatus
                        };
                        string jso2n = Newtonsoft.Json.JsonConvert.SerializeObject(intf);
                        Log.Information($"ntf cancel: {jso2n}");
                        sendNtf(jso2n);
                    }
                    //}
                }
                else
                    return null;
            }
            catch (Exception e)
            {
                Log.Error($"kiemTraTuongTac: {e.ToString()}");
            }

            return o;
        }
        public void runSetCIF(string jsonString)
        {
            try
            {
                var msg = JsonConvert.DeserializeObject<INCIF>(jsonString);

                var request = getRequestCIF(msg.user_id, msg.channel_id);

                if (!string.IsNullOrEmpty(msg.cif))
                {
                    updateCIF(request.id, msg.cif, "Thành công");
                    updateContact_CIF(request.fkKH, msg.cif);
                }
                else
                {
                    updateCIF(request.id, "", "Thất bại");
                }
            }
            catch (Exception e)
            {
                Log.Error($"runSetCIF: {e.ToString()}");
            }
            finally
            {
                Log.Information($"runSetCIF: {jsonString}");
            }
        }
        //get request cif
        private ContactCIF getRequestCIF(string idSync, string source)
        {
            ContactCIF c = null;
            try
            {
                string queryString = "select id, fkKH from omni_contact_cif"
                + " where syncId = @syncId and source = @source and status = 'Đã gửi yêu cầu' order by dateCreated asc limit 1"
                ;
                using (var connection = ConnectionManager.GetConnection())
                {
                    MySqlCommand command = connection.CreateCommand();
                    command.CommandText = queryString;
                    command.Parameters.AddWithValue("@syncId", idSync);
                    command.Parameters.AddWithValue("@source", source);
                    MySqlDataReader reader = command.ExecuteReader();

                    while (reader.Read())
                    {
                        c = new ContactCIF()
                        {
                            id = reader[0].ToString(),
                            fkKH = reader[1].ToString()
                        };

                    }
                }
            }
            catch (Exception e)
            {
                Log.Error($"getRequestCIF: {e.ToString()}");
            }
            return c;
        }
        //Cập nhật CIF
        private void updateCIF(string id, string cif, string status)
        {
            try
            {
                string queryString = "update omni_contact_cif"
                + " set status = @status, cif = @cif"
                + " where id = @id"
                ;
                using (var connection = ConnectionManager.GetConnection())
                {
                    MySqlCommand command = connection.CreateCommand();
                    command.CommandText = queryString;
                    command.Parameters.AddWithValue("@id", id);
                    command.Parameters.AddWithValue("@cif", cif);
                    command.Parameters.AddWithValue("@status", status);

                    command.ExecuteNonQuery();
                }
            }
            catch (Exception e)
            {
                Log.Error($"updateCIF: {e.ToString()}");
            }
        }
        private void updateContact_CIF(string id, string cif)
        {
            try
            {
                string queryString = "update omni_contacts"
                + " set cif = @cif"
                + " where id = @id"
                ;
                using (var connection = ConnectionManager.GetConnection())
                {
                    MySqlCommand command = connection.CreateCommand();
                    command.CommandText = queryString;
                    command.Parameters.AddWithValue("@id", id);
                    command.Parameters.AddWithValue("@cif", cif);

                    command.ExecuteNonQuery();
                }
            }
            catch (Exception e)
            {
                Log.Error($"updateContact_CIF: {e.ToString()}");
            }
        }
        private void insertLogStatus(string id, string agent, string status)
        {
            try
            {
                string queryString = "INSERT INTO omni_ticket_logs"
                + "(id,dateCreated,createdBy,"
                + "status,agent,fkTicket)"
                + "VALUES (uuid(),now(),@agent,"
                + $"@status,@agent,@fkTicket)"
                ;
                using (var connection = ConnectionManager.GetConnection())
                {
                    MySqlCommand command = connection.CreateCommand();
                    command.CommandText = queryString;
                    command.Parameters.AddWithValue("@agent", agent);
                    command.Parameters.AddWithValue("@status", status);
                    command.Parameters.AddWithValue("@fkTicket", id);

                    command.ExecuteNonQuery();
                }
            }
            catch (Exception e)
            {
                Log.Error($"insertLogStatus: {e.ToString()}");
            }
        }
        // Lấy thông tin tin nhắn từ CRM
        public string getNTFSLA()
        {
            string msg = "";
            try
            {
                string queryString = "call omni.get_ntf_sla()";

                using (var connection = ConnectionManager.GetConnection())
                {
                    MySqlCommand command = connection.CreateCommand();
                    command.CommandText = queryString;
                    MySqlDataReader reader = command.ExecuteReader();

                    while (reader.Read())
                    {
                        msg = reader[0].ToString();
                    }
                }
                return msg;
            }
            catch (Exception e)
            {
                Log.Error($"getNTFSLA: {e.ToString()}");
            }

            return "";
        }
        public string get_ds_ext_available()
        {
            string kq = "";
            try
            {
                DataTable data = new DataTable();
                using (var db = new NpgsqlConnection(Program.appSettings.PostgreSQLConnection))
                {
                    db.Open();
                    using (var command = db.CreateCommand())
                    {
                        command.CommandText = "select * from prds_ext_available(); ";
                        command.CommandType = CommandType.Text;
                        NpgsqlDataAdapter da = new NpgsqlDataAdapter(command);
                        da.Fill(data);
                    }
                    db.Close();
                }
                if (data != null && data.Rows.Count > 0)
                {
                    kq = data.Rows[0][0].ToString();
                }
            }
            catch (Exception ex)
            {
                Log.Error("Loi get_ds_ext_available " + ex.ToString());
            }
            return kq;
        }

        private bool checkBotMessage(MsgInbound msg)
        {
            bool isBot = false;
            try
            {
                string queryString = "select name from jwdb.omni_su_source";

                using (var connection = ConnectionManager.GetConnection())
                {
                    MySqlCommand command = connection.CreateCommand();
                    command.CommandText = queryString;
                    MySqlDataReader reader = command.ExecuteReader();

                    while (reader.Read())
                    {
                        if (reader[0].ToString() == msg.user.name) { 
                            isBot = true;
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Log.Error($"checkBotMessage: {e.ToString()}");
            }
            return isBot;
        }
        private async Task<FileDetails> downloadOmniFile(string in_url, string in_folder, string in_type)
        {
            FileDetails filePath = new FileDetails();
            string fileName = "";
            try
            {
                using (var httpClient = new HttpClient())
                {
                    // Tạo CancellationTokenSource với timeout là 10 giây
                    using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10)))
                    {
                        HttpResponseMessage response = await httpClient.GetAsync(in_url, HttpCompletionOption.ResponseHeadersRead, cts.Token);
                        response.EnsureSuccessStatusCode();

                        // Kiểm tra kích thước tệp
                        long? contentLength = response.Content.Headers.ContentLength;
                        const long FileSizeLimitBytes = 100L * 1024L * 1024L; // 100 MB

                        if (contentLength.HasValue && contentLength.Value > FileSizeLimitBytes)
                        {
                            filePath.error = "Kích thước tệp vượt quá giới hạn cho phép (100 MB).";
                            throw new Exception("Kích thước tệp vượt quá giới hạn cho phép (100 MB).");
                        }

                        if (in_type == "image" || in_type == "gif")
                        {
                            fileName = Path.GetFileName(in_url);
                            if (string.IsNullOrEmpty(fileName))
                            {
                                fileName = $"{in_type}_file.ext";
                                filePath.error = "Không tìm thấy tên hình.";
                                Log.Warning($"Không tìm thấy tên hình tại: {in_url}, fileName: {fileName}");
                            }
                        }
                        else if (in_type == "sticker")
                        {
                            long milliseconds = DateTimeOffset.Now.ToUnixTimeMilliseconds();
                            // Tạo một số ngẫu nhiên từ 1 đến 20
                            Random random = new Random();
                            int randomNumber = random.Next(1, 21); // 21 là giới hạn trên không bao gồm

                            fileName = $"{milliseconds}_{randomNumber}.sticker";
                            if (string.IsNullOrEmpty(fileName))
                            {
                                fileName = $"{in_type}_file.ext";
                                filePath.error = "Không tìm thấy tên sticker.";
                                Log.Warning($"Không tìm thấy tên sticker tại: {in_url}, fileName: {fileName}");
                            }
                        }
                        else {
                            var contentDisposition = response.Content.Headers.ContentDisposition;
                            fileName = contentDisposition?.FileName?.Trim('\"');
                            if (string.IsNullOrEmpty(fileName))
                            {
                                fileName = $"{in_type}_file.ext";
                                filePath.error = "Không tìm thấy tên tệp đính kèm.";
                                Log.Warning($"Không tìm thấy tên tệp đính kèm tại: {in_url}, fileName: {fileName}");
                            }
                        }


                        filePath.name = fileName;
                        filePath.url = Path.Combine(in_folder, fileName);

                        await using (var fileStream = new FileStream(filePath.url, FileMode.Create, FileAccess.Write, FileShare.None))
                        using (var stream = await response.Content.ReadAsStreamAsync())
                        {
                            await stream.CopyToAsync(fileStream, cts.Token);
                        }
                    }
                }
            }
            catch (OperationCanceledException)
            {
                Log.Error("downloadOmniFile: Hành động đã bị hủy bỏ do quá thời gian 10 giây.");
                filePath.error = "Hành động đã bị hủy bỏ do quá thời gian 10 giây.";
            }
            catch (Exception e)
            {
                Log.Error($"downloadOmniFile: {e.ToString()}");
            }
            return filePath;
        }


        private FileDetails handleDownload(string in_url, string in_type) {
            var localFile = new FileDetails();
            try {
                // Tạo thư mục theo ngày và giờ hiện tại
                string dateFolder = DateTime.Now.ToString("yyyy-MM-dd");
                string hourFolder = DateTime.Now.ToString("HH"); // Giờ hiện tại với định dạng 24 giờ
                string directoryPath = Path.Combine(Program.appSettings.Zendesk.localPath, dateFolder, hourFolder);

                // Kiểm tra nếu thư mục không tồn tại thì tạo mới
                if (!Directory.Exists(directoryPath))
                {
                    Directory.CreateDirectory(directoryPath);
                }

                localFile = downloadOmniFile(in_url, directoryPath, in_type).GetAwaiter().GetResult();

            }
            catch (Exception e) {
                Log.Error($"handleDownload: {e.ToString()}");
            }
            return localFile;
        }

    }
}
