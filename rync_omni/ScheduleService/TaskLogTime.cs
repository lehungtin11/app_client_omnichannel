using rync_omni;
using Serilog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static System.Net.Mime.MediaTypeNames;

namespace rsync_omni.ScheduleService
{
    /*
    internal class TaskLogTime : ITaskLogTime
    {
        
        public async Task DoWork(CancellationToken cancellationToken, config cfg)
        {
            await Execute(cfg);
        }
        
        public async Task Execute(config cfg)
        {
            try
            {
                Service service = new Service();
                string z = service.getNTFSLA();

                if (!string.IsNullOrEmpty(z))
                {
                    var ls = Newtonsoft.Json.JsonConvert.DeserializeObject<List<NTF_SLA>>(z);

                    // 1: quá hạn, 2: sắp đến hạn, 3: trong hạn
                    var ls_sla = ls.Where(x => (x.sla_message == "1" || x.sla_message == "2" || x.sla_message == "3") && !string.IsNullOrEmpty(x.isNew)).ToList();
                    bool overTime = false;

                    foreach (var item in ls_sla)
                    {
                        if (!string.IsNullOrEmpty(item.fkNguoiXuLy))
                        {
                            string message = Newtonsoft.Json.JsonConvert.SerializeObject(item);
                            // Log.Information(message);
                            service.sendExchange($"agent-{item.fkNguoiXuLy}", message);
                        }
                    }

                    // reAsignNewAgent
                    // ds ext_available (1) 
                    // ds user trong nhóm của kênh lsOmni (2)
                    // ds user cuối cùng (3) = (1) join (2)
                    // Tổng SL Ticket (4)
                    // Phân công (5) = (4) / (3)
                    var ls_sla_conversation = ls.Where(x => (x.sla_conversation == "1")).ToList();
                    List<NTF_SLA> ls_phan_cong = new List<NTF_SLA>();
                    if (ls_sla_conversation.Any())
                    {
                        // Init reAsignNewAgent variable
                        List<OmniSuSource> lsOmni = service.getOmniChannel();
                        var _time = service.getReasignTime();
                        int reasign_time = Int32.Parse(_time.times);

                        string ext_available = service.get_ds_ext_available(); // (1)
                        string o_message = Newtonsoft.Json.JsonConvert.SerializeObject(ext_available);
                        Log.Information($"ext_available: {o_message}");

                        var groupedByKenhTiepNhan = ls_sla_conversation.GroupBy(x => x.kenh_tiep_nhan).ToList();

                        foreach (var item in groupedByKenhTiepNhan) {
                            OmniSuSource omni = lsOmni.FirstOrDefault(x => x.id == item.Key);
                            if (omni == null)
                                continue;
                            var listAgentPollingString = service.getListAgent(omni.pollingStrategy); // (2)

                            var listAgentPolling = Newtonsoft.Json.JsonConvert.DeserializeObject<List<Agent_Polling>>(listAgentPollingString);

                            var dsAgentFinal = listAgentPolling
                                .Where(agent => ext_available.Contains(agent.id)).ToList(); // (3)
                            o_message = Newtonsoft.Json.JsonConvert.SerializeObject(dsAgentFinal);
                            Log.Information($"dsAgentFinal: {o_message}");

                            var ds_ticket_can_chia = ls_sla_conversation.Where(x => x.kenh_tiep_nhan == item.Key && ((x.reAssignDate.HasValue && x.reAssignDate.Value < DateTime.Now) || !x.reAssignDate.HasValue));

                            float sl_can_chia_cho_agent = (float)ds_ticket_can_chia.Count() / dsAgentFinal.Count(); // (4)
                            int roundedValue = (int)Math.Ceiling(sl_can_chia_cho_agent);

                            Log.Information($"sl_can_chia_cho_agent: {roundedValue}");

                            foreach (var item_agent in dsAgentFinal)
                            {
                                for (int i = 0; i < (int)roundedValue; i++)
                                {
                                    // Chỉ lấy ticket chưa có trong danh sách ls_phan_cong
                                    var ticket_duoc_gan = ds_ticket_can_chia.FirstOrDefault(ticket =>
                                        !ls_phan_cong.Any(p => p.id == ticket.id)
                                    );

                                    if (ticket_duoc_gan == null)
                                        continue;

                                    ticket_duoc_gan.reAssignDate = DateTime.Now.AddMinutes(reasign_time);
                                    ticket_duoc_gan.fkNguoiXuLy = item_agent.id; // (5)

                                    bool saveStatus = service.reAsignNewAgent(ticket_duoc_gan);

                                    if (!saveStatus)
                                        continue;

                                    string message = Newtonsoft.Json.JsonConvert.SerializeObject(ticket_duoc_gan);
                                    service.sendExchange($"agent-{item_agent.id}", message);

                                    ls_phan_cong.Add(ticket_duoc_gan);
                                }
                            }
                        }
                    }

                    var ls_cancel = ls.Where(x => string.IsNullOrEmpty(x.isNew)).ToList();

                    if (ls_cancel.Count > 0)
                        service.jobCancel_Ticket(ls_cancel);
                }
            }
            catch (Exception e)
            {
                Log.Error($"Execute: {e.ToString()}");
            }
        }

}
    */
}
