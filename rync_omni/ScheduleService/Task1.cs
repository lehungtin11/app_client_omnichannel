using Microsoft.Extensions.DependencyInjection;
using Quartz;
using rync_omni;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace rsync_omni.ScheduleService
{
    internal class Task1 : IJob
    {
        private readonly IServiceProvider _serviceProvider;
        public Task1(IServiceProvider serviceProvider)
        {
            this._serviceProvider = serviceProvider;
        }
        public async Task Execute(IJobExecutionContext context)
        {
            JobDataMap dataMap = context.JobDetail.JobDataMap;

            config cfg = Newtonsoft.Json.JsonConvert.DeserializeObject<config>(dataMap.GetString("cfg"));

            using var scope = _serviceProvider.CreateScope();
            var svc = scope.ServiceProvider.GetRequiredService<ITaskLogTime>();

            await svc.DoWork(context.CancellationToken, cfg);

            await Task.CompletedTask;
        }
    }
}
