using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Quartz;
using Serilog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace rsync_omni.ScheduleService
{
    /*
    public class BackupSchedule
    {
        public string _id;
        public string _cron;
        public string _cfg;
        private IHostBuilder CreateHostBuilder() =>
    Host.CreateDefaultBuilder()
    .UseWindowsService()
    .ConfigureServices(services =>
    {
        ConfigureQuartzService(services);

        services.AddScoped<ITaskLogTime, TaskLogTime>();
    });

        public BackupSchedule(string id, string cron, string cfg)
        {
            this._id = id;
            this._cron = cron;
            this._cfg = cfg;
        }

        public async Task runBAsync()
        {
            try
            {
                var Host = CreateHostBuilder().Build();
                Host.RunAsync();
            }
            catch (Exception e)
            {
                Log.Error("runBAsync " + e.ToString());
            }
        }
        private void ConfigureQuartzService(IServiceCollection services)
        {
            try
            {
                Log.Information($"ConfigureQuartzService: {_id} {_cron}");
                services.Configure<QuartzOptions>(options =>
                {
                    options.Scheduling.IgnoreDuplicates = true;
                    options.Scheduling.OverWriteExistingData = true;
                });
                services.AddQuartz(q => {
                    q.UseMicrosoftDependencyInjectionJobFactory();
                    var jobKey = new JobKey($"Task_{_id}");
                    q.AddJob<Task1>(opts => opts.WithIdentity(jobKey).UsingJobData("cfg", _cfg));

                    q.AddTrigger(opts => opts.ForJob(jobKey)
                        .WithIdentity($"Task_{_id}-trigger")
                        .WithCronSchedule(_cron)); // run every 5 seconds "0/5 * * * * ?"
                });
                services.AddQuartzHostedService(q => q.WaitForJobsToComplete = true);
            }
            catch (Exception e)
            {
                Log.Error("ConfigureQuartzService " + e.ToString());
            }
        }
    }
    */
}
