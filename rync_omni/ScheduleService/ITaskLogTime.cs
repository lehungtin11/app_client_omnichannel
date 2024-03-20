using rync_omni;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace rsync_omni.ScheduleService
{
    internal interface ITaskLogTime
    {
        Task DoWork(CancellationToken cancellationToken, config cfg);
        Task Execute(config cfg);
    }
}
