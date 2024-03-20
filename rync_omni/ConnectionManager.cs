using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace rync_omni
{
    internal class ConnectionManager
    {

        private static readonly MySqlConnectionStringBuilder connectionStringBuilder = new MySqlConnectionStringBuilder(Program.appSettings.MySQL.connectstr)
        {
            MaximumPoolSize = (uint)Program.appSettings.MySQL.maxPoolSize
        };

        public static MySqlConnection GetConnection()
        {
            var connection = new MySqlConnection(connectionStringBuilder.ConnectionString);
            connection.Open();

            return connection;
        }
    }
}
