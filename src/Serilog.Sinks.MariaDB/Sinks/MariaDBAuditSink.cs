using System;
using System.Linq;
using MySqlConnector;
using Serilog.Core;
using Serilog.Debugging;
using Serilog.Events;

namespace Serilog.Sinks.MariaDB.Sinks
{
    public class MariaDBAuditSink : ILogEventSink
    {
        readonly string ConnectionString;
        readonly MariaDBSinkCore Core;

        public MariaDBAuditSink(
            string connectionString,
            IFormatProvider formatProvider,
            MariaDBSinkOptions options,
            string tableName,
            bool autoCreateTable
        )
        {
            ConnectionString = connectionString;
            Core = new MariaDBSinkCore(connectionString, formatProvider, options, tableName, autoCreateTable);
        }

        public void Emit(LogEvent logEvent)
        {
            try
            {
                using var conn = new MySqlConnection(ConnectionString);
                conn.Open();

                var columnValues = Core.GetColumnsAndValues(logEvent).ToList();
                using var cmd = new MySqlCommand(Core.GetInsertStatement(columnValues), conn);
                columnValues.Where(x => x.Value != null).ToList().ForEach(x => cmd.Parameters.AddWithValue(x.Key, x.Value));
                cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                SelfLog.WriteLine("Unable to write log event to the database due to following error: {1}", ex.Message);
                throw;
            }
        }
    }
}
