using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MySqlConnector;
using Serilog.Debugging;
using Serilog.Events;
using Serilog.Sinks.PeriodicBatching;

namespace Serilog.Sinks.MariaDB.Sinks
{
    public class MariaDBSink : PeriodicBatchingSink
    {
        public const int DefaultBatchPostingLimit = 50;
        public static readonly TimeSpan DefaultPeriod = TimeSpan.FromSeconds(5);

        readonly string ConnectionString;
        readonly MariaDBSinkCore Core;
        readonly bool UseBulkInsert;

        public MariaDBSink(string connectionString,
            IFormatProvider formatProvider,
            int batchPostingLimit,
            int queueSizeLimit,
            TimeSpan period,
            MariaDBSinkOptions options,
            string tableName,
            bool autoCreateTable,
            bool useBulkInsert) : base(batchPostingLimit, period, queueSizeLimit)
        {
            ConnectionString = connectionString;
            UseBulkInsert = useBulkInsert;
            Core = new MariaDBSinkCore(connectionString, formatProvider, options, tableName, autoCreateTable);
        }

        protected override async Task EmitBatchAsync(IEnumerable<LogEvent> events)
        {
            try
            {
                using var connection = new MySqlConnection(ConnectionString);
                await connection.OpenAsync().ConfigureAwait(false);

                if (UseBulkInsert)
                    await BulkInsert(events, connection).ConfigureAwait(false);
                else
                    await Insert(events, connection).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                SelfLog.WriteLine("Unable to write {0} log events to the database due to following error: {1}", events.Count(), ex.Message);
            }
        }

        async Task BulkInsert(IEnumerable<LogEvent> events, MySqlConnection connection)
        {
            var eventData = events.Select(i => Core.GetColumnsAndValues(i)).ToList();
            var commandText = Core.GetBulkInsertStatement(eventData);

            using (var cmd = new MySqlCommand(commandText, connection))
            {
                var i = 0;
                eventData.ForEach(columnValues => {
                    columnValues.Where(x => x.Value != null).ToList().ForEach(x => cmd.Parameters.AddWithValue($"{x.Key}{i}", x.Value));
                    i++;
                });
                await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }

        async Task Insert(IEnumerable<LogEvent> events, MySqlConnection connection)
        {
            foreach (var log in events)
            {
                try
                {
                    var columnValues = Core.GetColumnsAndValues(log).ToList();
                    var commandText = Core.GetInsertStatement(columnValues);

                    using var cmd = new MySqlCommand(commandText, connection);
                    columnValues.Where(x => x.Value != null).ToList().ForEach(x => cmd.Parameters.AddWithValue(x.Key, x.Value));
                    await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    SelfLog.WriteLine("Unable to write log event to the database due to following error: {0}", ex.Message);
                }
            }
        }
    }
}
