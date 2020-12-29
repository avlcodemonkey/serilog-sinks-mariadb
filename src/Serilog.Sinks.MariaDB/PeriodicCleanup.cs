using System;
using System.Threading;
using MySqlConnector;

namespace Serilog.Sinks.MariaDB
{
    internal class PeriodicCleanup
    {
        readonly string ConnectionString;
        readonly string TableName;
        readonly string ColumnNameWithTime;
        readonly TimeSpan RecordsExpiration;
        readonly TimeSpan CleanupFrequency;
        readonly bool TimeInUtc;
        readonly int DeleteLimit;

        public PeriodicCleanup(string connectionString, string tableName, string columnNameWithTime, TimeSpan recordsExpiration, TimeSpan cleanupFrequency, bool timeInUtc, int deleteLimit)
        {
            if (string.IsNullOrEmpty(columnNameWithTime))
                throw new ArgumentNullException(nameof(columnNameWithTime));
            if (DeleteLimit < 0)
                throw new ArgumentOutOfRangeException(nameof(deleteLimit));

            ColumnNameWithTime = columnNameWithTime;
            ConnectionString = connectionString;
            TableName = tableName;
            RecordsExpiration = recordsExpiration;
            CleanupFrequency = cleanupFrequency;
            TimeInUtc = timeInUtc;
            DeleteLimit = deleteLimit;
        }

        public void Start()
        {
            // Delaying cleanup for 2 seconds just to avoid semi-expensive query at startup
            _ = new Timer(EnsureCleanup, null, 2000, (int)CleanupFrequency.TotalMilliseconds);
        }

        void EnsureCleanup(object state)
        {
            try
            {
                using var conn = new MySqlConnection(ConnectionString);
                conn.Open();
                var affectedRows = 0;
                do
                {
                    var sql = $"DELETE FROM `{TableName}` WHERE `{ColumnNameWithTime}` < @expiration LIMIT {DeleteLimit}";
                    using var cmd = new MySqlCommand(sql, conn);
                    var deleteFromTime = TimeInUtc ? DateTimeOffset.UtcNow - RecordsExpiration : DateTimeOffset.Now - RecordsExpiration;
                    cmd.Parameters.AddWithValue("expiration", deleteFromTime);
                    affectedRows = cmd.ExecuteNonQuery();
                } while (affectedRows >= DeleteLimit && affectedRows > 1);
            }
            catch (Exception ex)
            {
                Debugging.SelfLog.WriteLine("Periodic database cleanup failed: " + ex.ToString());
            }
        }
    }
}
