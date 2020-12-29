using System.Collections.Generic;
using System.Text;
using MySqlConnector;

namespace Serilog.Sinks.MariaDB
{
    internal class SqlTableCreator
    {
        readonly string ConnectionString;
        readonly string TableName;
        readonly IReadOnlyCollection<string> Columns;

        public SqlTableCreator(string connectionString, string tableName, IReadOnlyCollection<string> columns)
        {
            ConnectionString = connectionString;
            TableName = tableName;
            Columns = columns;
        }

        public int CreateTable()
        {
            using (var conn = new MySqlConnection(ConnectionString))
            {
                using (var cmd = new MySqlCommand(GetSqlForTable(), conn))
                {
                    conn.Open();
                    return cmd.ExecuteNonQuery();
                }
            }
        }

        string GetSqlForTable()
        {
            var sql = new StringBuilder();
            var i = 1;

            sql.AppendLine($"CREATE TABLE IF NOT EXISTS `{TableName}` ( ");
            sql.AppendLine("`Id` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,");
            foreach (var column in Columns)
            {
                sql.Append($"`{column}` TEXT NULL");
                if (Columns.Count > i++)
                    sql.Append(',');
                sql.AppendLine();
            }
            sql.AppendLine(");");
            return sql.ToString();
        }
    }
}
