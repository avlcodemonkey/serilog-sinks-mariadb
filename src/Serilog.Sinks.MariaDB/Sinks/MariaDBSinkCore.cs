using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using Serilog.Debugging;
using Serilog.Events;

namespace Serilog.Sinks.MariaDB.Sinks
{
    internal sealed class MariaDBSinkCore
    {
        readonly string TableName;
        readonly IFormatProvider FormatProvider;
        readonly MariaDBSinkOptions Options;
        readonly PeriodicCleanup Cleaner;

        public MariaDBSinkCore(
            string connectionString,
            IFormatProvider formatProvider,
            MariaDBSinkOptions options,
            string tableName,
            bool autoCreateTable
        )
        {
            if (string.IsNullOrWhiteSpace(connectionString))
                throw new ArgumentNullException(nameof(connectionString));
            if (string.IsNullOrWhiteSpace(tableName))
                throw new ArgumentNullException(nameof(tableName));

            TableName = tableName;
            FormatProvider = formatProvider;
            Options = options ?? throw new ArgumentNullException(nameof(options));

            Options.PropertiesToColumnsMapping = Options.PropertiesToColumnsMapping
                .Where(i => !string.IsNullOrWhiteSpace(i.Value))
                .ToDictionary(k => k.Key, v => v.Value);

            if (autoCreateTable)
            {
                try
                {
                    new SqlTableCreator(connectionString, TableName, Options.PropertiesToColumnsMapping.Values).CreateTable();
                }
                catch (Exception ex)
                {
                    SelfLog.WriteLine($"Exception creating table {TableName}:\n{ex}");
                }
            }

            if (Options.LogRecordsExpiration.HasValue && Options.LogRecordsExpiration > TimeSpan.Zero && Options.LogRecordsCleanupFrequency > TimeSpan.Zero)
            {
                Cleaner = new PeriodicCleanup(connectionString,
                    tableName,
                    Options.PropertiesToColumnsMapping["Timestamp"],
                    Options.LogRecordsExpiration.Value,
                    Options.LogRecordsCleanupFrequency,
                    Options.TimestampInUtc,
                    Options.DeleteChunkSize);
                Cleaner.Start();
            }
        }

        public IEnumerable<KeyValuePair<string, object>> GetColumnsAndValues(LogEvent logEvent)
        {
            foreach (var map in Options.PropertiesToColumnsMapping)
            {
                if (map.Key.Equals("Message", StringComparison.OrdinalIgnoreCase))
                {
                    yield return new KeyValuePair<string, object>(map.Value, logEvent.RenderMessage(FormatProvider));
                    continue;
                }

                if (map.Key.Equals("MessageTemplate", StringComparison.OrdinalIgnoreCase))
                {
                    if (Options.HashMessageTemplate)
                    {
                        using var hasher = SHA256.Create();
                        var hash = hasher.ComputeHash(Encoding.Unicode.GetBytes(logEvent.MessageTemplate.Text));

                        yield return new KeyValuePair<string, object>(map.Value, Convert.ToBase64String(hash));
                        continue;
                    }

                    yield return new KeyValuePair<string, object>(map.Value, logEvent.MessageTemplate.Text);
                    continue;
                }

                if (map.Key.Equals("Level", StringComparison.OrdinalIgnoreCase))
                {
                    yield return new KeyValuePair<string, object>(map.Value, Options.EnumsAsInts ? (object)logEvent.Level : logEvent.Level.ToString());
                    continue;
                }

                if (map.Key.Equals("Timestamp", StringComparison.OrdinalIgnoreCase))
                {
                    yield return new KeyValuePair<string, object>(map.Value, Options.TimestampInUtc ? logEvent.Timestamp.ToUniversalTime().DateTime : logEvent.Timestamp.DateTime);
                    continue;
                }

                if (map.Key.Equals("Exception", StringComparison.OrdinalIgnoreCase))
                {
                    yield return new KeyValuePair<string, object>(map.Value, logEvent.Exception?.ToString());
                    continue;
                }

                if (map.Key.Equals("Properties", StringComparison.OrdinalIgnoreCase))
                {
                    var properties = logEvent.Properties.AsEnumerable();
                    if (Options.ExcludePropertiesWithDedicatedColumn)
                        properties = properties.Where(i => !Options.PropertiesToColumnsMapping.ContainsKey(i.Key));

                    yield return new KeyValuePair<string, object>(map.Value, Options.PropertiesFormatter(
                        new ReadOnlyDictionary<string, LogEventPropertyValue>(
                            properties.ToDictionary(k => k.Key, v => v.Value)
                        )
                    ));

                    continue;
                }

                if (!logEvent.Properties.TryGetValue(map.Key, out var property))
                {
                    yield return new KeyValuePair<string, object>(map.Value, null);
                    continue;
                }

                if (!(property is ScalarValue scalarValue))
                {
                    var sb = new StringBuilder();
                    using (var writer = new StringWriter(sb))
                    {
                        property.Render(writer, formatProvider: FormatProvider);
                    }

                    yield return new KeyValuePair<string, object>(map.Value, sb.ToString());
                    continue;
                }

                if (scalarValue.Value == null)
                {
                    yield return new KeyValuePair<string, object>(map.Value, DBNull.Value);
                    continue;
                }

                var isEnum = scalarValue.Value is Enum;

                if (isEnum && !Options.EnumsAsInts)
                {
                    yield return new KeyValuePair<string, object>(map.Value, scalarValue.Value.ToString());
                    continue;
                }

                yield return new KeyValuePair<string, object>(map.Value, scalarValue.Value);
            }
        }

        public string GetBulkInsertStatement(IEnumerable<IEnumerable<KeyValuePair<string, object>>> columnValues)
        {
            var commandText = new StringBuilder();
            AppendInsertStatement(commandText);
            var i = 0;
            foreach (var value in columnValues)
            {
                if (i != 0)
                    commandText.AppendLine(",");
                AppendValuesPart(commandText, value, i);
                i++;
            }
            return commandText.ToString();
        }

        public string GetInsertStatement(IEnumerable<KeyValuePair<string, object>> columnValues)
        {
            var commandText = new StringBuilder();
            AppendInsertStatement(commandText);
            AppendValuesPart(commandText, columnValues);
            return commandText.ToString();
        }

        public void AppendInsertStatement(StringBuilder output)
        {
            var columnNames = Options.PropertiesToColumnsMapping.Where(x => !string.IsNullOrEmpty(x.Value)).Select(i => i.Value).ToList();
            output.AppendLine($"INSERT INTO {TableName} ({string.Join(", ", columnNames)})");
            output.AppendLine("VALUES");
        }

        public static void AppendValuesPart(StringBuilder output, IEnumerable<KeyValuePair<string, object>> columnValues, int? identifier = null)
        {
            var parameters = columnValues.Where(x => !string.IsNullOrEmpty(x.Key)).Select(x => x.Value == null ? "DEFAULT" : $"@{x.Key}{(identifier.HasValue ? identifier.ToString() : "")}").ToList();
            output.Append('(');
            output.Append(string.Join(", ", parameters));
            output.Append(')');
        }
    }
}
