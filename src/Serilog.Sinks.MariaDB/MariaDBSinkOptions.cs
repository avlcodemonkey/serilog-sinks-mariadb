using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Serilog.Events;
using Serilog.Formatting.Json;

namespace Serilog.Sinks.MariaDB
{
    public class MariaDBSinkOptions
    {
        /// <summary>
        /// Event property name to SQL table name mapping
        /// </summary>
        public Dictionary<string, string> PropertiesToColumnsMapping { get; set; } = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) {
            ["Exception"] = "Exception",
            ["Level"] = "LogLevel",
            ["Message"] = "Message",
            ["MessageTemplate"] = "MessageTemplate",
            ["Properties"] = "Properties",
            ["Timestamp"] = "Timestamp",
        };

        /// <summary>
        /// Custom formatter for serializing property values to string
        /// </summary>
        public Func<IReadOnlyDictionary<string, LogEventPropertyValue>, string> PropertiesFormatter { get; set; } = DefaultFormatter;

        /// <summary>
        /// If true, uses hash of message template string (to save space)
        /// </summary>
        public bool HashMessageTemplate { get; set; } = false;

        /// <summary>
        /// If true, uses UTC timestamp instead of local time
        /// </summary>
        public bool TimestampInUtc { get; set; } = true;

        public bool ExcludePropertiesWithDedicatedColumn { get; set; } = false;

        /// <summary>
        /// If true, uses enum int value instead of name
        /// </summary>
        public bool EnumsAsInts { get; set; } = false;

        /// <summary>
        /// Older records than this timespan will be periodically deleted
        /// </summary>
        public TimeSpan? LogRecordsExpiration { get; set; }

        /// <summary>
        /// Interval of calling delete query to purge old records
        /// </summary>
        public TimeSpan LogRecordsCleanupFrequency { get; set; } = TimeSpan.FromMinutes(12);

        /// <summary>
        /// Chunk size for DELETE operation (used in `LIMIT x`)
        /// </summary>
        public int DeleteChunkSize { get; set; } = 2000;

        static string DefaultFormatter(IReadOnlyDictionary<string, LogEventPropertyValue> properties)
        {
            var valueFormatter = new JsonValueFormatter(null);
            var propertiesBuilder = new StringBuilder();

            using (var writer = new StringWriter(propertiesBuilder))
            {
                var delimiter = "";
                writer.Write("{");

                foreach (var property in properties)
                {
                    writer.WriteLine(delimiter);
                    delimiter = ",";
                    JsonValueFormatter.WriteQuotedJsonString(property.Key, writer);
                    writer.Write(":");
                    valueFormatter.Format(property.Value, writer);
                }

                writer.WriteLine();
                writer.Write("}");
            }

            return propertiesBuilder.ToString();
        }
    }
}
