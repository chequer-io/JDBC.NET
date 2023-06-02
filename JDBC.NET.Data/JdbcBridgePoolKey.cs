using System;
using System.Collections.Generic;
using System.Linq;
using JDBC.NET.Data.Models;

namespace JDBC.NET.Data
{
    internal sealed class JdbcBridgePoolKey : IEquatable<JdbcBridgePoolKey>
    {
        public string DriverPath { get; }

        public string DriverClass { get; }

        public string[] LibraryJarFiles { get; }

        public IReadOnlyDictionary<string, string> ConnectionProperties { get; }

        private JdbcBridgePoolKey(string driverPath, string driverClass, string[] libraryJarFiles, IReadOnlyDictionary<string, string> connectionProperties)
        {
            DriverPath = driverPath;
            DriverClass = driverClass;
            LibraryJarFiles = libraryJarFiles;
            ConnectionProperties = connectionProperties;
        }

        public bool Equals(JdbcBridgePoolKey other)
        {
            if (ReferenceEquals(null, other))
                return false;

            if (ReferenceEquals(this, other))
                return true;

            if (DriverPath != other.DriverPath || DriverClass != other.DriverClass || !LibraryJarFiles.SequenceEqual(other.LibraryJarFiles))
                return false;

            if (ReferenceEquals(ConnectionProperties, other.ConnectionProperties))
                return true;

            if (ConnectionProperties is null || other.ConnectionProperties is null)
                return false;

            if (ConnectionProperties.Count != other.ConnectionProperties.Count)
                return false;

            using var properties1 = ConnectionProperties.GetEnumerator();
            using var properties2 = other.ConnectionProperties.GetEnumerator();

            while (properties1.MoveNext())
            {
                properties2.MoveNext();

                if (!Equals(properties1.Current, properties2.Current))
                    return false;
            }

            return true;
        }

        public override bool Equals(object obj)
        {
            return ReferenceEquals(this, obj) || obj is JdbcBridgePoolKey other && Equals(other);
        }

        public override int GetHashCode()
        {
            var hasCode = new HashCode();

            hasCode.Add(DriverClass);
            hasCode.Add(DriverPath);

            if (ConnectionProperties is not null)
            {
                foreach (var (key, value) in ConnectionProperties)
                {
                    hasCode.Add(key);
                    hasCode.Add(value);
                }
            }
            else
            {
                hasCode.Add(-1);
            }

            return hasCode.ToHashCode();
        }

        public static JdbcBridgePoolKey Create(JdbcBridgeOptions options)
        {
            return new JdbcBridgePoolKey(options.DriverPath, options.DriverClass, options.LibraryJarFiles, options.ConnectionProperties);
        }
    }
}
