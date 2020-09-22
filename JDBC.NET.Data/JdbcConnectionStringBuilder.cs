using System;
using System.Data.Common;

namespace JDBC.NET.Data
{
    public class JdbcConnectionStringBuilder : DbConnectionStringBuilder
    {
        #region Properties
        public int FetchSize
        {
            get => GetValue<int>(nameof(FetchSize));
            set => SetValue(nameof(FetchSize), value);
        }

        public int ChunkSize
        {
            get => GetValue<int>(nameof(ChunkSize));
            set => SetValue(nameof(ChunkSize), value);
        }

        public string DriverPath
        {
            get => GetValue<string>(nameof(DriverPath));
            set => SetValue(nameof(DriverPath), value);
        }

        public string DriverClass
        {
            get => GetValue<string>(nameof(DriverClass));
            set => SetValue(nameof(DriverClass), value);
        }

        public string JdbcUrl
        {
            get => GetValue<string>(nameof(JdbcUrl));
            set => SetValue(nameof(JdbcUrl), value);
        }
        #endregion

        #region Constructor
        public JdbcConnectionStringBuilder()
        {
            FetchSize = 10;
            ChunkSize = 1000;
        }

        public JdbcConnectionStringBuilder(string connectionString) : this()
        {
            ConnectionString = connectionString;
        }
        #endregion

        #region Private Methods
        private T GetValue<T>(string key)
        {
            if (TryGetValue(key, out var value))
                return (T)Convert.ChangeType(value, typeof(T));

            return default;
        }

        private void SetValue<T>(string key, T value)
        {
            if (!string.IsNullOrEmpty(value?.ToString()))
            {
                this[key] = value;
            }
            else
            {
                Remove(key);
            }
        }
        #endregion
    }
}
