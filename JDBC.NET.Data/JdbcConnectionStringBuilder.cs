using System;
using System.Data.Common;
using JDBC.NET.Data.Utilities;

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

        public string AppInstallationPath
        {
            get => GetValue<string>(nameof(AppInstallationPath));
            set => SetValue(nameof(AppInstallationPath), value);
        }

        public string[] LibraryJarFiles
        {
            get
            {
                var value = GetValue<string>(nameof(LibraryJarFiles));

                if (string.IsNullOrEmpty(value))
                    return Array.Empty<string>();

                var bytes = Convert.FromBase64String(value);
                return SimpleSerializer.DeserializeStringArray(bytes);
            }
            set
            {
                if (value is null)
                {
                    SetValue(nameof(LibraryJarFiles), (string)null);
                    return;
                }

                var bytes = SimpleSerializer.SerializeStringArray(value);
                var base64 = Convert.ToBase64String(bytes);

                SetValue(nameof(LibraryJarFiles), base64);
            }
        }
        public string[] LibraryPath
        {
            get
            {
                var value = GetValue<string>(nameof(LibraryPath));

                if (string.IsNullOrEmpty(value))
                    return Array.Empty<string>();

                var bytes = Convert.FromBase64String(value);
                return SimpleSerializer.DeserializeStringArray(bytes);
            }
            set
            {
                if (value is null)
                {
                    SetValue(nameof(LibraryPath), (string)null);
                    return;
                }

                var bytes = SimpleSerializer.SerializeStringArray(value);
                var base64 = Convert.ToBase64String(bytes);

                SetValue(nameof(LibraryPath), base64);
            }
        }
        #endregion

        #region Constructor
        public JdbcConnectionStringBuilder()
        {
            FetchSize = 10;
            ChunkSize = 512 * 1024;
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
                this[key] = value;
            else
                Remove(key);
        }
        #endregion
    }
}
