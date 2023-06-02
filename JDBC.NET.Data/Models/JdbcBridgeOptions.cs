namespace JDBC.NET.Data.Models
{
    internal sealed class JdbcBridgeOptions
    {
        public string DriverPath { get; }

        public string DriverClass { get; }

        public string[] LibraryJarFiles { get; set; }

        public JdbcConnectionProperties ConnectionProperties { get; set; }

        public JdbcBridgeOptions(string driverPath, string driverClass)
        {
            DriverPath = driverPath;
            DriverClass = driverClass;
        }
    }
}
