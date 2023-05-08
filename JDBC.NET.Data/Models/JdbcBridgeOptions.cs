namespace JDBC.NET.Data.Models
{
    internal record JdbcBridgeOptions(string DriverPath, string DriverClass)
    {
        public string LibraryJarFiles { get; set; }

        public JdbcConnectionProperties ConnectionProperties { get; set; }
    }
}
