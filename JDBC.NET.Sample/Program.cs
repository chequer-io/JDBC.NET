using System;
using JDBC.NET.Data;

namespace JDBC.NET.Sample
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            var builder = new JdbcConnectionStringBuilder
            {
                DriverPath = @"C:\Users\Kevin\Downloads\mysql-connector-java-8.0.21.jar",
                DriverClass = "com.mysql.cj.jdbc.Driver",
                JdbcConnectionString = ""
            };

            using var connection = new JdbcConnection(builder);
            connection.Open();
            connection.Close();

            using var connection2 = new JdbcConnection(builder);
            connection2.Open();

            Console.WriteLine("Hello World!");
        }
    }
}
