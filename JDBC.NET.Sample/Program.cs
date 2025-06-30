using System;
using System.Data.Common;
using System.Security.Principal;
using JDBC.NET.Data;

namespace JDBC.NET.Sample
{
    internal class Program
    {
        private const string DriversPath = @"C:\Projects\sources\datprof-applications\Privacy\Privacy\bin\Debug\Drivers";
        private const string DriverJar = @"C:\Projects\sources\datprof-applications\Privacy\Privacy\bin\Debug\Drivers\mssql-jdbc-12.4.2.jre11.jar";
        private const string DriverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        private const string AppInstallationPath = @"C:\Program Files\DATPROF\DATPROF Privacy";
        private const string JdbcUrl = "jdbc:sqlserver://dpf-sqlserver-2:2233;user=AL_T01;password=AL_T01;integratedSecurity=false;trustServerCertificate=true;database=AL_T01";

        private static void Main(string[] args)
        {

            Console.WriteLine($"Application Installation Path: {AppInstallationPath}");
            Console.WriteLine($"Driver Path : {DriverJar}");
            Console.WriteLine($"Driver Class : {DriverClass}");
            Console.WriteLine($"JDBC Url : {JdbcUrl}");
            
            var builder = new JdbcConnectionStringBuilder
            {
                FetchSize = -1,
                DriverPath = DriverJar,
                DriverClass = DriverClass,
                JdbcUrl = JdbcUrl,
                LibraryPath = new []{DriversPath},
                AppInstallationPath = AppInstallationPath
            };

            try
            {
                Console.WriteLine("Connecting...");
                using var connection = new JdbcConnection(builder);
                connection.Open();
                Console.WriteLine("Connection established...");

                while (true)
                {
                    Console.Write("SQL > ");

                    var sql = Console.ReadLine();

                    using var command = connection.CreateCommand(sql);

                    try
                    {
                        using var reader = command.ExecuteReader();
                        ConsoleUtils.PrintResult(reader);
                    }
                    catch (DbException ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"Connection error: {e.Message}");
            }
        }
    }
}
