using System;
using JDBC.NET.Data;

namespace JDBC.NET.Sample
{
    class Program
    {
        static void Main(string[] args)
        {
            var connection = new JdbcConnection();
            connection.Connect();

            Console.WriteLine("Hello World!");
        }
    }
}
