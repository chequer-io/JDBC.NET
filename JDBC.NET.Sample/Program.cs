using System;
using JDBC.NET.Data;

namespace JDBC.NET.Sample
{
    class Program
    {
        static void Main(string[] args)
        {
            using var connection = new JdbcConnection();
            Console.WriteLine("Hello World!");
        }
    }
}
