using System;
using System.Collections.Generic;
using System.Data.Common;
using JDBC.NET.Data;

namespace JDBC.NET.Sample
{
    internal class Program
    {
        #region Constants
        private const int tableWidth = 100;
        private const int displayLimit = 10;
        #endregion

        #region Console Method
        private static void PrintLine()
        {
            Console.WriteLine(new string('-', tableWidth));
        }

        private static string AlignCenter(string text, int width)
        {
            text = text.Length > width ? text.Substring(0, width - 3) + "..." : text;

            return !string.IsNullOrEmpty(text)
                ? text.PadRight(width - (width - text.Length) / 2).PadLeft(width)
                : new string(' ', width);
        }

        private static void PrintRow(params string[] columns)
        {
            var width = (tableWidth - columns.Length) / columns.Length;
            var row = "|";

            foreach (var column in columns)
            {
                row += AlignCenter(column, width) + "|";
            }

            Console.WriteLine(row);
        }
        #endregion

        private static void Main(string[] args)
        {
            Console.Write("Driver Path : ");
            var driverPath = Console.ReadLine();

            Console.Write("Driver Class : ");
            var driverClass = Console.ReadLine();

            Console.Write("JDBC Url : ");
            var jdbcUrl = Console.ReadLine();

            var builder = new JdbcConnectionStringBuilder
            {
                FetchSize = -1,
                DriverPath = driverPath,
                DriverClass = driverClass,
                JdbcUrl = jdbcUrl
            };

            using var connection = new JdbcConnection(builder);
            connection.Open();

            while (true)
            {
                Console.Write("SQL > ");

                var sql = Console.ReadLine();

                using var command = connection.CreateCommand(sql);

                try
                {
                    using var reader = command.ExecuteReader();
                    PrintResult(reader);
                }
                catch (DbException ex)
                {
                    Console.WriteLine(ex.Message);
                }
            }
        }

        private static void PrintResult(DbDataReader reader)
        {
            Console.Clear();
            PrintLine();

            var columns = new List<string>();

            for (var i = 0; i < reader.FieldCount; i++)
            {
                columns.Add(reader.GetName(i));
            }

            PrintRow(columns.ToArray());
            PrintLine();

            var displayCount = 0;

            while (reader.Read())
            {
                if (displayCount >= displayLimit)
                {
                    PrintRow($"Only the top {displayLimit} results were displayed.");
                    PrintLine();
                    break;
                }

                var items = new List<string>();

                for (var i = 0; i < reader.FieldCount; i++)
                {
                    items.Add(reader.GetString(i));
                }

                PrintRow(items.ToArray());
                PrintLine();

                displayCount++;
            }
        }
    }
}
