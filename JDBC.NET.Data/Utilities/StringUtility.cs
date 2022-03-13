using System.Security.Cryptography;

namespace JDBC.NET.Data.Utilities
{
    internal static class StringUtility
    {
        public static string CreateRandom(int length)
        {
            const string table = "ABCDEFGHIJKLNMOPQRSTUVWXYZabcdefghijklnmopqrstuvwxyz0123456789";

            return string.Create(length, length, static (buffer, length) =>
            {
                for (int i = 0; i < length; i++)
                    buffer[i] = table[RandomNumberGenerator.GetInt32(table.Length)];
            });
        }
    }
}
