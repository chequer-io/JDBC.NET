using System.IO;
using Google.Protobuf;

namespace JDBC.NET.Data.Utilities;

public static class SimpleSerializer
{
    public static byte[] SerializeStringArray(string[] value)
    {
        using var buffer = new MemoryStream();
        using var output = new CodedOutputStream(buffer);

        // is not null
        output.WriteBool(value is not null);

        if (value is not null)
        {
            output.WriteLength(value.Length);

            foreach (var s in value)
            {
                // is not null
                output.WriteBool(s is not null);

                if (s is not null)
                    output.WriteString(s);
            }
        }

        output.Flush();

        return buffer.ToArray();
    }

    public static string[] DeserializeStringArray(byte[] value)
    {
        using var input = new CodedInputStream(value);

        // is not null
        if (!input.ReadBool())
            return null;

        var length = input.ReadLength();
        var result = new string[length];

        for (int i = 0; i < length; i++)
        {
            // is not null
            if (input.ReadBool())
                result[i] = input.ReadString();
        }

        return result;
    }
}
