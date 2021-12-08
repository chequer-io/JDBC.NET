using System.Collections.Generic;

namespace JDBC.NET.Data.Models
{
    /// <summary>
    /// [4 BYTE COUNT]:{[1 BYTE TYPE]:[4 BYTE LENGTH]:[N BYTE DATA]}
    /// </summary>
    public class JdbcDataRow : List<JdbcDataItem>
    {
    }
}
