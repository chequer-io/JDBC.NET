using JDBC.NET.Proto;

namespace JDBC.NET.Data.Models
{
    public class JdbcDataItem
    {
        public byte[] Value { get; set; }
        
        public JdbcItemType Type { get; set; }
    }
}
