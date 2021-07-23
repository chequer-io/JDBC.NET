using System.Collections.Generic;

namespace JDBC.NET.Data
{
    public class JdbcConnectionProperties : Dictionary<string, string>
    {
        public JdbcConnectionProperties()
        {
        }

        public JdbcConnectionProperties(int capacity) : base(capacity)
        {
        }

        public JdbcConnectionProperties(IEqualityComparer<string> comparer) : base(comparer)
        {
        }

        public JdbcConnectionProperties(int capacity, IEqualityComparer<string> comparer) : base(capacity, comparer)
        {
        }

        public JdbcConnectionProperties(IDictionary<string, string> dictionary) : base(dictionary)
        {
        }

        public JdbcConnectionProperties(IDictionary<string, string> dictionary, IEqualityComparer<string> comparer) : base(dictionary, comparer)
        {
        }

        public JdbcConnectionProperties(IEnumerable<KeyValuePair<string, string>> collection) : base(collection)
        {
        }

        public JdbcConnectionProperties(IEnumerable<KeyValuePair<string, string>> collection, IEqualityComparer<string> comparer) : base(collection, comparer)
        {
        }
    }
}
