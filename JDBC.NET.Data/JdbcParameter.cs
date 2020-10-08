using System.Data;
using System.Data.Common;
using JDBC.NET.Data.Utilities;

namespace JDBC.NET.Data
{
    public class JdbcParameter : DbParameter
    {
        #region Fields
        private object _value;
        #endregion

        #region Properties
        public override DbType DbType { get; set; }

        public override ParameterDirection Direction { get; set; }

        public override bool IsNullable { get; set; }

        public override string ParameterName { get; set; }

        public override string SourceColumn { get; set; }

        public override bool SourceColumnNullMapping { get; set; }

        public override object Value
        {
            get => _value;
            set
            {
                DbType = ParameterTypeUtility.Convert(value);
                _value = value;
            }
        }

        public override int Size { get; set; }
        #endregion

        #region Constructor
        internal JdbcParameter()
        {
        }
        #endregion

        #region Public Methods
        public override void ResetDbType()
        {
            DbType = DbType.String;
        }
        #endregion
    }
}
