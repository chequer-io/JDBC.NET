using System;
using System.Threading;

namespace JDBC.NET.Data.Models
{
    internal sealed class JdbcBridgeReference : IDisposable
    {
        #region Fields
        private int _count;
        #endregion

        #region Properties
        public int Count => _count;

        public JdbcBridge Bridge { get; set; }
        #endregion

        #region Public Methods
        public void Increment()
        {
            Interlocked.Increment(ref _count);
        }

        public void Decrement()
        {
            Interlocked.Decrement(ref _count);
        }
        #endregion

        #region IDisposable
        public void Dispose()
        {
            Bridge?.Dispose();
        }
        #endregion
    }
}
