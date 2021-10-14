using System.Collections.Generic;
using System.Runtime.CompilerServices;
using JDBC.NET.Data.Models;

namespace JDBC.NET.Data
{
    internal static class JdbcBridgePool
    {
        #region Fields
        private static readonly Dictionary<string, JdbcBridgeReference> _bridges = new();
        #endregion

        #region Public Methods
        [MethodImpl(MethodImplOptions.Synchronized)]
        public static JdbcBridge Lease(string driverPath, string driverClass, JdbcConnectionProperties connectionProperties)
        {
            var key = JdbcBridge.GenerateKey(driverPath, driverClass);

            if (!_bridges.TryGetValue(key, out var reference))
            {
                reference = new JdbcBridgeReference
                {
                    Bridge = JdbcBridge.FromDriver(driverPath, driverClass, connectionProperties)
                };

                _bridges.Add(key, reference);
            }

            reference.Increment();

            return reference.Bridge;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void Release(string key)
        {
            if (_bridges.TryGetValue(key, out var reference))
            {
                reference.Decrement();

                if (reference.Count <= 0)
                {
                    _bridges.Remove(key, out _);
                    reference.Dispose();
                }
            }
        }
        #endregion
    }
}
