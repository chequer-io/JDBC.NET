using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using JDBC.NET.Data.Models;

namespace JDBC.NET.Data
{
    internal static class JdbcBridgePool
    {
        #region Fields
        private static readonly ConcurrentDictionary<JdbcBridgePoolKey, JdbcBridgeReference> _bridges = new();
        #endregion

        #region Public Methods
        [MethodImpl(MethodImplOptions.Synchronized)]
        public static JdbcBridge Lease(string driverPath, string driverClass, JdbcConnectionProperties connectionProperties)
        {
            var key = JdbcBridgePoolKey.Create(driverPath, driverClass, connectionProperties);

            if (!_bridges.TryGetValue(key, out var reference))
            {
                reference = new JdbcBridgeReference
                {
                    Bridge = JdbcBridge.FromDriver(driverPath, driverClass, connectionProperties)
                };

                _bridges[key] = reference;
            }

            reference.Increment();

            return reference.Bridge;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void Release(JdbcBridgePoolKey key)
        {
            if (!_bridges.TryGetValue(key, out var reference))
                return;

            if (reference.Decrement() > 0)
                return;

            _bridges.Remove(key, out _);
            reference.Dispose();
        }
        #endregion
    }
}
