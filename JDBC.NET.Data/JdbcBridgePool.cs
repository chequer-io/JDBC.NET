using System.Collections.Concurrent;
using JDBC.NET.Data.Models;

namespace JDBC.NET.Data
{
    internal static class JdbcBridgePool
    {
        #region Fields
        private static readonly ConcurrentDictionary<string, JdbcBridgeReference> _bridges = new ConcurrentDictionary<string, JdbcBridgeReference>();
        #endregion

        #region Public Methods
        public static JdbcBridge Lease(string driverPath, string driverClass)
        {
            var key = JdbcBridge.GenerateKey(driverPath, driverClass);

            if (!_bridges.TryGetValue(key, out var reference))
            {
                reference = new JdbcBridgeReference
                {
                    Bridge = JdbcBridge.FromDriver(driverPath, driverClass)
                };

                _bridges.TryAdd(key, reference);
            }

            reference.Increment();

            return reference.Bridge;
        }

        public static void Release(string key)
        {
            if (_bridges.TryGetValue(key, out var reference))
            {
                reference.Decrement();

                if (reference.Count <= 0)
                {
                    _bridges.TryRemove(key, out _);
                    reference.Dispose();
                }
            }
        }
        #endregion
    }
}
