using System;
using System.Collections.Generic;
using System.Data;
using JDBC.NET.Proto;

namespace JDBC.NET.Data.Converters
{
    internal static class IsolationLevelConverter
    {
        #region Fields
        private static readonly Dictionary<IsolationLevel, TransactionIsolation> _dictionary = new()
        {
            { IsolationLevel.Unspecified, TransactionIsolation.None },
            { IsolationLevel.ReadCommitted, TransactionIsolation.ReadCommitted },
            { IsolationLevel.ReadUncommitted, TransactionIsolation.ReadUncommitted },
            { IsolationLevel.RepeatableRead, TransactionIsolation.RepeatableRead },
            { IsolationLevel.Serializable, TransactionIsolation.Serializable }
        };
        #endregion

        #region Public Methods
        public static TransactionIsolation Convert(IsolationLevel level)
        {
            return !_dictionary.TryGetValue(level, out var Isolation)
                ? throw new NotSupportedException($"Transaction isolation level {level.ToString()} is not supported.")
                : Isolation;
        }
        #endregion
    }
}
