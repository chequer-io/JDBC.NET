using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;

namespace JDBC.NET.Data
{
    public class JdbcParameterCollection : DbParameterCollection
    {
        #region Fields
        private readonly List<JdbcParameter> _internalList = new();
        #endregion

        #region Properties
        public override int Count => _internalList.Count;

        public override object SyncRoot => ((ICollection)_internalList).SyncRoot;
        #endregion

        #region Constructor
        internal JdbcParameterCollection()
        {
        }
        #endregion

        #region Public Methods
        public override int Add(object value)
        {
            if (!(value is JdbcParameter jdbcParameter))
                throw new ArgumentException();

            _internalList.Add(jdbcParameter);

            return Count - 1;
        }

        public JdbcParameter Add(string parameterName, DbType dbType)
        {
            var parameter = new JdbcParameter
            {
                ParameterName = parameterName,
                DbType = dbType
            };

            _internalList.Add(parameter);

            return parameter;
        }

        public JdbcParameter AddWithValue(string parameterName, object value)
        {
            var parameter = new JdbcParameter
            {
                ParameterName = parameterName,
                Value = value
            };

            _internalList.Add(parameter);

            return parameter;
        }

        public override void AddRange(Array values)
        {
            foreach (var value in values)
            {
                Add(value);
            }
        }

        public override void Insert(int index, object value)
        {
            if (!(value is JdbcParameter jdbcParameter))
                throw new ArgumentException();

            _internalList.Insert(index, jdbcParameter);
        }

        public override void Remove(object value)
        {
            _internalList.Remove(value as JdbcParameter);
        }

        public override void RemoveAt(int index)
        {
            Remove(_internalList[index]);
        }

        public override void RemoveAt(string parameterName)
        {
            var parameter = _internalList
                .FirstOrDefault(x => x.ParameterName == parameterName);

            if (parameter == null)
                throw new KeyNotFoundException();

            Remove(parameter);
        }

        public override bool Contains(object value)
        {
            return _internalList.Contains(value as JdbcParameter);
        }

        public override bool Contains(string parameterName)
        {
            var parameter = _internalList
                .FirstOrDefault(x => x.ParameterName == parameterName);

            if (parameter == null)
                throw new KeyNotFoundException();

            return Contains(parameter);
        }

        public override int IndexOf(object value)
        {
            return _internalList.IndexOf(value as JdbcParameter);
        }

        public override int IndexOf(string parameterName)
        {
            var parameter = _internalList
                .FirstOrDefault(x => x.ParameterName == parameterName);

            if (parameter == null)
                throw new KeyNotFoundException();

            return IndexOf(parameter);
        }

        public override IEnumerator GetEnumerator()
        {
            return _internalList.GetEnumerator();
        }

        public override void CopyTo(Array array, int index)
        {
            ((ICollection)_internalList).CopyTo(array, index);
        }

        public override void Clear()
        {
            _internalList.Clear();
        }
        #endregion

        #region Protected Methods
        protected override DbParameter GetParameter(int index)
        {
            return _internalList[index];
        }

        protected override DbParameter GetParameter(string parameterName)
        {
            return GetParameter(IndexOf(parameterName));
        }

        protected override void SetParameter(int index, DbParameter value)
        {
            if (!(value is JdbcParameter jdbcParameter))
                throw new ArgumentException();

            _internalList[index] = jdbcParameter;
        }

        protected override void SetParameter(string parameterName, DbParameter value)
        {
            SetParameter(IndexOf(parameterName), value);
        }
        #endregion
    }
}
