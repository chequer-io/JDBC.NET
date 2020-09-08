using J2NET;

namespace JDBC.NET.Data
{
    public class JdbcConnection
    {
        public void Connect()
        {
            JavaRuntime.ExecuteJar("JDBC.NET.Bridge.jar");
        }
    }
}
