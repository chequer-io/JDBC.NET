using System.Diagnostics.Tracing;

namespace JDBC.NET.Data;

[EventSource(Name = Name)]
public class JdbcEventSource : EventSource
{ 
    public new const string Name = "JDBC.NET.Data.JdbcEventSource";
    
    public static JdbcEventSource Log = new();

    private JdbcEventSource()
    {
    }

    [Event(1, Level = EventLevel.Verbose)]
    public void StandardOutputDataReceived(string data)
    {
        WriteEvent(1, data);
    }

    [Event(2, Level = EventLevel.Verbose)]
    public void StandardErrorDataReceived(string data)
    {
        WriteEvent(2, data);
    }
}
