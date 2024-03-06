using System;
using System.Diagnostics.Tracing;
using System.Linq;
using JDBC.NET.Data;

namespace JDBC.NET.Sample;

public class JdbcEventListener : EventListener
{
    protected override void OnEventSourceCreated(EventSource eventSource)
    {
        base.OnEventSourceCreated(eventSource);

        if (eventSource.Name != JdbcEventSource.Name)
            return;

        EnableEvents(eventSource, EventLevel.LogAlways, EventKeywords.All);
    }

    protected override void OnEventWritten(EventWrittenEventArgs eventData)
    {
        base.OnEventWritten(eventData);

        if (eventData.EventSource.Name != JdbcEventSource.Name)
            return;

        switch (eventData.EventId)
        {
            case 1:
                Console.WriteLine($"[JDBC.NET] {eventData.Payload?.First()}");
                return;

            case 2:
                Console.Error.WriteLine($"[JDBC.NET] {eventData.Payload?.First()}");
                return;
        }
    }
}
