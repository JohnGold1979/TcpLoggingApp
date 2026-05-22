using System;

namespace TcpLoggingApp.Server
{
    internal readonly struct LogMessage
    {
        public string Content { get; }
        public string ClientEndpoint { get; }
        public DateTime Timestamp { get; }

        public LogMessage(string content, string clientEndpoint, DateTime timestamp)
        {
            Content = content;
            ClientEndpoint = clientEndpoint;
            Timestamp = timestamp;
        }
    }
}