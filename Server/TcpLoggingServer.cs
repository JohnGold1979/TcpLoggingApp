using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace TcpLoggingApp.Server
{
    public sealed class TcpLoggingServer : BackgroundService
    {
        private readonly TcpListener _listener;
        private readonly Channel<LogMessage> _channel;
        private readonly string _logFilePath;
        private readonly ILogger<TcpLoggingServer> _logger;
        private readonly CancellationTokenSource _cts;
        private readonly SemaphoreSlim _connectionLimiter;

        // Оптимизированные настройки для высокой нагрузки
        private const int MaxConcurrentClients = 50000;
        private const int ReceiveBufferSize = 8192;
        private const int ChannelCapacity = 50000;
        private const int MaxMessageSize = 4096;
        private const int BacklogSize = 2000; // Размер очереди ожидания
        private const int MaxPendingConnections = 10000;

        // Счетчики производительности
        private long _totalMessagesReceived = 0;
        private long _totalBytesReceived = 0;
        private long _activeConnections = 0;
        private long _totalConnections = 0;
        private long _rejectedConnections = 0;
        private DateTime _lastStatsTime = DateTime.UtcNow;

        public TcpLoggingServer(IPEndPoint endPoint, string logFilePath, ILogger<TcpLoggingServer> logger)
        {
            _listener = new TcpListener(endPoint);
            _logFilePath = logFilePath;
            _logger = logger;
            _cts = new CancellationTokenSource();
            _connectionLimiter = new SemaphoreSlim(MaxConcurrentClients, MaxConcurrentClients);

            // Увеличиваем лимиты для сокетов
            var socket = _listener.Server;
            socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);

            _channel = Channel.CreateBounded<LogMessage>(new BoundedChannelOptions(ChannelCapacity)
            {
                SingleWriter = false,
                SingleReader = true,
                FullMode = BoundedChannelFullMode.DropOldest // Лучше потерять старые сообщения, чем блокироваться
            });
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, _cts.Token);
            var token = linkedCts.Token;

            var writerTask = FileWriterAsync(token);
            var listenerTask = StartListenerAsync(token);
            var statsTask = StatsReporterAsync(token);

            await Task.WhenAny(listenerTask, writerTask, statsTask);
            _cts.Cancel();
            await Task.WhenAll(listenerTask, writerTask, statsTask);
        }

        private async Task StartListenerAsync(CancellationToken token)
        {
            try
            {
                // Настройка сокета для высокой нагрузки
                _listener.Start(BacklogSize);

                // Увеличиваем лимиты системы (требует прав администратора)
                try
                {
                    var socket = _listener.Server;
                    socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
                    socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.NoDelay, true);
                }
                catch { /* Игнорируем ошибки настроек */ }

                _logger.LogInformation("TCP сервер запущен на {Endpoint}", _listener.LocalEndpoint);
                _logger.LogInformation("Максимальное количество клиентов: {MaxClients}", MaxConcurrentClients);
                _logger.LogInformation("Размер буфера канала: {ChannelCapacity}", ChannelCapacity);

                while (!token.IsCancellationRequested)
                {
                    try
                    {
                        // Используем таймаут для предотвращения блокировки
                        var acceptTask = _listener.AcceptTcpClientAsync();
                        if (await Task.WhenAny(acceptTask, Task.Delay(100, token)) == acceptTask)
                        {
                            var tcpClient = await acceptTask;

                            // Проверяем лимиты перед обработкой
                            if (Interlocked.Read(ref _activeConnections) < MaxConcurrentClients)
                            {
                                Interlocked.Increment(ref _totalConnections);
                                _ = Task.Run(() => HandleClientWithSemaphoreAsync(tcpClient, token));
                            }
                            else
                            {
                                Interlocked.Increment(ref _rejectedConnections);
                                tcpClient.Close();
                                _logger.LogWarning("Отклонено подключение: превышен лимит активных соединений");
                            }
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Ошибка при принятии подключения");
                        await Task.Delay(100, token);
                    }
                }
            }
            finally
            {
                _listener.Stop();
                _logger.LogInformation("Сервер остановлен");
                _logger.LogInformation("Статистика за сессию: Всего подключений: {Total}, Отклонено: {Rejected}",
                    _totalConnections, _rejectedConnections);
            }
        }

        private async Task HandleClientWithSemaphoreAsync(TcpClient tcpClient, CancellationToken token)
        {
            await _connectionLimiter.WaitAsync(token);
            Interlocked.Increment(ref _activeConnections);

            try
            {
                await HandleClientAsync(tcpClient, token);
            }
            finally
            {
                Interlocked.Decrement(ref _activeConnections);
                _connectionLimiter.Release();
            }
        }

        private async Task HandleClientAsync(TcpClient tcpClient, CancellationToken token)
        {
            using var _ = tcpClient;
            var clientEndpoint = tcpClient.Client.RemoteEndPoint?.ToString() ?? "unknown";

            // Оптимизированные настройки для производительности
            tcpClient.ReceiveBufferSize = ReceiveBufferSize;
            tcpClient.SendBufferSize = 16384;
            tcpClient.NoDelay = true;
            tcpClient.ReceiveTimeout = 60000; // 60 секунд таймаут
            tcpClient.SendTimeout = 30000;
            tcpClient.LingerState = new LingerOption(false, 0); // Быстрое закрытие

            using var stream = tcpClient.GetStream();
            var buffer = new byte[ReceiveBufferSize];
            var messageBuilder = new StringBuilderPool();

            try
            {
                int bytesRead;
                while (!token.IsCancellationRequested &&
                       (bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length, token)) > 0)
                {
                    Interlocked.Add(ref _totalBytesReceived, bytesRead);
                    ProcessReceivedData(buffer.AsSpan(0, bytesRead), messageBuilder, clientEndpoint);
                }
            }
            catch (IOException ex) when (ex.InnerException is SocketException se &&
                   (se.SocketErrorCode == SocketError.ConnectionReset ||
                    se.SocketErrorCode == SocketError.ConnectionAborted))
            {
                _logger.LogDebug("Клиент {Client} отключился", clientEndpoint);
            }
            catch (OperationCanceledException)
            {
                // Нормальное завершение
            }
            catch (Exception ex)
            {
                _logger.LogDebug("Ошибка при обработке клиента {Client}: {Message}", clientEndpoint, ex.Message);
            }
            finally
            {
                if (messageBuilder.Length > 0)
                {
                    await TryWriteToChannel(messageBuilder.ToString(), clientEndpoint);
                }
                messageBuilder.Dispose();
            }
        }

        private void ProcessReceivedData(
            ReadOnlySpan<byte> data,
            StringBuilderPool messageBuilder,
            string clientEndpoint)
        {
            int startPos = 0;

            for (int i = 0; i < data.Length; i++)
            {
                if (data[i] == '\n')
                {
                    if (i > startPos)
                    {
                        var slice = data.Slice(startPos, i - startPos);

                        if (slice.Length > 0 && slice[^1] == '\r')
                        {
                            slice = slice[0..^1];
                        }

                        if (slice.Length > 0)
                        {
                            AppendToMessageBuilder(messageBuilder, slice);

                            var message = messageBuilder.ToString();
                            if (!string.IsNullOrEmpty(message))
                            {
                                TryWriteToChannelFast(message, clientEndpoint);
                                messageBuilder.Clear();
                            }
                        }
                    }
                    startPos = i + 1;
                }

                if (i - startPos > MaxMessageSize)
                {
                    messageBuilder.Clear();
                    startPos = i + 1;
                }
            }

            if (startPos < data.Length)
            {
                var remaining = data.Slice(startPos);
                AppendToMessageBuilder(messageBuilder, remaining);
            }
        }

        private void AppendToMessageBuilder(StringBuilderPool builder, ReadOnlySpan<byte> span)
        {
            int charCount = Encoding.UTF8.GetCharCount(span);

            if (charCount <= 512)
            {
                Span<char> charBuffer = stackalloc char[512];
                int actualCharCount = Encoding.UTF8.GetChars(span, charBuffer);
                builder.Append(charBuffer[..actualCharCount]);
            }
            else
            {
                char[] rentedArray = System.Buffers.ArrayPool<char>.Shared.Rent(charCount);
                try
                {
                    int actualCharCount = Encoding.UTF8.GetChars(span, rentedArray.AsSpan(0, charCount));
                    builder.Append(rentedArray.AsSpan(0, actualCharCount));
                }
                finally
                {
                    System.Buffers.ArrayPool<char>.Shared.Return(rentedArray);
                }
            }
        }

        private void TryWriteToChannelFast(string message, string clientEndpoint)
        {
            var logMessage = new LogMessage(message, clientEndpoint, DateTime.UtcNow);
            Interlocked.Increment(ref _totalMessagesReceived);

            // Неблокирующая запись - если канал переполнен, просто пропускаем
            _channel.Writer.TryWrite(logMessage);
        }

        private async Task TryWriteToChannel(string message, string clientEndpoint)
        {
            var logMessage = new LogMessage(message, clientEndpoint, DateTime.UtcNow);
            Interlocked.Increment(ref _totalMessagesReceived);
            await _channel.Writer.WriteAsync(logMessage, CancellationToken.None);
        }

        private async Task FileWriterAsync(CancellationToken token)
        {
            var directory = Path.GetDirectoryName(_logFilePath);
            if (!string.IsNullOrEmpty(directory))
                Directory.CreateDirectory(directory);

            // Используем буфер для batch записи
            var batch = new StringBuilder(65536);
            var batchCount = 0;
            var lastFlush = DateTime.UtcNow;

            await using var fileStream = new FileStream(
                _logFilePath,
                FileMode.Append,
                FileAccess.Write,
                FileShare.Read,
                131072, // 128KB буфер
                FileOptions.Asynchronous);

            await using var writer = new StreamWriter(fileStream, Encoding.UTF8, 131072);
            var reader = _channel.Reader;

            try
            {
                await foreach (var message in reader.ReadAllAsync(token))
                {
                    // Формируем сообщение
                    batch.Append('[');
                    batch.Append(message.Timestamp.ToString("yyyy-MM-dd HH:mm:ss.fff"));
                    batch.Append("] [");
                    batch.Append(message.ClientEndpoint);
                    batch.Append("] ");
                    batch.Append(message.Content);
                    batch.Append(Environment.NewLine);

                    batchCount++;

                    // Пакетная запись каждые 1000 сообщений или каждые 100ms
                    if (batchCount >= 1000 || (DateTime.UtcNow - lastFlush).TotalMilliseconds >= 100)
                    {
                        await writer.WriteAsync(batch.ToString());
                        batch.Clear();
                        batchCount = 0;
                        lastFlush = DateTime.UtcNow;
                    }
                }

                // Записываем остаток
                if (batchCount > 0)
                {
                    await writer.WriteAsync(batch.ToString());
                    await writer.FlushAsync();
                }
            }
            catch (OperationCanceledException)
            {
                // Обрабатываем оставшиеся сообщения
                while (reader.TryRead(out var remainingMessage))
                {
                    await writer.WriteAsync($"[{remainingMessage.Timestamp:yyyy-MM-dd HH:mm:ss.fff}] [{remainingMessage.ClientEndpoint}] {remainingMessage.Content}{Environment.NewLine}");
                }
                await writer.FlushAsync();
            }
        }

        private async Task StatsReporterAsync(CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                await Task.Delay(5000, token); // Каждые 5 секунд

                var now = DateTime.UtcNow;
                var elapsed = now - _lastStatsTime;
                var messages = Interlocked.Exchange(ref _totalMessagesReceived, 0);
                var bytes = Interlocked.Exchange(ref _totalBytesReceived, 0);

                if (elapsed.TotalSeconds > 0 && messages > 0)
                {
                    var msgPerSec = messages / elapsed.TotalSeconds;
                    var mbPerSec = (bytes / 1024.0 / 1024.0) / elapsed.TotalSeconds;

                    _logger.LogInformation(
                        "📊 Статистика: {Messages:N0} msgs, {Bytes:N0} bytes | {MsgPerSec:F0} msg/сек, {MbPerSec:F2} MB/сек | Активно: {ActiveConnections}",
                        messages, bytes, msgPerSec, mbPerSec, Interlocked.Read(ref _activeConnections));
                }

                _lastStatsTime = now;
            }
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Остановка сервера...");
            _cts.Cancel();
            await base.StopAsync(cancellationToken);
        }
    }
}