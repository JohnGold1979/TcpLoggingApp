using System.Diagnostics;
using System.Net.Sockets;
using System.Text;

namespace TcpLoggingApp.Client
{
    public static class ClientRunner
    {
        public static async Task RunAsync(string serverIp, int port, int clientCount, int messagesPerClient)
        {
            Console.Clear();
            Console.WriteLine("╔════════════════════════════════════════════════════════════════╗");
            Console.WriteLine("║                    TCP ЛОГ КЛИЕНТ - ТЕСТ НАГРУЗКИ              ║");
            Console.WriteLine("╚════════════════════════════════════════════════════════════════╝");
            Console.WriteLine();
            Console.WriteLine($"📡 Сервер: {serverIp}:{port}");
            Console.WriteLine($"🖥️  Клиентов: {clientCount:N0}");
            Console.WriteLine($"📨 Сообщений на клиента: {messagesPerClient:N0}");
            Console.WriteLine($"📊 Всего сообщений: {(long)clientCount * messagesPerClient:N0}");
            Console.WriteLine();
            Console.WriteLine("⏳ Запуск теста...");
            Console.WriteLine();

            var startTime = DateTime.UtcNow;
            var stopwatch = Stopwatch.StartNew();

            var tasks = new List<Task<(int clientId, int sentCount, bool success, string error)>>();
            var progressLock = new object();
            var completedClients = 0;
            var lastProgressUpdate = 0;

            // Создаем и запускаем всех клиентов
            for (int i = 0; i < clientCount; i++)
            {
                var clientId = i;
                tasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        using var client = new TcpClient();
                        await client.ConnectAsync(serverIp, port);
                        await using var stream = client.GetStream();

                        // Настройки для производительности
                        client.SendBufferSize = 65536;
                        client.NoDelay = true;

                        var sentCount = 0;
                        var batch = new StringBuilder(65536);

                        for (int j = 0; j < messagesPerClient; j++)
                        {
                            // Формируем сообщение с разным размером для реалистичности
                            var dataSize = (j % 3) switch
                            {
                                0 => "short",
                                1 => "medium size message with some additional text",
                                _ => "longer message that contains more detailed information about the test and the current status of the client"
                            };

                            var message = $"[Client={clientId:0000}] Msg {j:00000}: {dataSize} test data\n";
                            batch.Append(message);
                            sentCount++;

                            // Отправляем батчем каждые 50 сообщений
                            if (j % 50 == 49 || j == messagesPerClient - 1)
                            {
                                var bytes = Encoding.UTF8.GetBytes(batch.ToString());
                                await stream.WriteAsync(bytes);
                                batch.Clear();
                            }

                            // Небольшая случайная задержка для имитации реальной нагрузки
                            if (clientId % 20 == 0 && j % 100 == 0)
                                await Task.Delay(1);
                        }

                        // Обновляем прогресс
                        lock (progressLock)
                        {
                            completedClients++;
                            if (completedClients - lastProgressUpdate >= 10 || completedClients == clientCount)
                            {
                                lastProgressUpdate = completedClients;
                                var percent = (completedClients * 100.0) / clientCount;
                                Console.Write($"\r📈 Прогресс: {completedClients}/{clientCount} клиентов ({percent:F1}%)    ");
                            }
                        }

                        return (clientId, sentCount, true, null);
                    }
                    catch (Exception ex)
                    {
                        lock (progressLock)
                        {
                            completedClients++;
                        }
                        return (clientId, 0, false, ex.Message);
                    }
                }));
            }

            // Ожидаем завершения всех клиентов
            var results = await Task.WhenAll(tasks);

            stopwatch.Stop();
            var duration = stopwatch.Elapsed;

            // Статистика
            var successful = results.Count(r => r.success);
            var failed = results.Count(r => !r.success);
            var totalSent = results.Sum(r => r.sentCount);
            var errors = results.Where(r => !r.success).Select(r => $"- Клиент {r.clientId}: {r.error}").Take(10).ToList();

            // Очищаем строку прогресса
            Console.WriteLine();
            Console.WriteLine();

            // Выводим результаты
            Console.WriteLine("╔════════════════════════════════════════════════════════════════╗");
            Console.WriteLine("║                       РЕЗУЛЬТАТЫ ТЕСТА                         ║");
            Console.WriteLine("╚════════════════════════════════════════════════════════════════╝");
            Console.WriteLine();
            Console.WriteLine($"⏱️  Время выполнения: {duration.TotalSeconds:F2} секунд");
            Console.WriteLine($"✅ Успешных клиентов: {successful}/{clientCount}");

            if (failed > 0)
            {
                Console.WriteLine($"❌ Неудачных клиентов: {failed}");
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine($"⚠️  {failed} клиентов не смогли подключиться или отправить данные");
                Console.ResetColor();
            }

            Console.WriteLine($"📨 Всего отправлено: {totalSent:N0} сообщений");
            Console.WriteLine($"⚡ Скорость отправки: {totalSent / duration.TotalSeconds:F0} msg/сек");
            Console.WriteLine($"💾 Пропускная способность: {(totalSent * 100.0) / duration.TotalSeconds:F0} msg/сек (средняя)");

            if (duration.TotalSeconds > 0)
            {
                var mbSent = totalSent * 100.0 / 1024 / 1024; // Приблизительно 100 байт на сообщение
                Console.WriteLine($"📊 Передано данных: ~{mbSent:F2} MB");
                Console.WriteLine($"🌐 Скорость передачи: ~{mbSent / duration.TotalSeconds:F2} MB/сек");
            }

            if (errors.Any())
            {
                Console.WriteLine();
                Console.WriteLine("╔════════════════════════════════════════════════════════════════╗");
                Console.WriteLine("║                         ОШИБКИ                                 ║");
                Console.WriteLine("╚════════════════════════════════════════════════════════════════╝");
                foreach (var error in errors)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine(error);
                    Console.ResetColor();
                }
                if (errors.Count < failed)
                    Console.WriteLine($"... и еще {failed - errors.Count} ошибок");
            }

            // Рекомендации
            Console.WriteLine();
            Console.WriteLine("╔════════════════════════════════════════════════════════════════╗");
            Console.WriteLine("║                       РЕКОМЕНДАЦИИ                             ║");
            Console.WriteLine("╚════════════════════════════════════════════════════════════════╝");
            Console.WriteLine();
            Console.WriteLine("📁 Проверьте лог-файл сервера для подтверждения получения всех сообщений");
            Console.WriteLine($"📊 Ожидаемое количество записей в логе: {(successful * messagesPerClient):N0}");

            if (failed > 0)
            {
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine("💡 Совет: Увеличьте таймауты на сервере или уменьшите количество клиентов");
                Console.ResetColor();
            }

            if (totalSent / duration.TotalSeconds > 50000)
            {
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine("🎉 Отличная производительность! Сервер справляется с высокой нагрузкой");
                Console.ResetColor();
            }

            // Ждем нажатия клавиши перед выходом
            Console.WriteLine();
            Console.WriteLine("═══════════════════════════════════════════════════════════════════");
            Console.WriteLine("Нажмите любую клавишу для возврата в главное меню...");
            Console.ReadKey(true);
        }
    }
}