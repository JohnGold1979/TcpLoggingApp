using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace TcpLoggingApp.Server
{
    public static class ServerRunner
    {
        public static async Task RunAsync(int port, string logFile)
        {
            var ipAddress = IPAddress.Any;
            var services = new ServiceCollection();

            services.AddLogging(builder =>
            {
                builder.ClearProviders();
                builder.AddConsole();
                builder.SetMinimumLevel(LogLevel.Information);
            });

            services.AddSingleton(provider =>
            {
                var logger = provider.GetRequiredService<ILogger<TcpLoggingServer>>();
                var endpoint = new IPEndPoint(ipAddress, port);
                return new TcpLoggingServer(endpoint, logFile, logger);
            });

            services.AddHostedService(provider => provider.GetRequiredService<TcpLoggingServer>());

            await using var serviceProvider = services.BuildServiceProvider();

            using var cts = new CancellationTokenSource();

            // Обработка Ctrl+C
            Console.CancelKeyPress += (sender, e) =>
            {
                e.Cancel = true;
                Console.WriteLine("\n⏸️  Получен сигнал остановки. Завершаем работу...");
                cts.Cancel();
            };

            try
            {
                var host = serviceProvider.GetRequiredService<IHostedService>();
                await host.StartAsync(cts.Token);

                // Ждем сигнала остановки
                var tcs = new TaskCompletionSource<bool>();
                using (cts.Token.Register(() => tcs.TrySetResult(true)))
                {
                    await tcs.Task;
                }

                Console.WriteLine("🛑 Останавливаем сервер...");
                await host.StopAsync(CancellationToken.None);
                Console.WriteLine("✅ Сервер успешно остановлен");
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("⏸️  Операция прервана");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Ошибка: {ex.Message}");
                Console.WriteLine(ex.StackTrace);
            }
        }
    }
}