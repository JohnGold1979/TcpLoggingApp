using System;
using System.Threading.Tasks;
using TcpLoggingApp.Server;
using TcpLoggingApp.Client;

namespace TcpLoggingApp
{
    class Program
    {
        static async Task Main(string[] args)
        {
            bool exit = false;

            while (!exit)
            {
                Console.Clear();
                Console.WriteLine("╔════════════════════════════════════════════════════════════════╗");
                Console.WriteLine("║                   TCP ЛОГ СЕРВЕР/КЛИЕНТ v1.0                   ║");
                Console.WriteLine("╚════════════════════════════════════════════════════════════════╝");
                Console.WriteLine();
                Console.WriteLine("1. 🚀 Запустить СЕРВЕР");
                Console.WriteLine("2. 🧪 Запустить КЛИЕНТ (тест нагрузки)");
                Console.WriteLine("3. ❌ Выход");
                Console.WriteLine();
                Console.Write("Выберите опцию (1-3): ");

                var choice = Console.ReadLine();
                Console.WriteLine();

                switch (choice)
                {
                    case "1":
                        await RunServerMode(args);
                        break;
                    case "2":
                        await RunClientMode(args);
                        break;
                    case "3":
                        exit = true;
                        Console.WriteLine("До свидания!");
                        break;
                    default:
                        Console.WriteLine("Неверный выбор. Нажмите любую клавишу...");
                        Console.ReadKey();
                        break;
                }
            }
        }

        private static async Task RunServerMode(string[] args)
        {
            var port = 8888;
            var logFile = "logs/app.log";

            // Парсинг аргументов
            for (int i = 0; i < args.Length; i++)
            {
                if (args[i] == "--port" && i + 1 < args.Length)
                    port = int.Parse(args[++i]);
                else if (args[i] == "--log" && i + 1 < args.Length)
                    logFile = args[++i];
            }

            Console.WriteLine("Запуск сервера...");
            Console.WriteLine($"Порт: {port}");
            Console.WriteLine($"Лог файл: {logFile}");
            Console.WriteLine();
            Console.WriteLine("═══════════════════════════════════════════════════════════════════");
            Console.WriteLine("Сервер запущен. Нажмите Ctrl+C для остановки");
            Console.WriteLine("═══════════════════════════════════════════════════════════════════");
            Console.WriteLine();

            await ServerRunner.RunAsync(port, logFile);

            Console.WriteLine();
            Console.WriteLine("Сервер остановлен. Нажмите любую клавишу для возврата в меню...");
            Console.ReadKey(true);
        }

        private static async Task RunClientMode(string[] args)
        {
            var serverIp = "127.0.0.1";
            var port = 8888;
            var clientCount = 100;
            var messagesPerClient = 1000;

            // Парсинг аргументов
            for (int i = 0; i < args.Length; i++)
            {
                if (args[i] == "--server" && i + 1 < args.Length)
                    serverIp = args[++i];
                else if (args[i] == "--port" && i + 1 < args.Length)
                    port = int.Parse(args[++i]);
                else if (args[i] == "--clients" && i + 1 < args.Length)
                    clientCount = int.Parse(args[++i]);
                else if (args[i] == "--messages" && i + 1 < args.Length)
                    messagesPerClient = int.Parse(args[++i]);
            }

            // Запрашиваем параметры у пользователя
            Console.WriteLine("Настройки теста нагрузки:");
            Console.Write($"IP адрес сервера [{serverIp}]: ");
            var input = Console.ReadLine();
            if (!string.IsNullOrWhiteSpace(input)) serverIp = input;

            Console.Write($"Порт сервера [{port}]: ");
            input = Console.ReadLine();
            if (!string.IsNullOrWhiteSpace(input) && int.TryParse(input, out var p)) port = p;

            Console.Write($"Количество клиентов [{clientCount}]: ");
            input = Console.ReadLine();
            if (!string.IsNullOrWhiteSpace(input) && int.TryParse(input, out var c)) clientCount = c;

            Console.Write($"Сообщений на клиента [{messagesPerClient}]: ");
            input = Console.ReadLine();
            if (!string.IsNullOrWhiteSpace(input) && int.TryParse(input, out var m)) messagesPerClient = m;

            Console.WriteLine();

            await ClientRunner.RunAsync(serverIp, port, clientCount, messagesPerClient);
        }
    }
}