using System;
using System.CommandLine;
using System.CommandLine.Invocation;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Replication;
using Npgsql.Replication.PgOutput;

namespace NpgsqlReplicationTester
{
    class Program
    {
        static async Task<int> Main(string[] args)
        {
            var rootCommand = new RootCommand
            {
                new Option<string>(
                    aliases: new[] {"-d", "--dbname"},
                    getDefaultValue: () => Environment.UserName,
                    description: "database name to connect to"),
                new Option<string>(
                    aliases: new[] {"-h", "--host"},
                    getDefaultValue: () => "::1",
                    description: "database server host or socket directory"),
                new Option<int>(
                    aliases: new[] {"-p", "--port"},
                    getDefaultValue: () => 5432,
                    description: "database server port"),
                new Option<string>(
                    aliases: new[] {"-U", "--username"},
                    getDefaultValue: () => Environment.UserName,
                    description: "database user name"),
                new Option<bool>(
                    aliases: new[] {"-w", "--no-password"},
                    description: "never prompt for password"),
                new Option<bool>(
                    aliases: new[] {"-W", "--password"},
                    description: "force password prompt (should happen automatically)"),
                new Option<string?>(
                    aliases: new[] {"-s", "--slotname"},
                    description: "replication slot name to create or use"),
                new Option<string[]>(
                    aliases: new[] {"-P", "--publication-names"},
                    description: "the publication names to subscribe to"),
                new Option<ulong?>(
                    alias: "--protocol-version",
                    description: "the protocol version to use"),
                new Option<bool>(
                    aliases: new[] {"-B", "--binary"},
                    description: "use binary format for tuple data"),
                new Option<bool>(
                    aliases: new[] {"-S", "--streaming"},
                    description: "enable streaming of transactions"),
                new Option<bool>(
                    alias: "--keep-empty-transactions",
                    description: "enable streaming of transactions"),
            };

            rootCommand.Description =
                "Console program to consume and display messages from the PostgreSQL Logical Streaming Replication Protocol via Npgsql";

            rootCommand.Handler = CommandHandler.Create<CommandLineOptions, CancellationToken>(RootCommandHandler);
            return await rootCommand.InvokeAsync(args);
        }

        static async Task<int> RootCommandHandler(CommandLineOptions options, CancellationToken cancellationToken)
        {
            try
            {
                var helper = new ReplicationHelper(options, cancellationToken);

                await using var connection = await helper.OpenConnectionAsync();
                if (connection == null)
                    return HandleError(helper.ErrorMessage, helper.ErrorCode);

                var messageEnumerable = await helper.StartReplication(connection);
                if (messageEnumerable == null)
                    return HandleError(helper.ErrorMessage, helper.ErrorCode);

                await foreach (var message in messageEnumerable.WithCancellation(cancellationToken))
                {
                    Console.WriteLine($"Received message type: {message.GetType().Name}");

                    // Always assign LastAppliedLsn and LastFlushedLsn so that Npgsql can inform the
                    // server which WAL files can be removed/recycled.
                    connection.LastAppliedLsn = message.WalEnd;
                    connection.LastFlushedLsn = message.WalEnd;
                }

                return (int) ErrorCodes.Success;
            }
            catch (OperationCanceledException)
            {
                return HandleError("The operation was aborted", ErrorCodes.Aborted);
            }

            int HandleError(string? errorMessage, ErrorCodes errorCode)
            {
                Console.Error.WriteLine(errorMessage ?? "An error occurred.");
                return (int) errorCode;
            }
        }
    }
}
