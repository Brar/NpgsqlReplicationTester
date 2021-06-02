using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using Npgsql.Replication;
using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;

namespace NpgsqlReplicationTester
{
    class ReplicationHelper
    {
        readonly CancellationToken _cancellationToken;
        readonly NpgsqlConnectionStringBuilder _connectionStringBuilder;
        readonly bool _generatedSlotName;

        public ReplicationHelper(CommandLineOptions options, CancellationToken cancellationToken)
        {
            _cancellationToken = cancellationToken;
            NoPassword = options.NoPassword;
            Password = options.Password;
            if (string.IsNullOrWhiteSpace(options.SlotName))
            {
                SlotName = $"slot_{Guid.NewGuid():N}";
                _generatedSlotName = true;
            }
            else
                SlotName = options.SlotName;

            PublicationNames = options.PublicationNames.ToImmutableList();
            ProtocolVersion = options.ProtocolVersion;
            Binary = options.Binary;
            Streaming = options.Streaming;
            KeepEmptyTransactions = options.KeepEmptyTransactions;

            _connectionStringBuilder = new()
            {
                Database = options.DbName,
                Host = options.Host,
                Port = options.Port,
                Username = options.Username
            };
        }

        public string DbName => _connectionStringBuilder.Database!;
        public string Host => _connectionStringBuilder.Host!;
        public int Port => _connectionStringBuilder.Port;
        public string Username => _connectionStringBuilder.Username!;
        public bool NoPassword { get; }
        public bool Password { get; }
        public string SlotName { get; }
        public IImmutableList<string> PublicationNames { get; }
        public ulong? ProtocolVersion { get; private set; }
        public bool Binary { get; }
        public bool Streaming { get; }
        public bool KeepEmptyTransactions { get; }
        public ErrorCodes ErrorCode { get; private set; }
        public Exception? ErrorException { get; private set; }
        public string? ErrorMessage  { get; private set; }

        public async Task<LogicalReplicationConnection?> OpenConnectionAsync()
        {
            try
            {
                ResetErrorInfo();
                LogicalReplicationConnection c;

                if (Password && !NoPassword && TryGetPassword(out var password))
                    _connectionStringBuilder.Password = password;

                c = new(_connectionStringBuilder.ConnectionString);
                try
                {
                    return await OpenConnection();
                }
                // GSS/SSPI authentication but IntegratedSecurity not enabled
                catch (NpgsqlException sspiException) when (sspiException.Source == "Npgsql" && sspiException.Message.Contains("IntegratedSecurity"))
                {
                    ResetErrorInfo();
                    _connectionStringBuilder.IntegratedSecurity = true;
                    c.ConnectionString = _connectionStringBuilder.ConnectionString;
                }
                catch (Exception e)
                {
                    if (NoPassword)
                    {
                        SetErrorInfo(e,
                            ErrorCodes.MissingPasswordOpenConnectionError,
                            "Error while connecting: The server expected a password but the --no-password commandline option was set.");
                        return null;
                    }
                    ResetErrorInfo();
                    if (!TryGetPassword(out password))
                    {
                        SetErrorInfo(e,
                            ErrorCodes.EmptyPasswordOpenConnectionError,
                            "Error while connecting: The server expected a password but an empty string was entered.");
                        return null;
                    }
                    _connectionStringBuilder.Password = password;
                    c.ConnectionString = _connectionStringBuilder.ConnectionString;
                }
                try
                {
                    return await OpenConnection();
                }
                catch (Exception e)
                {
                    SetErrorInfo(e,
                        ErrorCodes.WrongPasswordOpenConnectionError);
                    return null;
                }
                
                async Task<LogicalReplicationConnection?> OpenConnection()
                {
                    await c.Open(_cancellationToken);
                    ProtocolVersion ??= c.PostgreSqlVersion.Major > 13 ? 2UL : 1UL;
                    return c;
                }
            }
            catch (Exception e)
            {
                SetErrorInfo(e,
                    ErrorCodes.UnhandledOpenConnectionError);
                return null;
            }

            static bool TryGetPassword(out string? password)
            {
                var passwordBuilder = new StringBuilder();
                ConsoleKey key;
                Console.Write("Password: ");
                do
                {
                    var keyInfo = Console.ReadKey(true);
                    key = keyInfo.Key;

                    if (key == ConsoleKey.Backspace && passwordBuilder.Length > 0)
                        passwordBuilder.Length--;
                    else if (!char.IsControl(keyInfo.KeyChar))
                        passwordBuilder.Append(keyInfo.KeyChar);
                } while (key != ConsoleKey.Enter);

                Console.Write("\b \b\b \b\b \b\b \b\b \b\b \b\b \b\b \b\b \b\b \b");

                password = passwordBuilder.Length > 0
                    ? passwordBuilder.ToString()
                    : null;

                return password != null;
            }

        }

        public async Task<IAsyncEnumerable<PgOutputReplicationMessage>?> StartReplication(
            LogicalReplicationConnection connection)
        {
            try
            {
                var options = new PgOutputReplicationOptions(PublicationNames, ProtocolVersion!.Value, Binary ? true : null, Streaming ? true : null);
                var slot = _generatedSlotName
                    ? await CreatePgOutputReplicationSlotInternal(connection)
                    : new PgOutputReplicationSlot(SlotName);
                if (slot == null)
                    return null;
                try
                {
                    IAsyncEnumerator<PgOutputReplicationMessage> enumerator;
                    try
                    {
                        enumerator = connection.StartReplication(slot, options, _cancellationToken)
                            .GetAsyncEnumerator(_cancellationToken);
                        return StartReplicationInternal(enumerator, await enumerator.MoveNextAsync());
                    }
                    // Replication slot does not exist
                    catch (PostgresException nonExistingSlotException)
                        when (nonExistingSlotException.SqlState =="42704")
                    {
                    }

                    slot = await CreatePgOutputReplicationSlotInternal(connection);
                    if (slot == null)
                        return null;

                    enumerator = connection.StartReplication(slot, options, _cancellationToken)
                        .GetAsyncEnumerator(_cancellationToken);
                    return StartReplicationInternal(enumerator, await enumerator.MoveNextAsync());
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    throw;
                }

                async Task<PgOutputReplicationSlot?> CreatePgOutputReplicationSlotInternal(LogicalReplicationConnection c)
                {
                    try
                    {
                        return await connection.CreatePgOutputReplicationSlot(SlotName, true,
                            LogicalSlotSnapshotInitMode.NoExport, _cancellationToken);
                    }
                    catch (Exception e)
                    {
                        SetErrorInfo(e,
                            ErrorCodes.UnhandledCreatePgOutputReplicationSlotError);
                        return null;
                    }
                }

                async IAsyncEnumerable<PgOutputReplicationMessage> StartReplicationInternal(
                    IAsyncEnumerator<PgOutputReplicationMessage> messageEnumerator, bool enumerate)
                {
                    if (!enumerate)
                    {
                        yield break;
                    }
                    do
                    {
                        if (KeepEmptyTransactions)
                        {
                            yield return messageEnumerator.Current;
                        }
                        else
                        {
                            if (messageEnumerator.Current is BeginMessage)
                            {
                                var current = messageEnumerator.Current.Clone();
                                if (!await messageEnumerator.MoveNextAsync())
                                {
                                    yield return current;
                                    yield break;
                                }

                                var next = messageEnumerator.Current;
                                if (next is CommitMessage)
                                    continue;

                                yield return current;
                                yield return next;
                                continue;
                            }

                            yield return messageEnumerator.Current;
                        }
                    } while (await messageEnumerator.MoveNextAsync());
                }
            }
            catch (Exception e)
            {
                SetErrorInfo(e,
                    ErrorCodes.UnhandledStartReplicationError);
                return null;
            }
        }

        void ResetErrorInfo()
        {
            ErrorException = null;
            ErrorMessage = null;
            ErrorCode = (int)ErrorCodes.Success;
        }

        void SetErrorInfo(Exception errorException, ErrorCodes errorCode, string? errorMessage = null)
        {
            ErrorException = errorException;
            ErrorMessage = errorMessage ?? errorException.Message;
            ErrorCode = errorCode;
        }
    }
}
