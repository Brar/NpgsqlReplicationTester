namespace NpgsqlReplicationTester
{
    record CommandLineOptions(
        string DbName,
        string Host,
        int Port,
        string Username,
        bool NoPassword,
        bool Password,
        string? SlotName,
        string[] PublicationNames,
        ulong? ProtocolVersion,
        bool Binary,
        bool Streaming,
        bool KeepEmptyTransactions);
}
