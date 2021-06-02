namespace NpgsqlReplicationTester
{
    enum ErrorCodes
    {
        Success = 0,
        Aborted= 1,

        // Connection errors
        UnhandledOpenConnectionError = 1000,
        MissingPasswordOpenConnectionError = 1001,
        EmptyPasswordOpenConnectionError = 1002,
        WrongPasswordOpenConnectionError = 1003,

        UnhandledCreatePgOutputReplicationSlotError = 2000,

        UnhandledStartReplicationError = 3000,
    }
}
