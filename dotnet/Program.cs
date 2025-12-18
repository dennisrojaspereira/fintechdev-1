using System.ComponentModel.DataAnnotations;
using Microsoft.AspNetCore.Http.HttpResults;
using Npgsql;
using OpenTelemetry.Metrics;
using System.Diagnostics.Metrics;
using Scalar.AspNetCore;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOpenApi();
builder.Services.ConfigureHttpJsonOptions(opts => 
    opts.SerializerOptions.PropertyNamingPolicy = System.Text.Json.JsonNamingPolicy.CamelCase);

builder.Services.AddOpenTelemetry()
    .WithMetrics(cfg => cfg.AddAspNetCoreInstrumentation()
        .AddMeter(TransferMetrics.MeterName)
        .AddPrometheusExporter());

builder.Services.AddSingleton<NpgsqlDataSource>(sp =>
{
    var config = sp.GetRequiredService<IConfiguration>();
    var connectionString = $"Host={config["DB_HOST"] ?? "localhost"};" +
        $"Port={config["DB_PORT"] ?? "5432"};" +
        $"Username={config["DB_USER"] ?? "fintech"};" +
        $"Password={config["DB_PASSWORD"] ?? "fintech"};" +
        $"Database={config["DB_NAME"] ?? "fintech"};Pooling=true;Maximum Pool Size=10;Minimum Pool Size=1";
    return NpgsqlDataSource.Create(connectionString);
});
builder.Services.AddSingleton<TransferMetrics>();

var app = builder.Build();

app.MapOpenApi();
app.MapScalarApiReference();
app.MapPrometheusScrapingEndpoint();

app.MapPost("/transfer", async Task<Results<Ok<TransferOutput>, Ok<TransferWithBalancesOutput>, BadRequest<TransferOutput>, InternalServerError<TransferOutput>>> (
    TransferInput input, NpgsqlDataSource dataSource, TransferMetrics metrics) =>
{
    try
    {
        var validationProblem = input.Validate(new ValidationContext(input)).FirstOrDefault();
        if (validationProblem is not null)
        {
            metrics.TransferInc(TransferMetrics.ValidationError);
            return TypedResults.BadRequest(TransferOutput.Error(validationProblem.ErrorMessage));
        }
        await using var connection = await dataSource.OpenConnectionAsync();
        await using var transaction = await connection.BeginTransactionAsync();

        var isOperationProcessed = await connection.ExecuteScalar("SELECT 1 FROM processed_ops WHERE operation_id=@id",
            transaction, ("id", input.OperationId));
        if (isOperationProcessed is not null)
        {
            await transaction.RollbackAsync();
            metrics.TransferInc(TransferMetrics.Duplicate);
            return TypedResults.Ok(TransferOutput.Ok("operation already processed"));
        }
        const string accountBalanceQuery = "SELECT balance FROM accounts WHERE id=@id FOR UPDATE";
        var fromBalance = (decimal?) await connection.ExecuteScalar(accountBalanceQuery, transaction,
            ("id", input.FromAccountId));
        var toBalance = (decimal?) await connection.ExecuteScalar(accountBalanceQuery, transaction,
            ("id", input.ToAccountId));

        if (fromBalance is null || toBalance is null)
        {
            await transaction.RollbackAsync();
            metrics.TransferInc(TransferMetrics.AccountNotFound);
            return TypedResults.BadRequest(TransferOutput.Error("account not found"));
        }
        if (fromBalance < input.Amount)
        {
            await transaction.RollbackAsync();
            metrics.TransferInc(TransferMetrics.InsufficientFunds);
            return TypedResults.BadRequest(TransferOutput.Error("insufficient funds"));
        }
        var newFromBalance = fromBalance - input.Amount;
        var newToBalance = toBalance + input.Amount;

        const string updateAccCommandSql = "UPDATE accounts SET balance=@balance WHERE id=@id";
        await connection.Execute(updateAccCommandSql, transaction, ("balance", newFromBalance), ("id", input.FromAccountId));
        await connection.Execute(updateAccCommandSql, transaction, ("balance", newToBalance), ("id", input.ToAccountId));

        var now = DateTime.UtcNow;
        const string insertLedgerCommandSql = "INSERT INTO ledger (type, account_id, amount, at) VALUES (@type, @accountId, @amount, @at)";
        await connection.Execute(insertLedgerCommandSql, transaction,
            ("type", "DEBIT"), ("accountId", input.FromAccountId), ("amount", input.Amount), ("at", now));
        await connection.Execute(insertLedgerCommandSql, transaction,
            ("type", "CREDIT"), ("accountId", input.ToAccountId), ("amount", input.Amount), ("at", now));

        await connection.Execute("INSERT INTO processed_ops (operation_id) VALUES (@id)", transaction, ("id", input.OperationId));

        await transaction.CommitAsync();
        metrics.TransferInc(TransferMetrics.Success);

        return TypedResults.Ok(TransferOutput.Ok("transfer completed", new() {
            { input.FromAccountId, newFromBalance.Value },
            { input.ToAccountId, newToBalance.Value }}));
    }
    catch (Exception e)
    {
        Console.WriteLine(e);
        return TypedResults.InternalServerError(TransferOutput.ServerError());
    }
});

app.MapGet("/debug/state", async (NpgsqlDataSource dataSource) =>
{
    await using var connection = await dataSource.OpenConnectionAsync();

    var accounts = await connection.ExecuteReader("SELECT id, balance FROM accounts ORDER BY id",
        reader => new Account(reader.Get<string>("id"), reader.Get<decimal>("balance")));

    var ledger = await connection.ExecuteReader(
        "SELECT type, account_id, amount, at FROM ledger ORDER BY id DESC LIMIT 100",
        r => new LedgerEntry(
            r.Get<string>("type"),
            r.Get<string>("account_id"),
            r.Get<decimal>("amount"),
            r.Get<DateTime>("at").ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'")));

    var processedOps = await connection.ExecuteReader(
        "SELECT operation_id FROM processed_ops ORDER BY created_at DESC LIMIT 100",
        reader => reader.Get<string>("operation_id"));

    return TypedResults.Ok(new DebugStateOutput(accounts.ToDictionary(a => a.Id, a => a), ledger, processedOps));
});

await Seed(app.Services.GetRequiredService<NpgsqlDataSource>());
app.Run();

async Task Seed(NpgsqlDataSource dataSource)
{
    await using var connection = await dataSource.OpenConnectionAsync();
    await connection.Execute(
        "INSERT INTO accounts (id, balance) VALUES (@id1, @balance1), (@id2, @balance2) ON CONFLICT (id) DO NOTHING", null,
        ("id1", "A"), ("balance1", 1000m),
        ("id2", "B"), ("balance2", 500m));
}

public record TransferInput(
    string FromAccountId,
    string ToAccountId,
    decimal Amount,
    string OperationId) : IValidatableObject
{
    public IEnumerable<ValidationResult> Validate(ValidationContext validationContext)
    {
        if (FromAccountId is null || ToAccountId is null)
            return [new ValidationResult("fromAccountId and toAccountId are required")];
        if (FromAccountId == ToAccountId)
            return [new ValidationResult("fromAccountId and toAccountId must differ")];
        if (Amount <= 0)
            return [new ValidationResult("amount must be > 0")];
        return [];
    }
}; 

public record TransferOutput(string Status, string Message)
{
    public static TransferOutput Error(string? message) => new("error", message ?? string.Empty);
    public static TransferOutput ServerError() => new("error", "internal error");
    public static TransferOutput Ok(string? message) => new("ok", message ?? string.Empty);
    public static TransferWithBalancesOutput Ok(string message, Dictionary<string, decimal>? balances) => new(message, balances);
}
public record TransferWithBalancesOutput(string Message, Dictionary<string, decimal>? Balances) : TransferOutput("ok", Message);

public record DebugStateOutput(
    Dictionary<string, Account> Accounts, 
    List<LedgerEntry> Ledger, 
    List<string> ProcessedOps);
public record Account(string Id, decimal Balance);
public record LedgerEntry(string Type, string AccountId, decimal Amount, string At);

public sealed class TransferMetrics(IMeterFactory meterFactory)
{
    public const string MeterName = "fintech.transfer";
    public const string ValidationError = "validation_error";
    public const string Duplicate = "duplicate";
    public const string AccountNotFound = "account_not_found";
    public const string InsufficientFunds = "insufficient_funds";
    public const string Success = "success";

    private readonly Counter<long> _transferCounter = meterFactory.Create(MeterName)
        .CreateCounter<long>(name: "transfer_requests_total",
            description: "Total de requisições de transferência por resultado");

    public void TransferInc(string result) => _transferCounter.Add(1, new KeyValuePair<string, object?>("result", result));
}

public static class NpgsqlExtensions
{
    extension (NpgsqlConnection connection)
    {
        private NpgsqlCommand PrepareCommand(
            string sql, NpgsqlTransaction? transaction = null,
            params (string Name, object? Value)[] parameters)
        {
            var cmd = new NpgsqlCommand(sql, connection, transaction);
            foreach (var (name, value) in parameters)
                cmd.Parameters.AddWithValue(name, value ?? DBNull.Value);
            return cmd;
        }

        public async Task<object?> ExecuteScalar(
            string sql, NpgsqlTransaction? transaction = null,
            params (string Name, object? Value)[] parameters)
        {
            await using var command = connection.PrepareCommand(sql, transaction, parameters);
            return await command.ExecuteScalarAsync();
        }

        public async Task<int> Execute(
            string sql, NpgsqlTransaction? transaction = null,
            params (string Name, object? Value)[] parameters)
        {
            await using var command = connection.PrepareCommand(sql, transaction, parameters);
            return await command.ExecuteNonQueryAsync();
        }

        public async Task<List<T>> ExecuteReader<T>(
            string sql, Func<NpgsqlDataReader, T> map,
            NpgsqlTransaction? transaction = null,
            params (string Name, object? Value)[] parameters)
        {
            await using var command = connection.PrepareCommand(sql, transaction, parameters);
            await using var reader = await command.ExecuteReaderAsync();
            var results = new List<T>();
            while (await reader.ReadAsync())
                results.Add(map(reader));
            return results;
        }
    }

    extension(NpgsqlDataReader reader)
    {
        public T Get<T>(string name) => reader.GetFieldValue<T>(reader.GetOrdinal(name));
    }
}