<?php

declare(strict_types=1);

$method = $_SERVER['REQUEST_METHOD'];
$uri    = parse_url($_SERVER['REQUEST_URI'], PHP_URL_PATH);

$metrics = [
    'transfer' => [
        'success' => 0,
        'validation_error' => 0,
        'account_not_found' => 0,
        'duplicate' => 0,
        'insufficient_funds' => 0,
    ],
];

$pdo = getPdo();
seed($pdo);

if ($uri === '/transfer' && $method === 'POST') {
    handleTransfer($pdo, $metrics);
} elseif ($uri === '/debug/state' && $method === 'GET') {
    handleDebugState($pdo);
} elseif ($uri === '/metrics' && $method === 'GET') {
    handleMetrics($pdo, $metrics);
} else {
    http_response_code(404);
    header('Content-Type: application/json');
    echo json_encode(['error' => 'Not found']);
}

function getPdo(): PDO
{
    $host = getenv('DB_HOST') ?: 'postgres';
    $port = getenv('DB_PORT') ?: '5432';
    $db   = getenv('DB_NAME') ?: 'fintech';
    $user = getenv('DB_USER') ?: 'fintech';
    $pass = getenv('DB_PASSWORD') ?: 'fintech';

    $dsn = "pgsql:host={$host};port={$port};dbname={$db}";
    $options = [
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
    ];

    return new PDO($dsn, $user, $pass, $options);
}

function seed(PDO $pdo): void
{
    $stmt = $pdo->prepare('INSERT INTO accounts (id, balance) VALUES (:a, :ab), (:b, :bb) ON CONFLICT (id) DO NOTHING');
    $stmt->execute([
        ':a' => 'A', ':ab' => 1000.0,
        ':b' => 'B', ':bb' => 500.0,
    ]);
}

function handleTransfer(PDO $pdo, array &$metrics): void
{
    $body = file_get_contents('php://input');
    $data = json_decode($body, true);

    if (!is_array($data)) {
        incrementTransferMetric($metrics, 'validation_error');
        respondJson(400, ['status' => 'error', 'message' => 'invalid json body']);
        return;
    }

    try {
        validateTransferPayload($data);
    } catch (InvalidArgumentException $e) {
        incrementTransferMetric($metrics, 'validation_error');
        respondJson(400, ['status' => 'error', 'message' => $e->getMessage()]);
        return;
    }

    $fromAccountId = $data['fromAccountId'];
    $toAccountId   = $data['toAccountId'];
    $amount        = (float) $data['amount'];
    $operationId   = $data['operationId'] ?? null;

    try {
        $pdo->beginTransaction();

        if ($operationId !== null) {
            $dupStmt = $pdo->prepare('SELECT 1 FROM processed_ops WHERE operation_id = :op');
            $dupStmt->execute([':op' => $operationId]);
            if ($dupStmt->fetch()) {
                incrementTransferMetric($metrics, 'duplicate');
                $pdo->rollBack();
                respondJson(200, ['status' => 'ok', 'message' => 'operation already processed']);
                return;
            }
        }

        $from = fetchAccountForUpdate($pdo, $fromAccountId);
        $to   = fetchAccountForUpdate($pdo, $toAccountId);

        if ($from === null || $to === null) {
            incrementTransferMetric($metrics, 'account_not_found');
            $pdo->rollBack();
            respondJson(400, ['status' => 'error', 'message' => 'account not found']);
            return;
        }

        if ($from['balance'] < $amount) {
            incrementTransferMetric($metrics, 'insufficient_funds');
            $pdo->rollBack();
            respondJson(400, ['status' => 'error', 'message' => 'insufficient funds']);
            return;
        }

        $newFrom = $from['balance'] - $amount;
        $newTo   = $to['balance'] + $amount;

        $update = $pdo->prepare('UPDATE accounts SET balance = :bal WHERE id = :id');
        $update->execute([':bal' => $newFrom, ':id' => $fromAccountId]);
        $update->execute([':bal' => $newTo, ':id' => $toAccountId]);

        $now = (new DateTimeImmutable('now', new DateTimeZone('UTC')))->format(DateTimeInterface::ATOM);
        $ledger = $pdo->prepare('INSERT INTO ledger (type, account_id, amount, at) VALUES (:type, :acc, :amt, :at)');
        $ledger->execute([':type' => 'DEBIT', ':acc' => $fromAccountId, ':amt' => $amount, ':at' => $now]);
        $ledger->execute([':type' => 'CREDIT', ':acc' => $toAccountId, ':amt' => $amount, ':at' => $now]);

        if ($operationId !== null) {
            $pdo->prepare('INSERT INTO processed_ops (operation_id) VALUES (:op)')->execute([':op' => $operationId]);
        }

        $pdo->commit();
        incrementTransferMetric($metrics, 'success');

        respondJson(200, [
            'status'  => 'ok',
            'message' => 'transfer completed',
            'balances' => [
                $fromAccountId => $newFrom,
                $toAccountId   => $newTo,
            ],
        ]);
    } catch (Throwable $e) {
        if ($pdo->inTransaction()) {
            $pdo->rollBack();
        }
        error_log('transfer error: ' . $e->getMessage());
        respondJson(500, ['status' => 'error', 'message' => 'internal error']);
    }
}

function handleDebugState(PDO $pdo): void
{
    $accounts = [];
    foreach ($pdo->query('SELECT id, balance FROM accounts ORDER BY id') as $row) {
        $accounts[$row['id']] = ['id' => $row['id'], 'balance' => (float) $row['balance']];
    }

    $ledger = [];
    foreach ($pdo->query('SELECT type, account_id, amount, at FROM ledger ORDER BY id DESC LIMIT 100') as $row) {
        $ledger[] = [
            'type' => $row['type'],
            'accountId' => $row['account_id'],
            'amount' => (float) $row['amount'],
            'at' => $row['at'],
        ];
    }

    $processed = [];
    foreach ($pdo->query('SELECT operation_id FROM processed_ops ORDER BY created_at DESC LIMIT 100') as $row) {
        $processed[] = $row['operation_id'];
    }

    respondJson(200, [
        'accounts'     => $accounts,
        'ledger'       => $ledger,
        'processedOps' => $processed,
    ]);
}

function fetchAccountForUpdate(PDO $pdo, string $id): ?array
{
    $stmt = $pdo->prepare('SELECT balance FROM accounts WHERE id = :id FOR UPDATE');
    $stmt->execute([':id' => $id]);
    $row = $stmt->fetch();
    if ($row === false) {
        return null;
    }
    return ['id' => $id, 'balance' => (float) $row['balance']];
}

function validateTransferPayload(array $data): void
{
    if (empty($data['fromAccountId']) || empty($data['toAccountId'])) {
        throw new InvalidArgumentException('fromAccountId e toAccountId são obrigatórios');
    }
    if ($data['fromAccountId'] === $data['toAccountId']) {
        throw new InvalidArgumentException('fromAccountId e toAccountId não podem ser iguais');
    }
    if (!isset($data['amount']) || !is_numeric($data['amount'])) {
        throw new InvalidArgumentException('amount deve ser numérico');
    }
    if ((float) $data['amount'] <= 0) {
        throw new InvalidArgumentException('amount deve ser > 0');
    }
}

function respondJson(int $statusCode, array $payload): void
{
    http_response_code($statusCode);
    header('Content-Type: application/json');
    echo json_encode($payload);
}

function incrementTransferMetric(array &$metrics, string $result): void
{
    if (!isset($metrics['transfer'][$result])) {
        $metrics['transfer'][$result] = 0;
    }
    $metrics['transfer'][$result]++;
}

function handleMetrics(PDO $pdo, array $metrics): void
{
    header('Content-Type', 'text/plain');
    echo "# TYPE transfer_requests_total counter\n";
    foreach ($metrics['transfer'] as $result => $value) {
        $safeResult = preg_replace('/[^a-zA-Z0-9_]/', '_', $result);
        echo "transfer_requests_total{result=\"{$safeResult}\"} {$value}\n";
    }
    echo "\n# TYPE account_balance gauge\n";
    foreach ($pdo->query('SELECT id, balance FROM accounts') as $row) {
        $safeAccount = preg_replace('/[^a-zA-Z0-9_]/', '_', $row['id']);
        echo "account_balance{account=\"{$safeAccount}\"} {$row['balance']}\n";
    }
}
