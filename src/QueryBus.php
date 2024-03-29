<?php

namespace inisire\mqtt\NetBus;

use BinSoul\Net\Mqtt as MQTT;
use inisire\fibers\Network\Exception\ConnectionException;
use inisire\fibers\Promise;
use inisire\mqtt\Connection;
use inisire\NetBus\Query\Query;
use inisire\NetBus\Query\QueryHandlerInterface;
use inisire\NetBus\Query\Result;
use inisire\NetBus\Query\ResultInterface;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use function inisire\fibers\asleep;
use function inisire\fibers\async;


class QueryBus implements LoggerAwareInterface
{
    private string $busId;

    private ?string $host = null;
    private ?Connection $connection = null;

    /**
     * @var array<string,Promise>
     */
    private array $waiting = [];

    /**
     * @var array<QueryHandlerInterface>
     */
    private array $handlers = [];

    private LoggerInterface $logger;

    public function __construct()
    {
        $this->logger = new NullLogger();
        $this->busId = uniqid();
        $this->connection = new Connection();
    }

    public function connect(string $host): bool
    {
        $this->connection = new Connection();
        $this->connection->setLogger($this->logger);

        $this->connection->onConnected([$this, 'onConnected']);
        $this->connection->onMessage([$this, 'handleMessage']);

        do {
            $this->logger->info('Trying to connect');
            $this->host = $host;
            $this->connection->connect($host);
        } while (!$this->connection->isConnected());

        if (!$this->connection->subscribe(new MQTT\DefaultSubscription(sprintf('query_bus/%s', $this->busId)))) {
            $this->logger->error('Result subscription error');
            $this->connection->close();
            return false;
        }

        $this->connection->onDisconnect([$this, 'onDisconnect']);

        return $this->connection->isConnected();
    }

    public function onConnected(): void
    {
        foreach ($this->handlers as $busId => $handler) {
            $this->sendSubscribe($busId);
        }
    }

    public function onDisconnect(): void
    {
        $this->logger->error('Disconnected');

        async(function () {
            do {
                asleep(5.0);
                $this->connect($this->host);
            } while (!$this->connection->isConnected());
        });
    }

    private function sendSubscribe(string $busId): void
    {
        $topic = sprintf('query_bus/%s', $busId);
        $this->connection->subscribe(new MQTT\DefaultSubscription($topic));
    }

    public function on(string $busId, callable $handler): void
    {
        $this->sendSubscribe($busId);
        $this->handlers[$busId] = $handler;
    }

    public function execute(string $destinationId, string $name, array $data = []): ResultInterface
    {
        $queryId = uniqid();

        $topic = sprintf('query_bus/%s', $destinationId);
        $payload = json_encode([
            'x' => 'query',
            'src' => $this->busId,
            'dst' => $destinationId,
            'id' => $queryId,
            'name' => $name,
            'data' => $data
        ]);

        $promise = new Promise();
        $this->waiting[$queryId] = $promise;

        $this->connection->publish(new MQTT\DefaultMessage($topic, $payload));

        return $promise->await(new Promise\Timeout(5, new Result(-1, ['error' => 'timeout'])));
    }

    private function handleResult(string $queryId, ResultInterface $result): void
    {
        $promise = $this->waiting[$queryId] ?? null;

        if (!$promise) {
            $this->logger->error('No query for received result');
            return;
        }

        $promise->resolve($result);
    }

    private function handleQuery(string $dstId, string $name, array $data): ResultInterface
    {
        $handler = $this->handlers[$dstId] ?? null;

        if (!$handler) {
            return new Result(-1, ['error' => 'Bad query']);
        }

        return call_user_func($handler, new Query($name, $data));
    }

    public function handleMessage(MQTT\Message $message): void
    {
        $payload = json_decode($message->getPayload(), true);

        $x = $payload['x'] ?? null;

        switch ($x) {
            case 'result': {
                $id = $payload['id'] ?? null;
                $code = $payload['code'] ?? null;
                $data = $payload['data'] ?? [];
                $this->handleResult($id, new Result($code, $data));
                break;
            }

            case 'query': {
                $srcId = $payload['src'] ?? null;
                $dstId = $payload['dst'] ?? null;
                $id = $payload['id'] ?? null;
                $name = $payload['name'] ?? null;
                $data = $payload['data'] ?? [];

                $result = $this->handleQuery($dstId, $name, $data);

                $this->connection->publish(new MQTT\DefaultMessage(
                    sprintf('query_bus/%s', $srcId),
                    json_encode([
                        'src' => $this->busId,
                        'x' => 'result',
                        'id' => $id,
                        'code' => $result->getCode(),
                        'data' => $result->getData()
                    ])
                ));
                break;
            }

            default: {
                $this->logger->error(sprintf('Bad message "%s"', $x));
            }
        }
    }

    public function setLogger(LoggerInterface $logger): void
    {
        $this->logger = $logger;
        $this->connection?->setLogger($logger);
    }
}