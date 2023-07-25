<?php

namespace inisire\mqtt\NetBus;

use BinSoul\Net\Mqtt as MQTT;
use inisire\fibers\Network\SocketFactory;
use inisire\fibers\Promise;
use inisire\mqtt\Connection;
use inisire\NetBus\Query\Query;
use inisire\NetBus\Query\QueryBusInterface;
use inisire\NetBus\Query\QueryHandlerInterface;
use inisire\NetBus\Query\QueryInterface;
use inisire\NetBus\Query\Result;
use inisire\NetBus\Query\ResultInterface;
use inisire\NetBus\Query\Route;
use Psr\Log\LoggerInterface;


class QueryBus implements QueryBusInterface
{
    private string $busId;

    private Connection $connection;

    /**
     * @var array<string,Promise>
     */
    private array $waiting = [];

    /**
     * @var array<QueryHandlerInterface>
     */
    private array $handlers = [];

    public function __construct(
        private readonly LoggerInterface $logger,
        SocketFactory   $socketFactory,
    )
    {
        $this->busId = uniqid();
        $this->connection = new Connection($logger, $socketFactory);
    }

    public function connect(string $host): bool
    {
        if (!$this->connection->connect($host)) {
            $this->logger->error('Connection error');
            return false;
        }

        if (!$this->connection->subscribe(new MQTT\DefaultSubscription(sprintf('query_bus/%s', $this->busId)))) {
            $this->logger->error('Result subscription error');
            return false;
        }

        $this->connection->onMessage([$this, 'handleMessage']);

        return true;
    }

    public function registerHandler(string $busId, QueryHandlerInterface $handler): void
    {
        $topic = sprintf('query_bus/%s', $busId);
        $this->connection->subscribe(new MQTT\DefaultSubscription($topic));

        foreach ($handler->getSubscribedQueries() as $name => $handler) {
            $this->handlers[$name] = $handler;
        }
    }

    public function on(Route $route, callable $handler): void
    {
        $topic = sprintf('query_bus/%s/%s', $route->getBusId(), $route->getQueryName());
        $this->connection->subscribe(new MQTT\DefaultSubscription($topic));
        $this->handlers[$route->getQueryName()] = $handler;
    }

    public function execute(string $destinationId, string $name, array $data = []): ResultInterface
    {
        $queryId = uniqid();

        $topic = sprintf('query_bus/%s', $destinationId);
        $payload = json_encode([
            'src' => $this->busId,
            'x' => 'query',
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

    private function handleQuery(QueryInterface $query): ResultInterface
    {
        $handler = $this->handlers[$query->getName()] ?? null;

        if (!$handler) {
            return new Result(-1, ['error' => 'Bad query']);
        }

        return call_user_func($handler, $query);
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
                $sourceId = $payload['src'] ?? null;
                $id = $payload['id'] ?? null;
                $name = $payload['name'] ?? null;
                $data = $payload['data'] ?? [];

                $result = $this->handleQuery(new Query($name, $data, $id));

                $this->connection->publish(new MQTT\DefaultMessage(
                    sprintf('query_bus/%s', $sourceId),
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
}