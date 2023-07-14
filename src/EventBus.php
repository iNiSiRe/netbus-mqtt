<?php

namespace inisire\mqtt\NetBus;

use BinSoul\Net\Mqtt as MQTT;
use inisire\fibers\Network\SocketFactory;
use inisire\mqtt\Connection;
use inisire\mqtt\Contract\MessageHandler;
use inisire\NetBus\Event\Event;
use inisire\NetBus\Event\EventBusInterface;
use inisire\NetBus\Event\EventInterface;
use inisire\NetBus\Event\EventSubscriber;
use inisire\NetBus\Event\RemoteEvent;
use Psr\Log\LoggerInterface;


class EventBus implements EventBusInterface, MessageHandler
{
    private ?Connection $connection = null;

    /**
     * @var array<EventSubscriber>
     */
    private array $subscribers = [];

    public function __construct(
        private readonly LoggerInterface $logger,
        private readonly SocketFactory   $socketFactory
    )
    {
    }

    public function connect(string $host): bool
    {
        $this->connection = new Connection($this->logger, $this->socketFactory);

        $connected = $this->connection->connect($host);
        $this->connection->registerMessageHandler($this);

        return $connected;
    }

    public function createSource(string $sourceId): EventSource
    {
        if (!$this->connection) {
            throw new \RuntimeException('Not connected');
        }

        return new EventSource($sourceId, $this->connection);
    }

    public function dispatch(string $sourceId, EventInterface $event): void
    {
        if (!$this->connection->isConnected()) {
            throw new \RuntimeException('Not connected');
        }

        $topic = sprintf('event_bus/%s/%s', $sourceId, $event->getName());
        $payload = json_encode([
            'src' => $sourceId,
            'x' => 'event',
            'name' => $event->getName(),
            'data' => $event->getData()
        ]);

        $this->connection->publish(new MQTT\DefaultMessage($topic, $payload));
    }

    public function subscribe(string $sourceId, EventSubscriber $subscriber): void
    {
        foreach ($subscriber->getSubscribedEvents() as $event => $handler) {
            $topic = sprintf('event_bus/%s/%s', $sourceId, $event);
            $this->connection->subscribe(new MQTT\DefaultSubscription($topic));
        }

        $this->subscribers[] = $subscriber;
    }

    private function handleEvent(EventInterface $event): void
    {
        foreach ($this->subscribers as $subscriber) {
            foreach ($subscriber->getSubscribedEvents() as $name => $handler) {
                if ($event->getName() === $name) {
                    call_user_func($handler, $event);
                }
            }
        }
    }

    public function handleMessage(MQTT\Message $message): void
    {
        $payload = json_decode($message->getPayload(), true);

        $x = $payload['x'] ?? null;

        switch ($x) {
            case 'event':
            {
                $source = $payload['src'] ?? null;
                $name = $payload['name'] ?? null;
                $data = $payload['data'] ?? [];
                $this->handleEvent(new RemoteEvent($source, $name, $data));
                break;
            }

            default:
            {
                $this->logger->error(sprintf('Bad message "%s"', $x));
            }
        }
    }
}