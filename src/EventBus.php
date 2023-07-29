<?php

namespace inisire\mqtt\NetBus;

use BinSoul\Net\Mqtt as MQTT;
use inisire\fibers\Network\SocketFactory;
use inisire\mqtt\Connection;
use inisire\NetBus\Event\EventBusInterface;
use inisire\NetBus\Event\EventInterface;
use inisire\NetBus\Event\EventSubscriber;
use inisire\NetBus\Event\RemoteEvent;
use inisire\NetBus\Event\RemoteEventInterface;
use inisire\NetBus\Event\SubscriptionInterface;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use function inisire\fibers\asleep;


class EventBus implements EventBusInterface, LoggerAwareInterface
{
    private ?Connection $connection = null;

    /**
     * @var array<SubscriptionInterface>
     */
    private array $subscribers = [];

    private LoggerInterface $logger;

    public function __construct()
    {
        $this->logger = new NullLogger();
    }

    public function connect(string $host): bool
    {
        $this->connection = new Connection($this->logger);

        $connected = $this->connection->connect($host);

        $this->connection->onMessage([$this, 'handleMessage']);
        $this->connection->onDisconnect(function () use ($host) {
            $this->logger->error('Disconnected');
            while (!$this->connection->connect($host)) {
                $this->logger->info('Can\'t connect. Trying to reconnect...');
                asleep(5);
            }
        });

        return $connected;
    }

    public function createSource(string $source): EventSource
    {
        if (!$this->connection) {
            throw new \RuntimeException('Not connected');
        }

        return new EventSource($source, $this);
    }

    public function dispatch(string $source, EventInterface $event): void
    {
        if (!$this->connection->isConnected()) {
            throw new \RuntimeException('Not connected');
        }

        $topic = sprintf('event_bus/%s/%s', $source, $event->getName());
        $payload = json_encode([
            'src' => $source,
            'x' => 'event',
            'name' => $event->getName(),
            'data' => $event->getData()
        ]);

        $this->connection->publish(new MQTT\DefaultMessage($topic, $payload));
    }

    public function subscribe(SubscriptionInterface $subscription): void
    {
        foreach ($subscription->getSubscribedSources()->getEntries() as $source) {
            $source = str_replace('*', '+', $source);
            foreach ($subscription->getSubscribedEvents()->getEntries() as $event) {
                $event = str_replace('*', '+', $event);
                $topic = sprintf('event_bus/%s/%s', $source, $event);
                $this->connection->subscribe(new MQTT\DefaultSubscription($topic));
            }
        }

        $this->subscribers[] = $subscription;
    }

    public function registerSubscriber(EventSubscriber $subscriber): void
    {
        foreach ($subscriber->getEventSubscriptions() as $subscription) {
            $this->subscribe($subscription);
        }
    }

    private function handleEvent(RemoteEventInterface $event): void
    {
        foreach ($this->subscribers as $subscription) {
            if ($subscription->getSubscribedSources()->match($event->getSource()) && $subscription->getSubscribedEvents()->match($event->getName())) {
                $subscription->handleEvent($event);
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

    public function setLogger(LoggerInterface $logger): void
    {
        $this->logger = $logger;
        $this->connection->setLogger($logger);
    }
}