<?php

namespace inisire\mqtt\NetBus;

use inisire\NetBus\Event\EventBusInterface;
use inisire\NetBus\Event\EventInterface;
use inisire\NetBus\Event\EventSourceInterface;


class EventSource implements EventSourceInterface
{
    public function __construct(
        private readonly string            $sourceId,
        private readonly EventBusInterface $eventBus
    )
    {
    }

    public function dispatch(EventInterface $event): void
    {
        $this->eventBus->dispatch($this->sourceId, $event);
    }
}