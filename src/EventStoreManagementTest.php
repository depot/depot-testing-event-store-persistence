<?php

namespace Depot\Testing\EventStore\Persistence;

use Depot\Contract\Contract;
use Depot\Contract\ContractResolver;
use Depot\Contract\SimplePhpFqcnContractResolver;
use Depot\EventStore\CommittedEventVisitor;
use Depot\EventStore\EventEnvelope;
use Depot\EventStore\Management\Criteria;
use Depot\EventStore\Management\EventStoreManagement;
use Depot\EventStore\Persistence\CommittedEvent;
use Depot\EventStore\Persistence\Persistence;
use Depot\EventStore\Transaction\CommitId;
use PHPUnit_Framework_TestCase as TestCase;

abstract class EventStoreManagementTest extends TestCase
{
    /**
     * @var Persistence|EventStoreManagement
     */
    protected $persistence;

    /**
     * @var \DateTimeImmutable
     */
    protected $now;

    /**
     * @var RecordingCommittedEventVisitor
     */
    protected $recordingCommittedEventVisitor;

    /**
     * @var ContractResolver
     */
    private $contractResolver;

    public function setUp()
    {
        parent::setUp();
        $this->now = new \DateTimeImmutable('now');
        $this->persistence = $this->createPersistence();
        $this->createAndInsertCommittedEventFixtures();
    }

    protected function visitCommittedEvents(Criteria $criteria = null)
    {
        $eventVisitor = new RecordingCommittedEventVisitor();
        $this->persistence->visitCommittedEvents($criteria, $eventVisitor);

        return $eventVisitor->getVisitedEvents();
    }

    private function getContractResolver()
    {
        if (!is_null($this->contractResolver)) {
            return $this->contractResolver;
        }

        return $this->contractResolver = new SimplePhpFqcnContractResolver();
    }

    /**
     * @return Persistence
     */
    abstract protected function createPersistence();

    /**
     * @return Persistence
     */
    abstract protected function getPersistence();

    public function testItVisitsAllCommittedEvents()
    {
        $visitedEvents = $this->visitCommittedEvents(Criteria::create());

        $this->assertVisitedEventsAreEquals($this->getCommittedEventFixtures(), $visitedEvents);
    }

    public function testItVisitsAggregateRootIds()
    {
        $visitedEvents = $this->visitCommittedEvents(Criteria::create()->withAggregateRootIds([
            $this->getUuid(1),
            $this->getUuid(3),
        ]));

        $this->assertVisitedEventsAreEquals([
            $this->createCommittedEvent(1, 'a', 1, 0, new Start()),
            $this->createCommittedEvent(2, 'a', 1, 1, new Middle('a')),
            $this->createCommittedEvent(2, 'a', 1, 2, new Middle('b')),
            $this->createCommittedEvent(8, 'a', 1, 3, new Middle('c')),
            $this->createCommittedEvent(9, 'a', 3, 0, new Start()),
            $this->createCommittedEvent(9, 'a', 3, 1, new Middle('a')),
            $this->createCommittedEvent(9, 'a', 3, 2, new Middle('b')),
            $this->createCommittedEvent(10, 'a', 3, 3, new Middle('c')),
            $this->createCommittedEvent(11, 'a', 1, 4, new Middle('d')),
            $this->createCommittedEvent(14, 'a', 3, 4, new Middle('d')),
            $this->createCommittedEvent(15, 'a', 1, 5, new End()),
            $this->createCommittedEvent(16, 'a', 3, 5, new End()),
        ], $visitedEvents);
    }

    public function testItVisitsEventTypes()
    {
        $visitedEvents = $this->visitCommittedEvents(Criteria::create()
            ->withEventTypes([
                $this->contractResolver->resolveFromClassName(Start::class),
                $this->contractResolver->resolveFromClassName(End::class),
            ])
        );

        $this->assertVisitedEventsAreEquals([
            $this->createCommittedEvent(1, 'a', 1, 0, new Start()),
            $this->createCommittedEvent(3, 'a', 2, 0, new Start()),
            $this->createCommittedEvent(7, 'a', 2, 5, new End()),
            $this->createCommittedEvent(9, 'a', 3, 0, new Start()),
            $this->createCommittedEvent(12, 'b', 4, 0, new Start()),
            $this->createCommittedEvent(13, 'b', 4, 5, new End()),
            $this->createCommittedEvent(15, 'a', 1, 5, new End()),
            $this->createCommittedEvent(16, 'a', 3, 5, new End()),
        ], $visitedEvents);
    }

    public function testItVisitsAggregateRootTypes()
    {
        $visitedEvents = $this->visitCommittedEvents(Criteria::create()
            ->withAggregateRootTypes([
                new Contract('test.A', 'test\A'),
            ])
        );

        $this->assertVisitedEventsAreEquals([
            $this->createCommittedEvent(1, 'a', 1, 0, new Start()),
            $this->createCommittedEvent(2, 'a', 1, 1, new Middle('a')),
            $this->createCommittedEvent(2, 'a', 1, 2, new Middle('b')),
            $this->createCommittedEvent(3, 'a', 2, 0, new Start()),
            $this->createCommittedEvent(4, 'a', 2, 1, new Middle('a')),
            $this->createCommittedEvent(5, 'a', 2, 2, new Middle('b')),
            $this->createCommittedEvent(5, 'a', 2, 3, new Middle('c')),
            $this->createCommittedEvent(6, 'a', 2, 4, new Middle('d')),
            $this->createCommittedEvent(7, 'a', 2, 5, new End()),
            $this->createCommittedEvent(8, 'a', 1, 3, new Middle('c')),
            $this->createCommittedEvent(9, 'a', 3, 0, new Start()),
            $this->createCommittedEvent(9, 'a', 3, 1, new Middle('a')),
            $this->createCommittedEvent(9, 'a', 3, 2, new Middle('b')),
            $this->createCommittedEvent(10, 'a', 3, 3, new Middle('c')),
            $this->createCommittedEvent(11, 'a', 1, 4, new Middle('d')),
            $this->createCommittedEvent(14, 'a', 3, 4, new Middle('d')),
            $this->createCommittedEvent(15, 'a', 1, 5, new End()),
            $this->createCommittedEvent(16, 'a', 3, 5, new End()),
        ], $visitedEvents);
    }

    public function testItVisitsAggregateRootTypesAndEventTypes()
    {
        $visitedEvents = $this->visitCommittedEvents(Criteria::create()
            ->withAggregateRootTypes([
                new Contract('test.A', 'test\A'),
            ])
            ->withEventTypes([
                $this->contractResolver->resolveFromClassName(Start::class),
            ])
        );

        $this->assertVisitedEventsAreEquals([
            $this->createCommittedEvent(1, 'a', 1, 0, new Start()),
            $this->createCommittedEvent(3, 'a', 2, 0, new Start()),
            $this->createCommittedEvent(9, 'a', 3, 0, new Start()),
        ], $visitedEvents);
    }

    protected function getCommittedEventFixtures()
    {
        return [
            $this->createCommittedEvent(1, 'a', 1, 0, new Start()),
            $this->createCommittedEvent(2, 'a', 1, 1, new Middle('a')),
            $this->createCommittedEvent(2, 'a', 1, 2, new Middle('b')),
            $this->createCommittedEvent(3, 'a', 2, 0, new Start()),
            $this->createCommittedEvent(4, 'a', 2, 1, new Middle('a')),
            $this->createCommittedEvent(5, 'a', 2, 2, new Middle('b')),
            $this->createCommittedEvent(5, 'a', 2, 3, new Middle('c')),
            $this->createCommittedEvent(6, 'a', 2, 4, new Middle('d')),
            $this->createCommittedEvent(7, 'a', 2, 5, new End()),
            $this->createCommittedEvent(8, 'a', 1, 3, new Middle('c')),
            $this->createCommittedEvent(9, 'a', 3, 0, new Start()),
            $this->createCommittedEvent(9, 'a', 3, 1, new Middle('a')),
            $this->createCommittedEvent(9, 'a', 3, 2, new Middle('b')),
            $this->createCommittedEvent(10, 'a', 3, 3, new Middle('c')),
            $this->createCommittedEvent(11, 'a', 1, 4, new Middle('d')),
            $this->createCommittedEvent(12, 'b', 4, 0, new Start()),
            $this->createCommittedEvent(13, 'b', 4, 1, new Middle('a')),
            $this->createCommittedEvent(13, 'b', 4, 2, new Middle('b')),
            $this->createCommittedEvent(13, 'b', 4, 3, new Middle('c')),
            $this->createCommittedEvent(13, 'b', 4, 4, new Middle('d')),
            $this->createCommittedEvent(13, 'b', 4, 5, new End()),
            $this->createCommittedEvent(14, 'a', 3, 4, new Middle('d')),
            $this->createCommittedEvent(15, 'a', 1, 5, new End()),
            $this->createCommittedEvent(16, 'a', 3, 5, new End()),
        ];
    }

    private function assertVisitedEventsAreEquals(array $expectedCommittedEvents, array $actualCommittedEvents)
    {
        $this->assertEquals(
            $this->groupEventsByAggregateTypeAndId($expectedCommittedEvents),
            $this->groupEventsByAggregateTypeAndId($actualCommittedEvents)
        );
    }

    private function groupEventsByAggregateTypeAndId(array $committedEvents)
    {
        /* @var CommittedEvent $committedEvent */
        $committedEventsByAggregateTypeAndId = [];
        foreach ($committedEvents as $committedEvent) {
            $type = (string) $committedEvent->getAggregateRootType();
            $id = (string) $committedEvent->getAggregateRootId();

            if (!array_key_exists($type, $committedEventsByAggregateTypeAndId)) {
                $committedEventsByAggregateTypeAndId[$type] = [];
            }

            if (!array_key_exists($id, $committedEventsByAggregateTypeAndId[$type])) {
                $committedEventsByAggregateTypeAndId[$type][$id] = [];
            }

            $committedEventsByAggregateTypeAndId[$type][$id][] = $committedEvent;
        }

        return $committedEventsByAggregateTypeAndId;
    }

    protected function getUuid($id)
    {
        return sprintf('%08d-%04d-4%03d-%04d-%012d', $id, $id, $id, $id, $id);
    }

    protected function getCommitId($commitId)
    {
        return sprintf('commit%03d', $commitId);
    }

    /**
     * @param $commitId
     * @param $aggregateRootType
     * @param $aggregateRootId
     * @param $eventVersion
     * @param $event
     *
     * @return CommittedEvent
     */
    protected function createCommittedEvent(
        $commitId,
        $aggregateRootType,
        $aggregateRootId,
        $eventVersion,
        $event
    ) {
        $contracts = [
            'a' => new Contract('test.A', 'test\A'),
            'b' => new Contract('test.B', 'test\B'),
        ];

        $eventType = $this->getContractResolver()->resolveFromObject($event);

        // We are going to reuse the UUID-ified version of our
        // aggregate root version for our event ID to make
        // us not have to enter yet another number on
        // the input for this method.
        $eventId = $this->getUuid($eventVersion);
        $aggregateRootVersion = $eventVersion - 1;

        return new CommittedEvent(
          CommitId::fromString($this->getCommitId($commitId)),
          $this->now,
          $contracts[$aggregateRootType],
          $this->getUuid($aggregateRootId),
          $aggregateRootVersion,
          new EventEnvelope(
              $eventType,
              $eventId,
              $event,
              $eventVersion,
              $this->now
          )
        );
    }

    private function createAndInsertCommittedEventFixtures()
    {
        /** @var CommittedEvent $committedEvent */
        foreach ($this->getCommittedEventFixtures() as $committedEvent) {
            $this->persistence->commit(
                $committedEvent->getCommitId(),
                $committedEvent->getAggregateRootType(),
                $committedEvent->getAggregateRootId(),
                $committedEvent->getAggregateRootVersion(),
                [$committedEvent->getEventEnvelope()],
                $committedEvent->getUtcCommittedTime()
            );
        }
    }
}

class RecordingCommittedEventVisitor implements CommittedEventVisitor
{
    /**
     * @var[]
     */
    private $visitedEvents = [];

    public function doWithCommittedEvent(CommittedEvent $committedEvent)
    {
        $this->visitedEvents[] = $committedEvent;
    }

    public function getVisitedEvents()
    {
        return $this->visitedEvents;
    }

    public function clearVisitedEvents()
    {
        $this->visitedEvents = [];
    }
}

class Event
{
    public static function deserialize(array $data)
    {
        return new static();
    }

    public function serialize()
    {
        return [];
    }
}

class Start extends Event
{
}

class Middle extends Event
{
    /**
     * @var string
     */
    public $position;

    public function __construct($position)
    {
        $this->position = $position;
    }

    public static function deserialize(array $data)
    {
        return new static($data['position']);
    }

    public function serialize()
    {
        return [
            'position' => $this->position,
        ];
    }
}

class End extends Event
{
}
