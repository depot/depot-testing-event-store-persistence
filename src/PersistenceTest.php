<?php

namespace Depot\Testing\EventStore\Persistence;

use Depot\Contract\ContractResolver;
use Depot\Contract\SimplePhpFqcnContractResolver;
use Depot\EventStore\EventEnvelope;
use Depot\EventStore\Persistence\Persistence;
use Depot\EventStore\Transaction\CommitId;
use Depot\Testing\Fixtures\Banking\Account\Account;
use Depot\Testing\Fixtures\Banking\Account\AccountBalanceDecreased;
use Depot\Testing\Fixtures\Banking\Account\AccountBalanceIncreased;
use Depot\Testing\Fixtures\Banking\Account\AccountWasOpened;
use PHPUnit_Framework_TestCase as TestCase;

abstract class PersistenceTest extends TestCase
{
    /**
     * @var ContractResolver
     */
    private $contractResolver;

    public function setUp()
    {
        parent::setUp();

        $this->loadFixtures($this->createPersistence());
    }

    private function getContractResolver()
    {
        if (!is_null($this->contractResolver)) {
            return $this->contractResolver;
        }

        return $this->contractResolver = new SimplePhpFqcnContractResolver();
    }

    /**
     * Create a persistence instance.
     *
     * This method should cause the implementation to create a new
     * persistence instance. If there is cached involved, this
     * method should reset and/or prime the cache.
     *
     * @return Persistence
     */
    abstract protected function createPersistence();

    /**
     * Get a persistence instance.
     *
     * @return Persistence
     */
    abstract protected function getPersistence();

    protected function createEventEnvelope($eventId, $event, $version)
    {
        return new EventEnvelope(
            $this->getContractResolver()->resolveFromObject($event),
            $eventId,
            $event,
            $version,
            new \DateTimeImmutable('2016-01-01 14:55:00')
        );
    }

    protected function createCommit(
        Persistence $persistence,
        $commitId,
        $aggregateRootClassName,
        $aggregateRootId,
        $expectedAggregateRootVersion,
        array $eventEnvelopes
    ) {
        $persistence->commit(
            CommitId::fromString($commitId),
            $this->getContractResolver()->resolveFromClassName($aggregateRootClassName),
            $aggregateRootId,
            $expectedAggregateRootVersion,
            $eventEnvelopes
        );
    }

    protected function loadFixtures()
    {
        $this->doLoadFixtures($this->createPersistence());
    }

    protected function doLoadFixtures(Persistence $persistence)
    {
        $aggregateRootClassName = Account::class;
        $aggregateRootId = 'fixture-account-000';
        $secondAggregateRootId = 'fixture-account-001';

        $this->createCommit(
            $persistence,
            '4A9F269C-27D5-46C2-9FDF-F7A7D61C55D4',
            $aggregateRootClassName,
            $aggregateRootId,
            -1,
            [
                $this->createEventEnvelope(
                    123,
                    new AccountWasOpened('fixture-account-000', 25),
                    0
                ),
            ]
        );

        $this->createCommit(
            $persistence,
            '75BCD437-F184-4305-AB61-784761536783',
            $aggregateRootClassName,
            $secondAggregateRootId,
            -1,
            [
                $this->createEventEnvelope(
                    124,
                    new AccountWasOpened('fixture-account-001', 10),
                    0
                ),
                $this->createEventEnvelope(
                    125,
                    new AccountBalanceIncreased('fixture-account-001', 15),
                    1
                ),
                $this->createEventEnvelope(
                    126,
                    new AccountBalanceDecreased('fixture-account-001', 5),
                    2
                ),
                $this->createEventEnvelope(
                    127,
                    new AccountBalanceIncreased('fixture-account-001', 45),
                    3
                ),
            ]
        );

        $this->createCommit(
            $persistence,
            '1264416A-7465-4241-A810-B5EFBD1988E2',
            $aggregateRootClassName,
            $aggregateRootId,
            0,
            [
                $this->createEventEnvelope(
                    128,
                    new AccountBalanceIncreased('fixture-account-000', 30),
                    1
                ),
            ]
        );

        $this->createCommit(
            $persistence,
            'D68A5BFD-6A61-44A7-BF10-ECEFE776A141',
            $aggregateRootClassName,
            $secondAggregateRootId,
            3,
            [
                $this->createEventEnvelope(
                    129,
                    new AccountBalanceDecreased('fixture-account-001', 75),
                    4
                ),
                $this->createEventEnvelope(
                    130,
                    new AccountBalanceIncreased('fixture-account-001', 90),
                    5
                ),
            ]
        );

        $this->createCommit(
            $persistence,
            'A8DA72AB-1405-463A-AF16-BF170A5D304E',
            $aggregateRootClassName,
            $secondAggregateRootId,
            5,
            [
                $this->createEventEnvelope(
                    131,
                    new AccountBalanceIncreased('fixture-account-001', 125),
                    6
                ),
                $this->createEventEnvelope(
                    132,
                    new AccountBalanceDecreased('fixture-account-001', 15),
                    7
                ),
            ]
        );
    }

    /**
     * @param $aggregateRootClassName
     * @param $aggregateRootId
     * @param $expectedEventEnvelopes
     * @dataProvider provideFetchData
     */
    public function testFetch($aggregateRootClassName, $aggregateRootId, $expectedEventEnvelopes)
    {
        $this->loadFixtures();

        $actualEventEnvelopes = $this->getPersistence()->fetch(
            $this->getContractResolver()->resolveFromClassName($aggregateRootClassName),
            $aggregateRootId
        );

        $this->assertEquals($expectedEventEnvelopes, $actualEventEnvelopes);
    }

    public function provideFetchData()
    {
        $aggregateRootClassName = Account::class;
        $aggregateRootType = $this->getContractResolver()->resolveFromClassName($aggregateRootClassName);
        $aggregateRootId = 'fixture-account-000';
        $secondAggregateRootId = 'fixture-account-001';

        return [
            [
                $aggregateRootClassName,
                $aggregateRootId,
                [
                    $this->createEventEnvelope(
                        123,
                        new AccountWasOpened('fixture-account-000', 25),
                        0
                    ),
                    $this->createEventEnvelope(
                        128,
                        new AccountBalanceIncreased('fixture-account-000', 30),
                        1
                    ),
                ],
            ],
            [
                $aggregateRootClassName,
                $secondAggregateRootId,
                [
                    $this->createEventEnvelope(
                        124,
                        new AccountWasOpened('fixture-account-001', 10),
                        0
                    ),
                    $this->createEventEnvelope(
                        125,
                        new AccountBalanceIncreased('fixture-account-001', 15),
                        1
                    ),
                    $this->createEventEnvelope(
                        126,
                        new AccountBalanceDecreased('fixture-account-001', 5),
                        2
                    ),
                    $this->createEventEnvelope(
                        127,
                        new AccountBalanceIncreased('fixture-account-001', 45),
                        3
                    ),
                    $this->createEventEnvelope(
                        129,
                        new AccountBalanceDecreased('fixture-account-001', 75),
                        4
                    ),
                    $this->createEventEnvelope(
                        130,
                        new AccountBalanceIncreased('fixture-account-001', 90),
                        5
                    ),
                    $this->createEventEnvelope(
                        131,
                        new AccountBalanceIncreased('fixture-account-001', 125),
                        6
                    ),
                    $this->createEventEnvelope(
                        132,
                        new AccountBalanceDecreased('fixture-account-001', 15),
                        7
                    ),
                ],
            ],
        ];
    }
}
