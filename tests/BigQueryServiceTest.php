<?php


namespace OneSite\NinePay\BigQuery\Tests;


use Google\Cloud\BigQuery\Dataset;
use Google\Cloud\BigQuery\QueryResults;
use Google\Cloud\BigQuery\Table;
use OneSite\BigQuery\BigQueryService;
use PHPUnit\Framework\TestCase;


class BigQueryServiceTest extends TestCase
{

    private $service;

    /**
     *
     */
    public function setUp(): void
    {
        parent::setUp();

        $this->service = new BigQueryService();
    }

    /**
     *
     */
    public function tearDown(): void
    {
        $this->service = null;

        parent::tearDown();
    }

    /**
     * PHPUnit test: vendor/bin/phpunit --filter testCreateDataset tests/BigQueryServiceTest.php
     */
    public function testCreateDataset()
    {
        $data = $this->service->createDataset(config('bigquery.dataset'));

        $this->assertInstanceOf(Dataset::class, $data);
    }

    /**
     * PHPUnit test: vendor/bin/phpunit --filter testGetDataset tests/BigQueryServiceTest.php
     */
    public function testGetDataset()
    {
        $data = $this->service->getDataset(config('bigquery.dataset'));

        $this->assertInstanceOf(Dataset::class, $data);
    }

    /**
     * PHPUnit test: vendor/bin/phpunit --filter testGetTable tests/BigQueryServiceTest.php
     */
    public function testGetTable()
    {
        $table = $this->service->getTable(config('bigquery.dataset'), config('bigquery.table'));

        $this->assertInstanceOf(Table::class, $table);
    }

    /**
     * PHPUnit test: vendor/bin/phpunit --filter testInsertRow tests/BigQueryServiceTest.php
     */
    public function testInsertRow()
    {
        $data = $this->service->insertRow(config('bigquery.dataset'), config('bigquery.table'), [
            'id' => 1,
            'request_id' => 'xxx',
            'session_id' => 'xxx',
        ]);

        echo "\n" . json_encode($data);

        $this->assertTrue(true);
    }

    /**
     * PHPUnit test: vendor/bin/phpunit --filter testCreateTableAndInsertRow tests/BigQueryServiceTest.php
     */
    public function testCreateTableAndInsertRow()
    {
        $this->service->insertRow(config('bigquery.dataset'), config('bigquery.table'), [
            'id' => 1,
            'request_id' => 'xxx',
            'session_id' => 'xxx',
        ], [
            'autoCreate' => true,
            'tableMetadata' => [
                'schema' => [
                    'fields' => [
                        [
                            'name' => 'id',
                            'type' => 'INTEGER',
                            'mode' => 'REQUIRED'
                        ],
                        [
                            'name' => 'request_id',
                            'type' => 'STRING'
                        ],
                        [
                            'name' => 'session_id',
                            'type' => 'STRING'
                        ],
                    ]
                ]
            ]
        ]);

        $this->assertTrue(true);
    }

    /**
     * PHPUnit test: vendor/bin/phpunit --filter testCreateEventsTable tests/BigQueryServiceTest.php
     */
    public function testCreateEventsTable()
    {
        $this->service->createTable(config('bigquery.dataset'), 'events', [
            [
                'name' => 'id',
                'type' => 'INTEGER',
                'mode' => 'REQUIRED'
            ],
            [
                'name' => 'type',
                'type' => 'STRING'
            ],
            [
                'name' => 'label',
                'type' => 'STRING'
            ],
            [
                'name' => 'request_id',
                'type' => 'STRING'
            ],
            [
                'name' => 'session_id',
                'type' => 'STRING'
            ],
            [
                'name' => 'user_id',
                'type' => 'INTEGER'
            ],
            [
                'name' => 'user_phone',
                'type' => 'STRING'
            ],
            [
                'name' => 'object_id',
                'type' => 'STRING'
            ],
            [
                'name' => 'object_type',
                'type' => 'STRING'
            ], [
                'name' => 'device_id',
                'type' => 'STRING'
            ], [
                'name' => 'device_name',
                'type' => 'STRING'
            ],
            [
                'name' => 'device_model',
                'type' => 'STRING'
            ],
            [
                'name' => 'browser_name',
                'type' => 'STRING'
            ],
            [
                'name' => 'browser_version',
                'type' => 'STRING'
            ],
            [
                'name' => 'ip',
                'type' => 'STRING'
            ],
            [
                'name' => 'platform',
                'type' => 'STRING'
            ],
            [
                'name' => 'platform_version',
                'type' => 'STRING'
            ],
            [
                'name' => 'network',
                'type' => 'STRING'
            ],
            [
                'name' => 'os',
                'type' => 'STRING'
            ],
            [
                'name' => 'user_agent',
                'type' => 'STRING'
            ],
            [
                'name' => 'country',
                'type' => 'STRING'
            ],
            [
                'name' => 'province',
                'type' => 'STRING'
            ],
            [
                'name' => 'district',
                'type' => 'STRING'
            ],
            [
                'name' => 'ward',
                'type' => 'STRING'
            ],
            [
                'name' => 'lat',
                'type' => 'FLOAT'
            ],
            [
                'name' => 'lng',
                'type' => 'FLOAT'
            ],
            [
                'name' => 'send_by',
                'type' => 'STRING'
            ],
            [
                'name' => 'feature',
                'type' => 'STRING'
            ],
            [
                'name' => 'screen_name',
                'type' => 'STRING'
            ],
            [
                'name' => 'screen_element',
                'type' => 'STRING'
            ],
            [
                'name' => 'time',
                'type' => 'TIMESTAMP'
            ],
            [
                'name' => 'created_at',
                'type' => 'TIMESTAMP'
            ],
            [
                'name' => 'updated_at',
                'type' => 'TIMESTAMP'
            ],
        ]);

        $this->assertTrue(true);
    }

    /**
     * PHPUnit test: vendor/bin/phpunit --filter testCreateUsersTable tests/BigQueryServiceTest.php
     */
    public function testCreateUsersTable()
    {
        $this->service->createTable(config('bigquery.dataset'), 'users', [
            [
                'name' => 'user_id',
                'type' => 'INTEGER',
                'mode' => 'REQUIRED'
            ],
            [
                'name' => 'request_id',
                'type' => 'STRING'
            ],
            [
                'name' => 'phone',
                'type' => 'STRING'
            ],
            [
                'name' => 'name',
                'type' => 'STRING'
            ],
            [
                'name' => 'email',
                'type' => 'STRING'
            ],
            [
                'name' => 'status',
                'type' => 'STRING'
            ],
            [
                'name' => 'address',
                'type' => 'STRING'
            ],
            [
                'name' => 'avatar_url',
                'type' => 'STRING'
            ],
            [
                'name' => 'birthday',
                'type' => 'DATE'
            ],
            [
                'name' => 'gender',
                'type' => 'STRING'
            ],
            [
                'name' => 'card_no',
                'type' => 'STRING'
            ],
            [
                'name' => 'card_issue_at',
                'type' => 'DATE'
            ],
            [
                'name' => 'card_issue_place',
                'type' => 'STRING'
            ],
            [
                'name' => 'card_expired_at',
                'type' => 'DATE'
            ],
            [
                'name' => 'card_image_front_url',
                'type' => 'STRING'
            ],
            [
                'name' => 'card_image_back_url',
                'type' => 'STRING'
            ],
            [
                'name' => 'is_verify_email',
                'type' => 'BOOL'
            ],
            [
                'name' => 'is_verify_phone',
                'type' => 'BOOL'
            ],
            [
                'name' => 'is_verify_info',
                'type' => 'BOOL'
            ],
            [
                'name' => 'dialing_code',
                'type' => 'INTEGER'
            ],
            [
                'name' => 'is_vip',
                'type' => 'BOOL'
            ],
            [
                'name' => 'country',
                'type' => 'STRING'
            ],
            [
                'name' => 'province',
                'type' => 'STRING'
            ],
            [
                'name' => 'district',
                'type' => 'STRING'
            ],
            [
                'name' => 'ward',
                'type' => 'STRING'
            ],
            [
                'name' => 'description',
                'type' => 'STRING'
            ],
            [
                'name' => 'created_at',
                'type' => 'TIMESTAMP'
            ],
            [
                'name' => 'updated_at',
                'type' => 'TIMESTAMP'
            ],
        ]);

        $this->assertTrue(true);
    }

    /**
     * PHPUnit test: vendor/bin/phpunit --filter testQuery tests/BigQueryServiceTest.php
     */
    public function testQuery()
    {
        $queryResults = $this->service->query("select * from test.events where id = @id", [
            'id' => 1
        ]);

        /*$queryResults = $this->service->query("update test.events set type = 'test' where id = @id", [
            'id' => 1
        ]);*/

        echo "\n" . json_encode($queryResults);

        $this->assertTrue(true);
    }

}
