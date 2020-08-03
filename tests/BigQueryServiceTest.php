<?php


namespace OneSite\NinePay\BigQuery\Tests;


use Google\Cloud\BigQuery\Dataset;
use Google\Cloud\BigQuery\QueryResults;
use Google\Cloud\BigQuery\Table;
use GuzzleHttp\Psr7\Response;
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
        $dataset = $this->service->getDataset(config('bigquery.dataset'));

        $table = $this->service->getTable($dataset, config('bigquery.table'));

        $this->assertInstanceOf(Table::class, $table);
    }

    /**
     * PHPUnit test: vendor/bin/phpunit --filter testInsertRow tests/BigQueryServiceTest.php
     */
    public function testInsertRow()
    {
        $dataset = $this->service->getDataset(config('bigquery.dataset'));

        $table = $this->service->getTable($dataset, config('bigquery.table'));

        $table->insertRow([
            'id' => 1,
            'request_id' => 'xxx',
            'session_id' => 'xxx',
        ]);

        $this->assertTrue(true);
    }

    /**
     * PHPUnit test: vendor/bin/phpunit --filter testQuery tests/BigQueryServiceTest.php
     */
    public function testQuery()
    {
        /**
         * @var QueryResults $data
         */
        $queryResults = $this->service->query("select * from test.events");

        if ($queryResults->isComplete()) {
            $rows = $queryResults->rows();
            foreach ($rows as $row) {
                printf("\nRow: %s, %s" . PHP_EOL, $row['id'], $row['request_id']);
            }

            $this->assertTrue(true);

            return;
        }

        $this->assertTrue(false);
    }

}
