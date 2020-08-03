<?php

namespace OneSite\BigQuery;

use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\Dataset;
use Google\Cloud\BigQuery\QueryResults;
use Google\Cloud\BigQuery\Table;

/**
 * Class BigQueryService
 * @package OneSite\BigQuery
 */
class BigQueryService
{

    /**
     * @var
     */
    private $connection;

    /**
     * BigQueryService constructor.
     */
    public function __construct()
    {
        $this->setConnection(new BigQueryClient([
            'projectId' => config('bigquery.project_id'),
            'keyFilePath' => storage_path(config('bigquery.key_file_path')),
        ]));
    }

    /**
     * @return mixed
     */
    public function getConnection()
    {
        return $this->connection;
    }

    /**
     * @param mixed $connection
     */
    public function setConnection($connection): void
    {
        $this->connection = $connection;
    }

    /**
     * @param $dataSet
     * @return mixed
     */
    public function createDataset($dataSet)
    {
        try {
            return $this->getConnection()->createDataset($dataSet);
        } catch (\Exception $exception) {
            return json_decode($exception->getMessage());
        }
    }

    /**
     * @param $dataSet
     * @return mixed
     */
    public function getDataset($dataSet)
    {
        try {
            return $this->getConnection()->dataset($dataSet);
        } catch (\Exception $exception) {
            return json_decode($exception->getMessage());
        }
    }

    /**
     * @param Dataset $dataSet
     * @param $table
     * @return \Google\Cloud\BigQuery\Table|mixed
     */
    public function getTable(Dataset $dataSet, $table)
    {
        try {
            return $dataSet->table($table);
        } catch (\Exception $exception) {
            return json_decode($exception->getMessage());
        }
    }

    /**
     * @param Table $table
     * @param $attributes
     * @return \Google\Cloud\BigQuery\InsertResponse|mixed
     */
    public function insertRow(Table $table, $attributes)
    {
        try {
            return $table->insertRow($attributes);
        } catch (\Exception $exception) {
            return json_decode($exception->getMessage());
        }
    }

    /**
     * @param Table $table
     * @param $data
     * @return \Google\Cloud\BigQuery\InsertResponse|mixed
     */
    public function insertRows(Table $table, $data)
    {
        try {
            return $table->insertRows($data);
        } catch (\Exception $exception) {
            return json_decode($exception->getMessage());
        }
    }

    /**
     * @param $query
     * @param array $params
     * @return QueryResults
     */
    public function query($query, $params = [])
    {
        try {
            $queryJobConfig = $this->getConnection()->query($query)->parameters($params);

            return $this->getConnection()->runQuery($queryJobConfig);
        } catch (\Exception $exception) {
            return json_decode($exception->getMessage());
        }
    }
}
