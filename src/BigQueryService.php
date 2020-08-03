<?php

namespace OneSite\BigQuery;

use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\QueryResults;

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
     * @param $dataset
     * @return mixed
     */
    public function createDataset($dataset)
    {
        try {
            return $this->getConnection()->createDataset($dataset);
        } catch (\Exception $exception) {
            return json_decode($exception->getMessage());
        }
    }

    /**
     * @param $dataset
     * @return mixed
     */
    public function getDataset($dataset)
    {
        try {
            return $this->getConnection()->dataset($dataset);
        } catch (\Exception $exception) {
            return json_decode($exception->getMessage());
        }
    }

    /**
     * @param $datasetName
     * @param $tableName
     * @param array $schema
     * @return mixed
     */
    public function createTable($datasetName, $tableName, $schema = [])
    {
        try {
            $dataset = $this->getConnection()->dataset($datasetName);

            $table = $dataset->table($tableName);

            if (!$table->exists()) {
                $dataset->createTable($table->id(), [
                    'schema' => [
                        'fields' => $schema
                    ]
                ]);
            }
        } catch (\Exception $exception) {
            return json_decode($exception->getMessage());
        }
    }


    /**
     * @param $datasetName
     * @param $tableName
     * @return mixed
     */
    public function getTable($datasetName, $tableName)
    {
        try {
            $dataset = $this->getConnection()->dataset($datasetName);

            return $dataset->table($tableName);
        } catch (\Exception $exception) {
            return json_decode($exception->getMessage());
        }
    }

    /**
     * @param $datasetName
     * @param $tableName
     * @param $attributes
     * @param array $options
     * @return mixed
     */
    public function insertRow($datasetName, $tableName, $attributes, $options = [])
    {
        try {
            $dataset = $this->getConnection()->dataset($datasetName);

            $table = $dataset->table($tableName);

            if (!empty($options)) {
                return $table->insertRow($attributes, $options);
            }

            return $table->insertRow($attributes);
        } catch (\Exception $exception) {
            return json_decode($exception->getMessage());
        }
    }


    /**
     * @param $datasetName
     * @param $tableName
     * @param $data
     * @param array $options
     * @return mixed
     */
    public function insertRows($datasetName, $tableName, $data, $options = [])
    {
        try {
            $dataset = $this->getConnection()->dataset($datasetName);

            $table = $dataset->table($tableName);

            return $table->insertRows($data, $options);
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

            /**
             * @var QueryResults $data
             */
            $queryResults = $this->getConnection()->runQuery($queryJobConfig);

            if ($queryResults->isComplete()) {
                $rows = $queryResults->rows();

                $data = [];
                foreach ($rows as $row) {
                    $data[] = $row;
                }

                return [
                    'data' => !empty($data) ? $data : null
                ];
            }
        } catch (\Exception $exception) {
            return json_decode($exception->getMessage());
        }
    }
}
