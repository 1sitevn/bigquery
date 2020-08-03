<?php
return [
    'bigquery' => [
        'project_id' => env('BIGQUERY_PROJECT_ID', null),
        'key_file_path' => env('BIGQUERY_KEY_FILE_PATH', null),
        'dataset' => env('BIGQUERY_DATASET', 'test'),
        'table' => env('BIGQUERY_TABLE', 'events'),
    ]
];
