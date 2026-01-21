# Storage

## MariaDB Storage Schema

The MariaDB storage contains the following tables:

### workers

| Column Name | Data Type | Not Null | Key / Uniqueness | Notes |
| --- | --- | --- | --- | --- |
| id | BINARY(16) | ✅ | Primary Key |  |
| heartbeat | TIMESTAMP | ✅ |  | DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP |

### schedulers

| Column Name | Data Type | Not Null | Key / Uniqueness | Notes |
| --- | --- | --- | --- | --- |
| id | BINARY(16) | ✅ | Primary Key |  |
| address | VARCHAR(40) | ✅ |  |  |
| port | INT UNSIGNED | ✅ |  |  |
| heartbeat | TIMESTAMP | ✅ |  | DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP |

### resource_groups

| Column Name | Data Type | Not Null | Key / Uniqueness | Notes |
| --- | --- | --- | --- | --- |
| id | BINARY(16) | ✅ | Primary Key |  |
| external_id | VARCHAR(256) | ✅ | Unique |  |

### jobs

| Column Name | Data Type | Not Null | Key / Uniqueness | Notes                                              |
| --- | --- | --- | --- |----------------------------------------------------|
| id | BINARY(16) | ✅ | Primary Key |                                                    |
| resource_group_id | VARBINARY(16) | ✅ | Index | Foreign Key → resource_groups(id) ON DELETE CASCADE |
| creation_time | TIMESTAMP | ✅ | Index | DEFAULT CURRENT_TIMESTAMP                          |
| state | ENUM ('RUNNING', 'PENDING_RETRY', 'SUCCEEDED', 'FAILED', 'CANCELLED') | ✅ | Index | DEFAULT 'RUNNING'                                  |
| max_num_retries | INT UNSIGNED | ✅ |  | DEFAULT 5                                          |
| num_retries | INT UNSIGNED | ✅ |  | DEFAULT 0                                          |

### tasks

| Column Name | Data Type | Not Null | Key / Uniqueness | Notes |
| --- | --- | --- | --- | --- |
| id | BINARY(16) | ✅ | Primary Key |  |
| job_id | BINARY(16) | ✅ |  | Foreign Key → jobs(id) ON DELETE CASCADE |
| func_name | VARCHAR(64) | ✅ |  |  |
| language | ENUM('CPP','RUST','PYTHON') | ✅ |  |  |
| state | ENUM(’PENDING’, ‘READY’, ‘RUNNING’, ‘SUCCEEDED’, ‘FAILED’, ‘CANCELLED’) | ✅ |  |  |
| num_parents | INT UNSIGNED | ✅ |  |  |
| num_succeeded_parents | INT UNSIGNED | ✅ |  | DEFAULT 0 |
| timeout | FLOAT |  |  |  |
| max_num_retries | INT UNSIGNED |  |  | DEFAULT 0 |
| num_retries | INT UNSIGNED |  |  | DEFAULT 0 |
| instance_id | BINARY(16) |  |  | Set to first finished task instance |

### complete_task_dependencies

| Column Name | Data Type | Not Null | Key / Uniqueness | Notes |
| --- | --- | --- | --- | --- |
| parent | BINARY(16) | ✅ | Index, Unique(parent, child) | Foreign key → tasks(id) ON DELETE CASCADE |
| child | BINARY(16) | ✅ | Index, Unique(parent, child) | Foreign key → tasks(id) ON DELETE CASCADE |

### input_tasks

| Column Name | Data Type | Not Null | Key / Uniqueness | Notes |
| --- | --- | --- | --- | --- |
| job_id | BINARY(16) | ✅ | Index | Foreign key → jobs(id) ON DELETE CASCADE |
| task_id | BINARY(16) | ✅ | Primary Key | Foreign key → tasks(id) ON DELETE CASCADE |
| position | INT UNSIGNED | ✅ |  |  |

### output_tasks

| Column Name | Data Type | Not Null | Key / Uniqueness | Notes |
| --- | --- | --- | --- | --- |
| job_id | BINARY(16) | ✅ | Index | Foreign key → jobs(id) ON DELETE CASCADE |
| task_id | BINARY(16) | ✅ | Primary Key | Foreign key → tasks(id) ON DELETE CASCADE |
| position | INT UNSIGNED | ✅ |  |  |

### data

| Column Name | Data Type | Not Null | Key / Uniqueness | Notes |
| --- | --- | --- | --- | --- |
| id | BINARY(16) | ✅ | Primary Key |  |
| payload | VARBINARY(1024) | ✅ |  |  |

### value

| Column Name | Data Type | Not Null | Key / Uniqueness | Notes |
| --- | --- | --- | --- | --- |
| id | INT UNSIGNED | ✅ | Primary Key | AUTO_INCREMENT |
| job_id | BINARY(16) | ✅ | Index | Foreign key → jobs(id) ON DELETE CASCADE |
| type | VARBINARY(1024) | ✅ |  |  |
| payload | VARBINARY(1024) |  |  |  |

### task_inputs

| Column Name | Data Type | Not Null | Key / Uniqueness | Notes |
| --- | --- |----------| --- | --- |
| task_id | BINARY(16) | ✅        | Primary Key (task_id, position) | Foreign key → tasks(id) ON DELETE CASCADE |
| position | INT UNSIGNED | ✅        | Primary Key (task_id, position) |  |
| type | ENUM (’VALUE’, ‘DATA’) | ✅        |  |  |
| value_id | INT UNSIGNED |          |  | Foreign key → value(id) |
| data_id | BINARY(16) |          |  | Foreign key → data(id) |

### task_outputs

| Column Name | Data Type | Not Null | Key / Uniqueness | Notes |
| --- | --- | --- | --- | --- |
| task_id | BINARY(16) | ✅ | Primary Key | Foreign key → tasks(id) |
| position | INT UNSIGNED | ✅ | Primary Key |  |
| type | ENUM (’VALUE’, ‘DATA’) | ✅ |  |  |
| value_id | INT UNSIGNED |  |  | Foreign key → value(id) |
| data_id | BINARY(16) |  |  | Foreign key → data(id) |

### task_dependencies

| Column Name | Data Type | Not Null | Key / Uniqueness | Notes |
| --- | --- | --- | --- | --- |
| parent | BINARY(16) | ✅ | Index | Foreign key → tasks(id) |
| child | BINARY(16) | ✅ | Index | Foreign key → tasks(id) |
| position | INT UNSIGNED | ✅ |  |  |

### task_instances

| Column Name | Data Type | Not Null | Key / Uniqueness | Notes |
| --- | --- | --- | --- | --- |
| id | BINARY(16) | ✅ | Primary Key |  |
| task_id | BINARY(16) | ✅ |  | Foreign key → tasks(id) |
| start_time | TIMESTAMP | ✅ |  | DEFAULT CURRENT_TIMESTAMP |

### data_locality

| Column Name | Data Type | Not Null | Key / Uniqueness | Notes |
| --- | --- | --- | --- | --- |
| id | BINARY(16) | ✅ | Index | Foreign key → data(id) |
| address | VARCHAR(40) | ✅ |  |  |

### resource_group_to_data_ref

| Column Name | Data Type | Not Null | Key / Uniqueness | Notes |
| --- | --- | --- | --- | --- |
| id | BINARY(16) | ✅ | Index | Foreign key → data(id) |
| resource_group_id | BINARY(16) | ✅ | Index | Foreign key → resource_groups(id) |

### job_to_data_ref

| Column Name | Data Type | Not Null | Key / Uniqueness | Notes |
| --- | --- | --- | --- | --- |
| id | BINARY(16) | ✅ | Index | Foreign key → data(id) |
| job_id | BINARY(16) | ✅ | Index | Foreign key → jobs(id) |
