# Storage

## MariaDB Storage Schema

The MariaDB storage contains the following tables.

### workers

This table contains worker information and their heartbeats.

| Column Name | Data Type   | Not Null | Key / Uniqueness | Notes                                                 |
|-------------|-------------|----------|------------------|-------------------------------------------------------|
| id          | BINARY(16)  | Yes      | Primary Key      |                                                       |
| heartbeat   | TIMESTAMP   | Yes      |                  | DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP |

### schedulers

This table contains scheduler information and their heartbeats.

| Column Name | Data Type     | Not Null | Key / Uniqueness | Notes                                                 |
|-------------|---------------|----------|------------------|-------------------------------------------------------|
| id          | BINARY(16)    | Yes      | Primary Key      |                                                       |
| address     | VARCHAR(40)   | Yes      |                  |                                                       |
| port        | INT UNSIGNED  | Yes      |                  |                                                       |
| heartbeat   | TIMESTAMP     | Yes      |                  | DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP |

### resource_groups

This table keeps track of all resource groups.

| Column Name  | Data Type    | Not Null | Key / Uniqueness  | Notes |
|--------------|--------------|----------|-------------------|-------|
| id           | BINARY(16)   | Yes      | Primary Key       |       |
| external_id  | VARCHAR(256) | Yes      | Unique            |       |

### jobs

This table contains the metadata of each job.

| Column Name       | Data Type                                                             | Not Null | Key / Uniqueness | Notes                                               |
|-------------------|-----------------------------------------------------------------------|----------|------------------|-----------------------------------------------------|
| id                | BINARY(16)                                                            | Yes      | Primary Key      |                                                     |
| resource_group_id | BINARY(16)                                                            | Yes      | Index            | Foreign Key → resource_groups(id) ON DELETE CASCADE |
| creation_time     | TIMESTAMP                                                             | Yes      | Index            | DEFAULT CURRENT_TIMESTAMP                           |
| state             | ENUM ('RUNNING', 'PENDING_RETRY', 'SUCCEEDED', 'FAILED', 'CANCELLED') | Yes      | Index            | DEFAULT 'RUNNING'                                   |
| max_num_retries   | INT UNSIGNED                                                          | Yes      |                  | DEFAULT 5                                           |
| num_retries       | INT UNSIGNED                                                          | Yes      |                  | DEFAULT 0                                           |

### tasks

This table contains the metadata of each task.

| Column Name             | Data Type                                                                | Not Null | Key / Uniqueness | Notes                                             |
|-------------------------|--------------------------------------------------------------------------|----------|------------------|---------------------------------------------------|
| id                      | BINARY(16)                                                               | Yes      |  Primary Key     |                                                   |
| job_id                  | BINARY(16)                                                               | Yes      |                  | Foreign Key → jobs(id) ON DELETE CASCADE          |
| package_name            | VARCHAR(64)                                                              | Yes      |                  |                                                   |
| func_name               | VARCHAR(64)                                                              | Yes      |                  |                                                   |
| language                | ENUM('CPP','RUST','PYTHON')                                              | Yes      |                  |                                                   |
| state                   | ENUM('PENDING', 'READY', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED')  | Yes      |                  |                                                   |
| num_parents             | INT UNSIGNED                                                             | Yes      |                  |                                                   |
| num_succeeded_parents   | INT UNSIGNED                                                             | Yes      |                  | DEFAULT 0                                         |
| task_graph_insertion_id | INT UNSIGNED                                                             | Yes      |                  | The insertion ID in the original task graph       |
| timeout                 | FLOAT                                                                    |          |                  |                                                   |
| max_num_retries         | INT UNSIGNED                                                             |          |                  | DEFAULT 0                                         |
| num_retries             | INT UNSIGNED                                                             |          |                  | DEFAULT 0                                         |
| instance_id             | BINARY(16)                                                               |          |                  | Set to the Id of the first finished task instance |

### completed_task_control_flow_deps

This table records unique parent-to-child relationships for tasks whose parent has completed
successfully. Each row represents a completed dependency and indicates that the parent task finished
for the given child.

The primary purpose of this table is to make updates to `tasks(num_succeeded_parents)` idempotent.

| Column Name  | Data Type  | Not Null | Key / Uniqueness              | Notes                                     |
|--------------|------------|----------|-------------------------------|-------------------------------------------|
| parent       | BINARY(16) | Yes      | Index, Unique(parent, child)  | Foreign key → tasks(id) ON DELETE CASCADE |
| child        | BINARY(16) | Yes      | Index, Unique(parent, child)  | Foreign key → tasks(id) ON DELETE CASCADE |

### input_tasks

This table records all input tasks for jobs.

| Column Name  | Data Type    | Not Null | Key / Uniqueness | Notes                                     |
|--------------|--------------|----------|------------------|-------------------------------------------|
| job_id       | BINARY(16)   | Yes      | Index            | Foreign key → jobs(id) ON DELETE CASCADE  |
| task_id      | BINARY(16)   | Yes      | Primary Key      | Foreign key → tasks(id) ON DELETE CASCADE |

### output_tasks

This table records all output tasks for jobs.

| Column Name | Data Type    | Not Null | Key / Uniqueness | Notes                                     |
|-------------|--------------|----------|------------------|-------------------------------------------|
| job_id      | BINARY(16)   | Yes      | Index            | Foreign key → jobs(id) ON DELETE CASCADE  |
| task_id     | BINARY(16)   | Yes      | Primary Key      | Foreign key → tasks(id) ON DELETE CASCADE |

### shared_values

This table keeps track of shareable values. The lifecycle of these values is managed based on
reference counting.

| Column Name        | Data Type       | Not Null | Key / Uniqueness | Notes                                  |
|--------------------|-----------------|----------|------------------|----------------------------------------|
| id                 | BINARY(16)      | Yes      | Primary Key      |                                        |
| task_graph_data_id | BINARY(16)      | Yes      |                  | The data ID in the original task graph |
| type               | VARBINARY(1024) | Yes      |                  |                                        |
| payload            | VARBINARY(1024) | Yes      |                  |                                        |

### resource_group_to_shared_value_ref

This table keeps track of all references from resource groups to shared values.

| Column Name       | Data Type  | Not Null | Key / Uniqueness | Notes                             |
|-------------------|------------|----------|------------------|-----------------------------------|
| id                | BINARY(16) | Yes      | Index            | Foreign key → shared_values(id)   |
| resource_group_id | BINARY(16) | Yes      | Index            | Foreign key → resource_groups(id) |

### job_to_shared_value_ref

This table keeps track of all references from jobs to shared values.

| Column Name | Data Type  | Not Null | Key / Uniqueness | Notes                           |
|-------------|------------|----------|------------------|---------------------------------|
| id          | BINARY(16) | Yes      | Index            | Foreign key → shared_values(id) |
| job_id      | BINARY(16) | Yes      | Index            | Foreign key → jobs(id)          |

### values

This table keeps track of all values. The lifecycle of these values binds to the lifecycle of the
owner job.

| Column Name        | Data Type       | Not Null | Key / Uniqueness | Notes                                    |
|--------------------|-----------------|----------|------------------|------------------------------------------|
| id                 | INT UNSIGNED    | Yes      | Primary Key      | AUTO_INCREMENT                           |
| task_graph_data_id | INT UNSIGNED    | Yes      |                  | The data ID in the original task graph   |
| owner_job_id       | BINARY(16)      | Yes      | Index            | Foreign key → jobs(id) ON DELETE CASCADE |
| type               | VARBINARY(1024) | Yes      |                  |                                          |
| payload            | VARBINARY(1024) |          |                  |                                          |

### task_inputs

This table records all task inputs, each references to a value or a shared value.

| Column Name     | Data Type                      | Not Null | Key / Uniqueness                | Notes                                     |
|-----------------|--------------------------------|----------|---------------------------------|-------------------------------------------|
| task_id         | BINARY(16)                     | Yes      | Primary Key (task_id, position) | Foreign key → tasks(id) ON DELETE CASCADE |
| position        | INT UNSIGNED                   | Yes      | Primary Key (task_id, position) |                                           |
| type            | ENUM ('VALUE', 'SHARED_VALUE') | Yes      |                                 |                                           |
| value_id        | INT UNSIGNED                   |          |                                 | Foreign key → values(id)                  |
| shared_value_id | BINARY(16)                     |          |                                 | Foreign key → shared_values(id)           |

### task_outputs

This table records all task outputs, each references to a value or a shared value.

| Column Name     | Data Type                      | Not Null | Key / Uniqueness | Notes                           |
|-----------------|--------------------------------|----------|------------------|---------------------------------|
| task_id         | BINARY(16)                     | Yes      | Primary Key      | Foreign key → tasks(id)         |
| position        | INT UNSIGNED                   | Yes      | Primary Key      |                                 |
| type            | ENUM ('VALUE', 'SHARED_VALUE') | Yes      |                  |                                 |
| value_id        | INT UNSIGNED                   |          |                  | Foreign key → values(id)        |
| shared_value_id | BINARY(16)                     |          |                  | Foreign key → shared_values(id) |

### task_control_flow_deps

This table records all parent-to-child relationships for tasks.

| Column Name | Data Type    | Not Null | Key / Uniqueness | Notes                   |
|-------------|--------------|----------|------------------|-------------------------|
| parent      | BINARY(16)   | Yes      | Index            | Foreign key → tasks(id) |
| child       | BINARY(16)   | Yes      | Index            | Foreign key → tasks(id) |

### task_instances

This table contains the metadata of all task instances.

| Column Name | Data Type  | Not Null | Key / Uniqueness | Notes                     |
|-------------|------------|----------|------------------|---------------------------|
| id          | BINARY(16) | Yes      | Primary Key      |                           |
| task_id     | BINARY(16) | Yes      |                  | Foreign key → tasks(id)   |
| start_time  | TIMESTAMP  | Yes      |                  | DEFAULT CURRENT_TIMESTAMP |

### shared_value_localities

This table keeps track of all localities associated with shared values.

| Column Name | Data Type   | Not Null | Key / Uniqueness | Notes                           |
|-------------|-------------|----------|------------------|---------------------------------|
| id          | BINARY(16)  | Yes      | Index            | Foreign key → shared_values(id) |
| address     | VARCHAR(40) | Yes      |                  |                                 |
