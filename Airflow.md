| DAG Name                       | Airflow Topics Covered                    | What This DAG Demonstrates    | Real-Time Production Example         |
| ------------------------------ | ----------------------------------------- | ----------------------------- | ------------------------------------ |
| batch_gcs_to_bq.py             | Scheduling, retries, dependencies         | Basic batch ingestion pattern | Daily sales files loaded to BigQuery |
| sqlserver_to_bq.py             | External DB integration, incremental load | Legacy to cloud migration     | SQL Server to BigQuery migration     |
| incremental_load.py            | Watermark logic, backfill                 | Delta processing              | Load updated customer records        |
| sensor_file_arrival.py         | Sensors, poke vs reschedule               | Event-based triggering        | Start pipeline on file arrival       |
| dynamic_task_mapping.py        | Dynamic task mapping                      | Runtime task scaling          | Process variable number of files     |
| multi_dag_dependency.py        | ExternalTaskSensor, TriggerDagRun         | Cross-DAG orchestration       | Raw DAG → curated DAG                |
| data_quality_checks.py         | Data validation, fail-fast                | Data correctness              | Block bad data before reporting      |
| taskflow_api_dag.py            | TaskFlow API, XCom                        | Modern DAG authoring          | Pythonic ETL pipelines               |
| subdag_pattern.py              | SubDAG (legacy pattern)                   | Modular workflows             | Older Airflow setups                 |
| dynamic_dag_factory.py         | Dynamic DAG generation                    | Config-driven DAGs            | One DAG per client/country           |
| branching_dag.py               | BranchPythonOperator                      | Conditional execution         | Full vs incremental path             |
| trigger_external_dag.py        | Event-driven orchestration                | Decoupled pipelines           | Notify downstream systems            |
| pool_control_dag.py            | Pools, slots                              | Resource throttling           | Limit heavy BigQuery jobs            |
| priority_weight_dag.py         | priority_weight, scheduling               | Task prioritization           | SLA-critical jobs first              |
| sla_monitoring_dag.py          | SLA miss callbacks                        | Monitoring & alerting         | Detect pipeline delays               |
| catchup_backfill_dag.py        | Catchup, start_date                       | Historical reprocessing       | Reload missed dates                  |
| dataset_driven_dag.py          | Datasets                                  | Event-based scheduling        | Trigger after table update           |
| concurrency_parallelism_dag.py | Concurrency, parallelism                  | System protection             | Prevent DB overload                  |
| failure_strategy_dag.py        | Retries, callbacks                        | Fault tolerance               | Auto-recovery pipelines              |
| variables_connections_dag.py   | Variables, connections                    | Environment portability       | Dev / QA / Prod configs              |
| deferrable_operator_dag.py     | Deferrable operators                      | Scheduler efficiency          | Long waits without blocking          |
| timezone_handling_dag.py       | Timezones, execution_date                 | Correct scheduling            | US/EU region pipelines               |
| queue_routing_dag.py           | Queues, executors                         | Workload isolation            | Spark jobs on separate workers       |
| idempotent_pipeline_dag.py     | Idempotency                               | Safe re-runs                  | Reprocessing without duplicates      |
| short_circuit_dag.py           | ShortCircuitOperator                      | Early termination             | Skip when no new data                |
| dag_versioning_strategy.py     | DAG versioning                            | Safe deployments              | Zero-downtime changes                |
| task_group_dag.py              | TaskGroup                                 | Logical task grouping         | Clean UI for large DAGs              |
| xcom_best_practices_dag.py     | XCom anti-patterns                        | Metadata DB safety            | Avoid large payloads                 |
| dag_with_params.py             | dag_run.conf                              | Ad-hoc execution              | Manual reprocessing                  |
| sla_vs_timeout_dag.py          | SLA vs timeout                            | Operational clarity           | SLA breach analysis                  |
| deadlock_prevention_dag.py     | Cycle detection                           | DAG correctness               | Prevent circular deps                |
| secrets_backend_dag.py         | Secrets backend                           | Security                      | Secure credential access             |
| dag_serialization_dag.py       | DAG serialization                         | UI performance                | Large Airflow deployments            |
| dataset_fanout_dag.py          | Dataset fan-out                           | One-to-many triggers          | One source → many consumers          |
