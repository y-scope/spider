use std::fmt::Debug;
use std::time::Duration;

use spider_core::job::JobState;
use spider_core::types::id::JobId;
use spider_core::types::id::ResourceGroupId;
use spider_core::types::id::SessionId;
use spider_core::types::io::TaskOutput;
use spider_storage::DatabaseConfig;
use spider_storage::db::DbError;
use spider_storage::db::ExternalJobOrchestration;
use spider_storage::db::InternalJobOrchestration;
use spider_storage::db::JobStateUpdate;
use spider_storage::db::MariaDbStorageConnector;
use spider_storage::db::SessionManagement;
use spider_storage::db::TreeTransition;
use spider_storage::job_submission::ValidatedJobSubmission;
use sqlx::Acquire;
use sqlx::MySqlPool;

use super::mariadb_infra::IsolatedTestDb;
use super::mariadb_infra::create_raw_pool;
use super::mariadb_infra::create_test_resource_group;
use super::mariadb_infra::root_credentials;
use super::mariadb_infra::test_fence_id;
use super::mariadb_infra::wait_for_queued_fence_bump;
use super::task_graph_builder::build_flat_task_graph;
use super::task_graph_builder::create_validated_submission;

const SELECT_FENCE_FOR_SHARE_QUERY: &str =
    "SELECT `session_id` FROM `storage_fence` WHERE `id` = ? LOCK IN SHARE MODE;";

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn newer_connector_fences_off_older_connector_with_same_fence_id() -> anyhow::Result<()> {
    let job_submission = flat_submission();
    let isolated_test_db = IsolatedTestDb::create().await;
    let config = isolated_test_db.config();
    let raw_pool = create_raw_pool(&config).await;
    let older = connect(&config).await;
    let rg_id = create_test_resource_group(&older).await;
    let ready_job_id = older.register(rg_id, &job_submission).await?;
    let running_job_id = older.register(rg_id, &job_submission).await?;
    older.start(running_job_id).await?;
    let expired_job_id = older.register(rg_id, &job_submission).await?;
    older.start(expired_job_id).await?;
    older.fail(expired_job_id, "expired".to_owned()).await?;
    sqlx::query("UPDATE `jobs` SET `ended_at` = NOW() - INTERVAL 1 HOUR WHERE `id` = ?;")
        .bind(expired_job_id)
        .execute(&raw_pool)
        .await?;
    let newer = connect(&config).await;

    let results = [
        (
            "register",
            older.register(rg_id, &job_submission).await.map(|_| ()),
        ),
        ("start", older.start(ready_job_id).await),
        (
            "set_state",
            older.set_state(running_job_id, JobState::Cancelled).await,
        ),
        (
            "commit_outputs",
            older
                .commit_outputs(running_job_id, vec![b"outputs".to_vec()], false)
                .await,
        ),
        ("cancel", older.cancel(running_job_id, false).await),
        (
            "fail",
            older.fail(running_job_id, "fenced".to_owned()).await,
        ),
        (
            "delete_expired_terminated_jobs",
            older.delete_expired_terminated_jobs(60).await.map(|_| ()),
        ),
    ];

    for (writer, result) in results {
        assert_fenced(
            &result,
            &older,
            &newer,
            &format!("{writer} with {config:?}"),
        );
    }
    assert_eq!(
        job_states(&raw_pool).await,
        vec![
            (ready_job_id, JobState::Ready),
            (running_job_id, JobState::Running),
            (expired_job_id, JobState::Failed),
        ],
        "{config:?}"
    );
    newer.start(ready_job_id).await?;
    newer
        .commit_outputs(running_job_id, vec![b"outputs".to_vec()], false)
        .await?;
    assert_eq!(
        newer.delete_expired_terminated_jobs(60).await?,
        vec![expired_job_id],
        "{config:?}"
    );
    newer.register(rg_id, &job_submission).await?;

    raw_pool.close().await;
    isolated_test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn connectors_with_different_fence_ids_are_independent() -> anyhow::Result<()> {
    let isolated_test_db = IsolatedTestDb::create().await;
    let first = connect(&DatabaseConfig {
        fence_id: Some(1),
        ..isolated_test_db.config()
    })
    .await;
    let second = connect(&DatabaseConfig {
        fence_id: Some(2),
        ..isolated_test_db.config()
    })
    .await;
    let rg_id = create_test_resource_group(&first).await;
    let job_submission = flat_submission();

    let first_job_id = first.register(rg_id, &job_submission).await?;
    let second_job_id = second.register(rg_id, &job_submission).await?;
    first.start(first_job_id).await?;
    second.start(second_job_id).await?;
    isolated_test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn connect_waits_for_transactions_that_passed_the_fence() -> anyhow::Result<()> {
    let isolated_test_db = IsolatedTestDb::create().await;
    let config = isolated_test_db.config();
    let unprivileged_config = DatabaseConfig {
        credentials: isolated_test_db
            .create_user_without_global_privileges()
            .await,
        ..config.clone()
    };
    let older = connect(&config).await;
    let rg_id = create_test_resource_group(&older).await;
    let job_id = older.register(rg_id, &flat_submission()).await?;
    let raw_pool = create_raw_pool(&config).await;
    let root_pool = create_root_pool(&config).await;
    let mut raw_connection = raw_pool.acquire().await?;
    let mut raw_tx = raw_connection.begin().await?;
    sqlx::query(SELECT_FENCE_FOR_SHARE_QUERY)
        .bind(test_fence_id(&config))
        .execute(&mut *raw_tx)
        .await?;

    let first_bump_attempt_deadline = tokio::time::Instant::now() + Duration::from_secs(4);
    let newer_connect_join_handle =
        tokio::spawn(async move { connect(&unprivileged_config).await });
    tokio::time::timeout_at(
        first_bump_attempt_deadline,
        wait_for_queued_fence_bump(&root_pool, test_fence_id(&config)),
    )
    .await
    .unwrap_or_else(|_| {
        panic!("connect should queue its fence bump behind the open transaction with {config:?}")
    });
    assert!(
        !newer_connect_join_handle.is_finished(),
        "connect should wait for the open transaction with {config:?}"
    );
    let starting_older = older.clone();
    let older_start_join_handle = tokio::spawn(async move { starting_older.start(job_id).await });
    let registering_older = older.clone();
    let older_register_join_handle =
        tokio::spawn(async move { registering_older.register(rg_id, &flat_submission()).await });
    tokio::time::timeout_at(
        first_bump_attempt_deadline,
        wait_for_blocked_statements(&root_pool, "%`storage_fence`%LOCK IN SHARE MODE%", 2),
    )
    .await
    .unwrap_or_else(|_| {
        panic!(
            "the older connector's start and register should queue behind the fence bump with \
             {config:?}"
        )
    });
    assert!(
        !older_start_join_handle.is_finished(),
        "the older connector's start should wait for the fence bump with {config:?}"
    );
    assert!(
        !older_register_join_handle.is_finished(),
        "the older connector's register should wait for the fence bump with {config:?}"
    );

    raw_tx.commit().await?;
    let newer = tokio::time::timeout(Duration::from_secs(5), newer_connect_join_handle).await??;
    let start_result =
        tokio::time::timeout(Duration::from_secs(5), older_start_join_handle).await??;
    let register_result =
        tokio::time::timeout(Duration::from_secs(5), older_register_join_handle).await??;
    for (writer, result) in [
        ("start", start_result),
        ("register", register_result.map(|_| ())),
    ] {
        assert_fenced(
            &result,
            &older,
            &newer,
            &format!("{writer} that starts while the fence bump is queued with {config:?}"),
        );
    }
    assert_eq!(
        job_states(&root_pool).await,
        vec![(job_id, JobState::Ready)],
        "{config:?}"
    );
    drop(raw_connection);
    raw_pool.close().await;
    root_pool.close().await;
    isolated_test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn writes_that_passed_the_fence_hold_it_until_they_commit() -> anyhow::Result<()> {
    let isolated_test_db = IsolatedTestDb::create().await;
    let config = isolated_test_db.config();
    let unprivileged_config = DatabaseConfig {
        credentials: isolated_test_db
            .create_user_without_global_privileges()
            .await,
        ..config.clone()
    };
    let root_pool = create_root_pool(&config).await;
    let first = connect(&config).await;
    let rg_id = create_test_resource_group(&first).await;
    let ready_job_id = first.register(rg_id, &flat_submission()).await?;

    let (registered_job_id, second) = commit_behind_a_queued_fence_bump(
        &root_pool,
        &unprivileged_config,
        "resource_groups",
        rg_id.get(),
        "INSERT INTO `jobs`%`storage_fence`%",
        async move { first.register(rg_id, &flat_submission()).await },
    )
    .await?;
    commit_behind_a_queued_fence_bump(
        &root_pool,
        &unprivileged_config,
        "jobs",
        ready_job_id.get(),
        "SELECT `state` FROM `jobs`%",
        async move { second.start(ready_job_id).await },
    )
    .await?;

    assert_eq!(
        job_states(&root_pool).await,
        vec![
            (ready_job_id, JobState::Running),
            (registered_job_id, JobState::Ready),
        ],
        "{config:?}"
    );
    root_pool.close().await;
    isolated_test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn connect_never_lowers_the_fence() -> anyhow::Result<()> {
    const SELECT_FENCE_QUERY: &str = "SELECT `session_id` FROM `storage_fence` WHERE `id` = ?;";

    let isolated_test_db = IsolatedTestDb::create().await;
    let config = isolated_test_db.config();
    let raw_pool = create_raw_pool(&config).await;
    let older = connect(&config).await;
    let rg_id = create_test_resource_group(&older).await;
    let newest_session_id = SessionId::MAX;
    sqlx::query("UPDATE `storage_fence` SET `session_id` = ? WHERE `id` = ?;")
        .bind(newest_session_id)
        .bind(test_fence_id(&config))
        .execute(&raw_pool)
        .await?;

    let newer = connect(&config).await;

    let fence_session_id: SessionId = sqlx::query_scalar(SELECT_FENCE_QUERY)
        .bind(test_fence_id(&config))
        .fetch_one(&raw_pool)
        .await?;
    assert_eq!(fence_session_id, newest_session_id);
    let result = newer.register(rg_id, &flat_submission()).await;
    assert!(
        matches!(
            result,
            Err(DbError::Fenced { current, newer: newer_session_id })
                if current == newer.session_id() && newer_session_id == newest_session_id
        ),
        "expected Fenced, got {result:?}"
    );
    raw_pool.close().await;
    isolated_test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn connect_kills_an_orphaned_fence_holder_when_privileged() -> anyhow::Result<()> {
    const SELECT_RESOURCE_GROUP_FOR_UPDATE_QUERY: &str =
        "SELECT `id` FROM `resource_groups` WHERE `id` = ? FOR UPDATE;";

    for is_root_the_killer in [true, false] {
        let isolated_test_db = IsolatedTestDb::create().await;
        let config = isolated_test_db.config();
        let root_config = DatabaseConfig {
            credentials: root_credentials(),
            ..config.clone()
        };
        let (user_config, killer_config) = if is_root_the_killer {
            (config, root_config.clone())
        } else {
            let process_privileged_config = DatabaseConfig {
                credentials: isolated_test_db
                    .create_user_with_the_process_privilege()
                    .await,
                ..config
            };
            (process_privileged_config.clone(), process_privileged_config)
        };
        let older = connect(&user_config).await;
        let rg_id = create_test_resource_group(&older).await;
        let raw_pool = create_raw_pool(&user_config).await;
        let root_pool = create_raw_pool(&root_config).await;
        let mut unrelated_holder_connection = raw_pool.acquire().await?;
        let mut unrelated_holder_tx = unrelated_holder_connection.begin().await?;
        sqlx::query(SELECT_RESOURCE_GROUP_FOR_UPDATE_QUERY)
            .bind(rg_id)
            .execute(&mut *unrelated_holder_tx)
            .await?;
        let unrelated_waiter_pool = raw_pool.clone();
        let unrelated_waiter_join_handle = tokio::spawn(async move {
            let mut unrelated_waiter_tx = unrelated_waiter_pool.begin().await?;
            sqlx::query(SELECT_RESOURCE_GROUP_FOR_UPDATE_QUERY)
                .bind(rg_id)
                .execute(&mut *unrelated_waiter_tx)
                .await?;
            unrelated_waiter_tx.commit().await
        });
        tokio::time::timeout(
            Duration::from_secs(5),
            wait_for_blocked_statements(
                &root_pool,
                "SELECT `id` FROM `resource_groups`%FOR UPDATE",
                1,
            ),
        )
        .await
        .expect("the unrelated waiter should block on the unrelated holder");
        let mut orphan_connection = raw_pool.acquire().await?;
        let mut orphan_tx = orphan_connection.begin().await?;
        sqlx::query(SELECT_FENCE_FOR_SHARE_QUERY)
            .bind(test_fence_id(&user_config))
            .execute(&mut *orphan_tx)
            .await?;

        tokio::time::timeout(Duration::from_secs(4), connect(&killer_config))
            .await
            .unwrap_or_else(|_| {
                panic!("connect should kill the orphaned fence holder with {killer_config:?}")
            });

        let commit_result = orphan_tx.commit().await;
        assert!(
            commit_result.is_err(),
            "the orphaned transaction's connection should have been killed with {killer_config:?}"
        );
        unrelated_holder_tx.commit().await?;
        tokio::time::timeout(Duration::from_secs(5), unrelated_waiter_join_handle).await???;
        drop(orphan_connection);
        drop(unrelated_holder_connection);
        raw_pool.close().await;
        root_pool.close().await;
        isolated_test_db.drop_database().await;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn connect_retries_the_fence_bump_without_the_process_privilege() -> anyhow::Result<()> {
    let isolated_test_db = IsolatedTestDb::create().await;
    let user_config = isolated_test_db.config();
    let unprivileged_config = DatabaseConfig {
        credentials: isolated_test_db
            .create_user_without_global_privileges()
            .await,
        ..user_config.clone()
    };
    let _older = connect(&user_config).await;
    let raw_pool = create_raw_pool(&user_config).await;
    let unprivileged_pool = create_raw_pool(&unprivileged_config).await;
    let mut orphan_connection = raw_pool.acquire().await?;
    let mut orphan_tx = orphan_connection.begin().await?;
    sqlx::query(SELECT_FENCE_FOR_SHARE_QUERY)
        .bind(test_fence_id(&user_config))
        .execute(&mut *orphan_tx)
        .await?;

    let newer_connect_join_handle =
        tokio::spawn(async move { MariaDbStorageConnector::connect(&unprivileged_config).await });
    tokio::time::timeout(
        Duration::from_secs(15),
        wait_for_reissued_statement(&unprivileged_pool, "UPDATE `storage_fence`%"),
    )
    .await
    .expect("connect should reissue its fence bump after a lock wait timeout");
    assert!(
        !newer_connect_join_handle.is_finished(),
        "connect should still wait for the orphaned transaction after one lock wait timeout"
    );

    orphan_tx.commit().await?;
    tokio::time::timeout(Duration::from_secs(5), newer_connect_join_handle).await???;
    drop(orphan_connection);
    raw_pool.close().await;
    unprivileged_pool.close().await;
    isolated_test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn a_state_mismatch_or_a_missing_fence_row_is_not_fenced() -> anyhow::Result<()> {
    let job_submission = flat_submission();
    let isolated_test_db = IsolatedTestDb::create().await;
    let config = isolated_test_db.config();
    let raw_pool = create_raw_pool(&config).await;
    let storage = connect(&config).await;
    let rg_id = create_test_resource_group(&storage).await;
    let running_job_id = storage.register(rg_id, &job_submission).await?;
    let ready_job_id = storage.register(rg_id, &job_submission).await?;
    storage.start(running_job_id).await?;

    let state_mismatch_result = storage.start(running_job_id).await;
    assert!(
        matches!(
            state_mismatch_result,
            Err(DbError::UnexpectedJobState {
                current: JobState::Running,
                ..
            })
        ),
        "expected UnexpectedJobState with {config:?}, got {state_mismatch_result:?}"
    );

    sqlx::query("DELETE FROM `storage_fence` WHERE `id` = ?;")
        .bind(test_fence_id(&config))
        .execute(&raw_pool)
        .await?;
    let results = [
        (
            "register",
            storage.register(rg_id, &job_submission).await.map(|_| ()),
        ),
        ("start", storage.start(ready_job_id).await),
    ];
    for (writer, result) in results {
        assert!(
            matches!(result, Err(DbError::CorruptedDbState(_))),
            "expected CorruptedDbState from {writer} with {config:?}, got {result:?}"
        );
    }
    assert_eq!(
        job_states(&raw_pool).await,
        vec![
            (running_job_id, JobState::Running),
            (ready_job_id, JobState::Ready),
        ],
        "{config:?}"
    );

    let newer = connect(&config).await;
    let fenced_result = storage.start(ready_job_id).await;
    assert_fenced(&fenced_result, &storage, &newer, &format!("{config:?}"));
    newer.start(ready_job_id).await?;

    raw_pool.close().await;
    isolated_test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn register_maps_a_missing_resource_group() {
    let missing_rg_id = ResourceGroupId::from(u64::MAX);
    let isolated_test_db = IsolatedTestDb::create().await;
    let config = isolated_test_db.config();
    let storage = connect(&config).await;

    let result = storage.register(missing_rg_id, &flat_submission()).await;

    assert!(
        matches!(result, Err(DbError::ResourceGroupNotFound(id)) if id == missing_rg_id),
        "expected ResourceGroupNotFound with {config:?}, got {result:?}"
    );
    isolated_test_db.drop_database().await;
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn a_tree_transition_that_passed_the_fence_holds_it_until_it_commits() -> anyhow::Result<()> {
    let isolated_test_db = IsolatedTestDb::create().await;
    let config = isolated_test_db.config();
    let unprivileged_config = DatabaseConfig {
        credentials: isolated_test_db
            .create_user_without_global_privileges()
            .await,
        ..config.clone()
    };
    let root_pool = create_root_pool(&config).await;
    let storage = connect(&config).await;
    let rg_id = create_test_resource_group(&storage).await;
    let job_submission = flat_submission();
    let first_job_id = storage.register(rg_id, &job_submission).await?;
    let second_job_id = storage.register(rg_id, &job_submission).await?;

    let starting_storage = storage.clone();
    let ((), newer) = commit_behind_a_queued_fence_bump(
        &root_pool,
        &unprivileged_config,
        "jobs",
        second_job_id.get(),
        "UPDATE `jobs`%",
        async move { start(&starting_storage, &[first_job_id, second_job_id]).await },
    )
    .await?;

    assert_eq!(
        job_states(&root_pool).await,
        vec![
            (first_job_id, JobState::Running),
            (second_job_id, JobState::Running),
        ],
        "{config:?}"
    );
    let result = start(&storage, &[first_job_id]).await;
    assert_fenced(&result, &storage, &newer, &format!("{config:?}"));
    root_pool.close().await;
    isolated_test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn a_tree_transition_issued_while_the_fence_bump_is_queued_is_fenced() -> anyhow::Result<()> {
    let isolated_test_db = IsolatedTestDb::create().await;
    let config = isolated_test_db.config();
    let unprivileged_config = DatabaseConfig {
        credentials: isolated_test_db
            .create_user_without_global_privileges()
            .await,
        ..config.clone()
    };
    let older = connect(&config).await;
    let rg_id = create_test_resource_group(&older).await;
    let job_submission = flat_submission();
    let mut job_ids = Vec::new();
    for _ in 0..3 {
        job_ids.push(older.register(rg_id, &job_submission).await?);
    }
    let raw_pool = create_raw_pool(&config).await;
    let root_pool = create_root_pool(&config).await;
    let mut raw_connection = raw_pool.acquire().await?;
    let mut raw_tx = raw_connection.begin().await?;
    sqlx::query(SELECT_FENCE_FOR_SHARE_QUERY)
        .bind(test_fence_id(&config))
        .execute(&mut *raw_tx)
        .await?;

    let first_bump_attempt_deadline = tokio::time::Instant::now() + Duration::from_secs(4);
    let newer_connect_join_handle =
        tokio::spawn(async move { connect(&unprivileged_config).await });
    tokio::time::timeout_at(
        first_bump_attempt_deadline,
        wait_for_queued_fence_bump(&root_pool, test_fence_id(&config)),
    )
    .await
    .unwrap_or_else(|_| {
        panic!("connect should queue its fence bump behind the open transaction with {config:?}")
    });
    let single_row_older = older.clone();
    let single_row_job_id = job_ids[0];
    let single_row_join_handle =
        tokio::spawn(async move { start(&single_row_older, &[single_row_job_id]).await });
    let several_rows_older = older.clone();
    let several_row_job_ids = job_ids[1..].to_vec();
    let several_rows_join_handle =
        tokio::spawn(async move { start(&several_rows_older, &several_row_job_ids).await });
    tokio::time::timeout_at(
        first_bump_attempt_deadline,
        wait_for_blocked_statements(&root_pool, "%`storage_fence`%LOCK IN SHARE MODE%", 2),
    )
    .await
    .unwrap_or_else(|_| {
        panic!(
            "the older connector's transitions should queue behind the fence bump with {config:?}"
        )
    });
    assert!(
        !single_row_join_handle.is_finished(),
        "the single-row transition should wait for the fence bump with {config:?}"
    );
    assert!(
        !several_rows_join_handle.is_finished(),
        "the transition of several rows should wait for the fence bump with {config:?}"
    );

    raw_tx.commit().await?;
    let newer = tokio::time::timeout(Duration::from_secs(5), newer_connect_join_handle).await??;
    let single_row_result =
        tokio::time::timeout(Duration::from_secs(5), single_row_join_handle).await??;
    let several_rows_result =
        tokio::time::timeout(Duration::from_secs(5), several_rows_join_handle).await??;
    for (transition, result) in [
        ("single-row", single_row_result),
        ("several-row", several_rows_result),
    ] {
        assert_fenced(
            &result,
            &older,
            &newer,
            &format!(
                "{transition} transition issued while the fence bump is queued with {config:?}"
            ),
        );
    }
    assert_eq!(
        job_states(&root_pool).await,
        job_ids
            .iter()
            .map(|job_id| (*job_id, JobState::Ready))
            .collect::<Vec<_>>(),
        "{config:?}"
    );
    drop(raw_connection);
    raw_pool.close().await;
    root_pool.close().await;
    isolated_test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn a_fenced_off_outputs_transition_writes_nothing() -> anyhow::Result<()> {
    let job_submission = flat_submission();
    let outputs = vec![b"outputs".to_vec()];
    let isolated_test_db = IsolatedTestDb::create().await;
    let config = isolated_test_db.config();
    let raw_pool = create_raw_pool(&config).await;
    for num_jobs in [1, 2] {
        let older = connect(&config).await;
        let rg_id = create_test_resource_group(&older).await;
        let mut job_ids = Vec::new();
        for _ in 0..num_jobs {
            job_ids.push(older.register(rg_id, &job_submission).await?);
        }
        start(&older, &job_ids).await?;
        let newer = connect(&config).await;

        let result = succeed(&older, &job_ids, &outputs).await;

        assert_fenced(&result, &older, &newer, &format!("{num_jobs} jobs"));
        for job_id in &job_ids {
            assert_eq!(
                newer.get_state(*job_id).await?,
                JobState::Running,
                "{num_jobs} jobs"
            );
            assert!(
                !has_outputs(&raw_pool, *job_id).await,
                "a fenced-off success of {num_jobs} jobs should write no outputs"
            );
        }
        succeed(&newer, &job_ids, &outputs).await?;
        for job_id in &job_ids {
            assert_eq!(
                newer.get_outputs(*job_id).await?,
                outputs,
                "{num_jobs} jobs"
            );
        }
    }
    raw_pool.close().await;
    isolated_test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn in_statement_guard_miss_is_corruption_not_fenced() -> anyhow::Result<()> {
    let isolated_test_db = IsolatedTestDb::create().await;
    let config = isolated_test_db.config();
    let storage = connect(&config).await;
    let rg_id = create_test_resource_group(&storage).await;
    let job_submission = flat_submission();
    let running_job_id = storage.register(rg_id, &job_submission).await?;
    let ready_job_id = storage.register(rg_id, &job_submission).await?;
    start(&storage, &[running_job_id]).await?;

    let guard_miss_result = start(&storage, &[running_job_id]).await;
    assert!(
        matches!(guard_miss_result, Err(DbError::CorruptedDbState(_))),
        "expected CorruptedDbState, got {guard_miss_result:?}"
    );
    assert_eq!(storage.get_state(running_job_id).await?, JobState::Running);

    let newer = connect(&config).await;
    let fenced_result = start(&storage, &[ready_job_id]).await;
    assert_fenced(&fenced_result, &storage, &newer, "start");
    assert_eq!(newer.get_state(ready_job_id).await?, JobState::Ready);

    isolated_test_db.drop_database().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires MariaDB"]
async fn guard_miss_in_one_statement_of_several_rows_writes_no_row() -> anyhow::Result<()> {
    let isolated_test_db = IsolatedTestDb::create().await;
    let config = isolated_test_db.config();
    let storage = connect(&config).await;
    let rg_id = create_test_resource_group(&storage).await;
    let job_submission = flat_submission();
    let ready_job_id = storage.register(rg_id, &job_submission).await?;
    let running_job_id = storage.register(rg_id, &job_submission).await?;
    start(&storage, &[running_job_id]).await?;

    let result = start(&storage, &[ready_job_id, running_job_id]).await;

    assert!(
        matches!(result, Err(DbError::CorruptedDbState(_))),
        "expected CorruptedDbState, got {result:?}"
    );
    assert_eq!(storage.get_state(ready_job_id).await?, JobState::Ready);
    assert_eq!(storage.get_state(running_job_id).await?, JobState::Running);
    isolated_test_db.drop_database().await;
    Ok(())
}

/// # Returns
///
/// A [`MariaDbStorageConnector`] connected with `config`.
///
/// # Panics
///
/// Panics if the connection fails.
async fn connect(config: &DatabaseConfig) -> MariaDbStorageConnector {
    MariaDbStorageConnector::connect(config)
        .await
        .expect("connect should succeed")
}

/// # Returns
///
/// A validated submission of a small flat task graph.
///
/// # Panics
///
/// Forwards [`build_flat_task_graph`]'s and [`create_validated_submission`]'s panics.
fn flat_submission() -> ValidatedJobSubmission {
    let (graph, task_graph_input) = build_flat_task_graph(1, 16, false, false);
    create_validated_submission(graph, task_graph_input)
}

/// Asserts that `result` is the error of `fenced`'s write after `newer` fenced it off.
///
/// # Type Parameters
///
/// * `ValueType` - The value that `result` holds on success.
///
/// # Panics
///
/// Panics if `result` is not [`DbError::Fenced`] with `fenced`'s and `newer`'s session IDs.
fn assert_fenced<ValueType: Debug>(
    result: &Result<ValueType, DbError>,
    fenced: &MariaDbStorageConnector,
    newer: &MariaDbStorageConnector,
    context: &str,
) {
    assert!(
        matches!(
            result,
            Err(DbError::Fenced { current, newer: newer_session_id })
                if *current == fenced.session_id() && *newer_session_id == newer.session_id()
        ),
        "expected Fenced ({context}), got {result:?}"
    );
}

/// # Returns
///
/// A pool connected to `config`'s database as the `MariaDB` root user.
///
/// # Panics
///
/// Forwards [`root_credentials`]'s and [`create_raw_pool`]'s panics.
async fn create_root_pool(config: &DatabaseConfig) -> MySqlPool {
    create_raw_pool(&DatabaseConfig {
        credentials: root_credentials(),
        ..config.clone()
    })
    .await
}

/// # Returns
///
/// The ID and the state of every row of the `jobs` table of `pool`'s database, by ascending ID.
///
/// # Panics
///
/// Panics if the query fails.
async fn job_states(pool: &MySqlPool) -> Vec<(JobId, JobState)> {
    sqlx::query_as("SELECT `id`, `state` FROM `jobs` ORDER BY `id`;")
        .fetch_all(pool)
        .await
        .expect("the job state query should succeed")
}

/// Runs `write` while a transaction locks the row `locked_row_id` of `locked_table` exclusively,
/// so that `write` blocks on that row in its statement that matches the `LIKE` pattern
/// `statement_pattern`. Then connects a newer connector with `newer_config`, checks that its fence
/// bump queues behind `write` and does not finish, releases the row, and waits for `write` and the
/// newer connector.
///
/// The row lock and the checks run over `root_pool`, which must be connected to `newer_config`'s
/// database as a user with the `PROCESS` privilege.
///
/// # Type Parameters
///
/// * `ValueType` - The value that `write` returns on success.
/// * `WriteType` - The type of `write`.
///
/// # Returns
///
/// A tuple on success, containing:
///
/// * The value of `write`.
/// * The newer connector.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`tokio::task::JoinError`] if the task running `write` or the newer connector's `connect`
///   panics or is cancelled.
/// * Forwards [`sqlx::Pool::acquire`]'s return values on failure.
/// * Forwards [`sqlx::Acquire::begin`]'s return values on failure.
/// * Forwards [`sqlx::query::Query::execute`]'s return values on failure.
/// * Forwards [`sqlx::Transaction::commit`]'s return values on failure.
/// * Forwards [`tokio::time::timeout`]'s return values on failure.
/// * Forwards `write`'s return values on failure.
///
/// # Panics
///
/// Panics if `write` does not block, if the fence bump does not queue within 5 seconds, or if it
/// finishes before `write` commits.
async fn commit_behind_a_queued_fence_bump<
    ValueType: Send + 'static,
    WriteType: Future<Output = Result<ValueType, DbError>> + Send + 'static,
>(
    root_pool: &MySqlPool,
    newer_config: &DatabaseConfig,
    locked_table: &str,
    locked_row_id: u64,
    statement_pattern: &str,
    write: WriteType,
) -> anyhow::Result<(ValueType, MariaDbStorageConnector)> {
    let mut root_connection = root_pool.acquire().await?;
    let mut root_tx = root_connection.begin().await?;
    sqlx::query(&format!(
        "SELECT `id` FROM `{locked_table}` WHERE `id` = ? FOR UPDATE;"
    ))
    .bind(locked_row_id)
    .execute(&mut *root_tx)
    .await?;
    let write_join_handle = tokio::spawn(write);
    tokio::time::timeout(
        Duration::from_secs(5),
        wait_for_blocked_statements(root_pool, statement_pattern, 1),
    )
    .await
    .unwrap_or_else(|_| panic!("`{statement_pattern}` should block with {newer_config:?}"));

    let connect_config = newer_config.clone();
    let newer_connect_join_handle = tokio::spawn(async move { connect(&connect_config).await });
    tokio::time::timeout(
        Duration::from_secs(5),
        wait_for_queued_fence_bump(root_pool, test_fence_id(newer_config)),
    )
    .await
    .unwrap_or_else(|_| {
        panic!(
            "connect should queue its fence bump behind `{statement_pattern}` with \
             {newer_config:?}"
        )
    });
    assert!(
        !newer_connect_join_handle.is_finished(),
        "connect should wait for `{statement_pattern}` with {newer_config:?}"
    );

    root_tx.commit().await?;
    let value = tokio::time::timeout(Duration::from_secs(5), write_join_handle).await???;
    let newer = tokio::time::timeout(Duration::from_secs(5), newer_connect_join_handle).await??;
    Ok((value, newer))
}

/// Waits until at least `num_statements` transactions on connections to `pool`'s database wait for
/// a lock in a statement whose text matches the `LIKE` pattern `statement_pattern`.
///
/// # Panics
///
/// Panics if the lock wait query fails, for example because `pool`'s user lacks the `PROCESS`
/// privilege.
async fn wait_for_blocked_statements(
    pool: &MySqlPool,
    statement_pattern: &str,
    num_statements: i64,
) {
    const QUERY: &str = "SELECT COUNT(*) FROM `information_schema`.`INNODB_TRX` `t` JOIN \
                         `information_schema`.`PROCESSLIST` `p` ON `p`.`ID` = \
                         `t`.`trx_mysql_thread_id` WHERE `p`.`DB` = DATABASE() AND \
                         `t`.`trx_state` = 'LOCK WAIT' AND `p`.`INFO` LIKE ?;";
    loop {
        let num_blocked_statements: i64 = sqlx::query_scalar(QUERY)
            .bind(statement_pattern)
            .fetch_one(pool)
            .await
            .expect("the lock wait query should succeed");
        if num_blocked_statements >= num_statements {
            return;
        }
        // InnoDB refreshes its `INNODB_TRX` cache only on a read that comes more than 100 ms after
        // the previous read by any connection. Polling this slowly leaves such gaps even while
        // other tests poll concurrently, so the cache cannot stay stale.
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Waits until a connection to `pool`'s database reissues a statement whose text matches the
/// `LIKE` pattern `statement_pattern`, that is, until the statement's running time drops below a
/// running time seen earlier.
///
/// # Panics
///
/// Panics if the process list query fails.
async fn wait_for_reissued_statement(pool: &MySqlPool, statement_pattern: &str) {
    const QUERY: &str = "SELECT CAST(`TIME_MS` AS UNSIGNED) FROM \
                         `information_schema`.`PROCESSLIST` WHERE `DB` = DATABASE() AND `INFO` \
                         LIKE ?;";

    let mut max_running_time_ms = 0;
    loop {
        let running_time_ms: Option<u64> = sqlx::query_scalar(QUERY)
            .bind(statement_pattern)
            .fetch_optional(pool)
            .await
            .expect("the process list query should succeed");
        if let Some(running_time_ms) = running_time_ms {
            if running_time_ms < max_running_time_ms {
                return;
            }
            max_running_time_ms = running_time_ms;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Starts every job of `job_ids` through `storage` in one tree transition.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`InternalJobOrchestration::apply_tree_transition`]'s return values on failure.
async fn start(storage: &MariaDbStorageConnector, job_ids: &[JobId]) -> Result<(), DbError> {
    storage
        .apply_tree_transition(TreeTransition {
            updates: job_ids
                .iter()
                .map(|job_id| JobStateUpdate::new(*job_id, JobState::Ready, JobState::Running))
                .collect(),
        })
        .await
}

/// Moves every job of `job_ids` from `Running` to `Succeeded` with `outputs` through `storage` in
/// one tree transition.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`InternalJobOrchestration::apply_tree_transition`]'s return values on failure.
async fn succeed(
    storage: &MariaDbStorageConnector,
    job_ids: &[JobId],
    outputs: &[TaskOutput],
) -> Result<(), DbError> {
    storage
        .apply_tree_transition(TreeTransition {
            updates: job_ids
                .iter()
                .map(|job_id| JobStateUpdate {
                    outputs: Some(outputs.to_vec()),
                    ..JobStateUpdate::new(*job_id, JobState::Running, JobState::Succeeded)
                })
                .collect(),
        })
        .await
}

/// # Returns
///
/// Whether `job_id` has serialized outputs.
///
/// # Panics
///
/// Panics if the query fails.
async fn has_outputs(raw_pool: &MySqlPool, job_id: JobId) -> bool {
    sqlx::query_scalar::<_, bool>(
        "SELECT `serialized_job_outputs` IS NOT NULL FROM `jobs` WHERE `id` = ?;",
    )
    .bind(job_id)
    .fetch_one(raw_pool)
    .await
    .expect("the outputs query should succeed")
}
