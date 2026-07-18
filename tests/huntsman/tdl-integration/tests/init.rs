//! End-to-end tests for the TDL package init hook against the `integration-test-tasks` cdylib.

use spider_core::types::id::JobId;
use spider_core::types::id::ResourceGroupId;
use spider_core::types::id::TaskId;
use spider_core::types::io::TaskInputsSerializer;
use spider_task_executor::ExecutorError;
use spider_task_executor::TdlPackageManager;
use spider_tdl::TaskContext;
use spider_tdl::TdlError;

/// # Returns
///
/// The absolute path of the staged `integration-test-tasks` cdylib, derived from the
/// `SPIDER_TDL_PACKAGE_DIR` environment variable.
fn package_path() -> std::path::PathBuf {
    const PACKAGE_NAME: &str = "integration_test_tasks";
    let dir = std::env::var_os("SPIDER_TDL_PACKAGE_DIR")
        .map(std::path::PathBuf::from)
        .expect("`SPIDER_TDL_PACKAGE_DIR` not set");
    dir.join(PACKAGE_NAME).join(format!("lib{PACKAGE_NAME}.so"))
}

/// # Returns
///
/// An encoded task context for testing.
fn encode_ctx() -> Vec<u8> {
    let ctx = TaskContext::new(
        JobId::random(),
        TaskId::Index(0),
        1,
        ResourceGroupId::random(),
        None,
    )
    .expect("failed to build `TaskContext`");
    rmp_serde::to_vec(&ctx).expect("failed to serialize `TaskContext`")
}

/// # Returns
///
/// A wire-format-encoded empty input.
fn encode_no_inputs() -> Vec<u8> {
    TaskInputsSerializer::new().release()
}

#[test]
#[ignore = "requires `integration-test-tasks` cdylib"]
fn init_hook_runs_before_task_dispatch() -> anyhow::Result<()> {
    let path = package_path();
    let mut manager = TdlPackageManager::new();
    manager.load(&path)?;
    let pkg = manager
        .get("integration_test_tasks")
        .expect("package should be loaded");
    pkg.execute_task("assert_initialized", &encode_ctx(), &encode_no_inputs())?;
    Ok(())
}

#[test]
#[ignore = "requires `integration-test-tasks` cdylib"]
fn failing_init_aborts_load() -> anyhow::Result<()> {
    const ENV_SPIDER_TEST_TDL_INIT_SHOULD_FAIL: &str = "SPIDER_TEST_TDL_INIT_SHOULD_FAIL";
    // SAFETY: `cargo nextest` (see taskfiles/test.yaml) runs each test in its own process
    // (https://nexte.st/docs/design/why-process-per-test/), so this can't leak into other tests;
    // and no other thread exists in this process yet, so nothing reads the environment
    // concurrently. Under plain `cargo test` this would be unsound and could fail unrelated tests.
    // The init hook failing with this var set is the behavior under test.
    unsafe { std::env::set_var(ENV_SPIDER_TEST_TDL_INIT_SHOULD_FAIL, "1") };
    let path = package_path();
    let mut manager = TdlPackageManager::new();
    let err = manager
        .load(&path)
        .expect_err("load should fail when init errors");
    // SAFETY: see above.
    unsafe { std::env::remove_var(ENV_SPIDER_TEST_TDL_INIT_SHOULD_FAIL) };
    let ExecutorError::PackageInitError(TdlError::ExecutionError(msg)) = &err else {
        panic!("unexpected error: {err:?}");
    };
    anyhow::ensure!(
        msg.contains("init failure requested"),
        "unexpected message: {msg}"
    );
    Ok(())
}
