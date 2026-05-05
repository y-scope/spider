//! End-to-end tests for the TDL package executor against the `huntsman-complex` example crate.

use huntsman_complex_types::{Complex, ComplexVec};
use spider_core::types::{
    id::{JobId, ResourceGroupId, TaskId},
    io::TaskInput,
};
use spider_task_executor::{ExecutorError, TdlPackageManager};
use spider_tdl::{
    TaskContext,
    TdlError,
    Version,
    wire::{TaskInputsSerializer, TaskOutputsSerializer},
};

const PACKAGE_NAME: &str = "complex";

/// Reads the absolute path of the `huntsman-complex` cdylib from the [`SPIDER_TDL_PACKAGE_COMPLEX`]
/// environment variable.
///
/// # Returns
///
/// The path of `huntsman-complex` example cdylib.
fn lib_path() -> std::path::PathBuf {
    std::env::var_os("SPIDER_TDL_PACKAGE_COMPLEX")
        .map(std::path::PathBuf::from)
        .expect("library not found")
}

/// # Returns
///
/// An encoded task context for testing.
fn encode_ctx() -> Vec<u8> {
    let ctx = TaskContext {
        job_id: JobId::new(),
        task_id: TaskId::new(),
        task_instance_id: 1,
        resource_group_id: ResourceGroupId::new(),
    };
    rmp_serde::to_vec(&ctx).expect("failed to serialize `TaskContext`")
}

/// # Returns
///
/// A wire-format-encoded empty input.
fn encode_no_inputs() -> Vec<u8> {
    TaskInputsSerializer::new().release()
}

/// # Returns
///
/// An encoded complex vector with a pair of complex numbers.
fn encode_complex_vec_pair(a: &ComplexVec, b: &ComplexVec) -> anyhow::Result<Vec<u8>> {
    let mut inputs = TaskInputsSerializer::new();
    inputs.append(TaskInput::ValuePayload(rmp_serde::to_vec(a)?))?;
    inputs.append(TaskInput::ValuePayload(rmp_serde::to_vec(b)?))?;
    Ok(inputs.release())
}

/// # Returns
///
/// A decoded complex number from the byte buffer.
fn decode_complex(output_bytes: &[u8]) -> anyhow::Result<Complex> {
    let outputs = TaskOutputsSerializer::deserialize(output_bytes)?;
    anyhow::ensure!(
        outputs.len() == 1,
        "expected exactly one output payload, got {}",
        outputs.len()
    );
    Ok(rmp_serde::from_slice(&outputs[0])?)
}

/// # Returns
///
/// A decoded complex vector from the byte buffer.
fn decode_complex_vec(output_bytes: &[u8]) -> anyhow::Result<ComplexVec> {
    let outputs = TaskOutputsSerializer::deserialize(output_bytes)?;
    anyhow::ensure!(
        outputs.len() == 1,
        "expected exactly one output payload, got {}",
        outputs.len()
    );
    Ok(rmp_serde::from_slice(&outputs[0])?)
}

#[test]
#[ignore = "requires `huntsman-complex`"]
fn load_and_query_name() -> anyhow::Result<()> {
    let path = lib_path();
    let mut manager = TdlPackageManager::new();
    let name = manager.load(&path)?;
    assert_eq!(name, PACKAGE_NAME);
    let pkg = manager
        .get(PACKAGE_NAME)
        .expect("just-loaded package should be retrievable");
    assert_eq!(pkg.name(), PACKAGE_NAME);
    Ok(())
}

#[test]
#[ignore = "requires `huntsman-complex`"]
fn version_is_compatible() -> anyhow::Result<()> {
    let path = lib_path();
    let mut manager = TdlPackageManager::new();
    manager.load(&path)?;
    let pkg = manager.get(PACKAGE_NAME).expect("package should be loaded");
    assert_eq!(pkg.version(), Version::SPIDER_TDL);
    assert!(Version::SPIDER_TDL.is_compatible_with(&pkg.version()));
    Ok(())
}

#[test]
#[ignore = "requires `huntsman-complex`"]
fn duplicate_load_rejected() -> anyhow::Result<()> {
    let path = lib_path();
    let mut manager = TdlPackageManager::new();
    manager.load(&path)?;
    let err = manager
        .load(&path)
        .expect_err("expected duplicate load to fail");
    assert!(
        matches!(err, ExecutorError::DuplicatePackage(ref name) if name == PACKAGE_NAME),
        "unexpected error: {err:?}",
    );
    Ok(())
}

#[test]
#[ignore = "requires `huntsman-complex`"]
fn add_round_trip() -> anyhow::Result<()> {
    let path = lib_path();
    let mut manager = TdlPackageManager::new();
    manager.load(&path)?;
    let pkg = manager.get(PACKAGE_NAME).expect("package should be loaded");

    let a = ComplexVec {
        items: vec![
            Complex { re: 1.0, im: 2.0 },
            Complex { re: 3.0, im: 4.0 },
            Complex { re: -1.5, im: 0.5 },
        ],
    };
    let b = ComplexVec {
        items: vec![
            Complex { re: 10.0, im: 20.0 },
            Complex { re: -3.0, im: 0.0 },
            Complex { re: 0.5, im: -0.5 },
        ],
    };
    let outputs = pkg.execute_task(
        "complex::add",
        &encode_ctx(),
        &encode_complex_vec_pair(&a, &b)?,
    )?;
    let result = decode_complex_vec(&outputs)?;
    assert_eq!(
        result,
        ComplexVec {
            items: vec![
                Complex { re: 11.0, im: 22.0 },
                Complex { re: 0.0, im: 4.0 },
                Complex { re: -1.0, im: 0.0 },
            ],
        }
    );
    Ok(())
}

#[test]
#[ignore = "requires `huntsman-complex`"]
fn sub_round_trip() -> anyhow::Result<()> {
    let path = lib_path();
    let mut manager = TdlPackageManager::new();
    manager.load(&path)?;
    let pkg = manager.get(PACKAGE_NAME).expect("package should be loaded");

    let a = ComplexVec {
        items: vec![Complex { re: 5.0, im: 5.0 }, Complex { re: 1.0, im: 2.0 }],
    };
    let b = ComplexVec {
        items: vec![Complex { re: 1.0, im: 1.0 }, Complex { re: -1.0, im: -2.0 }],
    };
    let outputs = pkg.execute_task(
        "complex::sub",
        &encode_ctx(),
        &encode_complex_vec_pair(&a, &b)?,
    )?;
    let result = decode_complex_vec(&outputs)?;
    assert_eq!(
        result,
        ComplexVec {
            items: vec![Complex { re: 4.0, im: 4.0 }, Complex { re: 2.0, im: 4.0 },],
        }
    );
    Ok(())
}

#[test]
#[ignore = "requires `huntsman-complex`"]
fn add_length_mismatch_returns_execution_error() -> anyhow::Result<()> {
    let path = lib_path();
    let mut manager = TdlPackageManager::new();
    manager.load(&path)?;
    let pkg = manager.get(PACKAGE_NAME).expect("package should be loaded");

    let a = ComplexVec {
        items: vec![Complex { re: 1.0, im: 0.0 }],
    };
    let b = ComplexVec {
        items: vec![Complex { re: 1.0, im: 0.0 }, Complex { re: 2.0, im: 0.0 }],
    };
    let err = pkg
        .execute_task(
            "complex::add",
            &encode_ctx(),
            &encode_complex_vec_pair(&a, &b)?,
        )
        .expect_err("expected length-mismatch error");
    let ExecutorError::TaskError(TdlError::ExecutionError(msg)) = &err else {
        panic!("unexpected error: {err:?}");
    };
    assert!(msg.contains("length mismatch"), "unexpected message: {msg}");
    Ok(())
}

#[test]
#[ignore = "requires `huntsman-complex`"]
fn dot_product_round_trip() -> anyhow::Result<()> {
    let path = lib_path();
    let mut manager = TdlPackageManager::new();
    manager.load(&path)?;
    let pkg = manager.get(PACKAGE_NAME).expect("package should be loaded");

    // Σ a_i * b_i with:
    //   (1+2i)*(3+4i) = (3-8) + (4+6)i  = -5 + 10i
    //   (5+6i)*(7+8i) = (35-48) + (40+42)i = -13 + 82i
    //   sum = -18 + 92i
    let a = ComplexVec {
        items: vec![Complex { re: 1.0, im: 2.0 }, Complex { re: 5.0, im: 6.0 }],
    };
    let b = ComplexVec {
        items: vec![Complex { re: 3.0, im: 4.0 }, Complex { re: 7.0, im: 8.0 }],
    };
    let outputs = pkg.execute_task(
        "complex::dot_product",
        &encode_ctx(),
        &encode_complex_vec_pair(&a, &b)?,
    )?;
    let result = decode_complex(&outputs)?;
    assert_eq!(
        result,
        Complex {
            re: -18.0,
            im: 92.0
        }
    );
    Ok(())
}

#[test]
#[ignore = "requires `huntsman-complex`"]
fn cross_product_round_trip_real_basis() -> anyhow::Result<()> {
    let path = lib_path();
    let mut manager = TdlPackageManager::new();
    manager.load(&path)?;
    let pkg = manager.get(PACKAGE_NAME).expect("package should be loaded");

    // Standard real-valued cross product: i_hat × j_hat = k_hat.
    let a = ComplexVec {
        items: vec![
            Complex { re: 1.0, im: 0.0 },
            Complex { re: 0.0, im: 0.0 },
            Complex { re: 0.0, im: 0.0 },
        ],
    };
    let b = ComplexVec {
        items: vec![
            Complex { re: 0.0, im: 0.0 },
            Complex { re: 1.0, im: 0.0 },
            Complex { re: 0.0, im: 0.0 },
        ],
    };
    let outputs = pkg.execute_task(
        "complex::cross_product",
        &encode_ctx(),
        &encode_complex_vec_pair(&a, &b)?,
    )?;
    let result = decode_complex_vec(&outputs)?;
    assert_eq!(
        result,
        ComplexVec {
            items: vec![
                Complex { re: 0.0, im: 0.0 },
                Complex { re: 0.0, im: 0.0 },
                Complex { re: 1.0, im: 0.0 },
            ],
        }
    );
    Ok(())
}

#[test]
#[ignore = "requires `huntsman-complex`"]
fn cross_product_wrong_length_returns_error() -> anyhow::Result<()> {
    let path = lib_path();
    let mut manager = TdlPackageManager::new();
    manager.load(&path)?;
    let pkg = manager.get(PACKAGE_NAME).expect("package should be loaded");

    let a = ComplexVec {
        items: vec![Complex { re: 1.0, im: 0.0 }, Complex { re: 2.0, im: 0.0 }],
    };
    let b = ComplexVec {
        items: vec![Complex { re: 3.0, im: 0.0 }, Complex { re: 4.0, im: 0.0 }],
    };
    let err = pkg
        .execute_task(
            "complex::cross_product",
            &encode_ctx(),
            &encode_complex_vec_pair(&a, &b)?,
        )
        .expect_err("expected length-3 error");
    let ExecutorError::TaskError(TdlError::ExecutionError(msg)) = &err else {
        panic!("unexpected error: {err:?}");
    };
    assert!(msg.contains("length 3"), "unexpected message: {msg}");
    Ok(())
}

#[test]
#[ignore = "requires `huntsman-complex`"]
fn always_fail_propagates_custom_error() -> anyhow::Result<()> {
    let path = lib_path();
    let mut manager = TdlPackageManager::new();
    manager.load(&path)?;
    let pkg = manager.get(PACKAGE_NAME).expect("package should be loaded");

    let err = pkg
        .execute_task("complex::always_fail", &encode_ctx(), &encode_no_inputs())
        .expect_err("`always_fail` should always fail");
    assert!(
        matches!(err, ExecutorError::TaskError(TdlError::Custom(_))),
        "unexpected error: {err:?}",
    );
    Ok(())
}

#[test]
#[ignore = "requires `huntsman-complex`"]
fn unknown_task_returns_task_not_found() -> anyhow::Result<()> {
    let path = lib_path();
    let mut manager = TdlPackageManager::new();
    manager.load(&path)?;
    let pkg = manager.get(PACKAGE_NAME).expect("package should be loaded");

    let err = pkg
        .execute_task("complex::nope", &encode_ctx(), &encode_no_inputs())
        .expect_err("unknown task should fail");
    let ExecutorError::TaskError(TdlError::TaskNotFound(name)) = &err else {
        panic!("unexpected error: {err:?}");
    };
    assert_eq!(name, "complex::nope");
    Ok(())
}
