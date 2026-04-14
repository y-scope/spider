use std::path::Path;

use spider_core::types::{
    id::{JobId, TaskId},
    io::TaskInput,
};
use spider_task_executor::TdlPackageLoader;
use spider_tdl::{
    TaskContext,
    wire::{TaskInputs, TaskOutputs},
};

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct Complex {
    re: f64,
    im: f64,
}

fn lib_path() -> Option<String> {
    std::env::var("SPIDER_TDL_PACKAGE_COMPLEX").ok()
}

fn encode_ctx() -> Vec<u8> {
    let ctx = TaskContext {
        job_id: JobId::new(),
        task_id: TaskId::new(),
        task_instance_id: 1,
    };
    rmp_serde::to_vec(&ctx).expect("TaskContext serialization failed")
}

fn encode_inputs(a: &Complex, b: &Complex) -> Vec<u8> {
    let mut inputs = TaskInputs::new();
    inputs
        .append(TaskInput::ValuePayload(
            rmp_serde::to_vec(a).expect("complex serialization failed"),
        ))
        .expect("append failed");
    inputs
        .append(TaskInput::ValuePayload(
            rmp_serde::to_vec(b).expect("complex serialization failed"),
        ))
        .expect("append failed");
    inputs.release()
}

fn encode_empty_inputs() -> Vec<u8> {
    TaskInputs::new().release()
}

fn decode_complex(output_bytes: &[u8]) -> Complex {
    let outputs = TaskOutputs::deserialize(output_bytes).expect("output deserialization failed");
    assert_eq!(outputs.len(), 1, "expected exactly one output element");
    rmp_serde::from_slice(&outputs[0]).expect("complex deserialization failed")
}

#[test]
fn load_and_get_package_name() -> anyhow::Result<()> {
    let Some(path) = lib_path() else {
        return Ok(());
    };
    let mut loader = TdlPackageLoader::new();
    let name = loader.load(Path::new(&path))?;
    assert_eq!(name, "complex");
    Ok(())
}

#[test]
fn duplicate_load_rejected() -> anyhow::Result<()> {
    let Some(path) = lib_path() else {
        return Ok(());
    };
    let mut loader = TdlPackageLoader::new();
    loader.load(Path::new(&path))?;

    let err = loader
        .load(Path::new(&path))
        .expect_err("expected duplicate package error");
    assert!(err.to_string().contains("duplicate"));
    Ok(())
}

#[test]
fn add() -> anyhow::Result<()> {
    let Some(path) = lib_path() else {
        return Ok(());
    };
    let mut loader = TdlPackageLoader::new();
    loader.load(Path::new(&path))?;
    let package = loader.get("complex").expect("package not loaded");

    let a = Complex { re: 1.0, im: 2.0 };
    let b = Complex { re: 3.0, im: 4.0 };
    let result = decode_complex(&package.execute_task(
        "complex::add",
        &encode_ctx(),
        &encode_inputs(&a, &b),
    )?);

    assert_eq!(result, Complex { re: 4.0, im: 6.0 });
    Ok(())
}

#[test]
fn sub() -> anyhow::Result<()> {
    let Some(path) = lib_path() else {
        return Ok(());
    };
    let mut loader = TdlPackageLoader::new();
    loader.load(Path::new(&path))?;
    let package = loader.get("complex").expect("package not loaded");

    let a = Complex { re: 5.0, im: 3.0 };
    let b = Complex { re: 2.0, im: 1.0 };
    let result = decode_complex(&package.execute_task(
        "complex::sub",
        &encode_ctx(),
        &encode_inputs(&a, &b),
    )?);

    assert_eq!(result, Complex { re: 3.0, im: 2.0 });
    Ok(())
}

#[test]
fn mul() -> anyhow::Result<()> {
    let Some(path) = lib_path() else {
        return Ok(());
    };
    let mut loader = TdlPackageLoader::new();
    loader.load(Path::new(&path))?;
    let package = loader.get("complex").expect("package not loaded");

    // (1 + 2i) * (3 + 4i) = (1*3 - 2*4) + (1*4 + 2*3)i = -5 + 10i
    let a = Complex { re: 1.0, im: 2.0 };
    let b = Complex { re: 3.0, im: 4.0 };
    let result = decode_complex(&package.execute_task(
        "complex::mul",
        &encode_ctx(),
        &encode_inputs(&a, &b),
    )?);

    assert_eq!(result, Complex { re: -5.0, im: 10.0 });
    Ok(())
}

#[test]
fn div() -> anyhow::Result<()> {
    let Some(path) = lib_path() else {
        return Ok(());
    };
    let mut loader = TdlPackageLoader::new();
    loader.load(Path::new(&path))?;
    let package = loader.get("complex").expect("package not loaded");

    // (4 + 2i) / (2 + 0i) = (2 + 1i)
    let a = Complex { re: 4.0, im: 2.0 };
    let b = Complex { re: 2.0, im: 0.0 };
    let result = decode_complex(&package.execute_task(
        "complex::div",
        &encode_ctx(),
        &encode_inputs(&a, &b),
    )?);

    assert_eq!(result, Complex { re: 2.0, im: 1.0 });
    Ok(())
}

#[test]
fn div_by_zero() -> anyhow::Result<()> {
    let Some(path) = lib_path() else {
        return Ok(());
    };
    let mut loader = TdlPackageLoader::new();
    loader.load(Path::new(&path))?;
    let package = loader.get("complex").expect("package not loaded");

    let a = Complex { re: 1.0, im: 0.0 };
    let b = Complex { re: 0.0, im: 0.0 };
    let err = package
        .execute_task("complex::div", &encode_ctx(), &encode_inputs(&a, &b))
        .expect_err("expected division by zero error");

    assert!(err.to_string().contains("division by zero"));
    Ok(())
}

#[test]
fn always_fail() -> anyhow::Result<()> {
    let Some(path) = lib_path() else {
        return Ok(());
    };
    let mut loader = TdlPackageLoader::new();
    loader.load(Path::new(&path))?;
    let package = loader.get("complex").expect("package not loaded");

    let err = package
        .execute_task(
            "complex::always_fail",
            &encode_ctx(),
            &encode_empty_inputs(),
        )
        .expect_err("expected always_fail error");

    assert!(err.to_string().contains("this task always fails"));
    Ok(())
}

#[test]
fn task_not_found() -> anyhow::Result<()> {
    let Some(path) = lib_path() else {
        return Ok(());
    };
    let mut loader = TdlPackageLoader::new();
    loader.load(Path::new(&path))?;
    let package = loader.get("complex").expect("package not loaded");

    let err = package
        .execute_task(
            "complex::nonexistent",
            &encode_ctx(),
            &encode_empty_inputs(),
        )
        .expect_err("expected task not found error");

    assert!(err.to_string().contains("not found"));
    Ok(())
}
