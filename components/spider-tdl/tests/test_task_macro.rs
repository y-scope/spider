//! End-to-end tests for the `#[task]` proc macro.
//!
//! These tests live in the integration-test crate so that the macro is consumed exactly the way a
//! downstream TDL package would consume it: through the `spider_tdl::task` re-export, with the
//! generated code resolving `::spider_tdl` and `::serde` as external crates.

use serde::{Deserialize, Serialize};
use spider_core::types::{
    id::{JobId, ResourceGroupId, TaskId},
    io::TaskInput,
};
use spider_tdl::{
    Task,
    TaskContext,
    TaskHandler,
    TaskHandlerImpl,
    TdlError,
    r#std::int32,
    task,
    wire::{TaskInputsSerializer, TaskOutputsSerializer},
};

type AliasedContext = TaskContext;
type _AliasedTdlError = TdlError;

#[derive(Deserialize, Serialize, Debug, PartialEq, Eq)]
struct Point {
    x: int32,
    y: int32,
}

#[task]
fn add(_ctx: TaskContext, a: int32, b: int32) -> Result<(int32,), TdlError> {
    Ok((a + b,))
}

#[task]
fn divmod(_ctx: TaskContext, a: int32, b: int32) -> Result<(int32, int32), TdlError> {
    if b == 0 {
        return Err(TdlError::ExecutionError("division by zero".to_owned()));
    }
    Ok((a / b, a % b))
}

#[task]
fn double_it(_ctx: TaskContext, x: int32) -> Result<int32, TdlError> {
    Ok(x * 2)
}

#[task]
fn answer(_ctx: TaskContext) -> Result<(int32,), TdlError> {
    Ok((42,))
}

#[task(name = "math::custom_name")]
fn renamed(_ctx: TaskContext, x: int32) -> Result<(int32,), TdlError> {
    Ok((x,))
}

#[task]
fn aliased_ctx(_ctx: AliasedContext, x: int32) -> Result<(int32,), TdlError> {
    Ok((x,))
}

#[task]
fn aliased_error(_ctx: TaskContext, x: int32) -> Result<(int32,), _AliasedTdlError> {
    Ok((x,))
}

#[task]
fn translate(_ctx: TaskContext, p: Point, dx: int32, dy: int32) -> Result<(Point,), TdlError> {
    Ok((Point {
        x: p.x + dx,
        y: p.y + dy,
    },))
}

/// # Returns
///
/// A mocked encoded task context for testing.
fn make_encoded_ctx() -> Vec<u8> {
    let ctx = TaskContext {
        job_id: JobId::new(),
        task_id: TaskId::new(),
        task_instance_id: 1,
        resource_group_id: ResourceGroupId::new(),
    };
    rmp_serde::to_vec(&ctx).expect("failed to serialize `TaskContext`")
}

/// Appends the given value to the input serializer.
///
/// # Type Parameters
///
/// * `Val` - The type of the value to append.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`rmp_serde::to_vec`]'s return values on failure.
/// * Forwards [`TaskInputsSerializer::append`]'s return values on failure.
fn append_value<Val: Serialize>(
    inputs: &mut TaskInputsSerializer,
    value: &Val,
) -> anyhow::Result<()> {
    inputs.append(TaskInput::ValuePayload(rmp_serde::to_vec(value)?))?;
    Ok(())
}

/// Decodes the wire-format output buffer and deserializes the payload at the given index.
///
/// # Type Parameters
///
/// * `Val` - The type to deserialize the indexed payload into.
///
/// # Returns
///
/// The deserialized value at `index` on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards [`TaskOutputsSerializer::deserialize`]'s return values on failure.
/// * Forwards [`rmp_serde::from_slice`]'s return values on failure.
///
/// # Panics
///
/// Panics if `index` is out of bounds for the decoded output payloads.
fn decode_outputs<Val: for<'de> Deserialize<'de>>(
    bytes: &[u8],
    index: usize,
) -> anyhow::Result<Val> {
    let outputs = TaskOutputsSerializer::deserialize(bytes)?;
    Ok(rmp_serde::from_slice(
        outputs.get(index).expect("index out of bound"),
    )?)
}

#[test]
fn task_name_defaults_to_function_name() {
    assert_eq!(<add as Task>::NAME, "add");
    assert_eq!(<divmod as Task>::NAME, "divmod");
    assert_eq!(<answer as Task>::NAME, "answer");
}

#[test]
fn task_name_can_be_overridden() {
    assert_eq!(<renamed as Task>::NAME, "math::custom_name");
}

#[test]
fn aliased_task_context_compiles_and_runs() -> anyhow::Result<()> {
    const INPUT: int32 = 7;

    let handler = TaskHandlerImpl::<aliased_ctx>::new();

    let mut inputs = TaskInputsSerializer::new();
    append_value(&mut inputs, &INPUT)?;
    let raw_args = inputs.release();

    let result = handler.execute_raw(&make_encoded_ctx(), &raw_args);
    let bytes = result.map_err(|e| anyhow::anyhow!("expected outputs, got error bytes: {e:?}"))?;

    let value: int32 = decode_outputs(&bytes, 0)?;
    assert_eq!(value, INPUT);
    Ok(())
}

#[test]
fn aliased_tdl_error_compiles_and_runs() -> anyhow::Result<()> {
    const INPUT: int32 = 11;

    let handler = TaskHandlerImpl::<aliased_error>::new();

    let mut inputs = TaskInputsSerializer::new();
    append_value(&mut inputs, &INPUT)?;
    let raw_args = inputs.release();

    let result = handler.execute_raw(&make_encoded_ctx(), &raw_args);
    let bytes = result.map_err(|e| anyhow::anyhow!("expected outputs, got error bytes: {e:?}"))?;

    let value: int32 = decode_outputs(&bytes, 0)?;
    assert_eq!(value, INPUT);
    Ok(())
}

#[test]
fn handler_runs_tuple_return_task() -> anyhow::Result<()> {
    const DIVIDEND: int32 = 17;
    const DIVISOR: int32 = 5;
    const EXPECTED_QUOTIENT: int32 = DIVIDEND / DIVISOR;
    const EXPECTED_REMAINDER: int32 = DIVIDEND % DIVISOR;

    let handler = TaskHandlerImpl::<divmod>::new();
    assert_eq!(handler.name(), "divmod");

    let mut inputs = TaskInputsSerializer::new();
    append_value(&mut inputs, &DIVIDEND)?;
    append_value(&mut inputs, &DIVISOR)?;
    let raw_args = inputs.release();

    let result = handler.execute_raw(&make_encoded_ctx(), &raw_args);
    let bytes = result.map_err(|e| anyhow::anyhow!("expected outputs, got error bytes: {e:?}"))?;

    let outputs = TaskOutputsSerializer::deserialize(&bytes)?;
    assert_eq!(outputs.len(), 2);
    let quotient: int32 = rmp_serde::from_slice(&outputs[0])?;
    let remainder: int32 = rmp_serde::from_slice(&outputs[1])?;
    assert_eq!(quotient, EXPECTED_QUOTIENT);
    assert_eq!(remainder, EXPECTED_REMAINDER);
    Ok(())
}

#[test]
fn handler_runs_single_value_return_task() -> anyhow::Result<()> {
    const INPUT: int32 = 21;
    const EXPECTED_OUTPUT: int32 = INPUT * 2;

    let handler = TaskHandlerImpl::<double_it>::new();

    let mut inputs = TaskInputsSerializer::new();
    append_value(&mut inputs, &INPUT)?;
    let raw_args = inputs.release();

    let result = handler.execute_raw(&make_encoded_ctx(), &raw_args);
    let bytes = result.map_err(|e| anyhow::anyhow!("expected outputs, got error bytes: {e:?}"))?;

    let value: int32 = decode_outputs(&bytes, 0)?;
    assert_eq!(value, EXPECTED_OUTPUT);
    Ok(())
}

#[test]
fn handler_runs_no_params_task() -> anyhow::Result<()> {
    const EXPECTED_VALUE: int32 = 42;

    let handler = TaskHandlerImpl::<answer>::new();

    let raw_args = TaskInputsSerializer::new().release();
    let result = handler.execute_raw(&make_encoded_ctx(), &raw_args);
    let bytes = result.map_err(|e| anyhow::anyhow!("expected outputs, got error bytes: {e:?}"))?;

    let value: int32 = decode_outputs(&bytes, 0)?;
    assert_eq!(value, EXPECTED_VALUE);
    Ok(())
}

#[test]
fn handler_runs_struct_param_task() -> anyhow::Result<()> {
    const INITIAL_POINT: Point = Point { x: 1, y: 2 };
    const DX: int32 = 10;
    const DY: int32 = 20;
    const EXPECTED_POINT: Point = Point {
        x: INITIAL_POINT.x + DX,
        y: INITIAL_POINT.y + DY,
    };

    let handler = TaskHandlerImpl::<translate>::new();

    let mut inputs = TaskInputsSerializer::new();
    append_value(&mut inputs, &INITIAL_POINT)?;
    append_value(&mut inputs, &DX)?;
    append_value(&mut inputs, &DY)?;
    let raw_args = inputs.release();

    let result = handler.execute_raw(&make_encoded_ctx(), &raw_args);
    let bytes = result.map_err(|e| anyhow::anyhow!("expected outputs, got error bytes: {e:?}"))?;

    let translated: Point = decode_outputs(&bytes, 0)?;
    assert_eq!(translated, EXPECTED_POINT);
    Ok(())
}

#[test]
fn handler_propagates_task_error() -> anyhow::Result<()> {
    const DIVIDEND: int32 = 10;
    const DIVISOR: int32 = 0;

    let handler = TaskHandlerImpl::<divmod>::new();

    let mut inputs = TaskInputsSerializer::new();
    append_value(&mut inputs, &DIVIDEND)?;
    append_value(&mut inputs, &DIVISOR)?;
    let raw_args = inputs.release();

    let result = handler.execute_raw(&make_encoded_ctx(), &raw_args);
    let Err(bytes) = result else {
        panic!("expected error bytes, got outputs");
    };

    let err: TdlError = rmp_serde::from_slice(&bytes)?;
    assert!(matches!(err, TdlError::ExecutionError(ref msg) if msg == "division by zero"));
    Ok(())
}

#[test]
fn direct_execute_call_round_trips() -> anyhow::Result<()> {
    const OPERAND_A: int32 = 7;
    const OPERAND_B: int32 = 35;
    const EXPECTED_SUM: int32 = OPERAND_A + OPERAND_B;

    let ctx = TaskContext {
        job_id: JobId::new(),
        task_id: TaskId::new(),
        task_instance_id: 1,
        resource_group_id: ResourceGroupId::new(),
    };

    let mut inputs = TaskInputsSerializer::new();
    append_value(&mut inputs, &OPERAND_A)?;
    append_value(&mut inputs, &OPERAND_B)?;
    let raw_args = inputs.release();

    let params = TaskInputsSerializer::deserialize::<<add as Task>::Params>(&raw_args)?;
    let (sum,) = <add as Task>::execute(ctx, params)?;
    assert_eq!(sum, EXPECTED_SUM);
    Ok(())
}
