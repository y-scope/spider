use spider_tdl::{TaskContext, TdlError, task, tdl_types::double};

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Complex {
    pub re: double,
    pub im: double,
}

#[task(name = "complex::add")]
fn add(ctx: TaskContext, a: Complex, b: Complex) -> Result<Complex, TdlError> {
    let _ = ctx;
    Ok(Complex {
        re: a.re + b.re,
        im: a.im + b.im,
    })
}

#[task(name = "complex::sub")]
fn sub(ctx: TaskContext, a: Complex, b: Complex) -> Result<Complex, TdlError> {
    let _ = ctx;
    Ok(Complex {
        re: a.re - b.re,
        im: a.im - b.im,
    })
}

#[task(name = "complex::mul")]
fn mul(ctx: TaskContext, a: Complex, b: Complex) -> Result<Complex, TdlError> {
    let _ = ctx;
    Ok(Complex {
        re: a.im.mul_add(-b.im, a.re * b.re),
        im: a.im.mul_add(b.re, a.re * b.im),
    })
}

#[task(name = "complex::div")]
fn div(ctx: TaskContext, a: Complex, b: Complex) -> Result<Complex, TdlError> {
    let _ = ctx;
    let denom = b.im.mul_add(b.im, b.re * b.re);
    if denom == 0.0 {
        return Err(TdlError::ExecutionError("division by zero".to_owned()));
    }
    Ok(Complex {
        re: a.im.mul_add(b.im, a.re * b.re) / denom,
        im: a.re.mul_add(-b.im, a.im * b.re) / denom,
    })
}

#[task(name = "complex::always_fail")]
fn always_fail(ctx: TaskContext) -> Result<(), TdlError> {
    let _ = ctx;
    Err(TdlError::Custom("this task always fails".to_owned()))
}

spider_tdl::register_tasks! {
    package_name: "complex",
    tasks: [add, sub, mul, div, always_fail]
}
