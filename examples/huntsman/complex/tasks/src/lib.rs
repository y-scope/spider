//! Reference TDL package: complex-vector arithmetic.

mod task_decl {
    use huntsman_complex_types::{Complex, ComplexVec};
    use spider_tdl::{TaskContext, TdlError, task};

    #[task(name = "complex::add")]
    pub fn add(_ctx: TaskContext, a: ComplexVec, b: ComplexVec) -> Result<ComplexVec, TdlError> {
        require_same_length(&a, &b, "add")?;
        let items = a
            .items
            .iter()
            .zip(b.items.iter())
            .map(|(x, y)| Complex {
                re: x.re + y.re,
                im: x.im + y.im,
            })
            .collect();
        Ok(ComplexVec { items })
    }

    #[task(name = "complex::sub")]
    pub fn sub(_ctx: TaskContext, a: ComplexVec, b: ComplexVec) -> Result<ComplexVec, TdlError> {
        require_same_length(&a, &b, "sub")?;
        let items = a
            .items
            .iter()
            .zip(b.items.iter())
            .map(|(x, y)| Complex {
                re: x.re - y.re,
                im: x.im - y.im,
            })
            .collect();
        Ok(ComplexVec { items })
    }

    #[task(name = "complex::dot_product")]
    pub fn dot_product(
        _ctx: TaskContext,
        a: ComplexVec,
        b: ComplexVec,
    ) -> Result<Complex, TdlError> {
        require_same_length(&a, &b, "dot_product")?;
        let mut acc = Complex { re: 0.0, im: 0.0 };
        for (x, y) in a.items.iter().zip(b.items.iter()) {
            let p = complex_mul(*x, *y);
            acc.re += p.re;
            acc.im += p.im;
        }
        Ok(acc)
    }

    #[task(name = "complex::cross_product")]
    pub fn cross_product(
        _ctx: TaskContext,
        a: ComplexVec,
        b: ComplexVec,
    ) -> Result<ComplexVec, TdlError> {
        if a.items.len() != 3 || b.items.len() != 3 {
            return Err(TdlError::ExecutionError(format!(
                "cross_product: requires both vectors of length 3 (got lhs={}, rhs={})",
                a.items.len(),
                b.items.len(),
            )));
        }
        let csub = |x: Complex, y: Complex| Complex {
            re: x.re - y.re,
            im: x.im - y.im,
        };
        let i = csub(
            complex_mul(a.items[1], b.items[2]),
            complex_mul(a.items[2], b.items[1]),
        );
        let j = csub(
            complex_mul(a.items[2], b.items[0]),
            complex_mul(a.items[0], b.items[2]),
        );
        let k = csub(
            complex_mul(a.items[0], b.items[1]),
            complex_mul(a.items[1], b.items[0]),
        );
        Ok(ComplexVec {
            items: vec![i, j, k],
        })
    }

    #[task(name = "complex::always_fail")]
    pub fn always_fail(_ctx: TaskContext) -> Result<(), TdlError> {
        Err(TdlError::Custom("this task always fails".to_owned()))
    }

    fn complex_mul(x: Complex, y: Complex) -> Complex {
        Complex {
            re: x.im.mul_add(-y.im, x.re * y.re),
            im: x.im.mul_add(y.re, x.re * y.im),
        }
    }

    fn require_same_length(a: &ComplexVec, b: &ComplexVec, op: &str) -> Result<(), TdlError> {
        if a.items.len() != b.items.len() {
            return Err(TdlError::ExecutionError(format!(
                "{op}: vector length mismatch (lhs={}, rhs={})",
                a.items.len(),
                b.items.len(),
            )));
        }
        Ok(())
    }
}

spider_tdl::register_tdl_package! {
    package_name: "complex",
    tasks: [
        task_decl::add,
        task_decl::sub,
        task_decl::dot_product,
        task_decl::cross_product,
        task_decl::always_fail
    ],
}
