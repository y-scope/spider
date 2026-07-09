//! Reference TDL package: dense-neuron computation.

#![allow(clippy::too_many_arguments)]

mod task_decl {
    use spider_tdl::TaskContext;
    use spider_tdl::TdlError;
    use spider_tdl::r#std::double;
    use spider_tdl::task;

    #[task(name = "neuron::dense_relu")]
    pub fn dense_relu(
        _ctx: TaskContext,
        x0: double,
        x1: double,
        x2: double,
        x3: double,
        x4: double,
        x5: double,
        x6: double,
        x7: double,
        x8: double,
        x9: double,
        x10: double,
        x11: double,
        x12: double,
        x13: double,
        x14: double,
        x15: double,
        x16: double,
        x17: double,
        x18: double,
        x19: double,
        x20: double,
        x21: double,
        x22: double,
        x23: double,
        x24: double,
    ) -> Result<double, TdlError> {
        Ok(huntsman_nn_core::dense_relu(&[
            x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16, x17, x18,
            x19, x20, x21, x22, x23, x24,
        ]))
    }

    #[task(name = "neuron::dense_sigmoid")]
    pub fn dense_sigmoid(
        _ctx: TaskContext,
        x0: double,
        x1: double,
        x2: double,
        x3: double,
        x4: double,
        x5: double,
        x6: double,
        x7: double,
        x8: double,
        x9: double,
        x10: double,
        x11: double,
        x12: double,
        x13: double,
        x14: double,
        x15: double,
        x16: double,
        x17: double,
        x18: double,
        x19: double,
        x20: double,
        x21: double,
        x22: double,
        x23: double,
        x24: double,
    ) -> Result<double, TdlError> {
        Ok(huntsman_nn_core::dense_sigmoid(&[
            x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16, x17, x18,
            x19, x20, x21, x22, x23, x24,
        ]))
    }

    #[task(name = "neuron::dense_identity")]
    pub fn dense_identity(
        _ctx: TaskContext,
        x0: double,
        x1: double,
        x2: double,
        x3: double,
        x4: double,
        x5: double,
        x6: double,
        x7: double,
        x8: double,
        x9: double,
        x10: double,
        x11: double,
        x12: double,
        x13: double,
        x14: double,
        x15: double,
        x16: double,
        x17: double,
        x18: double,
        x19: double,
        x20: double,
        x21: double,
        x22: double,
        x23: double,
        x24: double,
    ) -> Result<double, TdlError> {
        Ok(huntsman_nn_core::dense_identity(&[
            x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16, x17, x18,
            x19, x20, x21, x22, x23, x24,
        ]))
    }
}

spider_tdl::register_tdl_package! {
    package_name: "nn",
    tasks: [
        task_decl::dense_relu,
        task_decl::dense_sigmoid,
        task_decl::dense_identity,
    ],
}
