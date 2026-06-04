use std::{env, fs, path::Path};

const GENERATED_ENV_VAR: &str = "SPIDER_PROTO_GENERATED_DIR";
const PROTO_FILES: &[&str] = &["../spider-proto/storage.proto"];
const PROTO_INCLUDES: &[&str] = &["../spider-proto"];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    for proto_file in PROTO_FILES {
        println!("cargo:rerun-if-changed={proto_file}");
    }
    println!("cargo:rerun-if-env-changed={GENERATED_ENV_VAR}");

    if let Some(generated_dir) = env::var_os(GENERATED_ENV_VAR) {
        let generated_dir = Path::new(&generated_dir);
        if generated_dir.exists() {
            fs::remove_dir_all(generated_dir)?;
        }
        fs::create_dir_all(generated_dir)?;
        compile_protos(generated_dir)?;
    }

    Ok(())
}

fn compile_protos(output_dir: impl AsRef<Path>) -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .out_dir(output_dir)
        .compile_protos(PROTO_FILES, PROTO_INCLUDES)?;

    Ok(())
}
