use std::env;
use std::fs;
use std::path::PathBuf;

/// The environment variable that, if set, forces the build script to regenerate the protobuf code.
const SPIDER_PROTO_RUST_GENERATE_FROM_SOURCE: &str = "SPIDER_PROTO_RUST_GENERATE_FROM_SOURCE";

/// The default destination directory for generated protobuf code, relative to the crate root.
const SPIDER_PROTO_RUST_GENERATED_DIR: &str = "src/generated";

/// The root of the protobuf source files.
const SPIDER_PROTO_ROOT: &str = "spider-proto";

/// The protobuf source files to compile, relative to [`SPIDER_PROTO_ROOT`].
const SPIDER_PROTO_SOURCE_FILES: &[&str] = &[
    "common/common.proto",
    "scheduler/scheduler.proto",
    "storage/storage.proto",
];

fn main() {
    // Rerun the build script whenever the generation gate is toggled or changes value.
    println!("cargo:rerun-if-env-changed={SPIDER_PROTO_RUST_GENERATE_FROM_SOURCE}");

    let crate_root = PathBuf::from(
        env::var_os("CARGO_MANIFEST_DIR").expect("`CARGO_MANIFEST_DIR` env var not set"),
    );
    let components_root = crate_root
        .parent()
        .expect("`CARGO_MANIFEST_DIR` is not a directory");

    let spider_proto_root = components_root.join(SPIDER_PROTO_ROOT);
    let spider_proto_sources = SPIDER_PROTO_SOURCE_FILES
        .iter()
        .map(|relative_path| {
            let abs_path = spider_proto_root.join(relative_path);
            println!("cargo:rerun-if-changed={}", abs_path.display());
            abs_path
        })
        .collect::<Vec<_>>();

    if env::var_os(SPIDER_PROTO_RUST_GENERATE_FROM_SOURCE).is_none() {
        // The committed generated code is used as-is.
        return;
    }

    let generate_from_source =
        env::var_os(SPIDER_PROTO_RUST_GENERATE_FROM_SOURCE).is_some_and(|val| {
            const ON: &str = "ON";
            const OFF: &str = "OFF";
            match val.to_str() {
                Some(ON) => true,
                Some(OFF) => false,
                _ => panic!(
                    "invalid value for {SPIDER_PROTO_RUST_GENERATE_FROM_SOURCE}: expected '{ON}' \
                     or '{OFF}'"
                ),
            }
        });

    if !generate_from_source {
        // The committed generated code is used as-is.
        return;
    }

    let out_dir = crate_root.join(SPIDER_PROTO_RUST_GENERATED_DIR);
    if out_dir.exists() {
        fs::remove_dir_all(&out_dir).expect("failed to remove existing generated code");
    }
    fs::create_dir_all(&out_dir).expect("failed to create output dir for generated code");

    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .out_dir(&out_dir)
        .compile_protos(spider_proto_sources.as_ref(), &[spider_proto_root])
        .inspect_err(|e| eprintln!("Failed to compile `spider-proto`: {e:?}"))
        .expect("proto compilation failed");

    // NOTE: The generated outputs are deliberately NOT tracked with `cargo:rerun-if-changed`. Cargo
    // compares the tracked paths' mtimes against the build script's recorded output file, whose
    // mtime is not guaranteed to postdate files written by this script in the same run, so tracking
    // our own outputs would make every subsequent build appear dirty.
}
