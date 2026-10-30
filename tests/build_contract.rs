//! Guards the build/publish contract documented in `README.md`.
//!
//! The container image is the only artefact of this repository: the pipeline
//! builds it on every pull request and on every push to `main`, and publishes
//! it on semver tag pushes only. The build is deliberately kept trivial (one
//! `cargo build` of the real sources, no build cache), and three things
//! silently broke a release before: the builder image had no `protoc` for the
//! `tonic-build` build script, the stub sources of a dependency cache step
//! were reused as if they were the real ones, and an unconditional image push
//! published non-release builds.

use std::path::{Path, PathBuf};

fn repo_file(relative: &str) -> String {
    let path: PathBuf = Path::new(env!("CARGO_MANIFEST_DIR")).join(relative);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("cannot read {}: {err}", path.display()))
}

/// Split a Dockerfile into logical instructions, joining the line
/// continuations of a multi-line `RUN`.
fn dockerfile_instructions() -> Vec<String> {
    let mut instructions = Vec::new();
    let mut current = String::new();
    for line in repo_file("Dockerfile").lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        match trimmed.strip_suffix('\\') {
            Some(head) => {
                current.push_str(head.trim());
                current.push(' ');
            }
            None => {
                current.push_str(trimmed);
                instructions.push(std::mem::take(&mut current));
            }
        }
    }
    instructions
}

#[test]
fn dockerfile_installs_protoc_in_the_builder_stage() {
    let dockerfile = repo_file("Dockerfile");

    let builder = dockerfile
        .find("FROM rust:")
        .expect("Dockerfile must have a Rust builder stage");
    let runtime = dockerfile
        .find("FROM gcr.io/distroless")
        .expect("Dockerfile must have a distroless runtime stage");
    let protoc = dockerfile
        .find("protobuf-compiler")
        .expect("Dockerfile must install the protobuf compiler");
    let first_cargo = dockerfile
        .find("cargo build")
        .expect("Dockerfile must run cargo build");

    assert!(
        builder < protoc && protoc < runtime,
        "protobuf-compiler must be installed in the builder stage"
    );
    assert!(
        protoc < first_cargo,
        "protoc must be installed before the first cargo invocation, \
         the build script needs it"
    );
}

#[test]
fn dockerfile_builds_the_real_sources_once() {
    let instructions = dockerfile_instructions();

    let builds: Vec<&String> = instructions
        .iter()
        .filter(|instruction| instruction.contains("cargo build"))
        .collect();
    assert_eq!(
        builds.len(),
        1,
        "the builder stage must compile the sources once, a stub build \
         leaves artefacts that cargo may reuse: {builds:?}"
    );
    assert!(
        !repo_file("Dockerfile").contains("dep cache"),
        "the Dockerfile must not fake a build to warm a dependency cache"
    );
}

#[test]
fn workflow_builds_on_pull_request_and_main() {
    let workflow = repo_file(".github/workflows/docker.yml");

    assert!(
        workflow.contains("pull_request:"),
        "the image must be built on every pull request"
    );
    assert!(
        workflow.contains("- main"),
        "the image must be built on every push to the default branch"
    );
    assert!(
        workflow.contains("v[0-9]+.[0-9]+.[0-9]+"),
        "semver tags must trigger the release"
    );
}

#[test]
fn workflow_publishes_only_on_release() {
    let workflow = repo_file(".github/workflows/docker.yml");

    // The `push:` key of the workflow triggers is unrelated, look at the
    // input of the build step itself.
    let build_step = workflow
        .find("docker/build-push-action")
        .expect("the workflow must use docker/build-push-action");
    let push_line = workflow[build_step..]
        .lines()
        .find(|line| line.trim_start().starts_with("push:"))
        .expect("the build step must declare a push mode");
    assert!(
        push_line.contains("${{"),
        "the push mode must be conditional, a constant `push: true` would \
         publish pull request builds: {push_line}"
    );
}

#[test]
fn workflow_keeps_no_build_cache() {
    let workflow = repo_file(".github/workflows/docker.yml");

    assert!(
        !workflow.contains("cache-from") && !workflow.contains("cache-to"),
        "the pipeline must not keep a docker build cache"
    );
}
