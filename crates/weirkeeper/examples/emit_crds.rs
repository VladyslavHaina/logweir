//! Write the six `logweir.dev/v1alpha1` CRDs to a directory.
//!
//! ```text
//! cargo run -p weirkeeper --example emit_crds -- --out config/crd
//! ```
//!
//! `just crds` is that line. The CI `crd drift` arm renders into a temporary
//! directory and `diff -u`s the checked-in files against it, exactly as the
//! schema arm does for the scorecard: a CRD change is a format change, and a
//! format change that appears as a diff is one a reviewer sees.
//!
//! EVERY BYTE COMES FROM `weirkeeper::crds::render_all`, and this file adds
//! none. That is what lets `crd_shape.rs` compare the checked-in files against
//! the renderer IN-PROCESS, with no subprocess and no shell, and still be
//! testing the same bytes CI diffs.

use std::path::PathBuf;

fn main() -> std::process::ExitCode {
    let mut out: Option<PathBuf> = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--out" => match args.next() {
                Some(dir) => out = Some(PathBuf::from(dir)),
                None => {
                    eprintln!("emit_crds: --out needs a directory");
                    return std::process::ExitCode::from(2);
                }
            },
            "-h" | "--help" => {
                println!("usage: emit_crds --out <dir>");
                return std::process::ExitCode::SUCCESS;
            }
            other => {
                eprintln!("emit_crds: unexpected argument `{other}`; usage: --out <dir>");
                return std::process::ExitCode::from(2);
            }
        }
    }
    let Some(out) = out else {
        eprintln!("emit_crds: --out <dir> is required");
        return std::process::ExitCode::from(2);
    };

    if let Err(e) = std::fs::create_dir_all(&out) {
        eprintln!("emit_crds: cannot create {}: {e}", out.display());
        return std::process::ExitCode::FAILURE;
    }

    for rendered in weirkeeper::crds::render_all() {
        let path = out.join(rendered.file_name);
        if let Err(e) = std::fs::write(&path, rendered.yaml.as_bytes()) {
            eprintln!("emit_crds: cannot write {}: {e}", path.display());
            return std::process::ExitCode::FAILURE;
        }
        println!("{} -> {}", rendered.kind, path.display());
    }
    std::process::ExitCode::SUCCESS
}
