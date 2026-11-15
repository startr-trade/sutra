//! Records the commit and build date the engine reports at startup. Shares the resolution
//! logic with the CLI's build script.
#[path = "../sutra-cli/build_info.rs"]
mod build_info;

fn main() {
    build_info::emit();
}
