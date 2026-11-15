//! Records the commit and build date `--version` reports. See `build_info.rs`.
mod build_info;

fn main() {
    build_info::emit();
}
