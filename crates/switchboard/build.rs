fn main() {
    // Declared so `#![cfg_attr(coverage_nightly, ...)]` in main.rs does not
    // trip the `unexpected_cfgs` lint; cargo-llvm-cov sets this cfg on
    // nightly coverage builds.
    println!("cargo::rustc-check-cfg=cfg(coverage_nightly)");
}
