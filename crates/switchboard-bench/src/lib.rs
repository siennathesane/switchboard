//! Shared machinery for the switchboard-bench and switchboard-soak
//! drivers: rate sampling / percentiles ([`stats`]) and the soak +
//! stability engine ([`soak`]).

#[global_allocator]
static GLOBAL_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

pub mod soak;
pub mod stats;
