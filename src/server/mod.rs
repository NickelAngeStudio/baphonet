/*
Copyright (c) 2026  NickelAnge.Studio
Email               mathieu.grenier@nickelange.studio
Git                 https://github.com/NickelAngeStudio/baphonet

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

use std::net::SocketAddr;

#[doc(hidden)]
mod server;

#[doc(hidden)]
mod builder;

pub(crate) mod client;

#[doc(hidden)]
pub mod error;

pub mod message;
pub mod supervisor;
mod task;
pub mod worker;

#[doc(hidden)]
pub mod status;

#[doc(hidden)]
pub mod channel;
mod sender;

pub use builder::ServerBuilder;
pub use error::ErrorServer;
pub use error::ErrorUpdate;
pub use server::Server;
pub use status::ServerStatus;

use crate::MAXIMUM_MESSAGE_SIZE;

pub type ClientId = u16;

/// Minimum size of incoming message.
pub const MINIMUM_INCOMING_SIZE: usize = 1;

/// Default maximum size of incoming message. (1KB)
pub const DEFAULT_INCOMING_SIZE: usize = 1024;

/// Maximum size of incoming message.
pub const MAXIMUM_INCOMING_SIZE: usize = MAXIMUM_MESSAGE_SIZE;

/// Current minimum client cap
pub const MINIMUM_CLIENT: usize = 1;

/// Default maximum client for builder
pub const DEFAULT_MAXIMUM_CLIENT: usize = 32;

/// Current maximum client cap
pub const MAXIMUM_CLIENT: usize = ClientId::MAX as usize;

/// Minimum worker count cap
pub const MINIMUM_WORKER: usize = 1;

/// Current minimum worker count
pub const DEFAULT_WORKER_COUNT: usize = 4;

/// Default pool rate of the supervisor worker per second.
pub const DEFAULT_POOL_RATE_PER_SECOND: u64 = 30;

/// Minimum pool rate that can be set.
pub const MINIMUM_POOL_RATE_PER_SECOND: u64 = 1;

/// Maximum pool rate that can be set.
pub const MAXIMUM_POOL_RATE_PER_SECOND: u64 = 1000;
