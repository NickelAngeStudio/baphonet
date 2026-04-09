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

#[doc(hidden)]
mod server;

#[doc(hidden)]
mod builder;

#[doc(hidden)]
pub(crate) mod client;

#[doc(hidden)]
pub mod error;

#[doc(hidden)]
pub mod transceiver;

#[doc(hidden)]
mod transmitter;

#[doc(hidden)]
pub mod message;

#[doc(hidden)]
pub mod supervisor;

#[doc(hidden)]
mod task;

#[doc(hidden)]
pub mod worker;

#[doc(hidden)]
pub mod status;

#[doc(hidden)]
pub mod channel;

pub use builder::ServerBuilder;
pub use error::ErrorServer;
pub use error::ErrorTransceiver;
pub use error::ErrorUpdate;
pub use message::ServerUpdate;
pub use server::Server;
pub use status::Status;
pub use transceiver::Transceiver;
pub use transmitter::Transmitter;

use crate::MAXIMUM_MESSAGE_SIZE;

pub type ClientId = u16;

/// Minimum size of incoming message.
pub const INCOMING_SIZE_MINIMUM: usize = 1;

/// Default maximum size of incoming message. (1KB)
pub const INCOMING_SIZE_DEFAULT: usize = 1024;

/// Maximum size of incoming message.
pub const INCOMING_SIZE_MAXIMUM: usize = MAXIMUM_MESSAGE_SIZE;

/// Minimum size of outgoing message.
pub const OUTGOING_SIZE_MINIMUM: usize = 1;

/// Default maximum size of outgoing message. (64KB)
pub const OUTGOING_SIZE_DEFAULT: usize = MAXIMUM_MESSAGE_SIZE;

/// Maximum size of outgoing message.
pub const OUTGOING_SIZE_MAXIMUM: usize = MAXIMUM_MESSAGE_SIZE;

/// Current minimum client cap
pub const MAXCLIENT_MINIMUM: usize = 1;

/// Default maximum client for builder
pub const MAXCLIENT_DEFAULT: usize = 32;

/// Current maximum client cap
pub const MAXCLIENT_MAXIMUM: usize = ClientId::MAX as usize;

/// Minimum worker count cap
pub const WORKER_COUNT_MINIMUM: usize = 1;

/// Current minimum worker count
pub const WORKER_COUNT_DEFAULT: usize = 4;

/// Default pool rate of the supervisor worker per second.
pub const POOL_RATE_PER_SECOND_DEFAULT: u64 = 30;

/// Minimum pool rate that can be set.
pub const POOL_RATE_PER_SECOND_MINIMUM: u64 = 1;

/// Maximum pool rate that can be set.
pub const POOL_RATE_PER_SECOND_MAXIMUM: u64 = 1000;
