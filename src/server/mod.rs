// Copyright (c) 2026  NickelAnge.Studio
// Email               mathieu.grenier@nickelange.studio
// Git                 https://github.com/NickelAngeStudio/baphonet
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

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

use crate::ConstRange;
use crate::MAXIMUM_MESSAGE_SIZE;

pub type ClientId = u16;

/// Incoming message size range and default.
///
/// - `default` is 1024 bytes.
/// - `minimum` is 1 byte.
/// - `maximum` is 64 kilobytes.
pub const INCOMING_MESSAGE_SIZE: ConstRange<usize> = ConstRange {
    default: 1024,
    minimum: 1,
    maximum: MAXIMUM_MESSAGE_SIZE,
};

/// Outgoing message size range and default.
///
/// - `default` is 64 kilobytes.
/// - `minimum` is 1 byte.
/// - `maximum` is 64 kilobytes.
pub const OUTGOING_MESSAGE_SIZE: ConstRange<usize> = ConstRange {
    default: MAXIMUM_MESSAGE_SIZE,
    minimum: 1,
    maximum: MAXIMUM_MESSAGE_SIZE,
};

/// Server maximum client size range and default.
///
/// - `default` is 32 clients.
/// - `minimum` is 1 client.
/// - `maximum` is 65535 clients.
pub const MAXIMUM_CLIENT_SIZE: ConstRange<usize> = ConstRange {
    default: 32,
    minimum: 1,
    maximum: ClientId::MAX as usize,
};

/// Server workers count range and default.
///
/// - `default` is 4 workers.
/// - `minimum` is 1 worker.
/// - `maximum` is 65535 workers.
pub const SERVER_WORKER_COUNT: ConstRange<usize> = ConstRange {
    default: 4,
    minimum: 1,
    maximum: ClientId::MAX as usize,
};

/// Server pool rate per second range and default.
///
/// - `default` is 30 pps.
/// - `minimum` is 1 pps.
/// - `maximum` is 1000 pps.
pub const POOL_RATE_PER_SECOND: ConstRange<u64> = ConstRange {
    default: 30,
    minimum: 1,
    maximum: 1000,
};
