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
pub mod client;

#[doc(hidden)]
pub mod builder;

#[doc(hidden)]
pub mod transceiver;

#[doc(hidden)]
pub mod transmitter;

#[doc(hidden)]
pub mod status;

#[doc(hidden)]
pub mod error;

#[doc(hidden)]
pub mod channel;

#[doc(hidden)]
pub mod message;

#[doc(hidden)]
pub(crate) mod worker;

pub use builder::ClientBuilder;
pub use client::Client;
pub use error::ErrorClient;
pub use error::ErrorTransceiver;
pub use error::ErrorWorker;
pub use message::ClientUpdate;
pub use status::Status as ClientStatus;
pub use transceiver::Transceiver;
pub use transmitter::Transmitter;

use crate::MAXIMUM_MESSAGE_SIZE;

/// Minimum size of outgoing message.
pub const OUTGOING_SIZE_MINIMUM: usize = 1;

/// Default maximum size of outgoing message. (1KB)
pub const OUTGOING_SIZE_DEFAULT: usize = 1024;

/// Maximum size of incoming message.
pub const OUTGOING_SIZE_MAXIMUM: usize = MAXIMUM_MESSAGE_SIZE;

/// Default pool rate of the client worker per second
/// used to receive message from server.
pub const POOL_RATE_PER_SECOND_DEFAULT: u64 = 30;

/// Minimum pool rate that can be set.
pub const POOL_RATE_PER_SECOND_MINIMUM: u64 = 1;

/// Maximum pool rate that can be set.
pub const POOL_RATE_PER_SECOND_MAXIMUM: u64 = 1000;
