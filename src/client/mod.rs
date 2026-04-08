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

pub mod builder;

pub mod transceiver;
pub mod transmitter;

pub mod status;

#[doc(hidden)]
pub mod error;

pub mod channel;

pub mod message;

pub(crate) mod worker;

pub use client::Client;
pub use error::ErrorClient;

use crate::MAXIMUM_MESSAGE_SIZE;

/// Minimum size of outgoing message.
pub const MINIMUM_OUTGOING_SIZE: usize = 1;

/// Default maximum size of outgoing message. (1KB)
pub const DEFAULT_OUTGOING_SIZE: usize = 1024;

/// Maximum size of incoming message.
pub const MAXIMUM_OUTGOING_SIZE: usize = MAXIMUM_MESSAGE_SIZE;

/// Default pool rate of the client worker per second
/// used to receive message from server.
pub const DEFAULT_POOL_RATE_PER_SECOND: u64 = 30;

/// Minimum pool rate that can be set.
pub const MINIMUM_POOL_RATE_PER_SECOND: u64 = 1;

/// Maximum pool rate that can be set.
pub const MAXIMUM_POOL_RATE_PER_SECOND: u64 = 1000;
