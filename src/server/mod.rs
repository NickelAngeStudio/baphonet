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

pub(crate) mod client;

#[doc(hidden)]
pub mod error;

pub mod message;
pub mod worker;
pub mod supervisor;
mod task;

#[doc(hidden)]
pub mod status;

#[doc(hidden)]
pub mod channel;

pub use error::ErrorServer as ErrorServer;
pub use error::ErrorUpdate as ErrorUpdate;
pub use status::ServerStatus as ServerStatus;
pub use server::Server as Server;

pub type ClientId = u16;


/// Bytes length of the size of [`ClientId`]
pub const SIZE_OF_CLIENT_ID : usize = size_of::<ClientId>();

/// Current minimum client cap
pub const SERVER_MINIMUM_CLIENT_CAP : usize = 1;

/// Current maximum client cap
pub const SERVER_MAXIMUM_CLIENT_CAP : usize = ClientId::MAX as usize;

/// Minimum worker count cap
pub const SERVER_MINIMUM_WORKER_CAP : usize = 1;

/// Default pool rate of the supervisor worker per second.
/// Each pool look for connection and receive incoming messages.
/// 
/// Pool rate can be overriden via [`Server::set_pool_rate()`].
pub const POOL_RATE_PER_SECOND : u64 = 10;

/// Minimum pool rate that can be set.
pub const MINIMUM_POOL_RATE_PER_SECOND : u64 = 1;

/// Maximum pool rate that can be set.
pub const MAXIMUM_POOL_RATE_PER_SECOND : u64 = 1000;
