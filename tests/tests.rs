/* 
Copyright (c) 2026  NickelAnge.Studio 
Email               mathieu.grenier@nickelange.studio
Git                 https://codeberg.org/NickelAngeStudio/baphonet

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

use std::net::Ipv4Addr;

/// Definition of clients size use for tests
pub struct ClientSize { pub none : usize, pub one : usize, pub some : usize, pub all : usize  }
pub const CLIENT_SIZE : ClientSize= ClientSize{ none: 0, one: 1, some: 32, all: 64 };

/// Definition of worker count used for tests
pub struct WorkerCount { pub one : usize, pub some : usize, pub all : usize  }
pub const WORKER_COUNT : WorkerCount= WorkerCount{ one: 1, some: 4, all: 16 };

/// IPv4 adress used for tests
pub const TEST_IPV4 : Ipv4Addr = Ipv4Addr::LOCALHOST;

/// TCP port used for tests
pub const TEST_TCP_PORT : u16 = 50000;

/// Trait Message implementation tests
pub mod message;

pub mod client;

mod server;