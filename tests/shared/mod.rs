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

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use baphonet::server::Server;
use baphonet::Message;

pub mod message;


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

/// Maximum loop wait time.
pub const LOOP_WAIT_TIME : Duration = std::time::Duration::from_millis(5000);


#[macro_export]
macro_rules! timeout_loop {

    ($duration : expr, $($arg:tt)*) => {

        let timestamp = std::time::Instant::now();
        
        loop {
            $($arg)*

            if timestamp.elapsed() > $duration {
                panic!("Timeout!");
            }
        }

    };

    ($($arg:tt)*) => {
        timeout_loop!($crate::shared::LOOP_WAIT_TIME, $($arg)*);
    };
}


/// Will create and start a server while finding a free port
pub fn create_server_and_port<IN : Message + Send + 'static, OUT : Message + Send + 'static>(max_client : usize, worker_count : usize) -> (Server<IN, OUT>, u16) {

    let mut server = Server::<IN, OUT>::new(max_client, worker_count).unwrap();
    let mut port : u16 = TEST_TCP_PORT;
    

    timeout_loop! {
        let socket = SocketAddr::new(IpAddr::V4(TEST_IPV4), port);

        match server.start(socket) {
            Ok(_) => break,
            Err(_) => port += 1,
        }
    }

    (server, port)
}