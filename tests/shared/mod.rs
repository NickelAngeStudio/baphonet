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

use baphonet::Message;
use baphonet::client::Client;
use baphonet::client::builder::ClientBuilder;
use baphonet::server::message::OutgoingMessage;
use baphonet::server::{ClientId, Server, ServerBuilder};

use crate::shared::message::{ClientToServerMessage, ServerToClientMessage};

pub mod message;

/// Definition of clients size use for tests
pub struct ClientSize {
    pub none: usize,
    pub one: usize,
    pub some: usize,
    pub all: usize,
}
pub const CLIENT_SIZE: ClientSize = ClientSize {
    none: 0,
    one: 1,
    some: 32,
    all: 64,
};

/// Definition of worker count used for tests
pub struct WorkerCount {
    pub one: usize,
    pub some: usize,
    pub all: usize,
}
pub const WORKER_COUNT: WorkerCount = WorkerCount {
    one: 1,
    some: 4,
    all: 16,
};

pub const DISPATCHER_COUNT: WorkerCount = WorkerCount {
    one: 1,
    some: 4,
    all: 16,
};

/// IPv4 adress used for tests
pub const TEST_IPV4: Ipv4Addr = Ipv4Addr::LOCALHOST;

/// TCP port used for tests
pub const TEST_TCP_PORT: u16 = 50000;

/// Maximum loop wait time.
pub const LOOP_WAIT_TIME: Duration = std::time::Duration::from_millis(5000);

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

/// Wrapper function to send client message
pub fn send_client_message<IN: Message + Send + 'static, OUT: Message + Send + 'static>(
    client: &mut Client<IN, OUT>,
    message: OUT,
) {
    client
        .transceiver()
        .as_mut()
        .unwrap()
        .send(message)
        .unwrap();
}

/// Wrapper function to send server message
pub fn send_server_message<IN: Message + Send + 'static, OUT: Message + Send + 'static>(
    server: &mut Server<IN, OUT>,
    client_id: ClientId,
    message: OUT,
) {
    server
        .transceiver()
        .as_mut()
        .unwrap()
        .send(client_id, message)
        .unwrap();
}

/// Create a test socket from a port
pub fn create_test_socket(port: u16) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(TEST_IPV4), port)
}

/// Will create and start a server while finding a free port
pub fn create_server_and_clients<IN: Message + Send + 'static, OUT: Message + Send + 'static>(
    max_client: usize,
    worker_count: usize,
    client_count: usize,
) -> (Server<IN, OUT>, Vec<Client<OUT, IN>>) {
    let (mut server, port) = create_server_and_port(max_client, worker_count);

    // Wait for server to be active
    timeout_loop!(match server.update() {
        Some(update) => match update {
            baphonet::server::message::ServerUpdate::Active => break,
            _ => {}
        },
        None => {}
    });

    let clients = create_connect_clients(client_count, port);

    let mut sum_client: usize = 0;
    // Wait for each client to be connected
    timeout_loop!(match server.update() {
        Some(update) => match update {
            baphonet::server::message::ServerUpdate::ClientConnected(_, _) => {
                sum_client += 1;
                if sum_client == client_count {
                    break;
                }
            }
            _ => {}
        },
        None => {}
    });

    (server, clients)
}

/// Will create and start a server while finding a free port
pub fn create_server_and_clients_default<
    IN: Message + Send + 'static,
    OUT: Message + Send + 'static,
>(
    client_count: usize,
) -> (Server<IN, OUT>, Vec<Client<OUT, IN>>) {
    create_server_and_clients(CLIENT_SIZE.all, WORKER_COUNT.some, client_count)
}

/// Will create and start a server while finding a free port
pub fn create_server_and_port<IN: Message + Send + 'static, OUT: Message + Send + 'static>(
    max_client: usize,
    worker_count: usize,
) -> (Server<IN, OUT>, u16) {
    let mut server = ServerBuilder::new()
        .maximum_client(max_client)
        .worker(worker_count)
        .build()
        .unwrap();
    let mut port: u16 = TEST_TCP_PORT;

    timeout_loop! {
        let socket = create_test_socket(port);

        match server.start(socket) {
            Ok(_) => break,
            Err(_) => port += 1,
        }
    }

    (server, port)
}

/// Create and connect an array of clients
pub fn create_connect_clients<IN: Message + Send + 'static, OUT: Message + Send + 'static>(
    client_count: usize,
    port: u16,
) -> Vec<Client<IN, OUT>> {
    let mut clients = Vec::<Client<IN, OUT>>::new();
    let client_builder = ClientBuilder::new();

    for _ in 0..client_count {
        let mut client = client_builder.build().unwrap();
        client.connect(create_test_socket(port)).unwrap();
        clients.push(client);
    }

    clients
}

/// Close clients from vector
pub fn close_clients<IN: Message + Send + 'static, OUT: Message + Send + 'static>(
    clients: &mut Vec<Client<IN, OUT>>,
) {
    for client in clients {
        client.close()
    }
}

/// Accumulate total from start to end
pub fn accumulate(end: usize) -> usize {
    let mut total: usize = 0;

    for i in 0..end {
        total += i;
    }

    total
}

/// Compare client to server message
pub fn compare_client_server_message(cts1: &ClientToServerMessage, cts2: &ClientToServerMessage) {
    // Compare values
    assert_eq!(cts1.p1, cts2.p1);
    assert_eq!(cts1.p2, cts2.p2);
    assert_eq!(cts1.p3, cts2.p3);
    assert_eq!(cts1.p4, cts2.p4);
    assert_eq!(cts1.ps.len(), cts2.ps.len());

    for i in 0..cts1.ps.len() {
        assert_eq!(cts1.ps[i], cts2.ps[i]);
    }
}

/// Compare server to client message
pub fn compare_server_client_message(stc1: &ServerToClientMessage, stc2: &ServerToClientMessage) {
    // Compare values
    assert_eq!(stc1.pu8, stc2.pu8);
    assert_eq!(stc1.pu16, stc2.pu16);
    assert_eq!(stc1.pu32, stc2.pu32);
    assert_eq!(stc1.pu64, stc2.pu64);
    assert_eq!(stc1.pu128, stc2.pu128);

    assert_eq!(stc1.pstring1, stc2.pstring1);
    assert_eq!(stc1.pstring2, stc2.pstring2);
    assert_eq!(stc1.pstring3, stc2.pstring3);
}
