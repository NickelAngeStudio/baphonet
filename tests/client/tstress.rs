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

use std::{
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use baphonet::{
    Message,
    client::{self, ClientBuilder, ClientUpdate, POOL_RATE_PER_SECOND},
    server::{self, message::ServerUpdate},
};

use crate::{
    shared::{CLIENT_SIZE, WORKER_COUNT, create_server_and_port_max_pool, create_test_socket},
    timeout_loop,
};

/// Number of server thread sending messages
const SERVER_TRANSMITTER_THREAD: usize = 16;

/// Count of message sent per server thread
const SERVER_MESSAGE_TO_SEND_PER_THREAD: usize = u16::MAX as usize;

/// Total message to receive from client
const SERVER_TOTAL_MESSAGE_TO_RECEIVE: usize =
    SERVER_TRANSMITTER_THREAD * SERVER_MESSAGE_TO_SEND_PER_THREAD;

/// Number of client thread sending messages
const CLIENT_TRANSMITTER_THREAD: usize = 4;

/// Count of message sent per client thread
const CLIENT_MESSAGE_TO_SEND_PER_THREAD: usize = u16::MAX as usize;

/// Total message to send from client
const CLIENT_TOTAL_MESSAGE_TO_RECEIVE: usize =
    CLIENT_TRANSMITTER_THREAD * CLIENT_MESSAGE_TO_SEND_PER_THREAD;

const MAX_TIME_PER_THREAD: Duration = Duration::from_secs(10);

struct StCMessage {
    p32: u32,
}

impl Message for StCMessage {
    fn serialize(&self, buffer: &mut [u8]) -> Result<usize, ()> {
        tampon::serialize!(buffer, size, (self.p32):u32);
        Ok(size)
    }

    fn deserialize(buffer: &[u8]) -> Result<Self, ()>
    where
        Self: Sized,
    {
        tampon::deserialize!(buffer, size, (p32):u32);
        Ok(StCMessage { p32 })
    }
}

struct CtSMessage {
    p32: i32,
}

impl Message for CtSMessage {
    fn serialize(&self, buffer: &mut [u8]) -> Result<usize, ()> {
        tampon::serialize!(buffer, size, (self.p32):i32);
        Ok(size)
    }

    fn deserialize(buffer: &[u8]) -> Result<Self, ()>
    where
        Self: Sized,
    {
        tampon::deserialize!(buffer, size, (p32):i32);
        Ok(CtSMessage { p32 })
    }
}

/// Client and server will blast message to each others
#[test]
#[ignore = "Long run time"]
fn client_stress_test() {
    let (mut server, port) = create_server_and_port_max_pool::<CtSMessage, StCMessage>(
        CLIENT_SIZE.all,
        WORKER_COUNT.all,
    );
    let mut client = ClientBuilder::new()
        .pool_rate(POOL_RATE_PER_SECOND.maximum)
        .build::<StCMessage, CtSMessage>()
        .unwrap();
    client.connect(create_test_socket(port)).unwrap();

    let mut cnt: usize = 0;
    timeout_loop! {
        match client.update() {
            Some(update) => match update {
                ClientUpdate::Connected => cnt += 1,
                _ => {}
            },
            None => {},
        }

        match server.update() {
            Some(update) => match update {
                ServerUpdate::ClientConnected(_, _) => cnt += 1,
                _ => {},
            },
            None => {},
        }

        if cnt >= 2 {
            break;
        }
    }

    let mut handles = Vec::<JoinHandle<()>>::new();
    let transceiver = client.transceiver().take().unwrap();
    // Client transmitter threads
    for i in 0..CLIENT_TRANSMITTER_THREAD {
        let transmitter = transceiver.transmitter();
        handles.push(thread::spawn(move || {
            handle_client_transmitter(i, transmitter);
        }));
    }

    // Send client transceiver in another thread
    handles.push(thread::spawn(move || {
        handle_client_transceiver(transceiver);
    }));

    let transceiver = server.transceiver().take().unwrap();
    // Server transmitter threads
    for i in 0..SERVER_TRANSMITTER_THREAD {
        let transmitter = transceiver.transmitter();
        handles.push(thread::spawn(move || {
            handle_server_transmitter(i, transmitter);
        }));
    }

    // Send server transceiver in another thread
    handles.push(thread::spawn(move || {
        handle_server_transceiver(transceiver);
    }));

    // Join all threads
    for handle in handles {
        handle.join().unwrap();
    }

    client.close();
    server.stop();
}

fn handle_client_transmitter(worker_id: usize, transmitter: client::Transmitter<CtSMessage>) {
    let mut sum_sent: usize = 0;
    let start = Instant::now();
    loop {
        transmitter
            .send(CtSMessage {
                p32: (worker_id + sum_sent) as i32,
            })
            .unwrap();
        sum_sent += 1;
        if sum_sent == CLIENT_MESSAGE_TO_SEND_PER_THREAD {
            break;
        }

        if start.elapsed() > MAX_TIME_PER_THREAD {
            panic!(
                "Client Transmitter {} timeout at {} messages sent.",
                worker_id, sum_sent
            )
        }
    }
    println!(
        "Client Transmitter {} finished {} messages in {}ms.",
        worker_id,
        CLIENT_MESSAGE_TO_SEND_PER_THREAD,
        start.elapsed().as_millis(),
    )
}

fn handle_client_transceiver(transceiver: client::Transceiver<StCMessage, CtSMessage>) {
    let mut sum_recv: usize = 0;
    let start = Instant::now();
    loop {
        match transceiver.receive_timeout(MAX_TIME_PER_THREAD) {
            Ok(_) => {
                sum_recv += 1;
            }
            Err(_) => panic!("Client Receiver timeout at {} messages received.", sum_recv),
        }

        if sum_recv == SERVER_TOTAL_MESSAGE_TO_RECEIVE {
            break;
        }
        if start.elapsed() > MAX_TIME_PER_THREAD {
            panic!("Client Receiver timeout at {} messages received.", sum_recv)
        }
    }
    println!(
        "Client Receiver finished receiving {} messages in {}ms.",
        SERVER_TOTAL_MESSAGE_TO_RECEIVE,
        start.elapsed().as_millis()
    )
}

fn handle_server_transmitter(worker_id: usize, transmitter: server::Transmitter<StCMessage>) {
    let mut sum_sent: usize = 0;
    let start = Instant::now();
    loop {
        transmitter
            .send(
                0,
                StCMessage {
                    p32: (worker_id + sum_sent) as u32,
                },
            )
            .unwrap();
        sum_sent += 1;
        if sum_sent == SERVER_MESSAGE_TO_SEND_PER_THREAD {
            break;
        }

        if start.elapsed() > MAX_TIME_PER_THREAD {
            panic!(
                "Server Transmitter {} timeout at {} messages sent.",
                worker_id, sum_sent
            )
        }
    }
    println!(
        "Server Transmitter {} finished {} messages in {}ms.",
        worker_id,
        CLIENT_MESSAGE_TO_SEND_PER_THREAD,
        start.elapsed().as_millis()
    )
}

fn handle_server_transceiver(transceiver: server::Transceiver<CtSMessage, StCMessage>) {
    let mut sum_recv: usize = 0;
    let start = Instant::now();
    loop {
        match transceiver.receive_timeout(MAX_TIME_PER_THREAD) {
            Ok(_) => {
                sum_recv += 1;
            }
            Err(_) => panic!("Server Receiver timeout at {} messages received.", sum_recv),
        }

        if sum_recv == CLIENT_TOTAL_MESSAGE_TO_RECEIVE {
            break;
        }
        if start.elapsed() > MAX_TIME_PER_THREAD {
            panic!("Server Receiver timeout at {} messages received.", sum_recv);
        }
    }
    println!(
        "Server Receiver finished receiving {} messages in {}ms.",
        CLIENT_TOTAL_MESSAGE_TO_RECEIVE,
        start.elapsed().as_millis(),
    )
}
