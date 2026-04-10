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
    iter::Inspect,
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use baphonet::{
    Message,
    client::{self, Client},
    server::{self, ClientId, POOL_RATE_PER_SECOND, ServerBuilder},
};

use crate::shared::{
    CLIENT_SIZE, TEST_TCP_PORT, WORKER_COUNT, close_clients, create_connect_clients,
    create_test_socket,
};

/// Count of message sent per client
const CLIENT_MESSAGE_SENT: usize = u16::MAX as usize;

/// Time between each message sent per thread
const TIME_BETWEEN_MESSAGE: Duration = Duration::from_micros(1);

const MAX_TIME_PER_CLIENT_THREAD: Duration = Duration::from_secs(600);

struct StCMessage {
    id: u16,
    payload: u16,
}

impl Message for StCMessage {
    fn serialize(&self, buffer: &mut [u8]) -> Result<usize, ()> {
        tampon::serialize!(buffer, size, (self.id, self.payload):u16);
        Ok(size)
    }

    fn deserialize(buffer: &[u8]) -> Result<Self, ()>
    where
        Self: Sized,
    {
        tampon::deserialize!(buffer, size, (id, payload):u16);
        Ok(StCMessage { id, payload })
    }
}

struct CtSMessage {
    payload: u16,
}

impl Message for CtSMessage {
    fn serialize(&self, buffer: &mut [u8]) -> Result<usize, ()> {
        tampon::serialize!(buffer, size, (self.payload):u16);
        Ok(size)
    }

    fn deserialize(buffer: &[u8]) -> Result<Self, ()>
    where
        Self: Sized,
    {
        tampon::deserialize!(buffer, size, (payload):u16);
        Ok(CtSMessage { payload })
    }
}

/// Multiple clients will blast the server with messages.
/// The server will acknowledge each message received.
#[test]
#[ignore = "Long run time"]
fn server_stress_test() {
    let dur1 =
        server_stress_with_worker_count(CLIENT_SIZE.all, 1, POOL_RATE_PER_SECOND.default, 16384);

    let dur16 = server_stress_with_worker_count(
        CLIENT_SIZE.all,
        WORKER_COUNT.all,
        POOL_RATE_PER_SECOND.default,
        16384,
    );

    println!("Dur1={}", dur1.as_millis());
    println!("Dur16={}", dur16.as_millis());
}

fn server_stress_with_worker_count(
    client_count: usize,
    worker_count: usize,
    pool_rate: u64,
    messages_per_client: usize,
) -> Duration {
    println!(
        "*** SERVER STRESS STARTED [{} clients, {} workers, {} pool_rate] ***",
        client_count, worker_count, pool_rate
    );

    let mut server = ServerBuilder::new()
        .maximum_client(client_count)
        .worker(worker_count)
        .pool_rate(pool_rate)
        .build::<CtSMessage, StCMessage>()
        .unwrap();

    let port: u16 = TEST_TCP_PORT + 2000;
    server.start(create_test_socket(port)).unwrap();

    // Send server transceiver in another thread
    let transceiver = server.transceiver().take().unwrap();
    let trns_thread = thread::spawn(move || {
        handle_server_transceiver(client_count, transceiver, messages_per_client);
    });

    let mut clients = create_connect_clients::<StCMessage, CtSMessage>(client_count, port);

    thread::sleep(Duration::from_millis(10));
    let mut handles = Vec::<JoinHandle<()>>::new();

    for client_id in 0..client_count {
        let transceiver = clients[client_id].transceiver().take().unwrap();
        // Client transmitter threads
        let transmitter = transceiver.transmitter();
        handles.push(thread::spawn(move || {
            handle_client_transmitter(client_id, transmitter, messages_per_client);
        }));

        // Client transceiver thread
        // Send client transceiver in another thread
        handles.push(thread::spawn(move || {
            handle_client_transceiver(client_id, transceiver, messages_per_client);
        }));
    }

    let start = Instant::now();
    trns_thread.join().unwrap();
    let duration = start.elapsed();

    // Join all threads
    for handle in handles {
        handle.join().unwrap();
    }

    close_clients(&mut clients);
    server.stop();

    println!(
        "*** SERVER STRESS ENDED [{} clients, {} workers, {} pool_rate] ***",
        client_count, worker_count, pool_rate
    );

    duration
}

fn handle_client_transmitter(
    client_id: usize,
    transmitter: client::Transmitter<CtSMessage>,
    messages_per_client: usize,
) {
    for _ in 0..messages_per_client {
        transmitter
            .send(CtSMessage {
                payload: client_id as u16,
            })
            .unwrap();

        //thread::sleep(TIME_BETWEEN_MESSAGE);
    }
    println!("Client ({}) Send finished!", client_id);
}

fn handle_client_transceiver(
    client_id: usize,
    transceiver: client::Transceiver<StCMessage, CtSMessage>,
    messages_per_client: usize,
) {
    let total_to_receive: usize = messages_per_client;
    let mut sum_recv: usize = 0;
    let transmitter = transceiver.transmitter();

    loop {
        // Send
        //println!("Client ({}) send {}.", client_id, sum_recv + 1);

        // Send waves of 256 messages.
        //transmitter
        //    .send(CtSMessage {
        //        payload: client_id as u16,
        //    })
        //    .unwrap();

        // Wait answer
        match transceiver.receive_timeout(MAX_TIME_PER_CLIENT_THREAD) {
            Ok(msg) => {
                assert_eq!(msg.id, client_id as u16);
                assert_eq!(msg.payload, client_id as u16);

                sum_recv += 1;

                //println!("Client ({}) received {}.", client_id, sum_recv);

                //if sum_recv % 1000 == 0 {
                //    println!(
                //        "Client ({}) Received {} of {} messages.",
                //        client_id, sum_recv, total_to_receive
                //    )
                //}
            }
            Err(err) => match err {
                client::ErrorTransceiver::ChannelDisconnected => {
                    panic!("Client Receive Disconnected!")
                }
                client::ErrorTransceiver::Timeout => panic!("Client Receive timedout!"),
            },
        }

        if sum_recv == total_to_receive {
            break;
        }
    }

    println!(
        "Client ({}) received {} messages!",
        client_id, total_to_receive
    );
}

fn handle_server_transceiver(
    client_count: usize,
    transceiver: server::Transceiver<CtSMessage, StCMessage>,
    messages_per_client: usize,
) {
    let total_to_receive: usize = client_count * messages_per_client;
    let mut sum_recv: usize = 0;
    let trigger = messages_per_client / 20;

    loop {
        match transceiver.receive_timeout(MAX_TIME_PER_CLIENT_THREAD) {
            Ok(msg) => {
                let client_id = msg.client_id;
                let payload = msg.message.payload;

                assert_eq!(client_id, payload);

                // Answer the client
                transceiver
                    .send(
                        client_id,
                        StCMessage {
                            id: client_id,
                            payload,
                        },
                    )
                    .unwrap();

                sum_recv += 1;

                if sum_recv % trigger == 0 {
                    println!("RECEIVED AND SENT {} of {}...", sum_recv, total_to_receive)
                }
            }
            Err(_) => panic!("Server Receive timedout!"),
        }

        if sum_recv == total_to_receive {
            break;
        }
    }

    println!("SERVER RECEIVED AND SENT {} MESSAGES!", total_to_receive);
}
