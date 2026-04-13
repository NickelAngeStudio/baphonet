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
    io::Write,
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use baphonet::{
    Message,
    client::{self},
    server::{self, ServerBuilder},
};

use crate::shared::{TEST_TCP_PORT, close_clients, create_connect_clients, create_test_socket};

/// Column width of result
const COLUMN_WIDTH: usize = 6;

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

/// Array of message count for tests.
///
/// Count higher than 8192 will cause `Resource temporarily unavailable (os error 11)`.
const MSG_COUNT: [usize; 4] = [256, 1024, 4096, 8192];

// Array of pool rate for tests
const POOL_RATE: [u64; 4] = [30, 60, 120, 240];

// Array of maximum clients for tests
const MAX_CLIENT: [usize; 5] = [16, 32, 64, 128, 256];

// Vector of worker count for tests
const WORKER_COUNT: [usize; 6] = [1, 2, 4, 8, 16, 32];

/// Multiple clients will blast the server with messages.
/// The server will acknowledge each message received.
///
/// The results help finding the best worker count.
#[test]
#[ignore = "Long run time"]
fn server_stress_test() {
    let total_width = WORKER_COUNT.len() * COLUMN_WIDTH + COLUMN_WIDTH * 3 + 6;

    // Keep track of best worker of each categories
    let mut best_worker_count = Vec::<usize>::new();
    best_worker_count.resize(WORKER_COUNT.len(), 0);

    for maxc in &MAX_CLIENT {
        write_table_header();

        for msgc in &MSG_COUNT {
            for pr in &POOL_RATE {
                table_line_header(*maxc, *msgc, *pr);

                let mut results = Vec::<u128>::new();
                results.resize(WORKER_COUNT.len(), 0);
                let mut res_index: usize = 0;

                for wc in &WORKER_COUNT {
                    let duration = server_stress_with_worker_count(*maxc, *wc, *pr, *msgc);
                    line_result(duration.as_millis());

                    results[res_index] = duration.as_millis();
                    res_index += 1;
                }
                print!(" *\n");
                increment_best_worker(&mut best_worker_count, &results);
            }
            println!("*-{}", align_right(format!("*"), total_width - 2, '-',));
        }
    }

    write_table_footer(total_width, &best_worker_count);
}

fn server_stress_with_worker_count(
    client_count: usize,
    worker_count: usize,
    pool_rate: u64,
    messages_per_client: usize,
) -> Duration {
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

    duration
}

/// Ugly code for pretty result
fn write_table_footer(total_width: usize, best_worker_count: &Vec<usize>) {
    // Write workers results
    println!("*={}", align_right(format!("*"), total_width - 2, '=',));
    print!("*                    |");

    for wc in WORKER_COUNT {
        print!("{}", align_right(format!("{}", wc), COLUMN_WIDTH, ' ',));
    }
    print!(" *\n");

    println!("*={}", align_right(format!("*"), total_width - 2, '=',));
    let base_line = format!(
        "[{}] WORKERS BEST  |",
        get_best_worker_count(&best_worker_count)
    );
    print!("* ");
    print!("{}", align_right(base_line, 20, ' '));

    for bw in best_worker_count {
        print!("{}", align_right(format!("{}", bw), COLUMN_WIDTH, ' ',));
    }
    print!(" *\n");
    println!("*={}", align_right(format!("*"), total_width - 2, '=',));
}

fn line_result(duration: u128) {
    print!(
        "{}",
        align_right(format!("{}", duration), COLUMN_WIDTH, ' ',)
    );
    std::io::stdout().flush().unwrap();
}

fn table_line_header(max_client: usize, msg_count: usize, pool_rate: u64) {
    print!(
        "* {}",
        align_left(format!("{}", max_client), COLUMN_WIDTH, ' ')
    );
    print!(
        "{}",
        align_right(format!("{}", msg_count), COLUMN_WIDTH, ' ')
    );
    print!(
        "{} |",
        align_right(format!("{}", pool_rate), COLUMN_WIDTH, ' ')
    );
}

fn get_best_worker_count(best_worker: &Vec<usize>) -> usize {
    let mut best_index: usize = 0;

    for index in 0..best_worker.len() {
        if best_worker[index] > best_worker[best_index] {
            best_index = index;
        }
    }

    WORKER_COUNT[best_index]
}

/// Increment the best worker according to results.
fn increment_best_worker(best_worker: &mut Vec<usize>, results: &Vec<u128>) {
    let mut best_index: usize = 0;

    for index in 0..results.len() {
        if results[index] < results[best_index] {
            best_index = index;
        }
    }

    best_worker[best_index] += 1;
}

fn align_left(value: String, length: usize, chr: char) -> String {
    let mut formatted = value.clone();

    loop {
        if formatted.len() >= length {
            break;
        }

        formatted.push(chr);
    }

    formatted
}

/// This code is ugly, only made it so result are pretty.
fn write_table_header() {
    let total_width = WORKER_COUNT.len() * COLUMN_WIDTH + COLUMN_WIDTH * 3 + 6;

    println!("{}", align_right(format!("*"), total_width, '*',));

    print!("{}", align_left(format!("CLIENTS"), COLUMN_WIDTH + 2, ' '));
    print!("{}", "  MSGS");
    print!("{}", "    PR |");

    for wc in WORKER_COUNT {
        print!("{}", align_right(format!("{}", wc), COLUMN_WIDTH, ' '));
    }
    print!(" *\n");
    println!("{}", align_right(format!("*"), total_width, '*',));
}

fn align_right(value: String, length: usize, chr: char) -> String {
    let mut formatted = value.clone();

    loop {
        if formatted.len() >= length {
            break;
        }

        formatted.insert(0, chr);
    }

    formatted
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
    }
}

fn handle_client_transceiver(
    client_id: usize,
    transceiver: client::Transceiver<StCMessage, CtSMessage>,
    messages_per_client: usize,
) {
    let total_to_receive: usize = messages_per_client;
    let mut sum_recv: usize = 0;

    loop {
        // Wait answer
        match transceiver.receive_timeout(MAX_TIME_PER_CLIENT_THREAD) {
            Ok(msg) => {
                assert_eq!(msg.id, client_id as u16);
                assert_eq!(msg.payload, client_id as u16);

                sum_recv += 1;
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
}

fn handle_server_transceiver(
    client_count: usize,
    transceiver: server::Transceiver<CtSMessage, StCMessage>,
    messages_per_client: usize,
) {
    let total_to_receive: usize = client_count * messages_per_client;
    let mut sum_recv: usize = 0;

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
            }
            Err(_) => panic!("Server Receive timedout!"),
        }

        if sum_recv == total_to_receive {
            break;
        }
    }
}
