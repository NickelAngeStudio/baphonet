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

use std::time::{Duration, Instant};

use crate::{
    shared::{
        CLIENT_SIZE, WORKER_COUNT, accumulate, close_clients, compare_client_server_message,
        create_server_and_clients, create_server_and_port,
        message::{ClientToServerMessage, ServerToClientMessage},
    },
    timeout_loop,
};
use baphonet::server::{ClientId, message::ServerUpdate};

/// Message sent per client
const MESSAGE_PER_CLIENT: usize = u8::MAX as usize;

#[test]
fn server_message_none() {
    let (mut server, _) = create_server_and_port::<ClientToServerMessage, ServerToClientMessage>(
        CLIENT_SIZE.all,
        WORKER_COUNT.all,
    );

    timeout_loop! {
        match server.update() {
            Some(_) => {},
            None => break,
        }
    }
}

/// Returns true if all message are received.
fn is_all_received(msg_rcv: &Vec<usize>) -> bool {
    for rcv in msg_rcv {
        if *rcv != MESSAGE_PER_CLIENT {
            return false;
        }
    }

    true
}

#[test]
fn server_message_some_incoming_one_client() {
    server_message_some_incoming_client(WORKER_COUNT.one, CLIENT_SIZE.one);
    server_message_some_incoming_client(WORKER_COUNT.some, CLIENT_SIZE.one);
    server_message_some_incoming_client(WORKER_COUNT.all, CLIENT_SIZE.one);
}

#[test]
fn server_message_some_incoming_some_client() {
    server_message_some_incoming_client(WORKER_COUNT.one, CLIENT_SIZE.some);
    server_message_some_incoming_client(WORKER_COUNT.some, CLIENT_SIZE.some);
    server_message_some_incoming_client(WORKER_COUNT.all, CLIENT_SIZE.some);
}

#[test]
fn server_message_some_incoming_all_client() {
    server_message_some_incoming_client(WORKER_COUNT.one, CLIENT_SIZE.all);
    server_message_some_incoming_client(WORKER_COUNT.some, CLIENT_SIZE.all);
    server_message_some_incoming_client(WORKER_COUNT.all, CLIENT_SIZE.all);
}

#[test]
fn server_message_update_active() {
    let (mut server, _) = create_server_and_port::<ClientToServerMessage, ServerToClientMessage>(
        CLIENT_SIZE.all,
        WORKER_COUNT.all,
    );

    timeout_loop! {
        match server.update() {
            Some(message) => match message {
                ServerUpdate::Active => break,
                _ => {}
            },
            None => {},
        }
    }
}

#[test]
fn server_message_update_client_connected_one() {
    server_message_update_client_connected(WORKER_COUNT.one, CLIENT_SIZE.one);
    server_message_update_client_connected(WORKER_COUNT.some, CLIENT_SIZE.one);
    server_message_update_client_connected(WORKER_COUNT.all, CLIENT_SIZE.one);
}

#[test]
fn server_message_update_client_connected_some() {
    server_message_update_client_connected(WORKER_COUNT.one, CLIENT_SIZE.some);
    server_message_update_client_connected(WORKER_COUNT.some, CLIENT_SIZE.some);
    server_message_update_client_connected(WORKER_COUNT.all, CLIENT_SIZE.some);
}

#[test]
fn server_message_update_client_connected_all() {
    server_message_update_client_connected(WORKER_COUNT.one, CLIENT_SIZE.all);
    server_message_update_client_connected(WORKER_COUNT.some, CLIENT_SIZE.all);
    server_message_update_client_connected(WORKER_COUNT.all, CLIENT_SIZE.all);
}

#[test]
fn server_message_update_client_disconnected_one() {
    server_message_update_client_disconnected(WORKER_COUNT.one, CLIENT_SIZE.one);
    server_message_update_client_disconnected(WORKER_COUNT.some, CLIENT_SIZE.one);
    server_message_update_client_disconnected(WORKER_COUNT.all, CLIENT_SIZE.one);
}

#[test]
fn server_message_update_client_disconnected_some() {
    server_message_update_client_disconnected(WORKER_COUNT.one, CLIENT_SIZE.some);
    server_message_update_client_disconnected(WORKER_COUNT.some, CLIENT_SIZE.some);
    server_message_update_client_disconnected(WORKER_COUNT.all, CLIENT_SIZE.some);
}

#[test]
fn server_message_update_client_disconnected_all() {
    server_message_update_client_disconnected(WORKER_COUNT.one, CLIENT_SIZE.all);
    server_message_update_client_disconnected(WORKER_COUNT.some, CLIENT_SIZE.all);
    server_message_update_client_disconnected(WORKER_COUNT.all, CLIENT_SIZE.all);
}

#[test]
fn server_message_update_pool_rate() {
    todo!()
}

#[test]
fn server_message_update_full() {
    todo!()
}

#[test]
fn server_message_update_error_connection_lost() {
    todo!()
}

#[test]
fn server_message_update_error_client_not_found() {
    todo!()
}

#[test]
fn server_message_update_error_outgoing_too_large() {
    todo!()
}

#[test]
fn server_message_update_error_incoming_too_large() {
    todo!()
}

#[test]
fn server_message_update_error_outgoing_serialize_error() {
    todo!()
}

#[test]
fn server_message_update_error_incoming_deserialize_error() {
    todo!()
}

#[test]
fn server_message_update_ended() {
    todo!()
}

/// Receive client connected update and add them up.
fn server_message_update_client_connected(worker_count: usize, count: usize) {
    let (mut server, mut clients) = create_server_and_clients::<
        ClientToServerMessage,
        ServerToClientMessage,
    >(CLIENT_SIZE.all, worker_count, count);

    let total_client_id: usize = accumulate(clients.len());
    let mut sum_client_id: usize = 0;
    timeout_loop! {
        match server.update() {
            Some(msg) => match msg {
                ServerUpdate::ClientConnected(client_id, _) => {
                    sum_client_id += client_id as usize;
                    if sum_client_id == total_client_id {
                        break;
                    }
                },
                _ => {},
            },
            None => {},
        }
    }

    close_clients(&mut clients);
    server.stop().unwrap();
}

/// Receive incoming message from parameters
fn server_message_some_incoming_client(worker_count: usize, client_count: usize) {
    let (mut server, mut clients) = create_server_and_clients::<
        ClientToServerMessage,
        ServerToClientMessage,
    >(CLIENT_SIZE.all, worker_count, client_count);

    let mut msg_rcv = Vec::<usize>::new();
    msg_rcv.resize(clients.len(), 0);

    for _ in 0..MESSAGE_PER_CLIENT {
        for client in &mut clients {
            client
                .dispatcher()
                .send(ClientToServerMessage::control())
                .unwrap()
        }
    }

    let instant = Instant::now();
    let control = ClientToServerMessage::control();
    let timeout = Duration::from_millis(100 * clients.len() as u64);
    timeout_loop! { timeout,
        match server.update(){
            Some(msg) => {
                match msg {
                    ServerUpdate::Incoming(incoming_message) => {
                        compare_client_server_message(&control, &incoming_message.message);
                        msg_rcv[incoming_message.client as usize] += 1;
                    },
                    _ => {},
                }
            },
            None => {},
        }

        if is_all_received(&msg_rcv){
            break;
        }

    }

    println!(
        "server_message_some_incoming_client({},{}) {}ms elapsed",
        worker_count,
        client_count,
        instant.elapsed().as_millis()
    );

    // Close clients
    close_clients(&mut clients);

    // Close server
    server.stop().unwrap();
}

/// Receive client connected update and add them up.
fn server_message_update_client_disconnected(worker_count: usize, count: usize) {
    let (mut server, mut clients) = create_server_and_clients::<
        ClientToServerMessage,
        ServerToClientMessage,
    >(CLIENT_SIZE.all, worker_count, count);

    let total_client_id: usize = accumulate(clients.len());
    let mut sum_client_id: usize = 0;

    // Close each connection
    for i in 0..clients.len() {
        server.close_connection(i as ClientId).unwrap();
    }

    timeout_loop! {
        match server.update() {
            Some(msg) => match msg {
                ServerUpdate::Update(update) => {
                    match update {
                        SupervisorUpdate::ClientDisconnected(client_id) => {
                            sum_client_id += client_id as usize;
                            if sum_client_id == total_client_id {
                                break;
                            }
                        },
                        _ => {},
                    }
                },
                _ => {},
            },
            None => {},
        }
    }

    close_clients(&mut clients);
    server.stop().unwrap();
}
