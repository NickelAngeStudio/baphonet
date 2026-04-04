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

use std::thread::{self, JoinHandle};

use baphonet::{
    client::{Client, ErrorClient, builder::ClientBuilder, status::ClientStatus},
    server::message::ServerMessage,
};

use crate::{
    shared::{
        CLIENT_SIZE, DISPATCHER_COUNT, WORKER_COUNT, close_clients, compare_client_server_message,
        create_server_and_clients_default, create_server_and_port,
        message::{ClientToServerMessage, ServerToClientMessage},
    },
    timeout_loop,
};

/// Messages sent by each dispatcher thread
const MSG_PER_DISPATCHER_THREAD: usize = 64;

#[test]
fn dispatcher_client_create_disconnected() {
    let (mut _server, _) = create_server_and_port::<ClientToServerMessage, ServerToClientMessage>(
        CLIENT_SIZE.all,
        WORKER_COUNT.all,
    );
    let mut client = ClientBuilder::new()
        .build::<ServerToClientMessage, ClientToServerMessage>()
        .unwrap();

    let mut dispatcher = client.dispatcher();
    assert_eq!(dispatcher.status(), ClientStatus::Disconnected);
}

#[test]
fn dispatcher_client_create_connected() {
    let (mut server, mut clients) = create_server_and_clients_default::<
        ClientToServerMessage,
        ServerToClientMessage,
    >(CLIENT_SIZE.one);

    timeout_loop! {
        match clients[0].message(){
            Some(_) => {},
            None => break,
        }
    }

    let mut dispatcher = clients[0].dispatcher();
    assert_eq!(dispatcher.status(), ClientStatus::Connected);

    close_clients(&mut clients);
    server.stop().unwrap();
}

#[test]
fn dispatcher_client_client_send_one() {
    dispatcher_client_send_dispatch(CLIENT_SIZE.one, DISPATCHER_COUNT.one);
    dispatcher_client_send_dispatch(CLIENT_SIZE.one, DISPATCHER_COUNT.some);
    dispatcher_client_send_dispatch(CLIENT_SIZE.one, DISPATCHER_COUNT.all);
}

#[test]
fn dispatcher_client_send_some() {
    dispatcher_client_send_dispatch(CLIENT_SIZE.some, DISPATCHER_COUNT.one);
    dispatcher_client_send_dispatch(CLIENT_SIZE.some, DISPATCHER_COUNT.some);
    dispatcher_client_send_dispatch(CLIENT_SIZE.some, DISPATCHER_COUNT.all);
}

#[test]
fn dispatcher_client_send_all() {
    dispatcher_client_send_dispatch(CLIENT_SIZE.all, DISPATCHER_COUNT.one);
    dispatcher_client_send_dispatch(CLIENT_SIZE.all, DISPATCHER_COUNT.some);
    dispatcher_client_send_dispatch(CLIENT_SIZE.all, DISPATCHER_COUNT.all);
}

#[test]
fn dispatcher_client_error_disconnected() {
    todo!()
}

#[test]
fn dispatcher_client_error_channel_disconnected() {
    todo!()
}

#[test]
fn client_send_error_too_large() {
    todo!()
}

fn dispatcher_client_send_dispatch(client_count: usize, dispatcher_client_count: usize) {
    let (mut server, mut clients) = create_server_and_clients_default::<
        ClientToServerMessage,
        ServerToClientMessage,
    >(client_count);

    let mut handles = Vec::<JoinHandle<()>>::new();
    for client in &mut clients {
        timeout_loop! {
            match client.message(){
                Some(_) => {},
                None => break,
            }
        }

        for _ in 0..dispatcher_client_count {
            let mut dispatcher = client.dispatcher();
            let handle = thread::spawn(move || {
                for _ in 0..MSG_PER_DISPATCHER_THREAD {
                    dispatcher.send(ClientToServerMessage::control()).unwrap();
                }
            });
            handles.push(handle);
        }
    }

    let incoming_total: usize = MSG_PER_DISPATCHER_THREAD * dispatcher_client_count * clients.len();
    let mut incoming_count: usize = 0;
    let control = ClientToServerMessage::control();
    println!(
        "dispatcher_client_send_dispatch C={}, D={}, I={} messages...",
        client_count, dispatcher_client_count, incoming_total
    );
    // Make sure server received messages.
    timeout_loop! {
        match server.message(){
            Some(msg) => match msg {
                ServerMessage::Incoming(incoming_message) => {
                    compare_client_server_message(&incoming_message.message, &control);
                    incoming_count += 1;
                    if incoming_total == incoming_count {
                        break ;
                    }
                },
                _ => {},
            },
            None => {},
        }
    }

    // Join threads
    for handle in handles {
        handle.join().unwrap()
    }

    close_clients(&mut clients);
    server.stop().unwrap();
}
