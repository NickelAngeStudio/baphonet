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
    time::Duration,
};

use baphonet::server::{ClientId, ErrorTransceiver, ServerBuilder, Transceiver, transceiver};

use crate::{
    shared::{
        CLIENT_SIZE, WORKER_COUNT, accumulate, close_clients, compare_client_server_message,
        compare_server_client_message, create_server_and_clients,
        message::{ClientToServerMessage, ServerToClientMessage},
        send_client_message,
    },
    timeout_loop,
};

/// Messages sent by each transmitter thread
const MSG_SENT_PER_THREAD: usize = 64;

/// Number of thread for multi thread
const MULTI_THREAD_COUNT: usize = 16;

/// Timeout for receive_timeout()
const RECEIVE_TIMEOUT: Duration = Duration::from_millis(1000);

#[test]
fn server_transceiver_same_thread_receive() {
    let (mut server, mut clients) = create_server_and_clients::<
        ClientToServerMessage,
        ServerToClientMessage,
    >(CLIENT_SIZE.all, WORKER_COUNT.one, CLIENT_SIZE.all);

    for client in &mut clients {
        send_client_message(client, ClientToServerMessage::control());
    }

    let total = accumulate(CLIENT_SIZE.all);
    let mut sum_client_id: usize = 0;
    let control = ClientToServerMessage::control();
    let transceiver = server.transceiver().as_ref().unwrap();
    timeout_loop! {
        match transceiver.receive() {
            Some(message) => {
                sum_client_id += message.client_id as usize;
                compare_client_server_message(&control, &message.message);

                if sum_client_id == total {
                    break;
                }
            },
            None => {},
        }
    }

    close_clients(&mut clients);
    server.stop();
}

#[test]
fn server_transceiver_diff_thread_receive() {
    let (mut server, mut clients) = create_server_and_clients::<
        ClientToServerMessage,
        ServerToClientMessage,
    >(CLIENT_SIZE.all, WORKER_COUNT.one, CLIENT_SIZE.all);

    for client in &mut clients {
        send_client_message(client, ClientToServerMessage::control());
    }

    let total = accumulate(CLIENT_SIZE.all);

    let transceiver = server.transceiver().take().unwrap();
    let handle = thread::spawn(move || {
        let control = ClientToServerMessage::control();
        let mut sum_client_id: usize = 0;
        timeout_loop! {
            match transceiver.receive() {
                Some(message) => {
                    sum_client_id += message.client_id as usize;
                    compare_client_server_message(&control, &message.message);

                    if sum_client_id == total {
                        break;
                    }
                },
                None => {},
            }
        }
    });

    handle.join().unwrap();
    close_clients(&mut clients);
    server.stop();
}

#[test]
fn server_transceiver_same_thread_receive_wait() {
    let (mut server, mut clients) = create_server_and_clients::<
        ClientToServerMessage,
        ServerToClientMessage,
    >(CLIENT_SIZE.all, WORKER_COUNT.one, CLIENT_SIZE.all);

    for client in &mut clients {
        send_client_message(client, ClientToServerMessage::control());
    }

    let total = accumulate(CLIENT_SIZE.all);
    let mut sum_client_id: usize = 0;
    let control = ClientToServerMessage::control();
    let transceiver = server.transceiver().as_ref().unwrap();
    timeout_loop! {
        match transceiver.receive_wait() {
            Ok(message) => {
                sum_client_id += message.client_id as usize;
                compare_client_server_message(&control, &message.message);

                if sum_client_id == total {
                    break;
                }
            },
            Err(_) => panic!("Shouldn't err()!"),
        }
    }

    close_clients(&mut clients);
    server.stop();
}

#[test]
fn server_transceiver_diff_thread_receive_wait() {
    let (mut server, mut clients) = create_server_and_clients::<
        ClientToServerMessage,
        ServerToClientMessage,
    >(CLIENT_SIZE.all, WORKER_COUNT.one, CLIENT_SIZE.all);

    for client in &mut clients {
        send_client_message(client, ClientToServerMessage::control());
    }

    let total = accumulate(CLIENT_SIZE.all);

    let transceiver = server.transceiver().take().unwrap();
    let handle = thread::spawn(move || {
        let control = ClientToServerMessage::control();
        let mut sum_client_id: usize = 0;
        timeout_loop! {
            match transceiver.receive_wait() {
                Ok(message) => {
                    sum_client_id += message.client_id as usize;
                    compare_client_server_message(&control, &message.message);

                    if sum_client_id == total {
                        break;
                    }
                },
                Err(_) => panic!("Shouldn't err()!"),
            }
        }
    });

    handle.join().unwrap();
    close_clients(&mut clients);
    server.stop();
}

#[test]
fn server_transceiver_receive_wait_err_disconnected() {
    let mut _trns: Option<Transceiver<ClientToServerMessage, ServerToClientMessage>> = None;
    {
        let mut server = ServerBuilder::new()
            .build::<ClientToServerMessage, ServerToClientMessage>()
            .unwrap();
        _trns = server.transceiver().take();
    }

    match _trns.as_mut().unwrap().receive_wait() {
        Ok(_) => panic!("Shouldn't be ok()!"),
        Err(err) => assert_eq!(err, ErrorTransceiver::ChannelDisconnected),
    }
}

#[test]
fn server_transceiver_same_thread_receive_timeout() {
    let (mut server, mut clients) = create_server_and_clients::<
        ClientToServerMessage,
        ServerToClientMessage,
    >(CLIENT_SIZE.all, WORKER_COUNT.one, CLIENT_SIZE.all);

    for client in &mut clients {
        send_client_message(client, ClientToServerMessage::control());
    }

    let total = accumulate(CLIENT_SIZE.all);
    let mut sum_client_id: usize = 0;
    let control = ClientToServerMessage::control();
    let transceiver = server.transceiver().as_ref().unwrap();
    timeout_loop! {
        match transceiver.receive_timeout(RECEIVE_TIMEOUT) {
            Ok(message) => {
                sum_client_id += message.client_id as usize;
                compare_client_server_message(&control, &message.message);

                if sum_client_id == total {
                    break;
                }
            },
            Err(_) => panic!("Shouldn't err()!"),
        }
    }

    close_clients(&mut clients);
    server.stop();
}

#[test]
fn server_transceiver_diff_thread_receive_timeout() {
    let (mut server, mut clients) = create_server_and_clients::<
        ClientToServerMessage,
        ServerToClientMessage,
    >(CLIENT_SIZE.all, WORKER_COUNT.one, CLIENT_SIZE.all);

    for client in &mut clients {
        send_client_message(client, ClientToServerMessage::control());
    }

    let total = accumulate(CLIENT_SIZE.all);

    let transceiver = server.transceiver().take().unwrap();
    let handle = thread::spawn(move || {
        let control = ClientToServerMessage::control();
        let mut sum_client_id: usize = 0;
        timeout_loop! {
            match transceiver.receive_timeout(RECEIVE_TIMEOUT) {
                Ok(message) => {
                    sum_client_id += message.client_id as usize;
                    compare_client_server_message(&control, &message.message);

                    if sum_client_id == total {
                        break;
                    }
                },
                Err(_) => panic!("Shouldn't err()!"),
            }
        }
    });

    handle.join().unwrap();
    close_clients(&mut clients);
    server.stop();
}

#[test]
fn server_transceiver_receive_timeout_err_disconnected() {
    let mut _trns: Option<Transceiver<ClientToServerMessage, ServerToClientMessage>> = None;
    {
        let mut server = ServerBuilder::new()
            .build::<ClientToServerMessage, ServerToClientMessage>()
            .unwrap();
        _trns = server.transceiver().take();
    }

    match _trns.as_mut().unwrap().receive_timeout(RECEIVE_TIMEOUT) {
        Ok(_) => panic!("Shouldn't be ok()!"),
        Err(err) => assert_eq!(err, ErrorTransceiver::ChannelDisconnected),
    }
}

#[test]
fn server_transceiver_receive_timeout_err_timeout() {
    let mut server = ServerBuilder::new()
        .build::<ClientToServerMessage, ServerToClientMessage>()
        .unwrap();

    let duration = Duration::from_millis(50);

    match server
        .transceiver()
        .as_mut()
        .unwrap()
        .receive_timeout(duration)
    {
        Ok(_) => panic!("Shouldn't be ok()!"),
        Err(err) => assert_eq!(err, ErrorTransceiver::Timeout),
    }
}

#[test]
fn server_transceiver_same_thread_send() {
    let (mut server, mut clients) = create_server_and_clients::<
        ClientToServerMessage,
        ServerToClientMessage,
    >(CLIENT_SIZE.all, WORKER_COUNT.one, CLIENT_SIZE.all);

    let server_trscv = server.transceiver().take().unwrap();

    let mut client_rcv = Vec::<usize>::new();
    client_rcv.resize(clients.len(), 0);

    for _ in 0..MSG_SENT_PER_THREAD {
        for client_id in 0..clients.len() {
            server_trscv
                .send(client_id as ClientId, ServerToClientMessage::control())
                .unwrap();
        }
    }

    let total_recv = MSG_SENT_PER_THREAD;
    let control = ServerToClientMessage::control();
    timeout_loop! {
        for client_id in 0..clients.len() {
            match clients[client_id].transceiver().as_mut().unwrap().receive(){
                Some(msg) => {
                    compare_server_client_message(&control, &msg);
                    client_rcv[client_id] += 1;
                },
                None => {},
            }
        }

        if is_all_message_recv(&client_rcv, total_recv){
            break;
        }
    }

    close_clients(&mut clients);
    server.stop();
}

/// Returns true is all element of vec is equal to total.
fn is_all_message_recv(recv: &Vec<usize>, total_recv: usize) -> bool {
    for r in recv {
        if *r != total_recv {
            return false;
        }
    }

    true
}

#[test]
fn server_transceiver_multi_thread_send() {
    let (mut server, mut clients) = create_server_and_clients::<
        ClientToServerMessage,
        ServerToClientMessage,
    >(CLIENT_SIZE.all, WORKER_COUNT.one, CLIENT_SIZE.all);

    let server_trscv = server.transceiver().take().unwrap();
    let transmitter = server_trscv.transmitter();

    let mut client_rcv = Vec::<usize>::new();
    client_rcv.resize(clients.len(), 0);

    let mut handles = Vec::<JoinHandle<()>>::new();
    let client_size = clients.len();

    for _ in 0..MULTI_THREAD_COUNT {
        let transmit_clone = transmitter.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..MSG_SENT_PER_THREAD {
                for client_id in 0..client_size {
                    transmit_clone
                        .send(client_id as ClientId, ServerToClientMessage::control())
                        .unwrap();
                }
            }
        }));
    }

    let total_recv = MSG_SENT_PER_THREAD * MULTI_THREAD_COUNT;
    let control = ServerToClientMessage::control();
    timeout_loop! {
        for client_id in 0..clients.len() {
            match clients[client_id].transceiver().as_mut().unwrap().receive(){
                Some(msg) => {
                    compare_server_client_message(&control, &msg);
                    client_rcv[client_id] += 1;
                },
                None => {},
            }
        }

        if is_all_message_recv(&client_rcv, total_recv){
            break;
        }
    }

    // Join all threads
    for handle in handles {
        handle.join().unwrap();
    }

    close_clients(&mut clients);
    server.stop();
}

#[test]
fn server_transceiver_send_err_disconnected() {
    let mut _trns: Option<Transceiver<ClientToServerMessage, ServerToClientMessage>> = None;
    {
        let mut server = ServerBuilder::new()
            .build::<ClientToServerMessage, ServerToClientMessage>()
            .unwrap();
        _trns = server.transceiver().take();
    }

    match _trns
        .as_mut()
        .unwrap()
        .send(0, ServerToClientMessage::control())
    {
        Ok(_) => panic!("Shouldn't be ok()!"),
        Err(err) => assert_eq!(err, ErrorTransceiver::ChannelDisconnected),
    }
}

#[test]
fn server_transceiver_same_thread_send_vec() {
    let (mut server, mut clients) = create_server_and_clients::<
        ClientToServerMessage,
        ServerToClientMessage,
    >(CLIENT_SIZE.all, WORKER_COUNT.one, CLIENT_SIZE.all);

    let server_trscv = server.transceiver().take().unwrap();

    let mut client_rcv = Vec::<usize>::new();
    client_rcv.resize(clients.len(), 0);

    let mut destinations = Vec::<ClientId>::new();
    for client_id in 0..clients.len() {
        destinations.push(client_id as ClientId);
    }

    for _ in 0..MSG_SENT_PER_THREAD {
        server_trscv
            .send_vec(&destinations, ServerToClientMessage::control())
            .unwrap();
    }

    let total_recv = MSG_SENT_PER_THREAD;
    let control = ServerToClientMessage::control();
    timeout_loop! {
        for client_id in 0..clients.len() {
            match clients[client_id].transceiver().as_mut().unwrap().receive(){
                Some(msg) => {
                    compare_server_client_message(&control, &msg);
                    client_rcv[client_id] += 1;
                },
                None => {},
            }
        }

        if is_all_message_recv(&client_rcv, total_recv){
            break;
        }
    }

    close_clients(&mut clients);
    server.stop();
}

#[test]
fn server_transceiver_multi_thread_send_vec() {
    let (mut server, mut clients) = create_server_and_clients::<
        ClientToServerMessage,
        ServerToClientMessage,
    >(CLIENT_SIZE.all, WORKER_COUNT.one, CLIENT_SIZE.all);

    let server_trscv = server.transceiver().take().unwrap();
    let transmitter = server_trscv.transmitter();

    let mut client_rcv = Vec::<usize>::new();
    client_rcv.resize(clients.len(), 0);

    let mut handles = Vec::<JoinHandle<()>>::new();

    for _ in 0..MULTI_THREAD_COUNT {
        let transmit_clone = transmitter.clone();
        let mut destinations = Vec::<ClientId>::new();
        for client_id in 0..clients.len() {
            destinations.push(client_id as ClientId);
        }
        handles.push(thread::spawn(move || {
            for _ in 0..MSG_SENT_PER_THREAD {
                transmit_clone
                    .send_vec(&destinations, ServerToClientMessage::control())
                    .unwrap();
            }
        }));
    }

    let total_recv = MSG_SENT_PER_THREAD * MULTI_THREAD_COUNT;
    let control = ServerToClientMessage::control();
    timeout_loop! {
        for client_id in 0..clients.len() {
            match clients[client_id].transceiver().as_mut().unwrap().receive(){
                Some(msg) => {
                    compare_server_client_message(&control, &msg);
                    client_rcv[client_id] += 1;
                },
                None => {},
            }
        }

        if is_all_message_recv(&client_rcv, total_recv){
            break;
        }
    }

    // Join all threads
    for handle in handles {
        handle.join().unwrap();
    }

    close_clients(&mut clients);
    server.stop();
}

#[test]
fn server_transceiver_send_vec_err_disconnected() {
    let mut _trns: Option<Transceiver<ClientToServerMessage, ServerToClientMessage>> = None;
    {
        let mut server = ServerBuilder::new()
            .build::<ClientToServerMessage, ServerToClientMessage>()
            .unwrap();
        _trns = server.transceiver().take();
    }

    let destinations = vec![0 as ClientId];
    match _trns
        .as_mut()
        .unwrap()
        .send_vec(&destinations, ServerToClientMessage::control())
    {
        Ok(_) => panic!("Shouldn't be ok()!"),
        Err(err) => assert_eq!(err, ErrorTransceiver::ChannelDisconnected),
    }
}

#[test]
fn server_transceiver_send_vec_no_destination() {
    let mut server = ServerBuilder::new()
        .build::<ClientToServerMessage, ServerToClientMessage>()
        .unwrap();
    let transceiver = server.transceiver().take().unwrap();

    let destinations = Vec::<ClientId>::new();
    match transceiver.send_vec(&destinations, ServerToClientMessage::control()) {
        Ok(_) => panic!("Shouldn't be ok()!"),
        Err(err) => assert_eq!(err, ErrorTransceiver::NoDestination),
    }
}
