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

use std::{
    thread::{self, JoinHandle},
    time::Duration,
};

use baphonet::client::{ErrorTransceiver, transceiver::Transceiver};

use crate::{
    shared::{
        CLIENT_SIZE, DISPATCHER_COUNT, WORKER_COUNT, close_clients, compare_client_server_message,
        compare_server_client_message, create_server_and_clients_default, create_server_and_port,
        message::{ClientToServerMessage, ServerToClientMessage},
        send_server_message,
    },
    timeout_loop,
};

/// Messages sent by each dispatcher thread
const MSG_SENT_PER_THREAD: usize = 64;
const TRANSMITTER_THREAD: usize = 64;

const DEFAULT_TIMEOUT: Duration = Duration::from_millis(1000);

#[test]
fn client_transceiver_same_thread_receive() {
    let (mut server, mut clients) = create_server_and_clients_default::<
        ClientToServerMessage,
        ServerToClientMessage,
    >(CLIENT_SIZE.one);

    for _ in 0..MSG_SENT_PER_THREAD {
        send_server_message(&mut server, 0, ServerToClientMessage::control());
    }

    let control = ServerToClientMessage::control();
    let mut sum_recv: usize = 0;
    let transceiver = clients[0].transceiver().as_ref().unwrap();
    timeout_loop! {
        match transceiver.receive() {
            Some(msg) => {
                compare_server_client_message(&control, &msg);

                sum_recv += 1;
                if sum_recv == MSG_SENT_PER_THREAD {
                    break;
                }
            },
            None => {},
        }
    }

    close_clients(&mut clients);
    server.stop().unwrap();
}

#[test]
fn client_transceiver_diff_thread_receive() {
    let (mut server, mut clients) = create_server_and_clients_default::<
        ClientToServerMessage,
        ServerToClientMessage,
    >(CLIENT_SIZE.one);

    for _ in 0..MSG_SENT_PER_THREAD {
        send_server_message(&mut server, 0, ServerToClientMessage::control());
    }

    let transceiver = clients[0].transceiver().take().unwrap();
    let handle = thread::spawn(move || {
        let control = ServerToClientMessage::control();
        let mut sum_recv: usize = 0;
        timeout_loop! {
            match transceiver.receive() {
                Some(msg) => {
                    compare_server_client_message(&control, &msg);

                    sum_recv += 1;
                    if sum_recv == MSG_SENT_PER_THREAD {
                        break;
                    }
                },
                None => {},
            }
        }
    });

    handle.join().unwrap();
    close_clients(&mut clients);
    server.stop().unwrap();
}

#[test]
fn client_transceiver_same_thread_receive_wait() {
    let (mut server, mut clients) = create_server_and_clients_default::<
        ClientToServerMessage,
        ServerToClientMessage,
    >(CLIENT_SIZE.one);

    for _ in 0..MSG_SENT_PER_THREAD {
        send_server_message(&mut server, 0, ServerToClientMessage::control());
    }

    let control = ServerToClientMessage::control();
    let mut sum_recv: usize = 0;
    let transceiver = clients[0].transceiver().as_ref().unwrap();
    timeout_loop! {
        match transceiver.receive_wait() {
            Ok(msg) =>{
                compare_server_client_message(&control, &msg);

                sum_recv += 1;
                if sum_recv == MSG_SENT_PER_THREAD {
                    break;
                }
            },
            Err(_) => panic!("Shouldn't Err()!"),
        }
    }

    close_clients(&mut clients);
    server.stop().unwrap();
}

#[test]
fn client_transceiver_diff_thread_receive_wait() {
    let (mut server, mut clients) = create_server_and_clients_default::<
        ClientToServerMessage,
        ServerToClientMessage,
    >(CLIENT_SIZE.one);

    for _ in 0..MSG_SENT_PER_THREAD {
        send_server_message(&mut server, 0, ServerToClientMessage::control());
    }

    let transceiver = clients[0].transceiver().take().unwrap();
    let handle = thread::spawn(move || {
        let control = ServerToClientMessage::control();
        let mut sum_recv: usize = 0;
        timeout_loop! {
            match transceiver.receive_wait() {
                Ok(msg) => {
                    compare_server_client_message(&control, &msg);

                    sum_recv += 1;
                    if sum_recv == MSG_SENT_PER_THREAD {
                        break;
                    }
                },
                Err(_) => panic!("Shouldn't err()!"),
            }
        }
    });

    handle.join().unwrap();
    close_clients(&mut clients);
    server.stop().unwrap();
}

#[test]
fn client_transceiver_receive_wait_err_disconnected() {
    let mut _trns: Option<Transceiver<ServerToClientMessage, ClientToServerMessage>> = None;

    {
        let (mut server, mut clients) = create_server_and_clients_default::<
            ClientToServerMessage,
            ServerToClientMessage,
        >(CLIENT_SIZE.one);

        _trns = Some(clients[0].transceiver().take().unwrap());

        close_clients(&mut clients);
        server.stop().unwrap();
    }

    match _trns.as_ref().unwrap().receive_wait() {
        Ok(_) => panic!("Shouldn't be ok()!"),
        Err(err) => assert_eq!(err, ErrorTransceiver::ChannelDisconnected),
    }
}

#[test]
fn client_transceiver_same_thread_receive_timeout() {
    let (mut server, mut clients) = create_server_and_clients_default::<
        ClientToServerMessage,
        ServerToClientMessage,
    >(CLIENT_SIZE.one);

    for _ in 0..MSG_SENT_PER_THREAD {
        send_server_message(&mut server, 0, ServerToClientMessage::control());
    }

    let control = ServerToClientMessage::control();
    let mut sum_recv: usize = 0;
    let transceiver = clients[0].transceiver().as_ref().unwrap();
    timeout_loop! {
        match transceiver.receive_timeout(DEFAULT_TIMEOUT) {
            Ok(msg) =>{
                compare_server_client_message(&control, &msg);

                sum_recv += 1;
                if sum_recv == MSG_SENT_PER_THREAD {
                    break;
                }
            },
            Err(_) => panic!("Shouldn't Err()!"),
        }
    }

    close_clients(&mut clients);
    server.stop().unwrap();
}

#[test]
fn client_transceiver_diff_thread_receive_timeout() {
    let (mut server, mut clients) = create_server_and_clients_default::<
        ClientToServerMessage,
        ServerToClientMessage,
    >(CLIENT_SIZE.one);

    for _ in 0..MSG_SENT_PER_THREAD {
        send_server_message(&mut server, 0, ServerToClientMessage::control());
    }

    let transceiver = clients[0].transceiver().take().unwrap();
    let handle = thread::spawn(move || {
        let control = ServerToClientMessage::control();
        let mut sum_recv: usize = 0;
        timeout_loop! {
            match transceiver.receive_timeout(DEFAULT_TIMEOUT) {
                Ok(msg) => {
                    compare_server_client_message(&control, &msg);

                    sum_recv += 1;
                    if sum_recv == MSG_SENT_PER_THREAD {
                        break;
                    }
                },
                Err(_) => panic!("Shouldn't err()!"),
            }
        }
    });

    handle.join().unwrap();
    close_clients(&mut clients);
    server.stop().unwrap();
}

#[test]
fn client_transceiver_receive_timeout_err_disconnected() {
    let mut _trns: Option<Transceiver<ServerToClientMessage, ClientToServerMessage>> = None;

    {
        let (mut server, mut clients) = create_server_and_clients_default::<
            ClientToServerMessage,
            ServerToClientMessage,
        >(CLIENT_SIZE.one);

        _trns = Some(clients[0].transceiver().take().unwrap());

        close_clients(&mut clients);
        server.stop().unwrap();
    }

    match _trns.as_ref().unwrap().receive_timeout(DEFAULT_TIMEOUT) {
        Ok(_) => panic!("Shouldn't be ok()!"),
        Err(err) => assert_eq!(err, ErrorTransceiver::ChannelDisconnected),
    }
}

#[test]
fn client_transceiver_receive_timeout_err_timeout() {
    let (mut server, mut clients) = create_server_and_clients_default::<
        ClientToServerMessage,
        ServerToClientMessage,
    >(CLIENT_SIZE.one);

    let transceiver = clients[0].transceiver().as_ref().unwrap();
    match transceiver.receive_timeout(DEFAULT_TIMEOUT) {
        Ok(_) => panic!("Shouldn't be ok()!"),
        Err(err) => assert_eq!(err, ErrorTransceiver::Timeout),
    }

    close_clients(&mut clients);
    server.stop().unwrap();
}

#[test]
fn client_transceiver_same_thread_send() {
    let (mut server, mut clients) = create_server_and_clients_default::<
        ClientToServerMessage,
        ServerToClientMessage,
    >(CLIENT_SIZE.one);

    let transceiver = clients[0].transceiver().take().unwrap();
    for _ in 0..MSG_SENT_PER_THREAD {
        transceiver.send(ClientToServerMessage::control()).unwrap();
    }

    let control = ClientToServerMessage::control();
    let total_msg = MSG_SENT_PER_THREAD;
    let mut sum_msg: usize = 0;
    timeout_loop! {
        match server.transceiver().as_mut().unwrap().receive() {
            Some(msg) => {
                compare_client_server_message(&control, &msg.message);
                sum_msg += 1;
                if sum_msg == total_msg {
                    break ;
                }
            },
            None => {},
        }
    }

    close_clients(&mut clients);
    server.stop().unwrap();
}

#[test]
fn client_transceiver_multi_thread_send() {
    let (mut server, mut clients) = create_server_and_clients_default::<
        ClientToServerMessage,
        ServerToClientMessage,
    >(CLIENT_SIZE.one);

    let mut handles = Vec::<JoinHandle<()>>::new();
    let transceiver = clients[0].transceiver().take().unwrap();
    for _ in 0..TRANSMITTER_THREAD {
        let transmitter = transceiver.transmitter();
        handles.push(thread::spawn(move || {
            for _ in 0..MSG_SENT_PER_THREAD {
                transmitter.send(ClientToServerMessage::control()).unwrap();
            }
        }));
    }

    // Join all threads
    for handle in handles {
        handle.join().unwrap();
    }

    let control = ClientToServerMessage::control();
    let total_msg = MSG_SENT_PER_THREAD * TRANSMITTER_THREAD;
    let mut sum_msg: usize = 0;
    timeout_loop! {
        match server.transceiver().as_mut().unwrap().receive() {
            Some(msg) => {
                compare_client_server_message(&control, &msg.message);
                sum_msg += 1;
                if sum_msg == total_msg {
                    break ;
                }
            },
            None => {},
        }
    }

    close_clients(&mut clients);
    server.stop().unwrap();
}

#[test]
fn client_transceiver_multi_clients_threads_send() {
    let (mut server, mut clients) = create_server_and_clients_default::<
        ClientToServerMessage,
        ServerToClientMessage,
    >(CLIENT_SIZE.all);

    let mut handles = Vec::<JoinHandle<()>>::new();

    for client in &mut clients {
        let transceiver = client.transceiver().take().unwrap();

        for _ in 0..TRANSMITTER_THREAD {
            let transmitter = transceiver.transmitter();
            handles.push(thread::spawn(move || {
                for _ in 0..MSG_SENT_PER_THREAD {
                    transmitter.send(ClientToServerMessage::control()).unwrap();
                }
            }));
        }
    }

    // Join all threads
    for handle in handles {
        handle.join().unwrap();
    }

    let control = ClientToServerMessage::control();
    let total_msg = MSG_SENT_PER_THREAD * TRANSMITTER_THREAD * CLIENT_SIZE.all;
    let mut sum_msg: usize = 0;
    timeout_loop! {
        match server.transceiver().as_mut().unwrap().receive() {
            Some(msg) => {
                compare_client_server_message(&control, &msg.message);
                sum_msg += 1;
                if sum_msg == total_msg {
                    break ;
                }
            },
            None => {},
        }
    }

    close_clients(&mut clients);
    server.stop().unwrap();
}

#[test]
fn client_transceiver_send_err_disconnected() {
    let mut _trns: Option<Transceiver<ServerToClientMessage, ClientToServerMessage>> = None;

    {
        let (mut server, mut clients) = create_server_and_clients_default::<
            ClientToServerMessage,
            ServerToClientMessage,
        >(CLIENT_SIZE.one);

        _trns = Some(clients[0].transceiver().take().unwrap());

        close_clients(&mut clients);
        server.stop().unwrap();
    }

    match _trns
        .as_ref()
        .unwrap()
        .send(ClientToServerMessage::control())
    {
        Ok(_) => panic!("Shouldn't be ok()!"),
        Err(err) => assert_eq!(err, ErrorTransceiver::ChannelDisconnected),
    }
}
