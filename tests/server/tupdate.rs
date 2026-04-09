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

use std::time::Duration;

use baphonet::server::{ClientId, ErrorUpdate, ServerBuilder, ServerUpdate};

use crate::{
    shared::{
        CLIENT_SIZE, TEST_TCP_PORT, close_clients, create_connect_clients,
        create_server_and_clients_default, create_test_socket,
        message::{ClientToServerMessage, ServerToClientMessage},
    },
    timeout_loop,
};

#[test]
fn server_update_none() {
    let mut server = ServerBuilder::new()
        .build::<ClientToServerMessage, ServerToClientMessage>()
        .unwrap();

    assert!(server.update().is_none());

    server.stop();
}

#[test]
fn server_update_some_active() {
    let mut server = ServerBuilder::new()
        .build::<ClientToServerMessage, ServerToClientMessage>()
        .unwrap();

    let port: u16 = TEST_TCP_PORT + 1000;
    server.start(create_test_socket(port)).unwrap();

    timeout_loop! {
        match server.update() {
            Some(update) => match update {
                ServerUpdate::Active => break,
                _ => {},
            },
            None => {},
        }
    }

    server.stop();
}

#[test]
fn server_update_some_client_connected() {
    let mut server = ServerBuilder::new()
        .maximum_client(CLIENT_SIZE.all)
        .build::<ClientToServerMessage, ServerToClientMessage>()
        .unwrap();

    let port: u16 = TEST_TCP_PORT + 1001;
    server.start(create_test_socket(port)).unwrap();

    let mut clients = create_connect_clients::<ClientToServerMessage, ServerToClientMessage>(
        CLIENT_SIZE.all,
        port,
    );

    let mut sum_connected: usize = 0;

    timeout_loop! {
        match server.update() {
            Some(update) => match update {
                ServerUpdate::ClientConnected(_,_) => sum_connected += 1,
                _ => {},
            },
            None => {},
        }
        if sum_connected == CLIENT_SIZE.all {
            break;
        }
    }

    close_clients(&mut clients);
    server.stop();
}

#[test]
fn server_update_some_client_disconnected() {
    let mut server = ServerBuilder::new()
        .maximum_client(CLIENT_SIZE.all)
        .build::<ClientToServerMessage, ServerToClientMessage>()
        .unwrap();

    let port: u16 = TEST_TCP_PORT + 1002;
    server.start(create_test_socket(port)).unwrap();

    let mut clients = create_connect_clients::<ClientToServerMessage, ServerToClientMessage>(
        CLIENT_SIZE.all,
        port,
    );

    std::thread::sleep(Duration::from_millis(50));

    timeout_loop! {
        match server.update() {
            Some(_) => {},
            None => break ,
        }
    }

    for client_id in 0..CLIENT_SIZE.all {
        server.close_connection(client_id as ClientId).unwrap();
    }

    let mut sum_disconnected: usize = 0;

    timeout_loop! {
        match server.update() {
            Some(update) => match update {
                ServerUpdate::ClientDisconnected(_) => sum_disconnected += 1,
                _ => {},
            },
            None => {},
        }
        if sum_disconnected == CLIENT_SIZE.all {
            break;
        }
    }

    close_clients(&mut clients);
    server.stop();
}

#[test]
fn server_update_some_full() {
    let mut server = ServerBuilder::new()
        .maximum_client(CLIENT_SIZE.all)
        .build::<ClientToServerMessage, ServerToClientMessage>()
        .unwrap();

    let port: u16 = TEST_TCP_PORT + 1003;
    server.start(create_test_socket(port)).unwrap();

    let mut clients = create_connect_clients::<ClientToServerMessage, ServerToClientMessage>(
        CLIENT_SIZE.all,
        port,
    );

    timeout_loop! {
        match server.update() {
            Some(update) => match update {
                ServerUpdate::Full => break,
                _ => {},
            },
            None => {},
        }
    }

    close_clients(&mut clients);
    server.stop();
}

#[test]
fn server_update_some_inactive() {
    let (mut server, mut clients) = create_server_and_clients_default::<
        ClientToServerMessage,
        ServerToClientMessage,
    >(CLIENT_SIZE.all);

    server.stop();
    timeout_loop! {
        match server.update() {
            Some(update) => match update {
                ServerUpdate::Inactive => break,
                _ => {},
            },
            None => {},
        }
    }

    close_clients(&mut clients);
}

#[test]
fn server_update_some_error_connection_lost() {
    let (mut server, mut clients) = create_server_and_clients_default::<
        ClientToServerMessage,
        ServerToClientMessage,
    >(CLIENT_SIZE.all);

    let client_id: usize = CLIENT_SIZE.all / 2;
    clients[client_id].close();

    timeout_loop! {
        match server.update() {
            Some(update) => match update {
                ServerUpdate::Error(error) => match error {
                    ErrorUpdate::ConnectionLost(id) => {
                        assert_eq!(client_id, id as usize);
                        break;
                    },
                    _ => {}
                },
                _ => {},
            },
            None => {},
        }
    }

    close_clients(&mut clients);
    server.stop();
}

#[test]
fn server_update_some_error_client_not_found() {
    let (mut server, mut clients) = create_server_and_clients_default::<
        ClientToServerMessage,
        ServerToClientMessage,
    >(CLIENT_SIZE.all);

    let base_cliend_not_found: usize = CLIENT_SIZE.all;
    let client_id: usize = base_cliend_not_found;
    let mut clients_ids: Vec<ClientId> = Vec::new();
    for i in 0..CLIENT_SIZE.all {
        clients_ids.push((CLIENT_SIZE.all + i) as ClientId);
    }

    let transceiver = server.transceiver().take().unwrap();
    transceiver
        .send(client_id as ClientId, ServerToClientMessage::control())
        .unwrap();
    transceiver
        .send_vec(&clients_ids, ServerToClientMessage::control())
        .unwrap();

    let mut not_found_list = Vec::<bool>::new();
    not_found_list.resize(CLIENT_SIZE.all, false);

    timeout_loop! {
        match server.update() {
            Some(update) => match update {
                ServerUpdate::Error(error) => match error {
                    ErrorUpdate::ClientNotFound(id) => {
                        not_found_list[(id as usize) - base_cliend_not_found] = true;
                    },
                    _ => {}
                },
                _ => {},
            },
            None => {},
        }
        if is_all_not_found(&not_found_list){
            break;
        }
    }

    close_clients(&mut clients);
    server.stop();
}

fn is_all_not_found(list: &Vec<bool>) -> bool {
    for b in list {
        if !b {
            return false;
        }
    }

    true
}

#[test]
fn server_update_some_error_outgoing_too_large() {
    todo!()
}

#[test]
fn server_update_some_error_outgoing_serialize_error() {
    todo!()
}

#[test]
fn server_update_some_error_incoming_too_large() {
    todo!()
}

#[test]
fn server_update_some_error_incoming_deserialize_error() {
    todo!()
}
