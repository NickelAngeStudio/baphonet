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

use std::{thread, time::Duration};

use baphonet::{
    client::status::Status,
    server::{
        ClientId, ServerBuilder,
        error::{ErrorServer, ErrorUpdate},
        message::ServerUpdate,
    },
};

use crate::{
    shared::{
        CLIENT_SIZE, WORKER_COUNT, close_clients, create_server_and_clients,
        message::{ClientToServerMessage, ServerToClientMessage},
    },
    timeout_loop,
};

#[test]
fn server_close_connection_ok_one() {
    server_close_connection_ok_client(WORKER_COUNT.one, CLIENT_SIZE.one);
    server_close_connection_ok_client(WORKER_COUNT.some, CLIENT_SIZE.one);
    server_close_connection_ok_client(WORKER_COUNT.all, CLIENT_SIZE.one);
}

#[test]
fn server_close_connection_ok_some() {
    server_close_connection_ok_client(WORKER_COUNT.one, CLIENT_SIZE.some);
    server_close_connection_ok_client(WORKER_COUNT.some, CLIENT_SIZE.some);
    server_close_connection_ok_client(WORKER_COUNT.all, CLIENT_SIZE.some);
}

#[test]
fn server_close_connection_ok_all() {
    server_close_connection_ok_client(WORKER_COUNT.one, CLIENT_SIZE.all);
    server_close_connection_ok_client(WORKER_COUNT.some, CLIENT_SIZE.all);
    server_close_connection_ok_client(WORKER_COUNT.all, CLIENT_SIZE.all);
}

#[test]
fn server_close_connection_err_inactive() {
    let mut server = ServerBuilder::new()
        .build::<ClientToServerMessage, ServerToClientMessage>()
        .unwrap();

    match server.close_connection(0) {
        Ok(_) => panic!("Shouldn't be Ok()!"),
        Err(err) => assert_eq!(err, ErrorServer::Inactive),
    }

    server.stop();
}

#[test]
fn server_close_connection_err_not_found() {
    let (mut server, mut clients) = create_server_and_clients::<
        ClientToServerMessage,
        ServerToClientMessage,
    >(CLIENT_SIZE.all, WORKER_COUNT.one, CLIENT_SIZE.all);

    let invalid_client_id = (CLIENT_SIZE.all + 1) as ClientId;
    server.close_connection(invalid_client_id).unwrap();

    timeout_loop! {
        match server.update() {
            Some(msg) => match msg {
                ServerUpdate::Error(error_update) => match error_update{
                    ErrorUpdate::ClientNotFound(client_id) => {
                        assert_eq!(client_id, invalid_client_id);
                        break;
                    },
                    _ => {},
                },
                _ => {},
            },
            None => {},
        }
    }

    close_clients(&mut clients);
    server.stop();
}

/// Close connections to clients
fn server_close_connection_ok_client(worker_count: usize, client_count: usize) {
    let (mut server, mut clients) = create_server_and_clients::<
        ClientToServerMessage,
        ServerToClientMessage,
    >(CLIENT_SIZE.all, worker_count, client_count);

    for client_id in 0..clients.len() {
        server.close_connection(client_id as u16).unwrap();
    }

    let mut client_disconnected = Vec::<bool>::new();
    client_disconnected.resize(client_count, false);

    timeout_loop! {
        match server.update() {
            Some(update) => match update {
                ServerUpdate::ClientDisconnected(client_id) => {
                    client_disconnected[client_id as usize] = true;
                },
                _ => {},
            },
            None => {},
        }

        if is_all_disconnected(&client_disconnected) {
            break;
        }

    }

    thread::sleep(Duration::from_millis(100));

    for client in &mut clients {
        'message:   // Fetch client message to update
        loop {
            match client.update(){
                Some(_) => {},
                None => break 'message,
            }
        }

        // All client should be disconnected
        assert_eq!(client.status(), Status::Disconnected);
    }

    server.stop();
}

/// Returns true if all client are disconnected
fn is_all_disconnected(cd: &Vec<bool>) -> bool {
    for b in cd {
        if !b {
            return false;
        }
    }

    true
}
