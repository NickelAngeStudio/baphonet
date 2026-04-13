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

use std::net::{Ipv4Addr, SocketAddr};

use baphonet::client::{ErrorClient, builder::ClientBuilder};

use crate::{
    run_tests,
    shared::{
        CLIENT_SIZE, WORKER_COUNT, create_server_and_port, create_test_socket,
        message::{ClientToServerMessage, ServerToClientMessage},
    },
};

run_tests!(client_connect_run_tests(
    client_connect_ok,
    client_connect_err_not_found,
    client_connect_err_already_connected
));

#[test]
#[ignore = "Executed in serial with `client_connect_run_tests`."]
fn client_connect_ok() {
    let (mut server, port) = create_server_and_port::<ClientToServerMessage, ServerToClientMessage>(
        CLIENT_SIZE.all,
        WORKER_COUNT.all,
    );
    let mut client = ClientBuilder::new()
        .build::<ServerToClientMessage, ClientToServerMessage>()
        .unwrap();

    match client.connect(create_test_socket(port)) {
        Ok(_) => {}
        Err(_) => panic!("Shouldn't be err()!"),
    }

    client.close();
    server.stop();
}

#[test]
#[ignore = "Executed in serial with `client_connect_run_tests`."]
fn client_connect_err_not_found() {
    let (mut server, port) = create_server_and_port::<ClientToServerMessage, ServerToClientMessage>(
        CLIENT_SIZE.all,
        WORKER_COUNT.all,
    );
    let mut client = ClientBuilder::new()
        .build::<ServerToClientMessage, ClientToServerMessage>()
        .unwrap();

    let socket = SocketAddr::new(
        std::net::IpAddr::V4(Ipv4Addr::new(255, 255, 255, 255)),
        port,
    );
    match client.connect(socket) {
        Ok(_) => panic!("Shouldn't be Ok()!"),
        Err(err) => assert_eq!(err, ErrorClient::ServerNotFound),
    }

    client.close();
    server.stop();
}

#[test]
#[ignore = "Executed in serial with `client_connect_run_tests`."]
fn client_connect_err_already_connected() {
    let (mut server, port) = create_server_and_port::<ClientToServerMessage, ServerToClientMessage>(
        CLIENT_SIZE.all,
        WORKER_COUNT.all,
    );
    let mut client = ClientBuilder::new()
        .build::<ServerToClientMessage, ClientToServerMessage>()
        .unwrap();

    let socket = create_test_socket(port);
    match client.connect(socket.clone()) {
        Ok(_) => {}
        Err(_) => panic!("Shouldn't be err()!"),
    }
    match client.connect(socket.clone()) {
        Ok(_) => panic!("Shouldn't be Ok()!"),
        Err(err) => assert_eq!(err, ErrorClient::AlreadyConnected),
    }

    client.close();
    server.stop();
}
