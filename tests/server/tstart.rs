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

use std::net::{IpAddr, SocketAddr};

use baphonet::server::{ErrorServer, Server, ServerBuilder, ServerStatus};

use crate::shared::{
    CLIENT_SIZE, TEST_IPV4, TEST_TCP_PORT, WORKER_COUNT,
    message::{ClientToServerMessage, ServerToClientMessage},
};

#[test]
fn server_start_ok() {
    let socket = SocketAddr::new(IpAddr::V4(TEST_IPV4), TEST_TCP_PORT - 1);
    let mut server = ServerBuilder::new()
        .build::<ClientToServerMessage, ServerToClientMessage>()
        .unwrap();

    match server.start(socket) {
        Ok(_) => assert_eq!(server.status(), ServerStatus::Starting),
        Err(err) => panic!("start() shouldn't err({:?})", err),
    }

    server.stop().unwrap();
}

#[test]
fn server_start_err_active() {
    let socket = SocketAddr::new(IpAddr::V4(TEST_IPV4), TEST_TCP_PORT - 2);
    let mut server = ServerBuilder::new()
        .build::<ClientToServerMessage, ServerToClientMessage>()
        .unwrap();

    server.start(socket).unwrap();
    match server.start(socket) {
        Ok(_) => panic!("start() shouldn't be Ok()!"),
        Err(err) => assert_eq!(err, ErrorServer::AlreadyActive),
    }

    server.stop().unwrap();
}

#[test]
fn server_start_err_address_already_used() {
    let socket = SocketAddr::new(IpAddr::V4(TEST_IPV4), TEST_TCP_PORT - 4);

    let mut server1 = ServerBuilder::new()
        .build::<ClientToServerMessage, ServerToClientMessage>()
        .unwrap();
    let mut server2 = ServerBuilder::new()
        .build::<ClientToServerMessage, ServerToClientMessage>()
        .unwrap();

    server1.start(socket).unwrap();
    match server2.start(socket) {
        Ok(_) => panic!("start() shouldn't be Ok()!"),
        Err(err) => assert_eq!(err, ErrorServer::SocketAddressAlreadyUsed),
    }

    server1.stop().unwrap();
    server2.stop().unwrap();
}
