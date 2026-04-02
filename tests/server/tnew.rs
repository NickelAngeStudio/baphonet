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

use baphonet::server::{ErrorServer, SERVER_MAXIMUM_CLIENT_CAP, SERVER_MINIMUM_CLIENT_CAP, SERVER_MINIMUM_WORKER_CAP, Server, ServerStatus};

use crate::shared::message::{ClientToServerMessage, ServerToClientMessage};


#[test]
fn server_new_ok_minimum_client() {
    match Server::<ClientToServerMessage, ServerToClientMessage>::new(SERVER_MINIMUM_CLIENT_CAP, SERVER_MINIMUM_WORKER_CAP) {
        Ok(server) => assert_eq!(server.status(), ServerStatus::Inactive),
        Err(err) => panic!("Shouldn't err({:?})!", err),
    }
}

#[test]
fn server_new_ok_maximum_client() {

    match Server::<ClientToServerMessage, ServerToClientMessage>::new(SERVER_MAXIMUM_CLIENT_CAP, SERVER_MINIMUM_WORKER_CAP) {
        Ok(server) => assert_eq!(server.status(), ServerStatus::Inactive),
        Err(err) => panic!("Shouldn't err({:?})!", err),
    }

}

#[test]
fn server_new_ok_minimum_worker() {
    match Server::<ClientToServerMessage, ServerToClientMessage>::new(SERVER_MINIMUM_CLIENT_CAP, SERVER_MINIMUM_WORKER_CAP) {
        Ok(server) => assert_eq!(server.status(), ServerStatus::Inactive),
        Err(err) => panic!("Shouldn't err({:?})!", err),
    }
}

#[test]
fn server_new_ok_maximum_worker() {
    match Server::<ClientToServerMessage, ServerToClientMessage>::new(SERVER_MAXIMUM_CLIENT_CAP, SERVER_MAXIMUM_CLIENT_CAP) {
        Ok(server) => assert_eq!(server.status(), ServerStatus::Inactive),
        Err(err) => panic!("Shouldn't err({:?})!", err),
    }
}

#[test]
fn server_new_err_client_below_minimum() {
    match Server::<ClientToServerMessage, ServerToClientMessage>::new(SERVER_MINIMUM_CLIENT_CAP - 1, SERVER_MINIMUM_WORKER_CAP) {
        Ok(_) => panic!("Shouldn't be Ok()!"),
        Err(err) =>assert_eq!(err, ErrorServer::MaximumClientBelowMinimum),
    }
}

#[test]
fn server_new_err_client_above_maximum() {
    match Server::<ClientToServerMessage, ServerToClientMessage>::new(SERVER_MAXIMUM_CLIENT_CAP + 1, SERVER_MINIMUM_WORKER_CAP) {
        Ok(_) => panic!("Shouldn't be Ok()!"),
        Err(err) =>assert_eq!(err, ErrorServer::MaximumClientAboveMaximum),
    }
}

#[test]
fn server_new_err_worker_below_minimum() {
    match Server::<ClientToServerMessage, ServerToClientMessage>::new(SERVER_MINIMUM_CLIENT_CAP, SERVER_MINIMUM_WORKER_CAP - 1) {
        Ok(_) => panic!("Shouldn't be Ok()!"),
        Err(err) =>assert_eq!(err, ErrorServer::WorkerCountBelowMinimum),
    }
}

#[test]
fn server_new_err_worker_above_maximum() {
    match Server::<ClientToServerMessage, ServerToClientMessage>::new(SERVER_MAXIMUM_CLIENT_CAP, SERVER_MAXIMUM_CLIENT_CAP + 1) {
        Ok(_) => panic!("Shouldn't be Ok()!"),
        Err(err) =>assert_eq!(err, ErrorServer::WorkerCountAboveMaximum),
    }
}
