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

use baphonet::{client::Client, server::message::{ServerMessage, SupervisorUpdate}};
use crate::{shared::{CLIENT_SIZE, WORKER_COUNT, create_server_and_clients, create_server_and_port, create_test_socket, message::{ClientToServerMessage, ServerToClientMessage}}, timeout_loop};

#[test]
fn server_message_none() {
    let (mut server, _) = create_server_and_port::<ClientToServerMessage, ServerToClientMessage>(CLIENT_SIZE.all, WORKER_COUNT.all);

    timeout_loop!{
        match server.message() {
            Some(_) => {},
            None => break,
        }
    }

}

#[test]
fn server_message_some_incoming() {
    todo!()
}

#[test]
fn server_message_update_active() {
    
    let (mut server, _) = create_server_and_port::<ClientToServerMessage, ServerToClientMessage>(CLIENT_SIZE.all, WORKER_COUNT.all);

    timeout_loop!{
        match server.message() {
            Some(message) => match message {
                ServerMessage::Incoming(_) => {},
                ServerMessage::Update(update) => match update{
                    SupervisorUpdate::Active => break ,
                    _ => {},
                },
            },
            None => {},
        }
    }

}

#[test]
fn server_message_update_client_connected_one() {
    
    let (mut server, _clients) = create_server_and_clients::<ClientToServerMessage, ServerToClientMessage>(CLIENT_SIZE.all, WORKER_COUNT.all, CLIENT_SIZE.one);
    
    timeout_loop!{
        match server.message() {
            Some(msg) => match msg {
                ServerMessage::Update(update) => {
                    match update {
                        SupervisorUpdate::ClientConnected(client_id) => {
                            assert_eq!(client_id, 0);
                            break;   
                        },
                        _ => {},
                    }
                },
                _ => {},
            },
            None => {},
        }
    }


}

#[test]
fn server_message_update_client_disconnected() {
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