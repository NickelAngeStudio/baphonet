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

use baphonet::server::{ClientId, message::{ServerMessage, SupervisorUpdate}};
use crate::{shared::{CLIENT_SIZE, WORKER_COUNT, accumulate, create_server_and_clients, create_server_and_port, message::{ClientToServerMessage, ServerToClientMessage}}, timeout_loop};

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
fn server_message_some_incoming_one_client() {
    todo!()
}

#[test]
fn server_message_some_incoming_some_client() {
    todo!()
}

#[test]
fn server_message_some_incoming_all_client() {
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
    server_message_update_client_connected(WORKER_COUNT.one,CLIENT_SIZE.one);
    server_message_update_client_connected(WORKER_COUNT.some,CLIENT_SIZE.one);
    server_message_update_client_connected(WORKER_COUNT.all,CLIENT_SIZE.one);
}


#[test]
fn server_message_update_client_connected_some() {
    server_message_update_client_connected(WORKER_COUNT.one,CLIENT_SIZE.some);
    server_message_update_client_connected(WORKER_COUNT.some,CLIENT_SIZE.some);
    server_message_update_client_connected(WORKER_COUNT.all,CLIENT_SIZE.some);
}

#[test]
fn server_message_update_client_connected_all() {
    server_message_update_client_connected(WORKER_COUNT.one,CLIENT_SIZE.all);
    server_message_update_client_connected(WORKER_COUNT.some,CLIENT_SIZE.all);
    server_message_update_client_connected(WORKER_COUNT.all,CLIENT_SIZE.all);
}



#[test]
fn server_message_update_client_disconnected_one() {
    server_message_update_client_disconnected(WORKER_COUNT.one,CLIENT_SIZE.one);
    server_message_update_client_disconnected(WORKER_COUNT.some,CLIENT_SIZE.one);
    server_message_update_client_disconnected(WORKER_COUNT.all,CLIENT_SIZE.one);
}

#[test]
fn server_message_update_client_disconnected_some() {
    server_message_update_client_disconnected(WORKER_COUNT.one,CLIENT_SIZE.some);
    server_message_update_client_disconnected(WORKER_COUNT.some,CLIENT_SIZE.some);
    server_message_update_client_disconnected(WORKER_COUNT.all,CLIENT_SIZE.some);
}

#[test]
fn server_message_update_client_disconnected_all() {
    server_message_update_client_disconnected(WORKER_COUNT.one,CLIENT_SIZE.all);
    server_message_update_client_disconnected(WORKER_COUNT.some,CLIENT_SIZE.all);
    server_message_update_client_disconnected(WORKER_COUNT.all,CLIENT_SIZE.all);
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
fn server_message_update_client_connected(worker_count : usize, count : usize){
    let (mut server, clients) = create_server_and_clients::<ClientToServerMessage, ServerToClientMessage>(CLIENT_SIZE.all, worker_count, count);
    
    let total_client_id : usize = accumulate(clients.len());
    let mut sum_client_id : usize = 0;
    timeout_loop!{
        match server.message() {
            Some(msg) => match msg {
                ServerMessage::Update(update) => {
                    match update {
                        SupervisorUpdate::ClientConnected(client_id) => {
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

    server.stop().unwrap();
}


/// Receive client connected update and add them up.
fn server_message_update_client_disconnected(worker_count : usize, count : usize){
    let (mut server, clients) = create_server_and_clients::<ClientToServerMessage, ServerToClientMessage>(CLIENT_SIZE.all, worker_count, count);
    
    let total_client_id : usize = accumulate(clients.len());
    let mut sum_client_id : usize = 0;

    // Close each connection
    for i in 0..clients.len() {
        server.close_connection(i as ClientId).unwrap();
    }

    timeout_loop!{
        match server.message() {
            Some(msg) => match msg {
                ServerMessage::Update(update) => {
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

    server.stop().unwrap();
}

