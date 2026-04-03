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

use std::time::Duration;

use baphonet::{
    client::message::ClientMessage,
    server::{ClientId, message::OutgoingMessage},
};

use crate::{
    shared::{
        CLIENT_SIZE, close_clients, create_server_and_clients_default,
        message::{ClientToServerMessage, ServerToClientMessage, assert_server_to_client_message},
    },
    timeout_loop,
};

/// Count of message sent to each client by server.
const SERVER_SEND_COUNT: usize = u8::MAX as usize;

#[test]
fn client_message_none() {
    let (mut server, mut clients) = create_server_and_clients_default::<
        ClientToServerMessage,
        ServerToClientMessage,
    >(CLIENT_SIZE.one);

    timeout_loop! {
        match clients[0].message() {
            Some(_) => {},
            None => break,
        }
    }

    close_clients(&mut clients);
    server.stop().unwrap();
}

#[test]
fn client_message_some_incoming() {
    let (mut server, mut clients) = create_server_and_clients_default::<
        ClientToServerMessage,
        ServerToClientMessage,
    >(CLIENT_SIZE.all);

    let mut client_rcv_count = Vec::<usize>::new();
    client_rcv_count.resize(clients.len(), 0);

    let control = ServerToClientMessage::control();
    for _ in 0..SERVER_SEND_COUNT {
        for client_id in 0..clients.len() {

            //let mut outgoing = OutgoingMessage::new(ServerToClientMessage::control());
            //outgoing.add_destination(client_id as ClientId);

            //server.send(outgoing).unwrap();
        }
    }

    let timeout = Duration::from_secs(10);
    timeout_loop! { timeout,

        for client_id in 0..clients.len() {
            match clients[client_id].message() {
                Some(msg) => match msg {
                    ClientMessage::Incoming(msg) => {
                        assert_server_to_client_message(&control, &msg);
                        client_rcv_count[client_id] += 1;
                    },
                    _ => {},
                },
                None => {},
            }
        }

        if is_all_received(&client_rcv_count){
            break;
        }



    }
}

/// Make sure all messages were received
fn is_all_received(rcv: &Vec<usize>) -> bool {
    for cpt in rcv {
        if *cpt != SERVER_SEND_COUNT {
            return false;
        }
    }

    true
}

#[test]
fn client_message_some_error_connection_lost() {
    todo!()
}

#[test]
fn client_message_some_error_outgoing_serialize() {
    todo!()
}

#[test]
fn client_message_some_error_outgoing_too_large() {
    todo!()
}

#[test]
fn client_message_some_error_incoming_deserialize() {
    todo!()
}

#[test]
fn client_message_some_status_active() {
    todo!()
}

#[test]
fn client_message_some_status_ended() {
    todo!()
}
