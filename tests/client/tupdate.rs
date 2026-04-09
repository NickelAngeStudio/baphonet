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

use baphonet::{
    Message,
    client::{ClientBuilder, ClientUpdate, ErrorWorker, OUTGOING_SIZE_DEFAULT},
    server::{INCOMING_SIZE_MINIMUM, message::ServerUpdate},
};

use crate::{
    shared::{
        CLIENT_SIZE, WORKER_COUNT, create_server_and_clients_default, create_server_and_port,
        create_test_socket,
        message::{ClientToServerMessage, ServerToClientMessage},
    },
    timeout_loop,
};

#[test]
fn client_update_none() {
    let mut client = ClientBuilder::new()
        .build::<ServerToClientMessage, ClientToServerMessage>()
        .unwrap();

    assert!(client.update().is_none());
}

#[test]
fn client_update_connected() {
    let (mut server, port) = create_server_and_port::<ClientToServerMessage, ServerToClientMessage>(
        CLIENT_SIZE.all,
        WORKER_COUNT.some,
    );
    let mut client = ClientBuilder::new()
        .build::<ServerToClientMessage, ClientToServerMessage>()
        .unwrap();

    client.connect(create_test_socket(port)).unwrap();

    timeout_loop! {
        match client.update() {
            Some(update) => match update{
                ClientUpdate::Connected => break ,
                _ => {},
            },
            None => {},
        }
    }

    client.close();
    server.stop();
}

#[test]
fn client_update_disconnected() {
    let (mut server, mut clients) = create_server_and_clients_default::<
        ClientToServerMessage,
        ServerToClientMessage,
    >(CLIENT_SIZE.one);
    let mut client = clients.pop().unwrap();

    client.close();
    timeout_loop! {
        match client.update() {
            Some(update) => match update{
                ClientUpdate::Disconnected => break ,
                _ => {},
            },
            None => {},
        }
    }

    server.stop();
}

#[test]
fn client_update_some_error_connection_lost() {
    let (mut server, mut clients) = create_server_and_clients_default::<
        ClientToServerMessage,
        ServerToClientMessage,
    >(CLIENT_SIZE.one);
    let mut client = clients.pop().unwrap();

    server.stop();
    timeout_loop! {
        match client.update() {
            Some(update) => match update{
                ClientUpdate::Error(error) => match error{
                    ErrorWorker::ConnectionLost => break ,
                    _ => {}
                } ,
                _ => {},
            },
            None => {},
        }
    }
    client.close();
}

macro_rules! create_test_message {
    ($struct:ident, {$($serialize:tt)*}, {$($deserialize:tt)*}) => {
        struct $struct {}
        impl Message for $struct {
            fn serialize(&self, _buffer: &mut [u8]) -> Result<usize, ()> {
                $($serialize)*
            }
            fn deserialize(_buffer: &[u8]) -> Result<Self, ()>
            where
                Self: Sized,
            {
                $($deserialize)*
            }
        }
    };
}
create_test_message!(OutgoingSerializeError, { Err(()) }, { todo!() });

#[test]
fn client_update_some_error_outgoing_serialize() {
    let (mut server, mut clients) = create_server_and_clients_default::<
        OutgoingSerializeError,
        OutgoingSerializeError,
    >(CLIENT_SIZE.one);
    let mut client = clients.pop().unwrap();

    client
        .transceiver()
        .as_mut()
        .unwrap()
        .send(OutgoingSerializeError {})
        .unwrap();

    timeout_loop! {
        match client.update() {
            Some(update) => match update{
                ClientUpdate::Error(error) => match error{
                    ErrorWorker::OutgoingSerializeError => break ,
                    _ => {}
                } ,
                _ => {},
            },
            None => {},
        }
    }
    client.close();
    server.stop();
}

create_test_message!(OutgoingTooLargeError, { Ok(OUTGOING_SIZE_DEFAULT + 1) }, {
    todo!()
});
#[test]
fn client_update_some_error_outgoing_too_large() {
    let (mut server, mut clients) = create_server_and_clients_default::<
        OutgoingTooLargeError,
        OutgoingTooLargeError,
    >(CLIENT_SIZE.one);
    let mut client = clients.pop().unwrap();

    client
        .transceiver()
        .as_mut()
        .unwrap()
        .send(OutgoingTooLargeError {})
        .unwrap();

    timeout_loop! {
        match client.update() {
            Some(update) => match update{
                ClientUpdate::Error(error) => match error{
                    ErrorWorker::OutgoingMessageTooLarge => break ,
                    _ => {}
                } ,
                _ => {},
            },
            None => {},
        }
    }
    client.close();
    server.stop();
}

#[test]
fn client_update_some_error_incoming_too_large() {
    let (mut server, port) = create_server_and_port::<ClientToServerMessage, ServerToClientMessage>(
        CLIENT_SIZE.all,
        WORKER_COUNT.some,
    );
    let mut client = ClientBuilder::new()
        .incoming_max_size(INCOMING_SIZE_MINIMUM)
        .build::<ServerToClientMessage, ClientToServerMessage>()
        .unwrap();
    client.connect(create_test_socket(port)).unwrap();

    let mut cnt: usize = 0;
    timeout_loop! {
        match client.update() {
            Some(update) => match update {
                ClientUpdate::Connected => cnt += 1,
                _ => {}
            },
            None => {},
        }

        match server.update() {
            Some(update) => match update {
                ServerUpdate::ClientConnected(_, _) => cnt += 1,
                _ => {},
            },
            None => {},
        }

        if cnt >= 2 {
            break;
        }
    }

    server
        .transceiver()
        .as_mut()
        .unwrap()
        .send(0, ServerToClientMessage::control())
        .unwrap();

    timeout_loop! {
        match client.update() {
            Some(update) => match update{
                ClientUpdate::Error(error) => match error{
                    ErrorWorker::IncomingMessageTooLarge => break ,
                    _ => {}
                } ,
                _ => {},
            },
            None => {},
        }
    }
    client.close();
    server.stop();
}

create_test_message!(IncomingDeserializeError, { Ok(OUTGOING_SIZE_DEFAULT) }, {
    Err(())
});
#[test]
fn client_update_some_error_incoming_message_error() {
    let (mut server, mut clients) = create_server_and_clients_default::<
        IncomingDeserializeError,
        IncomingDeserializeError,
    >(CLIENT_SIZE.one);
    let mut client = clients.pop().unwrap();

    server
        .transceiver()
        .as_mut()
        .unwrap()
        .send(0, IncomingDeserializeError {})
        .unwrap();

    timeout_loop! {
        match client.update() {
            Some(update) => match update{
                ClientUpdate::Error(error) => match error{
                    ErrorWorker::IncomingMessageDeserializeError => break ,
                    _ => {}
                } ,
                _ => {},
            },
            None => {},
        }
    }
    client.close();
    server.stop();
}
