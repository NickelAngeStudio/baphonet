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

use baphonet::server::{ClientId, ServerBuilder, error::ErrorServer};

use crate::{
    shared::{
        CLIENT_SIZE, close_clients, create_server_and_clients_default,
        message::{ClientToServerMessage, ServerToClientMessage},
    },
    timeout_loop,
};

#[test]
fn server_clients_none() {
    let (mut server, mut _clients) = create_server_and_clients_default::<
        ClientToServerMessage,
        ServerToClientMessage,
    >(CLIENT_SIZE.none);

    let list = server.clients().unwrap();
    assert_eq!(list.len(), 0);
}

#[test]
fn server_clients_one() {
    test_server_clients(CLIENT_SIZE.one);
}

#[test]
fn server_clients_some() {
    test_server_clients(CLIENT_SIZE.some);
}

#[test]
fn server_clients_all() {
    test_server_clients(CLIENT_SIZE.all);
}

#[test]
fn server_clients_err_inactive() {
    let mut server = ServerBuilder::new()
        .build::<ClientToServerMessage, ServerToClientMessage>()
        .unwrap();

    match server.clients() {
        Ok(_) => panic!("Shouldn't be Ok()!"),
        Err(err) => assert_eq!(err, ErrorServer::Inactive),
    }

    server.stop().unwrap();
}

fn test_server_clients(client_count: usize) {
    let (mut server, mut clients) = create_server_and_clients_default::<
        ClientToServerMessage,
        ServerToClientMessage,
    >(client_count);

    let list = server.clients().unwrap();
    assert_eq!(list.len(), client_count);

    // Disconnect half client
    for client_id in 0..client_count {
        server.close_connection(client_id as ClientId).unwrap();
        if client_count > 1 {
            if client_id + 1 >= client_count / 2 {
                break;
            }
        } else {
            break;
        }
    }

    let mut count: usize = 0;
    // Wait confirmation
    timeout_loop! {
        match server.update(){
            Some(update) => match update {
                baphonet::server::message::ServerUpdate::ClientDisconnected(_) => {
                    count += 1;
                    if count >= (client_count / 2){
                        break ;
                    }
                } ,
                _ => {},
            },
            None => {},
        }
    }

    let list = server.clients().unwrap();
    assert_eq!(list.len(), (client_count / 2));
    for client in list {
        assert!(client.client_id() as usize > client_count / 2 - 1);
    }

    if client_count > 1 {
        // Lose connection of top 5
        let size = clients.len();
        clients[size - 1].close();
        clients[size - 2].close();
        clients[size - 3].close();
        clients[size - 4].close();
        clients[size - 5].close();

        let mut count: usize = 0;
        // Wait confirmation
        timeout_loop! {
            match server.update(){
                Some(update) => match update {
                    baphonet::server::message::ServerUpdate::Error(error_update) => match error_update {
                        baphonet::server::error::ErrorUpdate::ConnectionLost(_) => {
                            count += 1;
                            if count >= 5{
                                break ;
                            }
                        },
                        _ => {},
                    },
                    _ => {}
                },
                None => {},
            }
        }

        let list = server.clients().unwrap();
        assert_eq!(list.len(), (client_count / 2) - 5);
    }

    close_clients(&mut clients);
    server.stop().unwrap()
}
