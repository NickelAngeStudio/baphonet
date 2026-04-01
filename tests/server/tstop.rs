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


use crate::shared::{CLIENT_SIZE, create_server_and_clients_default, message::{ClientToServerMessage, ServerToClientMessage}};


#[test]
fn server_stop_ok_client_none() {
    let (mut server, _clients) = create_server_and_clients_default::<ClientToServerMessage, ServerToClientMessage>(CLIENT_SIZE.none);

    match server.stop() {
        Ok(_) => {},
        Err(err) => panic!("Shouldn't be err({:?})", err),
    }
}

#[test]
fn server_stop_ok_client_one() {
        let (mut server, _clients) = create_server_and_clients_default::<ClientToServerMessage, ServerToClientMessage>(CLIENT_SIZE.one);


    match server.stop() {
        Ok(_) => {},
        Err(err) => panic!("Shouldn't be err({:?})", err),
    }
}

#[test]
fn server_stop_ok_client_some() {
    let (mut server, _clients) = create_server_and_clients_default::<ClientToServerMessage, ServerToClientMessage>(CLIENT_SIZE.some);

    match server.stop() {
        Ok(_) => {},
        Err(err) => panic!("Shouldn't be err({:?})", err),
    }
}

#[test]
fn server_stop_ok_client_all() {
    let (mut server, _clients) = create_server_and_clients_default::<ClientToServerMessage, ServerToClientMessage>(CLIENT_SIZE.all);

    match server.stop() {
        Ok(_) => {},
        Err(err) => panic!("Shouldn't be err({:?})", err),
    }
}