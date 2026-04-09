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

use std::thread;

use baphonet::server::transceiver;

use crate::{
    shared::{
        CLIENT_SIZE, WORKER_COUNT, accumulate, close_clients, compare_client_server_message,
        create_server_and_clients,
        message::{ClientToServerMessage, ServerToClientMessage},
        send_client_message,
    },
    timeout_loop,
};

/*
#[test]
fn tt() {
    let (mut server, mut clients) = create_server_and_clients::<
        ClientToServerMessage,
        ServerToClientMessage,
    >(CLIENT_SIZE.all, WORKER_COUNT.one, CLIENT_SIZE.all);

    let trcv = server.transceiver().take().unwrap();

    let handle = thread::spawn(move || match trcv.receive_wait() {
        Ok(_) => todo!(),
        Err(_) => todo!(),
    });
}
*/

#[test]
fn server_transceiver_same_thread_receive() {
    let (mut server, mut clients) = create_server_and_clients::<
        ClientToServerMessage,
        ServerToClientMessage,
    >(CLIENT_SIZE.all, WORKER_COUNT.one, CLIENT_SIZE.all);

    for client in &mut clients {
        send_client_message(client, ClientToServerMessage::control());
    }

    let total = accumulate(CLIENT_SIZE.all);
    let mut sum_client_id: usize = 0;
    let control = ClientToServerMessage::control();
    let transceiver = server.transceiver().as_ref().unwrap();
    timeout_loop! {
        match transceiver.receive() {
            Some(message) => {
                sum_client_id += message.client as usize;
                compare_client_server_message(&control, &message.message);

                if sum_client_id == total {
                    break;
                }
            },
            None => {},
        }
    }

    close_clients(&mut clients);
    server.stop();
}

#[test]
fn server_transceiver_diff_thread_receive() {
    todo!()
}

#[test]
fn server_transceiver_same_thread_receive_wait() {
    todo!()
}

#[test]
fn server_transceiver_diff_thread_receive_wait() {
    todo!()
}

#[test]
fn server_transceiver_receive_wait_err_disconnected() {
    todo!()
}

#[test]
fn server_transceiver_same_thread_receive_timeout() {
    todo!()
}

#[test]
fn server_transceiver_diff_thread_receive_timeout() {
    todo!()
}

#[test]
fn server_transceiver_receive_timeout_err_disconnected() {
    todo!()
}

#[test]
fn server_transceiver_receive_timeout_err_timeout() {
    todo!()
}

#[test]
fn server_transceiver_same_thread_send() {
    todo!()
}

#[test]
fn server_transceiver_multi_thread_send() {
    todo!()
}

#[test]
fn server_transceiver_send_err_disconnected() {
    todo!()
}

#[test]
fn server_transceiver_same_thread_send_vec() {
    todo!()
}

#[test]
fn server_transceiver_multi_thread_send_vec() {
    todo!()
}

#[test]
fn server_transceiver_send_vec_err_disconnected() {
    todo!()
}

#[test]
fn server_transceiver_send_vec_no_destination() {
    todo!()
}
