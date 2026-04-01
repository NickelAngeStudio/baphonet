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

use baphonet::{client::{Client, ErrorClient, MINIMUM_POOL_RATE_PER_SECOND, POOL_RATE_PER_SECOND}, server::MAXIMUM_POOL_RATE_PER_SECOND};

use crate::shared::{CLIENT_SIZE, WORKER_COUNT, create_server_and_port, create_test_socket, message::{ClientToServerMessage, ServerToClientMessage}};

#[test]
fn client_pool_rate_default() {
    let client = Client::<ServerToClientMessage, ClientToServerMessage>::new();
    assert_eq!(client.pool_rate(), POOL_RATE_PER_SECOND);
}

#[test]
fn client_pool_rate_set_before_connect_ok() {
    let mut client = Client::<ServerToClientMessage, ClientToServerMessage>::new();
    for pool_rate in MINIMUM_POOL_RATE_PER_SECOND..MAXIMUM_POOL_RATE_PER_SECOND {
        client.set_pool_rate(pool_rate).unwrap();
        assert_eq!(client.pool_rate(), pool_rate);
    }
    
}


#[test]
fn client_pool_rate_set_after_connect_ok() {

    let (mut server, port) = create_server_and_port::<ClientToServerMessage, ServerToClientMessage>(CLIENT_SIZE.all, WORKER_COUNT.all);
    let mut client = Client::<ServerToClientMessage, ClientToServerMessage>::new();

    client.connect(create_test_socket(port)).unwrap();
    for pool_rate in MINIMUM_POOL_RATE_PER_SECOND..MAXIMUM_POOL_RATE_PER_SECOND {
        client.set_pool_rate(pool_rate).unwrap();
        assert_eq!(client.pool_rate(), pool_rate);
    }

    client.close().unwrap();
    server.stop().unwrap();

}

#[test]
fn client_pool_rate_set_err_below_min() {
    let mut client = Client::<ServerToClientMessage, ClientToServerMessage>::new();

    match client.set_pool_rate(MINIMUM_POOL_RATE_PER_SECOND - 1) {
        Ok(_) => panic!("Shouldn't be Ok()!"),
        Err(err) => assert_eq!(err, ErrorClient::PoolRateBelowMinimum),
    } 
}

#[test]
fn client_pool_rate_set_err_above_max() {
    let mut client = Client::<ServerToClientMessage, ClientToServerMessage>::new();

    match client.set_pool_rate(MAXIMUM_POOL_RATE_PER_SECOND + 1) {
        Ok(_) => panic!("Shouldn't be Ok()!"),
        Err(err) => assert_eq!(err, ErrorClient::PoolRateAboveMaximum),
    } 
}