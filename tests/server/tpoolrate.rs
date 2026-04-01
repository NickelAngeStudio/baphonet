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

use baphonet::server::{ErrorServer, MAXIMUM_POOL_RATE_PER_SECOND, MINIMUM_POOL_RATE_PER_SECOND, POOL_RATE_PER_SECOND, Server, message::{ServerMessage, SupervisorUpdate}};

use crate::{shared::{CLIENT_SIZE, TEST_IPV4, TEST_TCP_PORT, WORKER_COUNT, message::{ClientToServerMessage, ServerToClientMessage}}, timeout_loop};



#[test]
fn server_pool_rate_default() {
    
    let server = Server::<ClientToServerMessage, ServerToClientMessage>::new(CLIENT_SIZE.all, WORKER_COUNT.some).unwrap();

    assert_eq!(server.pool_rate(), POOL_RATE_PER_SECOND);

}

#[test]
fn server_pool_rate_set_before_start_ok() {
    let mut server = Server::<ClientToServerMessage, ServerToClientMessage>::new(CLIENT_SIZE.all, WORKER_COUNT.some).unwrap();

    for pool_rate in MINIMUM_POOL_RATE_PER_SECOND..MAXIMUM_POOL_RATE_PER_SECOND {
        server.set_pool_rate(pool_rate).unwrap();
        assert_eq!(server.pool_rate(), pool_rate);
    }
    

}

#[test]
fn server_pool_rate_set_after_start_ok() {
    let socket = SocketAddr::new(IpAddr::V4(TEST_IPV4), TEST_TCP_PORT - 100);
    let mut server = Server::<ClientToServerMessage, ServerToClientMessage>::new(CLIENT_SIZE.all, WORKER_COUNT.some).unwrap();
    server.start(socket).unwrap();

    for pool_rate in MINIMUM_POOL_RATE_PER_SECOND..MAXIMUM_POOL_RATE_PER_SECOND {
        server.set_pool_rate(pool_rate).unwrap();
        assert_eq!(server.pool_rate(), pool_rate);

        timeout_loop!{  // Make sure supervisor received the new pool rate
            match server.message() {
                Some(msg) => match msg {
                    ServerMessage::Update(update) => match update {
                        SupervisorUpdate::PoolRate(pr) => {
                            assert_eq!(pr, pool_rate);
                            break;
                        },
                        _ => {},
                    },
                    _ => {},
                },
                None => {},
            }
        }
    }

}

#[test]
fn server_pool_rate_set_err_below_min() {

    let mut server = Server::<ClientToServerMessage, ServerToClientMessage>::new(CLIENT_SIZE.all, WORKER_COUNT.some).unwrap();

    match server.set_pool_rate(MINIMUM_POOL_RATE_PER_SECOND - 1) {
        Ok(_) => panic!("Shouldn't be Ok()!"),
        Err(err) => assert_eq!(err, ErrorServer::PoolRateBelowMinimum),
    }

}

#[test]
fn server_pool_rate_set_err_above_max() {
     let mut server = Server::<ClientToServerMessage, ServerToClientMessage>::new(CLIENT_SIZE.all, WORKER_COUNT.some).unwrap();

    match server.set_pool_rate(MAXIMUM_POOL_RATE_PER_SECOND + 1) {
        Ok(_) => panic!("Shouldn't be Ok()!"),
        Err(err) => assert_eq!(err, ErrorServer::PoolRateAboveMaximum),
    }
}