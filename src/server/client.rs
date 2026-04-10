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

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream},
    sync::{Arc, Mutex},
};

use crate::server::ClientId;

/// Definition of the shared client list.
pub(crate) type Clients = Arc<Vec<Mutex<Option<Client>>>>;

/// Client of the server
pub(crate) struct Client {
    /// TcpStream used to communicate with client
    pub stream: TcpStream,

    /// Incoming message size if any
    pub inc_msg_size: Option<usize>,
}

impl Client {
    pub fn new(stream: TcpStream) -> Client {
        Client {
            stream,
            inc_msg_size: None,
        }
    }
}

/// A client of the server given from [`Server::clients()`](super::Server::clients());
pub struct ServerClient {
    client_id: ClientId,
    addr: SocketAddr,
}

impl ServerClient {
    /// Create a [`ServerClient`] entry from [`Client`].
    pub(crate) fn from_client(client_id: ClientId, client: &Client) -> ServerClient {
        let addr = match client.stream.peer_addr() {
            Ok(addr) => addr,
            Err(_) => SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
        };

        ServerClient { client_id, addr }
    }

    /// Get the client_id of client
    pub fn client_id(&self) -> u16 {
        self.client_id
    }

    /// Get the IP address of the client.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
}
