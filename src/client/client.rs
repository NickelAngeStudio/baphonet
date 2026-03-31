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

use std::{net::SocketAddr, thread::JoinHandle};

use crate::{Message, client::{Error, channel::{ClientChannel, create_client_worker_channels}, status::ClientStatus, worker::{self, Worker}}};

/// Status of a task
pub(crate) enum TaskStatus {
    /// Task is ready to be executed
    Ready,

    /// Task is currently in progress
    InProgress,

}


/// Client that connect to a Server
pub struct Client<IN : Message + Send + 'static,OUT : Message + Send + 'static>  {

    /// Thread communication channels
    channels : Option<ClientChannel<IN, OUT>>,

    /// Handle of the worker thread
    worker_handle : Option<JoinHandle<()>>,

    /// Current status of the client
    status : ClientStatus,

    /// Message reception task status
    receive : TaskStatus
}

impl <IN : Message + Send + 'static,OUT : Message + Send + 'static>  Client<IN, OUT> {
    
    /// Create a new [`Client`] for [`Server`](crate::server::Server).
    /// 
    /// # Returns
    /// A new [`Client`]
    pub fn new() -> Client<IN, OUT> {
        Client { channels: None, worker_handle: None, status: ClientStatus::Disconnected, receive: TaskStatus::Ready }
    }

    /// Connect the client to [`Server`](crate::server::Server) from a [`SocketAddr`].
    /// 
    /// # Returns
    /// - [`Result`]
    ///     - Ok(()) if client is connected to server.
    ///     - Err([`Error::InvalidSocket`]) if given socket is invalid.
    ///     - Err([`Error::ServerNotFound`]) if socket address is incorrect or server is down.
    ///     - Err([`Error::ClientAlreadyConnected`]) if client is already connected.
    ///     - Err([`Error::ConnectionRefused`]) if server refused client connection.
    pub fn connect(&mut self, addr : SocketAddr) -> Result<(), Error> {
        
        match self.status {
            ClientStatus::Disconnected => {
                // Create channels
                let (client_channels, worker_channels) = create_client_worker_channels::<IN, OUT>();

                match Worker::new(addr, worker_channels) {
                    Ok(mut worker) => {
                        self.channels = Some(client_channels);
                        self.status = ClientStatus::Connecting;
                        self.worker_handle = Some(std::thread::spawn(move || { worker.execute(); }));

                        Ok(())
                    },
                    Err(err) => Err(err),
                }
            },
            _ => Err(Error::ClientAlreadyConnected)
        }

    }

    pub fn send(&mut self, message : OUT) -> Result<(), Error>{
        todo!()
    }

    

    pub fn close() {

    }


}