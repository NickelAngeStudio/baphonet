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

use std::{marker::PhantomData, net::SocketAddr};

use crate::{Message, server::{channel::ServerChannel, error::Error, message::{OutgoingMessage, ServerMessage}}};

#[doc(hidden)]
pub mod error;

pub mod message;
pub mod worker;
pub mod supervisor;

#[doc(hidden)]
pub mod status;

#[doc(hidden)]
pub mod channel;

pub use error::Error as ServerError;
pub use status::ServerStatus as ServerStatus;

pub type ClientId = u16;

/// Current minimum client cap
pub const SERVER_MINIMUM_CLIENT_CAP : usize = 1;

/// Current maximum client cap
pub const SERVER_MAXIMUM_CLIENT_CAP : usize = ClientId::MAX as usize;

/// Minimum worker count cap
pub const SERVER_MINIMUM_WORKER_CAP : usize = 1;


pub struct ServerClient {
    client_id : ClientId,
    addr : SocketAddr,
}


/// Server end of baphonet
pub struct Server<IN : Message, OUT : Message> {

    /// Maximum client connection allowed
    maximum_client : usize,

    /// Count of worker threads allowed
    worker_count : usize,

    /// Communication channels between threads
    channels : ServerChannel<IN, OUT>,

    /// Current status of the server
    status : ServerStatus,

}

impl<IN: Message, OUT: Message> Server<IN, OUT> {

    /// Create new server that client can connect to.
    /// 
    /// # Parameters
    /// - `maximum_client` : Maximum possible client that are allowed to connect to server. Must
    ///     be between [`SERVER_MINIMUM_CLIENT_CAP`] and [`SERVER_MAXIMUM_CLIENT_CAP`].
    /// - `worker_count` : Count of worker used to manage connection, receive client message, etc. Must
    ///     be between [`SERVER_MINIMUM_WORKER_CAP`] and `maximum_client` parameter.
    /// 
    /// # Returns
    /// - [`Result`]
    ///     - Ok([`Server`]) on success.
    ///     - Err([`ServerError::MaximumClientBelowMinimum`]) if maximum client is below [`SERVER_MINIMUM_CLIENT_CAP`].
    ///     - Err([`ServerError::MaximumClientAboveMaximum`]) if maximum  client is above [`SERVER_MAXIMUM_CLIENT_CAP`].
    ///     - Err([`ServerError::WorkerCountBelowMinimum`]) if worker count is below [`SERVER_MINIMUM_WORKER_CAP`].
    ///     - Err([`ServerError::WorkerCountAboveMaximum`]) if worker count is above `maximum_client` parameter.
    pub fn new(maximum_client : usize, worker_count : usize) -> Result<Server<IN, OUT>, Error> {
        
        Ok(Server { maximum_client, worker_count,
            status: ServerStatus::Active, channels : ServerChannel::new() })
    }

    /// Returns current server status
    pub fn status(&self) ->  ServerStatus {
        self.status
    }

    /// Get message from supervisor and workers. 
    /// 
    /// Returns :
    /// - [`Result`]:
    ///     - Ok(Some([`ServerMessage`])) if message found.
    ///     - Ok(None) if no message found.
    pub fn message(&mut self) -> Option<ServerMessage<IN>> {
        todo!()
    }

    /// Get list of currently connected clients
    /// 
    /// Returns
    /// - [`Result`]
    ///     - Ok(Vec<[`ServerClient`]>) if request was successful
    ///     - Err([`ServerError::ServerInactive`]) if server hasn't started
    ///     - Err([`ServerError::ClientSocketError`]) if client a address can't be fetched.
    pub fn clients(&mut self) -> Result<Vec<ServerClient>, Error> {       
        todo!()
    }

    /// Close a client connection from client id
    /// 
    /// Returns :
    /// - [`Result`]:
    ///     - Ok(()) if close connection was sent to thread.
    ///     - Err([`ServerError::ServerInactive`]) if server hasn't started
    ///     - Err([`ServerError::Unexpected`]) if unexpected error happened.
    pub fn close_connection(&mut self, client_id : ClientId) -> Result<(),Error>  {
        todo!()
    }

    /// Send message to connected clients.
    /// 
    /// Returns :
    /// - [`Result`]:
    ///     - Ok(()) if message was sent to thread.
    ///     - Err([`ServerError::ServerInactive`]) if server hasn't started
    ///     - Err([`ServerError::ServerPaused`]) if server is paused.
    ///     - Err([`ServerError::ServerSendNoDestination`]) if no destination specified
    ///     - Err([`ServerError::Unexpected`]) if unexpected error happened.
    pub fn send(&mut self, message : OutgoingMessage<OUT>) -> Result<(),Error> {
        todo!()
    }

    /// Start the server at specified socketr addr
    /// 
    /// # Returns
    /// - [`Result`]
    ///     - Ok(()) if server is starting
    ///     - Err([`ServerError::ServerActive`]) if server already active.
    pub fn start(&mut self, addr : SocketAddr) -> Result<(), Error> {
        todo!()
        

       
    }

    /// Pause the server. Message are ignored but connections are maintained.
    /// 
    /// Returns :
    /// - [`Result`]:
    ///     - Ok(()) if pause was sent to thread.
    ///     - Err([`ServerError::ServerInactive`]) if server hasn't started
    ///     - Err([`ServerError::ServerPaused`]) if server is already paused.
    ///     - Err([`ServerError::Unexpected`]) if unexpected error happened.
    pub fn pause(&mut self) -> Result<(),Error> {
        todo!()

    }

    /// Resume the server if paused.
    /// 
    /// Returns :
    /// - [`Result`]:
    ///     - Ok(()) if resume was sent to thread.
    ///     - Err([`ServerError::ServerInactive`]) if server hasn't started
    ///     - Err([`ServerError::ServerActive`]) if server is already active.
    ///     - Err([`ServerError::Unexpected`]) if unexpected error happened.
    pub fn resume(&mut self) -> Result<(),Error> {

        todo!()

    }

    /// Stop the server, disconnecting all clients.
    /// Stop should ALWAYS be called before closing program.
    /// 
    /// Returns :
    /// - [`Result`]:
    ///     - Ok(()) if stop was sent to thread.
    ///     - Err([`ServerError::ServerInactive`]) if server hasn't started.
    ///     - Err([`ServerError::ServerShutdownTimeout`]) if server took too much time to shutdown.
    ///     - Err([`ServerError::ServerJoinThreadError`]) if thread join resulted in error.
    ///     - Err([`ServerError::Unexpected`]) if unexpected error happened.
    pub fn stop(&mut self) -> Result<(),Error> {
        todo!()
    }

}