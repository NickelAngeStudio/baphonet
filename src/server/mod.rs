/* 
Copyright (c) 2026  NickelAnge.Studio 
Email               mathieu.grenier@nickelange.studio
Git                 https://codeberg.org/NickelAngeStudio/baphonet

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

use std::net::SocketAddr;

use crate::server::{error::Error, message::Message};

pub mod error;
pub mod message;


pub type ClientId = u16;

/// Possible server statuses
#[derive(Clone, Copy, PartialEq, Debug)]
pub enum ServerStatus {

}



pub struct Server {

    status : ServerStatus,

}

impl Server {

    /// Create new server
    /// 
    /// # Returns
    /// - [`Result`]
    ///     - Ok([`EthosNetServer`]) if successful
    ///     - Err([`ServerError::ServerMaxClientHardCap`]) if client hard cap reached.
    ///     - Err([`ServerError::ServerMaxClientTooLow`]) if lower than minimum.
    pub fn new(max_client : usize, worker_count : usize) -> Result<Server, Error> {
        todo!()
    }

    /// Returns current server status
    pub fn status(&self) ->  ServerStatus {
        self.status
    }

    /// Get message from supervisor and workers. 
    /// 
    /// Returns :
    /// - [`Result`]:
    ///     - Ok(Some([`Message`])) if message found.
    ///     - Ok(None) if no message found.
    pub fn message(&mut self) -> Option<Message> {
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
    pub fn send(&mut self, message : ServerMessage) -> Result<(),Error> {
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