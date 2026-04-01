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

use std::{net::SocketAddr, sync::{Arc, Mutex}, thread::{self, JoinHandle}};

use crate::{Message, server::{ ClientId, MAXIMUM_POOL_RATE_PER_SECOND, MINIMUM_POOL_RATE_PER_SECOND, POOL_RATE_PER_SECOND, SERVER_MAXIMUM_CLIENT_CAP, SERVER_MINIMUM_CLIENT_CAP, SERVER_MINIMUM_WORKER_CAP, ServerStatus, channel::ServerChannel, client::{Client, Clients, ServerClient}, error::ErrorServer, message::{OutgoingMessage, ServerMessage, SupervisorMessage, SupervisorServerMessage, SupervisorUpdate}, supervisor::Supervisor}};



/// Server end of baphonet
pub struct Server<IN : Message + Send + 'static,OUT : Message + Send + 'static> {

    /// Maximum client connection allowed
    maximum_client : usize,

    /// Count of worker threads allowed
    worker_count : usize,

    /// Communication channels between threads
    channels : Option<ServerChannel<IN, OUT>>,

    /// Shared clients list
    clients : Clients,

    /// Current status of the server
    status : ServerStatus,

    /// Supervisor thread handle
    supervisor_handle : Option<JoinHandle<()>>,

    /// Pool rate of the server
    pool_rate : u64,

}

impl<IN : Message + Send + 'static,OUT : Message + Send + 'static> Server<IN, OUT> {

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
    ///     - Err([`Error::MaximumClientBelowMinimum`]) if maximum client is below [`SERVER_MINIMUM_CLIENT_CAP`].
    ///     - Err([`Error::MaximumClientAboveMaximum`]) if maximum  client is above [`SERVER_MAXIMUM_CLIENT_CAP`].
    ///     - Err([`Error::WorkerCountBelowMinimum`]) if worker count is below [`SERVER_MINIMUM_WORKER_CAP`].
    ///     - Err([`Error::WorkerCountAboveMaximum`]) if worker count is above `maximum_client` parameter.
    pub fn new(maximum_client : usize, worker_count : usize) -> Result<Server<IN, OUT>, ErrorServer> {

        // Verify maximum_client and worker_count ranges
        if maximum_client < SERVER_MINIMUM_CLIENT_CAP { return Err(ErrorServer::MaximumClientBelowMinimum) }
        if maximum_client > SERVER_MAXIMUM_CLIENT_CAP { return Err(ErrorServer::MaximumClientAboveMaximum) }
        if worker_count < SERVER_MINIMUM_WORKER_CAP { return Err(ErrorServer::WorkerCountBelowMinimum) }
        if worker_count > maximum_client { return Err(ErrorServer::WorkerCountAboveMaximum) }

        // Create shared client list
        let mut clients  = Vec::<Mutex<Option<Client>>>::with_capacity(maximum_client);
        clients.resize_with(maximum_client, || { Mutex::new(None) });

        // Return new created server
        Ok(Server { maximum_client, worker_count, clients : Arc::new(clients),
            status: ServerStatus::Inactive, channels : None, supervisor_handle : None,
            pool_rate : POOL_RATE_PER_SECOND })
    }

    /// Start the server at specified socketr address.
    /// 
    /// # Returns
    /// - [`Result`]
    ///     - Ok(()) if server is starting
    ///     - Err([`Error::ServerAlreadyActive`]) if server already active.
    ///     - Err([`Error::SocketInvalid`]) if provided socket is invalid.
    ///     - Err([`Error::SocketAddressAlreadyUsed`]) if provided socket is invalid.
    ///     - Err([`Error::SetNonblockingFailed`]) if listener could not be set non-blocking
    ///     - Err([`Error::UnhandledIOError`]) if any unexpecxted IO error occurred
    pub fn start(&mut self, socket : SocketAddr) -> Result<(), ErrorServer> {
        
        match self.status {
            ServerStatus::Inactive => self.create_supervisor(socket),
            _ => Err(ErrorServer::ServerAlreadyActive),
        }
        
    }


    /// Set the number of pool made per second. This can be set anytime.
    /// 
    /// Each pool look for connection and receive incoming messages.
    /// 
    /// # Returns
    /// - [`Result`]
    ///     - Ok(()) if pool rate was changed with success.
    ///     - Err([`ErrorServer::PoolRateBelowMinimum`]) if pool rate is below [`MINIMUM_POOL_RATE_PER_SECOND`](super::MINIMUM_POOL_RATE_PER_SECOND).
    ///     - Err([`ErrorServer::PoolRateAboveMaximum`]) if pool rate is above [`MAXIMUM_POOL_RATE_PER_SECOND`](super::MAXIMUM_POOL_RATE_PER_SECOND).
    pub fn set_pool_rate(&mut self, pool_rate : u64) -> Result<(),ErrorServer> {

        if pool_rate < MINIMUM_POOL_RATE_PER_SECOND {
            return Err(ErrorServer::PoolRateBelowMinimum)
        }

        if pool_rate > MAXIMUM_POOL_RATE_PER_SECOND {
            return Err(ErrorServer::PoolRateAboveMaximum)
        }

        match self.channels.as_mut() {
            Some(channels) => {
                match channels.sdr_supervisor.send(SupervisorMessage::FromServer(SupervisorServerMessage::PoolRate(pool_rate))){
                    Ok(_) => {
                        self.pool_rate = pool_rate;
                        Ok(())
                    },
                    Err(_) => todo!(),  // TODO: handle server channel lost #13
                }
            },
            None => {
                self.pool_rate = pool_rate;
                Ok(())
            },
        }

    }

    /// Current pool rate of the server.
    pub fn pool_rate(&self) -> u64 {
        self.pool_rate
    }

    /// Returns current server status
    pub fn status(&self) ->  ServerStatus {
        self.status
    }

    /// Get incoming client message and/or server update.
    /// 
    /// Returns :
    /// - [`Result`]:
    ///     - Ok(Some([`ServerMessage`])) if message found.
    ///     - Ok(None) if no message found.
    pub fn message(&mut self) -> Option<ServerMessage<IN>> {

        match self.channels.as_mut() {
            Some(channels) => {
                match channels.rcv_server.try_recv() {
                    Ok(message) => {
                        match &message {
                            ServerMessage::Update(supervisor_update) => {
                                match supervisor_update{
                                    SupervisorUpdate::Active => { 
                                        self.status = ServerStatus::Active;
                                        Some(message)
                                    },
                                    _ => {
                                        Some(message)
                                    }
                                }
                            },
                            _ => Some(message),
                        }
                        
                    },
                    Err(_) => {
                        None
                    },
                }
            },
            None => None,
        }
        

    }

    /// Get list of currently connected clients
    /// 
    /// Returns
    /// - [`Result`]
    ///     - Ok(Vec<[`Client`]>) if request was successful
    ///     - Err([`Error::ServerInactive`]) if server hasn't started
    ///     - Err([`Error::ClientSocketError`]) if client a address can't be fetched.
    pub fn clients(&mut self) -> Result<Vec<ServerClient>, ErrorServer> {       
        todo!()
    }

    /// Close a client connection from client id
    /// 
    /// Returns :
    /// - [`Result`]:
    ///     - Ok(()) if close connection was sent to thread.
    ///     - Err([`Error::ServerInactive`]) if server hasn't started
    ///     - Err([`Error::Unexpected`]) if unexpected error happened.
    pub fn close_connection(&mut self, client_id : ClientId) -> Result<(),ErrorServer>  {
        todo!()
    }

    /// Send message to connected clients.
    /// 
    /// Returns :
    /// - [`Result`]:
    ///     - Ok(()) if message was sent to thread.
    ///     - Err([`Error::ServerInactive`]) if server hasn't started
    ///     - Err([`Error::ServerPaused`]) if server is paused.
    ///     - Err([`Error::ServerSendNoDestination`]) if no destination specified
    ///     - Err([`Error::Unexpected`]) if unexpected error happened.
    pub fn send(&mut self, message : OutgoingMessage<OUT>) -> Result<(),ErrorServer> {
        todo!()
    }

   

    /// Pause the server. Message are ignored but connections are maintained.
    /// 
    /// Returns :
    /// - [`Result`]:
    ///     - Ok(()) if pause was sent to thread.
    ///     - Err([`Error::ServerInactive`]) if server hasn't started
    ///     - Err([`Error::ServerPaused`]) if server is already paused.
    ///     - Err([`Error::Unexpected`]) if unexpected error happened.
    pub fn pause(&mut self) -> Result<(),ErrorServer> {
        todo!()

    }

    /// Resume the server if paused.
    /// 
    /// Returns :
    /// - [`Result`]:
    ///     - Ok(()) if resume was sent to thread.
    ///     - Err([`Error::ServerInactive`]) if server hasn't started
    ///     - Err([`Error::ServerActive`]) if server is already active.
    ///     - Err([`Error::Unexpected`]) if unexpected error happened.
    pub fn resume(&mut self) -> Result<(),ErrorServer> {

        todo!()

    }

    /// Stop the server, disconnecting all clients.
    /// Stop should ALWAYS be called before closing program.
    /// 
    /// Returns :
    /// - [`Result`]:
    ///     - Ok(()) if stop was sent to thread.
    ///     - Err([`Error::ServerInactive`]) if server hasn't started.
    ///     - Err([`Error::ServerShutdownTimeout`]) if server took too much time to shutdown.
    ///     - Err([`Error::ServerJoinThreadError`]) if thread join resulted in error.
    ///     - Err([`Error::Unexpected`]) if unexpected error happened.
    pub fn stop(&mut self) -> Result<(),ErrorServer> {
        todo!()
    }

    /// Create the supervisor thread
    #[inline]
    fn create_supervisor(&mut self, socket : SocketAddr) -> Result<(), ErrorServer>{

        // Create channels
        let (server_channels, supervisor_channels) = ServerChannel::new();

        match Supervisor::<IN, OUT>::new(socket, self.maximum_client, self.worker_count, self.pool_rate, self.clients.clone(), supervisor_channels){
            Ok(mut supervisor) => {

                self.supervisor_handle = Some(thread::spawn(move || {
                    supervisor.execute();
                }));

                self.channels = Some(server_channels);
                self.status = ServerStatus::Starting;

                Ok(())
            },
            Err(err) => Err(err),
        }

    }

}