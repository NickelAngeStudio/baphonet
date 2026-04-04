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
    net::SocketAddr,
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use crate::{
    Message,
    server::{
        ClientId, ServerStatus,
        channel::ServerChannel,
        client::{Clients, ServerClient},
        dispatcher::Dispatcher,
        error::ErrorServer,
        message::{
            ServerMessage, SupervisorMessage, SupervisorServerMessage, SupervisorUpdate,
            WorkerMessage,
        },
        supervisor::Supervisor,
    },
};

/// Milliseconds of wait time per worker.
const MS_JOIN_WAIT_DURATION_PER_WORKER: u64 = 50;

/// Server end of baphonet
pub struct Server<IN: Message + Send + 'static, OUT: Message + Send + 'static> {
    /// Maximum client connection allowed
    pub(super) maximum_client: usize,

    /// Count of worker threads allowed
    pub(super) worker_count: usize,

    /// Pool rate of the server
    pub(super) pool_rate: u64,

    /// Maximum size of incoming message
    pub(super) incoming_max_size: usize,

    /// Communication channels between threads
    pub(super) channels: ServerChannel<IN, OUT>,

    /// Shared clients list
    pub(super) clients: Clients,

    /// Current status of the server
    pub(super) status: ServerStatus,

    /// Supervisor thread handle
    pub(super) supervisor_handle: Option<JoinHandle<()>>,
}

impl<IN: Message + Send + 'static, OUT: Message + Send + 'static> Server<IN, OUT> {
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
    pub fn start(&mut self, socket: SocketAddr) -> Result<(), ErrorServer> {
        match self.status {
            ServerStatus::Inactive => self.create_supervisor(socket),
            _ => Err(ErrorServer::AlreadyActive),
        }
    }

    /// Get a new [`Dispatcher`] that can send message to clients.
    ///
    /// This can be used to create a sender for each thread
    /// that can send a message to client.
    pub fn dispatcher(&mut self) -> Dispatcher<OUT> {
        self.channels.dispatcher(self.status())
    }

    /// Returns current server status
    pub fn status(&self) -> ServerStatus {
        self.status
    }

    /// Get incoming client message and/or server update.
    ///
    /// Returns :
    /// - [`Result`]:
    ///     - Some([`ServerMessage`]) if message found.
    ///     - None if no message found.
    pub fn message(&mut self) -> Option<ServerMessage<IN>> {
        match self.channels.rcv_server.as_mut() {
            Some(channel) => match channel.try_recv() {
                Ok(message) => match &message {
                    ServerMessage::Update(supervisor_update) => match supervisor_update {
                        SupervisorUpdate::Active => {
                            self.status = ServerStatus::Active;
                            Some(message)
                        }
                        _ => Some(message),
                    },
                    _ => Some(message),
                },
                Err(_) => None,
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
    ///     - Err([`ErrorServer::ServerInactive`]) if server hasn't started
    ///     - Err([`ErrorServer::UnexpectedError`]) if unexpected error happened.
    pub fn close_connection(&mut self, client_id: ClientId) -> Result<(), ErrorServer> {
        match self.channels.sdr_worker.as_mut() {
            Some(channel) => match channel.send(WorkerMessage::Disconnect(client_id)) {
                Ok(_) => Ok(()),
                Err(_) => Err(ErrorServer::UnexpectedError),
            },
            None => Err(ErrorServer::Inactive),
        }
    }

    /// Pause the server. Message are ignored but connections are maintained.
    ///
    /// Returns :
    /// - [`Result`]:
    ///     - Ok(()) if pause was sent to thread.
    ///     - Err([`Error::ServerInactive`]) if server hasn't started
    ///     - Err([`Error::ServerPaused`]) if server is already paused.
    ///     - Err([`Error::Unexpected`]) if unexpected error happened.
    pub fn pause(&mut self) -> Result<(), ErrorServer> {
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
    pub fn resume(&mut self) -> Result<(), ErrorServer> {
        todo!()
    }

    /// Stop the server, disconnecting all clients.
    /// Stop should ALWAYS be called before closing program.
    ///
    /// Returns :
    /// - [`Result`]:
    ///     - Ok(()) if stop was sent to thread.
    ///     - Err([`ErrorServer::ServerStopTimeout`]) if server took too much time to shutdown.
    ///     - Err([`ErrorServer::ServerStopJoinError`]) if thread join resulted in error.
    ///     - Err([`ErrorServer::ServerStopUnexpectedError`]) if unexpected error happened.
    pub fn stop(&mut self) -> Result<(), ErrorServer> {
        match self.channels.sdr_supervisor.as_mut() {
            Some(channel) => {
                match channel.send(SupervisorMessage::FromServer(SupervisorServerMessage::Stop)) {
                    Ok(_) => {
                        self.status = ServerStatus::Ending;
                        self.join_threads_timeout()
                    }
                    Err(_) => todo!(), // TODO: Handle channel lost #13
                }
            }
            None => Ok(()), // Server already stopped
        }
    }

    /// Shutdown server threads
    ///
    /// Returns :
    /// - [`Result`]:
    ///     - Ok(()) if all threads joined.
    ///     - Err([`ErrorServer::ServerStopTimeout`]) if server took too much time to join threads.
    ///     - Err([`ErrorServer::ServerStopJoinError`]) if thread join resulted in error.
    ///     - Err([`ErrorServer::ServerStopUnexpectedError`]) if unexpected error happened.
    #[inline]
    fn join_threads_timeout(&mut self) -> Result<(), ErrorServer> {
        let join_wait_duration = Duration::from_millis(
            MS_JOIN_WAIT_DURATION_PER_WORKER * (1 + (self.maximum_client as u64)),
        );
        let ts = Instant::now();

        'join: loop {
            match self.supervisor_handle.as_ref() {
                Some(th) => {
                    if th.is_finished() {
                        // If thread is finished, join it.
                        match self.supervisor_handle.take() {
                            Some(th) => match th.join() {
                                Ok(_) => {
                                    break 'join;
                                }
                                Err(_) => return Err(ErrorServer::StopJoinError),
                            },
                            None => return Err(ErrorServer::UnexpectedError), // Should never happens
                        };
                    }
                }
                None => return Err(ErrorServer::UnexpectedError), // Should never happens
            }

            if ts.elapsed() > join_wait_duration {
                // Join took too long
                return Err(ErrorServer::StopTimeout);
            }
        }

        self.channels.clear(); // Clear channels
        self.status = ServerStatus::Inactive;
        Ok(())
    }

    /// Create the supervisor thread
    #[inline]
    fn create_supervisor(&mut self, socket: SocketAddr) -> Result<(), ErrorServer> {
        match Supervisor::<IN, OUT>::new(
            socket,
            self.maximum_client,
            self.worker_count,
            self.incoming_max_size,
            self.pool_rate,
            self.clients.clone(),
            self.channels.supervisor_channels(),
        ) {
            Ok(mut supervisor) => {
                self.supervisor_handle = Some(thread::spawn(move || {
                    supervisor.execute();
                }));

                self.status = ServerStatus::Starting;

                Ok(())
            }
            Err(err) => Err(err),
        }
    }
}

/// Drop implemented only for Debug. Will warn of server not shutting down properly.
#[cfg(debug_assertions)]
impl<IN: Message + Send + 'static, OUT: Message + Send + 'static> Drop for Server<IN, OUT> {
    fn drop(&mut self) {
        match self.channels.sdr_supervisor {
            Some(_) => eprintln!("Server::stop() should be called before program end!"),
            None => {}
        }
    }
}
