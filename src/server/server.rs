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
    net::{SocketAddr, TcpListener},
    sync::{Arc, Mutex},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use crate::{
    Message,
    server::{
        ClientId, ServerBuilder, Status,
        channel::ServerChannel,
        client::{Client, Clients, ServerClient},
        error::ErrorServer,
        message::{ServerUpdate, SupervisorMessage, SupervisorServerMessage, WorkerActiveMessage},
        supervisor::Supervisor,
        transceiver::Transceiver,
    },
};

/// Milliseconds of wait time per worker.
const MS_JOIN_WAIT_DURATION_PER_WORKER: u64 = 50;

/// Server end of baphonet
pub struct Server<IN: Message + Send + 'static, OUT: Message + Send + 'static> {
    /// Count of worker threads allowed
    pub(super) worker_count: usize,

    /// Communication channels between threads
    pub(super) channels: ServerChannel<IN, OUT>,

    /// Shared clients list
    pub(super) clients: Clients,

    /// Current status of the server
    pub(super) status: Status,

    /// Supervisor thread handle
    pub(super) supervisor_handle: Option<JoinHandle<()>>,
}

impl<IN: Message + Send + 'static, OUT: Message + Send + 'static> Server<IN, OUT> {
    /// Create a new server from builder.
    pub(crate) fn build(builder: &ServerBuilder) -> Server<IN, OUT> {
        // Create shared client list
        let mut clients = Vec::<Mutex<Option<Client>>>::with_capacity(builder.maximum_client);
        clients.resize_with(builder.maximum_client, || Mutex::new(None));
        let clients = Arc::new(clients);

        // Channels
        let (server_channels, supervisor_channels) =
            ServerChannel::create_server_supervisor_channels();

        // Create supervisor
        let mut supervisor = Supervisor::<IN, OUT>::new(
            builder.maximum_client,
            builder.worker_count,
            builder.incoming_max_size,
            builder.pool_rate,
            clients.clone(),
            supervisor_channels,
        );

        let supervisor_handle = Some(thread::spawn(move || {
            supervisor.execute();
        }));

        // Return new created server
        Server {
            worker_count: builder.worker_count,
            channels: server_channels,
            clients: clients,
            status: super::Status::Inactive,
            supervisor_handle,
        }
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
    pub fn start(&mut self, socket: SocketAddr) -> Result<(), ErrorServer> {
        match self.status {
            Status::Inactive => {
                // Create listener
                match Self::create_tcp_listener(socket) {
                    Ok(listener) => {
                        // Start supervisor
                        let listener = Arc::new(Mutex::new(listener));
                        match self
                            .channels
                            .sdr_supervisor
                            .send(SupervisorMessage::FromServer(
                                SupervisorServerMessage::Start(listener),
                            )) {
                            Ok(_) => {
                                self.status = Status::Starting;
                                Ok(())
                            }
                            Err(_) => Err(ErrorServer::UnexpectedError),
                        }
                    }
                    Err(err) => Err(err),
                }
            }
            _ => Err(ErrorServer::AlreadyActive),
        }
    }

    /// Returns current server status
    pub fn status(&self) -> Status {
        self.status
    }

    /// Returns the transceiver used to receive and send message.
    ///
    /// The transceiver ownership can be taken with Take() to move
    /// to another thread.
    pub fn transceiver(&mut self) -> &mut Option<Transceiver<IN, OUT>> {
        &mut self.channels.transceiver
    }

    /// Get server update.
    ///
    /// Returns :
    /// - [`Result`]:
    ///     - Some([`ServerMessage`]) if message found.
    ///     - None if no message found.
    pub fn update(&mut self) -> Option<ServerUpdate> {
        match self.channels.rcv_server.try_recv() {
            Ok(update) => self.handle_update(update),
            Err(_) => None,
        }
    }

    /// Get list of currently connected clients with addresses.
    ///
    /// Returns
    /// - [`Result`]
    ///     - Ok(Vec<[`ServerClient`]>) if request was successful
    ///     - Err([`ErrorServer::Inactive`]) if server is inactive.
    ///     - Err([`ErrorServer::UnexpectedError`]) for mutex errors.
    pub fn clients(&mut self) -> Result<Vec<ServerClient>, ErrorServer> {
        match self.status() {
            Status::Active => {
                let mut list = Vec::<ServerClient>::with_capacity(self.clients.len());
                for client_id in 0..self.clients.len() {
                    match self.clients[client_id].lock() {
                        Ok(client) => match client.as_ref() {
                            Some(client) => {
                                list.push(ServerClient::from_client(client_id as u16, client));
                            }
                            None => {}
                        },
                        Err(_) => return Err(ErrorServer::UnexpectedError),
                    }
                }

                Ok(list)
            }
            _ => Err(ErrorServer::Inactive),
        }
    }

    /// Close a client connection from client id
    ///
    /// Returns :
    /// - [`Result`]:
    ///     - Ok(()) if close connection was sent to thread.
    ///     - Err([`ErrorServer::ServerInactive`]) if server hasn't started
    ///     - Err([`ErrorServer::UnexpectedError`]) if unexpected error happened.
    pub fn close_connection(&mut self, client_id: ClientId) -> Result<(), ErrorServer> {
        match self.status {
            Status::Active => match self
                .channels
                .sdr_worker
                .send(WorkerActiveMessage::Disconnect(client_id))
            {
                Ok(_) => Ok(()),
                Err(_) => Err(ErrorServer::UnexpectedError),
            },
            _ => Err(ErrorServer::Inactive),
        }
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
        match self
            .channels
            .sdr_supervisor
            .send(SupervisorMessage::FromServer(SupervisorServerMessage::Stop))
        {
            Ok(_) => {
                self.status = Status::Stopping;
                Ok(())
                //self.join_threads_timeout()
            }
            Err(_) => todo!(), // TODO: Handle channel lost #13
        }
    }

    /// Handle the server update before returning it.
    #[inline]
    fn handle_update(&mut self, update: ServerUpdate) -> Option<ServerUpdate> {
        match update {
            ServerUpdate::Active => self.status = Status::Active,
            ServerUpdate::Inactive => self.status = Status::Inactive,
            _ => {}
        }
        Some(update)
    }

    /// Create the TcpListener from [`SocketAddr`].
    #[inline]
    fn create_tcp_listener(socket: SocketAddr) -> Result<TcpListener, ErrorServer> {
        match TcpListener::bind(socket) {
            Ok(listener) => {
                // This should crash instead of having a blocking listener
                listener.set_nonblocking(true).unwrap();
                Ok(listener)
            }
            Err(err) => match err.kind() {
                std::io::ErrorKind::AddrInUse => Err(ErrorServer::SocketAddressAlreadyUsed),
                std::io::ErrorKind::InvalidInput => Err(ErrorServer::SocketInvalid),
                _ => Err(ErrorServer::UnhandledIOError(err.kind())),
            },
        }
    }

    /// Join server threads with a maximum wait time.
    #[inline]
    fn join_server_threads(&mut self) {
        let join_wait_duration = Duration::from_millis(
            MS_JOIN_WAIT_DURATION_PER_WORKER * (1 + (self.worker_count as u64)),
        );
        let ts = Instant::now();

        match self
            .channels
            .sdr_supervisor
            .send(SupervisorMessage::FromServer(SupervisorServerMessage::End))
        {
            Ok(_) => {}
            Err(_) => {}
        }

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
                                Err(_) => {
                                    #[cfg(debug_assertions)]
                                    eprintln!("join_server_threads : join() error!");
                                }
                            },
                            None => {} // Should never happens
                        };
                        break 'join;
                    }
                }
                None => break 'join, // Should never happens
            }

            if ts.elapsed() > join_wait_duration {
                #[cfg(debug_assertions)]
                eprintln!("join_server_threads : Timeout!");
                break 'join;
            }
        }
    }
}

/// Drop implemented only for Debug. Will warn of server not shutting down properly.
impl<IN: Message + Send + 'static, OUT: Message + Send + 'static> Drop for Server<IN, OUT> {
    fn drop(&mut self) {
        match self.status {
            Status::Inactive | Status::Stopping => {}
            _ => {
                #[cfg(debug_assertions)]
                eprintln!("Server::stop() should be called before program end!")
            }
        }
        self.join_server_threads();
    }
}
