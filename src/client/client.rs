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

use std::{
    net::{SocketAddr, TcpStream},
    thread::JoinHandle,
    time::{Duration, Instant},
};

use crate::{
    Message,
    client::{
        ErrorClient,
        builder::ClientBuilder,
        channel::ClientChannel,
        error::ErrorWorker,
        message::{ClientUpdate, WorkerMessage},
        status::Status,
        transceiver::Transceiver,
        worker::Worker,
    },
};

/// Wait a maximum of 100 milliseconds to join worker thread.
const MS_JOIN_WAIT_FOR_WORKER: u64 = 100;

/// Client that connect to a Server
pub struct Client<IN: Message + Send + 'static, OUT: Message + Send + 'static> {
    /// Thread communication channels
    pub(super) channels: ClientChannel<IN, OUT>,

    /// Handle of the worker thread
    pub(super) worker_handle: Option<JoinHandle<()>>,

    /// Current status of the client
    pub(super) status: Status,
}

impl<IN: Message + Send + 'static, OUT: Message + Send + 'static> Client<IN, OUT> {
    /// Create a new client from builder.
    pub(crate) fn build(builder: &ClientBuilder) -> Client<IN, OUT> {
        let (client_channels, worker_channels) = ClientChannel::create_client_worker_channels();

        let mut worker = Worker::new(
            builder.outgoing_max_size,
            builder.pool_rate,
            worker_channels,
        );

        let worker_handle = Some(std::thread::spawn(move || {
            worker.execute();
        }));

        Client {
            channels: client_channels,
            worker_handle,
            status: Status::Disconnected,
        }
    }

    /// Connect the client to [`Server`](crate::server::Server) from a [`SocketAddr`].
    ///
    /// # Returns
    /// - [`Result`]
    ///     - Ok(()) if client is connected to server.
    ///     - Err([`ErrorClient::InvalidSocket`]) if given socket is invalid.
    ///     - Err([`ErrorClient::ServerNotFound`]) if socket address is incorrect or server is down.
    ///     - Err([`ErrorClient::ClientAlreadyConnected`]) if client is already connected.
    ///     - Err([`ErrorClient::ConnectionRefused`]) if server refused client connection.
    pub fn connect(&mut self, addr: SocketAddr) -> Result<(), ErrorClient> {
        match self.status {
            Status::Disconnected => match Self::open_connection(addr) {
                Ok(stream) => {
                    self.status = Status::Connecting;
                    match self
                        .channels
                        .sdr_worker
                        .send(WorkerMessage::Connect(stream))
                    {
                        Ok(_) => Ok(()),
                        Err(_) => Err(ErrorClient::UnexpectedError),
                    }
                }
                Err(err) => Err(err),
            },
            _ => Err(ErrorClient::AlreadyConnected),
        }
    }

    /// Get incoming client worker update.
    ///
    /// Returns :
    /// - [`Result`]:
    ///     - Some([`ClientMessage`]) if update found.
    ///     - None if no update found.
    pub fn update(&mut self) -> Option<ClientUpdate> {
        match self.channels.rcv_update.try_recv() {
            Ok(update) => self.handle_client_update(update),
            Err(_) => None,
        }
    }

    // Returns the transceiver used to receive and send message.
    ///
    /// The transceiver ownership can be taken with Take() to move
    /// to another thread.
    pub fn transceiver(&mut self) -> &mut Option<Transceiver<IN, OUT>> {
        &mut self.channels.transceiver
    }

    /// Close the connection to the server.
    ///
    /// Close should ALWAYS be called before closing program.
    pub fn close(&mut self) {
        match self.channels.sdr_worker.send(WorkerMessage::Stop) {
            Ok(_) => {}
            Err(_) => {}
        }
        self.status = Status::Disconnecting;
    }

    /// Get status of the client
    pub fn status(&self) -> Status {
        self.status
    }

    /// Open connection to [`SocketAddr`]
    fn open_connection(addr: SocketAddr) -> Result<TcpStream, ErrorClient> {
        match TcpStream::connect(addr) {
            Ok(stream) => match stream.set_nonblocking(true) {
                Ok(_) => match stream.set_nodelay(true) {
                    Ok(_) => Ok(stream),
                    Err(err) => Err(ErrorClient::UnhandledIOError(err.kind())),
                },
                Err(err) => Err(ErrorClient::UnhandledIOError(err.kind())),
            },
            Err(err) => match err.kind() {
                std::io::ErrorKind::InvalidInput | std::io::ErrorKind::InvalidData => {
                    Err(ErrorClient::InvalidSocket)
                }

                std::io::ErrorKind::HostUnreachable
                | std::io::ErrorKind::NetworkUnreachable
                | std::io::ErrorKind::NotFound
                | std::io::ErrorKind::AddrNotAvailable
                | std::io::ErrorKind::NetworkDown => Err(ErrorClient::ServerNotFound),

                std::io::ErrorKind::PermissionDenied
                | std::io::ErrorKind::ConnectionRefused
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::ConnectionAborted
                | std::io::ErrorKind::NotConnected => Err(ErrorClient::ConnectionRefused),

                _ => Err(ErrorClient::UnhandledIOError(err.kind())),
            },
        }
    }

    /// Handle client update and change status accordingly
    #[inline]
    fn handle_client_update(&mut self, update: ClientUpdate) -> Option<ClientUpdate> {
        match &update {
            ClientUpdate::Connected => self.status = Status::Connected,
            ClientUpdate::Error(error_worker) => match error_worker {
                ErrorWorker::ConnectionLost => self.close(),
                _ => {}
            },
            ClientUpdate::Disconnected => self.status = Status::Disconnected,
            _ => {}
        }

        Some(update)
    }

    /// Join worker thread
    #[inline]
    fn join_worker_thread(&mut self) {
        let join_wait_duration = Duration::from_millis(MS_JOIN_WAIT_FOR_WORKER);
        let ts = Instant::now();

        match self.channels.sdr_worker.send(WorkerMessage::End) {
            Ok(_) => {}
            Err(_) => {}
        }

        'join: loop {
            match self.worker_handle.as_ref() {
                Some(th) => {
                    if th.is_finished() {
                        // If thread is finished, join it.
                        match self.worker_handle.take() {
                            Some(th) => match th.join() {
                                Ok(_) => {
                                    break 'join;
                                }
                                Err(_) => {
                                    #[cfg(debug_assertions)]
                                    eprintln!("join_worker_thread : Join Error!");
                                }
                            },
                            None => {
                                #[cfg(debug_assertions)]
                                eprintln!("join_worker_thread : Unexpected Error!");
                            } // Should never happens
                        };
                    }
                }
                None => {
                    #[cfg(debug_assertions)]
                    eprintln!("join_worker_thread : Unexpected Error!");
                } // Should never happens
            }

            if ts.elapsed() > join_wait_duration {
                #[cfg(debug_assertions)]
                eprintln!("join_worker_thread : Timeout!");

                break 'join;
            }
        }

        self.status = Status::Ended;
    }
}

/// Drop implemented only for Debug. Will warn of client not closing connection properly.
impl<IN: Message + Send + 'static, OUT: Message + Send + 'static> Drop for Client<IN, OUT> {
    fn drop(&mut self) {
        match self.status {
            Status::Disconnecting | Status::Disconnected => {}
            _ => {
                #[cfg(debug_assertions)]
                eprintln!("Client::close() should be called before program end!")
            }
        }
        self.join_worker_thread();
    }
}
