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
    net::SocketAddr,
    thread::JoinHandle,
    time::{Duration, Instant},
};

use crate::{
    Message,
    client::{
        ErrorClient, MAXIMUM_POOL_RATE_PER_SECOND, MINIMUM_POOL_RATE_PER_SECOND,
        POOL_RATE_PER_SECOND,
        channel::{self, ClientChannel, create_client_worker_channels},
        message::{ClientMessage, WorkerMessage},
        status::{ClientStatus, WorkerStatus},
        worker::Worker,
    },
};

/// Wait a maximum of 100 milliseconds to join worker thread.
const MS_JOIN_WAIT_FOR_WORKER: u64 = 100;

/// Client that connect to a Server
pub struct Client<IN: Message + Send + 'static, OUT: Message + Send + 'static> {
    /// Thread communication channels
    channels: Option<ClientChannel<IN, OUT>>,

    /// Handle of the worker thread
    worker_handle: Option<JoinHandle<()>>,

    /// Current status of the client
    status: ClientStatus,

    /// Worker pool rate
    pool_rate: u64,
}

impl<IN: Message + Send + 'static, OUT: Message + Send + 'static> Client<IN, OUT> {
    /// Create a new [`Client`] for [`Server`](crate::server::Server).
    ///
    /// # Returns
    /// A new [`Client`]
    pub fn new() -> Client<IN, OUT> {
        Client {
            channels: None,
            worker_handle: None,
            status: ClientStatus::Disconnected,
            pool_rate: POOL_RATE_PER_SECOND,
        }
    }

    /// Set the number of pool made per second. This can be set anytime.
    ///
    /// Each pool try to receive incoming messages.
    ///
    /// Higher pool rate will consume more resources and could be
    /// necessary for action game. (30 or 60 should be good for action).
    ///
    /// # Returns
    /// - [`Result`]
    ///     - Ok(()) if pool rate was changed with success.
    ///     - Err([`ErrorClient::PoolRateBelowMinimum`]) if pool rate is below [`MINIMUM_POOL_RATE_PER_SECOND`](super::MINIMUM_POOL_RATE_PER_SECOND).
    ///     - Err([`ErrorClient::PoolRateAboveMaximum`]) if pool rate is above [`MAXIMUM_POOL_RATE_PER_SECOND`](super::MAXIMUM_POOL_RATE_PER_SECOND).
    pub fn set_pool_rate(&mut self, pool_rate: u64) -> Result<(), ErrorClient> {
        if pool_rate < MINIMUM_POOL_RATE_PER_SECOND {
            return Err(ErrorClient::PoolRateBelowMinimum);
        }

        if pool_rate > MAXIMUM_POOL_RATE_PER_SECOND {
            return Err(ErrorClient::PoolRateAboveMaximum);
        }

        match self.channels.as_mut() {
            Some(channels) => {
                match channels.sdr_worker.send(WorkerMessage::PoolRate(pool_rate)) {
                    Ok(_) => {
                        self.pool_rate = pool_rate;
                        Ok(())
                    }
                    Err(_) => todo!(), // TODO: handle client channel lost #13
                }
            }
            None => {
                self.pool_rate = pool_rate;
                Ok(())
            }
        }
    }

    /// Current pool rate of the client.
    pub fn pool_rate(&self) -> u64 {
        self.pool_rate
    }

    /// Get status of the client
    pub fn status(&self) -> ClientStatus {
        self.status
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
            ClientStatus::Disconnected => {
                // Create channels
                let (client_channels, worker_channels) = create_client_worker_channels::<IN, OUT>();

                match Worker::new(addr, self.pool_rate, worker_channels) {
                    Ok(mut worker) => {
                        self.channels = Some(client_channels);
                        self.status = ClientStatus::Connecting;
                        self.worker_handle = Some(std::thread::spawn(move || {
                            worker.execute();
                        }));

                        Ok(())
                    }
                    Err(err) => Err(err),
                }
            }
            _ => Err(ErrorClient::AlreadyConnected),
        }
    }

    /// Get incoming sever message and/or worker update.
    ///
    /// Returns :
    /// - [`Result`]:
    ///     - Some([`ClientMessage`]) if message found.
    ///     - None if no message found.
    pub fn message(&mut self) -> Option<ClientMessage<IN>> {
        match self.channels.as_mut() {
            Some(channels) => match channels.rcv_client.try_recv() {
                Ok(message) => {
                    match &message {
                        ClientMessage::StatusChanged(worker_status) => match worker_status {
                            WorkerStatus::Active => self.status = ClientStatus::Connected,
                            WorkerStatus::Ended => {
                                if self.channels.is_some() {
                                    self.close().unwrap(); // Close connection since error occurred for worker to ended before close().
                                }
                            }
                            _ => {}
                        },
                        _ => {}
                    }
                    Some(message)
                }
                Err(_) => None,
            },
            None => None,
        }
    }

    /// Send outgoing message to server.
    ///
    /// Returns :
    /// - [`Result`]:
    ///     - Ok(()) if message was sent.
    ///     - Err([`ErrorClient::Disconnected`]) if client is not connected.
    ///     - Err([`ErrorClient::UnexpectedError`]) if channel was lost or unable to contact worker.
    pub fn send(&mut self, message: OUT) -> Result<(), ErrorClient> {
        match self.channels.as_mut() {
            Some(channels) => match channels.sdr_worker.send(WorkerMessage::Send(message)) {
                Ok(_) => Ok(()),
                Err(_) => Err(ErrorClient::UnexpectedError),
            },
            None => Err(ErrorClient::Disconnected),
        }
    }

    /// Close the connection to the server and join worker thread.
    ///
    /// Close should ALWAYS be called before closing program.
    ///
    /// Returns :
    /// - [`Result`]:
    ///     - Ok(()) if close was successful.
    ///     - Err([`ErrorClient::ClientCloseJoinError`]) if client took too much time to shutdown.
    ///     - Err([`ErrorClient::ClientCloseUnexpectedError`]) if thread join resulted in error.
    ///     - Err([`ErrorClient::ClientCloseTimeout`]) if unexpected error happened.
    pub fn close(&mut self) -> Result<(), ErrorClient> {
        match self.channels.as_mut() {
            Some(channels) => match channels.sdr_worker.send(WorkerMessage::Stop) {
                _ => {
                    self.status = ClientStatus::Disconnecting;
                    self.join_thread_timeout()
                }
            },
            None => Ok(()), // Server already stopped
        }
    }

    /// Shutdown worker threads
    ///
    /// Returns :
    /// - [`Result`]:
    ///     - Ok(()) if close was successful.
    ///     - Err([`ErrorClient::ClientCloseJoinError`]) if client took too much time to shutdown.
    ///     - Err([`ErrorClient::ClientCloseUnexpectedError`]) if thread join resulted in error.
    ///     - Err([`ErrorClient::ClientCloseTimeout`]) if unexpected error happened.
    #[inline]
    fn join_thread_timeout(&mut self) -> Result<(), ErrorClient> {
        let join_wait_duration = Duration::from_millis(MS_JOIN_WAIT_FOR_WORKER);
        let ts = Instant::now();

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
                                Err(_) => return Err(ErrorClient::CloseJoinError),
                            },
                            None => return Err(ErrorClient::UnexpectedError), // Should never happens
                        };
                    }
                }
                None => return Err(ErrorClient::UnexpectedError), // Should never happens
            }

            if ts.elapsed() > join_wait_duration {
                // Join took too long
                return Err(ErrorClient::CloseTimeout);
            }
        }

        // Remove channels
        self.channels = None;
        self.status = ClientStatus::Disconnected;
        Ok(())
    }
}

/// Drop implemented only for Debug. Will warn of client not closing connection properly.
#[cfg(debug_assertions)]
impl<IN: Message + Send + 'static, OUT: Message + Send + 'static> Drop for Client<IN, OUT> {
    fn drop(&mut self) {
        match self.channels.as_mut() {
            Some(_) => eprintln!("Client::close() should be called before program end!"),
            None => {}
        }
    }
}
