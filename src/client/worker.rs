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
    io::{Read, Write},
    net::TcpStream,
    sync::mpsc::RecvTimeoutError,
    time::{Duration, Instant},
};

use crate::{
    MAXIMUM_MESSAGE_SIZE, Message, SIZE_OF_MESSAGE_SIZE,
    client::{
        channel::WorkerChannel,
        error::ErrorWorker,
        message::{ClientUpdate, WorkerMessage},
        status::Status,
    },
};

/// Client Worker thread.
pub struct Worker<IN: Message + Send + 'static, OUT: Message + Send + 'static> {
    /// Channels of worker thread
    channels: WorkerChannel<IN, OUT>,

    /// Status of worker thread
    status: Status,

    /// Size of incoming message if any
    inc_size: Option<usize>,

    /// Maximum incoming size for message
    incoming_max_size: usize,

    /// Maximum outgoing size for message
    outgoing_max_size: usize,

    /// Worker last pool instant
    last_pool: Instant,

    /// Supervisor pool rate duration in milliseconds
    pool_rate_duration: Duration,
}

impl<IN: Message + Send + 'static, OUT: Message + Send + 'static> Worker<IN, OUT> {
    /// Create new worker from socket address and channels.
    pub fn new(
        incoming_max_size: usize,
        outgoing_max_size: usize,
        pool_rate: u64,
        channels: WorkerChannel<IN, OUT>,
    ) -> Worker<IN, OUT> {
        Worker {
            channels,
            status: Status::Disconnected,
            inc_size: None,
            incoming_max_size,
            outgoing_max_size,
            last_pool: Instant::now(),
            pool_rate_duration: Duration::from_millis(1000 / pool_rate),
        }
    }

    /// Execute the worker thread routine
    pub(crate) fn execute(&mut self) {
        // Create buffer on stack from MAXIMUM_MESSAGE_SIZE.
        let mut buffer = [0u8; MAXIMUM_MESSAGE_SIZE];

        // Worker inactive loop
        'inactive: loop {
            match self.status {
                Status::Ended => break 'inactive,
                _ => {
                    self.status = Status::Disconnected;
                    match self.channels.rcv_worker.recv() {
                        Ok(msg) => match msg {
                            WorkerMessage::Connect(tcp_stream) => {
                                self.active(tcp_stream, &mut buffer)
                            }
                            WorkerMessage::End => self.status = Status::Ended,
                            _ => {} // Ignore other messages
                        },
                        Err(_) => break 'inactive, // Channel lost, end worker
                    }
                }
            }
        }

        // Send status changed
        self.update_client(ClientUpdate::Ended);
    }

    /// Worker active routine.
    fn active(&mut self, mut tcp_stream: TcpStream, buffer: &mut [u8]) {
        // Set as connected
        self.status = Status::Connected;
        self.update_client(ClientUpdate::Connected);

        'worker: loop {
            match self.status {
                Status::Connected => {
                    if self.last_pool.elapsed() > self.pool_rate_duration {
                        self.receive(&mut tcp_stream, buffer); // Receive message from server since pool expired
                    }
                    match self
                        .channels
                        .rcv_worker
                        .recv_timeout(self.pool_rate_duration)
                    {
                        Ok(message) => match message {
                            WorkerMessage::Send(msg) => self.send(msg, &mut tcp_stream, buffer),
                            WorkerMessage::Stop => self.status = Status::Disconnecting,
                            WorkerMessage::Connect(_) => {} // Already conected
                            WorkerMessage::End => self.status = Status::Ended,
                        },
                        Err(err) => match err {
                            RecvTimeoutError::Timeout => self.receive(&mut tcp_stream, buffer), // Receive message from server on timeout
                            RecvTimeoutError::Disconnected => self.status = Status::Ended, // Channel is lost, end worker
                        },
                    }
                }
                _ => break 'worker,
            }
        }

        // Shutdown stream
        match tcp_stream.shutdown(std::net::Shutdown::Both) {
            Ok(_) => self.update_client(ClientUpdate::Disconnected),
            Err(_) => {}
        }

        // Set as disconnected
    }

    /// Receive message from server
    fn receive(&mut self, tcp_stream: &mut TcpStream, buffer: &mut [u8]) {
        'receive: loop {
            // Fetch message size if any
            self.inc_size = self.get_incoming_message_size(tcp_stream, buffer);

            // Fetch message if any
            match self.inc_size {
                Some(size) => match self.get_incoming_message(tcp_stream, &mut buffer[..size]) {
                    Some(incoming) => self.send_incoming(incoming),
                    None => break 'receive,
                },
                None => break 'receive,
            }
        }

        // Refresh last pool
        self.last_pool = Instant::now();
    }

    /// Get incoming message size.
    #[inline]
    fn get_incoming_message_size(
        &mut self,
        tcp_stream: &mut TcpStream,
        buffer: &mut [u8],
    ) -> Option<usize> {
        match self.inc_size {
            Some(size) => Some(size), // If size is already read, keep it
            None => {
                match tcp_stream.read_exact(&mut buffer[..SIZE_OF_MESSAGE_SIZE]) {
                    Ok(_) => {
                        let size =
                            u16::from_le_bytes(buffer[..SIZE_OF_MESSAGE_SIZE].try_into().unwrap())
                                as usize;

                        if size <= self.incoming_max_size {
                            Some(size)
                        } else {
                            // Clear stream.
                            Self::clear_stream(tcp_stream, buffer);
                            // Notify supervisor
                            self.update_client(ClientUpdate::Error(
                                ErrorWorker::IncomingMessageTooLarge,
                            ));
                            None
                        }
                    }
                    Err(err) => {
                        match err.kind() {
                            std::io::ErrorKind::WouldBlock => None,
                            _ => {
                                // Client connection lost
                                self.handle_connection_lost();
                                None
                            }
                        }
                    }
                }
            }
        }
    }

    /// Get incoming message
    #[inline]
    fn get_incoming_message(
        &mut self,
        tcp_stream: &mut TcpStream,
        buffer: &mut [u8],
    ) -> Option<IN> {
        match tcp_stream.read_exact(buffer) {
            Ok(_) => match IN::deserialize(buffer) {
                Ok(message) => {
                    self.inc_size = None; // Reset incoming size.
                    Some(message)
                }
                Err(_) => {
                    Self::clear_stream(tcp_stream, buffer); // Clear stream
                    self.update_client(ClientUpdate::Error(
                        ErrorWorker::IncomingMessageDeserializeError,
                    ));
                    None
                }
            },
            Err(err) => {
                match err.kind() {
                    std::io::ErrorKind::WouldBlock => None,
                    _ => {
                        // Client connection lost
                        self.handle_connection_lost();
                        None
                    }
                }
            }
        }
    }

    /// Send message to server
    fn send(&mut self, msg: OUT, tcp_stream: &mut TcpStream, buffer: &mut [u8]) {
        match msg.serialize(buffer) {
            Ok(size) => {
                if size <= self.outgoing_max_size {
                    // Get bytes of message size
                    let size_bytes = (size as u16).to_le_bytes();

                    // Send size
                    match tcp_stream.write_all(&size_bytes) {
                        Ok(_) => {
                            // Send message
                            match tcp_stream.write_all(&buffer[..size]) {
                                Ok(_) => {}
                                Err(err) => match err.kind() {
                                    std::io::ErrorKind::WouldBlock => self.requeue_message(msg),
                                    _ => {
                                        self.handle_connection_lost();
                                    }
                                },
                            }
                        }
                        Err(err) => match err.kind() {
                            std::io::ErrorKind::WouldBlock => self.requeue_message(msg),
                            _ => {
                                self.handle_connection_lost();
                            }
                        },
                    }
                } else {
                    self.update_client(ClientUpdate::Error(
                        crate::client::error::ErrorWorker::OutgoingMessageTooLarge,
                    ));
                }
            }
            Err(_) => self.update_client(ClientUpdate::Error(
                crate::client::error::ErrorWorker::OutgoingSerializeError,
            )),
        }
    }

    /// Handle worker connection lost
    #[inline]
    fn handle_connection_lost(&mut self) {
        self.update_client(ClientUpdate::Error(
            super::error::ErrorWorker::ConnectionLost,
        ));
        self.status = Status::Disconnecting;
    }

    /// Clear a TcpStream with a buffer.
    #[inline]
    fn clear_stream(stream: &mut TcpStream, buffer: &mut [u8]) {
        // Clear read buffer
        'clear: loop {
            match stream.read(buffer) {
                Ok(size) => {
                    if size == 0 {
                        // Nothing else to read
                        break 'clear;
                    }
                }
                Err(_) => break 'clear,
            }
        }
    }

    /// Send incoming message to transceiver
    fn send_incoming(&mut self, incoming: IN) {
        match self.channels.sdr_incoming.send(incoming) {
            Ok(_) => {}
            Err(_) => self.status = Status::Ended,
        }
    }

    /// Send a client message to client.
    fn update_client(&mut self, msg: ClientUpdate) {
        #[cfg(debug_assertions)]
        {
            match &msg {
                // Print error in debug mode
                ClientUpdate::Error(err) => println!("Client Worker {:?}", err),
                _ => {}
            }
        }

        match self.channels.sdr_update.send(msg) {
            Ok(_) => {} // Message send with success.
            Err(_) => {
                // Channel is closed, communication to client is lost, end worker.
                self.status = Status::Ended;
            }
        }
    }

    /// Put unsent message back on pile when sending a message would block.
    fn requeue_message(&mut self, message: OUT) {
        /*
        match self.channels.sdr_worker.send(WorkerMessage::Send(message)) {
            Ok(_) => {}
            Err(_) => {
                // Channel is closed, communication to client is lost, end worker.
                self.status = Status::Ended;
            }
        }
        */
    }
}
