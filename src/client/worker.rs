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

use std::{io::{Read, Write}, net::{SocketAddr, TcpStream}, sync::mpsc::RecvTimeoutError, time::{Duration, Instant}};

use crate::{MAXIMUM_MESSAGE_SIZE, Message, SIZE_OF_MESSAGE_SIZE, client::{ErrorClient, channel::WorkerChannel, error::ErrorWorker, message::{ClientMessage, WorkerMessage}, status::WorkerStatus}};


/// Client Worker thread.
pub struct Worker<IN : Message + Send + 'static,OUT : Message + Send + 'static> {

    /// TCP stream to server
    stream : TcpStream,

    /// Channels of worker thread
    channels : WorkerChannel<IN, OUT>,

    /// Status of worker thread
    status : WorkerStatus,

    /// Size of incoming message if any
    inc_size : Option<usize>,

    /// Worker last pool instant
    last_pool : Instant,

    /// Supervisor pool rate duration in milliseconds
    pool_rate_duration : Duration,

    
}

impl <IN : Message + Send + 'static,OUT : Message + Send + 'static> Worker<IN, OUT> {

    /// Create new worker from socket address and channels.
    pub fn new(addr : SocketAddr, pool_rate : u64, channels : WorkerChannel<IN, OUT>) -> Result<Worker<IN, OUT>, ErrorClient> {

        match TcpStream::connect(addr) {
            Ok(stream) => {
                match stream.set_nonblocking(true) {
                    Ok(_) => match stream.set_nodelay(true) {
                        Ok(_) => {
                            Ok(Worker{ stream, channels, 
                                status: WorkerStatus::Starting, inc_size: None, 
                                last_pool: Instant::now(),
                                pool_rate_duration : Duration::from_millis(1000/pool_rate) })
                        },
                        Err(err) => Err(ErrorClient::UnhandledIOError(err.kind())),
                    },
                    Err(err) => Err(ErrorClient::UnhandledIOError(err.kind())),
                }
            },
            Err(err) => {
                match err.kind() {
                    std::io::ErrorKind::InvalidInput |
                    std::io::ErrorKind::InvalidData => Err(ErrorClient::InvalidSocket),

                    std::io::ErrorKind::HostUnreachable |
                    std::io::ErrorKind::NetworkUnreachable |
                    std::io::ErrorKind::NotFound |
                    std::io::ErrorKind::AddrNotAvailable |
                    std::io::ErrorKind::NetworkDown => Err(ErrorClient::ServerNotFound),

                    std::io::ErrorKind::PermissionDenied |
                    std::io::ErrorKind::ConnectionRefused |
                    std::io::ErrorKind::ConnectionReset |
                    std::io::ErrorKind::ConnectionAborted |
                    std::io::ErrorKind::NotConnected => Err(ErrorClient::ConnectionRefused),                    
                    
                    _ => Err(ErrorClient::UnhandledIOError(err.kind())),
                }
            },
        }

    }

    /// Execute the worker thread routine
    pub(crate)  fn execute(&mut self) {

        // Create buffer on stack from MAXIMUM_MESSAGE_SIZE.
        let mut buffer = [0u8; MAXIMUM_MESSAGE_SIZE];

        // Set as active
        self.status = WorkerStatus::Active;
        self.send_message_client(ClientMessage::StatusChanged(WorkerStatus::Active));

        'worker:
        loop {
            match self.status {
                WorkerStatus::Starting | WorkerStatus::Active => {
                    if self.last_pool.elapsed() > self.pool_rate_duration {
                        self.receive(&mut buffer); // Receive message from server since pool expired
                    }
                    match self.channels.rcv_worker.recv_timeout(self.pool_rate_duration) {
                        Ok(message) => match message {
                            WorkerMessage::PoolRate(pool_rate) => self.set_pool_rate(pool_rate),
                            WorkerMessage::Send(msg) => self.send(msg, &mut buffer),
                            WorkerMessage::Stop => self.status = WorkerStatus::Ended,
                        },
                        Err(err) => match err {
                            RecvTimeoutError::Timeout => self.receive(&mut buffer), // Receive message from server on timeout
                            RecvTimeoutError::Disconnected => self.status = WorkerStatus::Ended,  // Channel is lost, end worker
                        }
                    }
                },
                WorkerStatus::Ended => break 'worker,
            }
        }

        // Shutdown stream
        match self.stream.shutdown(std::net::Shutdown::Both) {
            Ok(_) => {},
            Err(_) => {},
        }


    }

    /// Receive message from server if any
    #[inline]
    fn set_pool_rate(&mut self, pool_rate : u64) {
        
        self.pool_rate_duration = Duration::from_millis(1000 / pool_rate);

        // Tell server reception is done
        self.send_message_client(ClientMessage::PoolRate(pool_rate));
    }

    /// Receive message from server
    fn receive(&mut self, buffer : &mut [u8]) {
        
        'receive:
        loop {
            // Fetch message size if any
            self.inc_size = self.get_incoming_message_size(buffer);

            // Fetch message if any
            match self.inc_size {
                Some(size) => match self.get_incoming_message(&mut buffer[..size]) {
                    Some(incoming) => self.send_message_client(ClientMessage::Incoming(incoming)),
                    None => break 'receive,
                }
                None => break 'receive,
            }
        }

        // Refresh last pool
        self.last_pool = Instant::now();
    }

    /// Get incoming message size.
    #[inline]
    fn get_incoming_message_size(&mut self, buffer : &mut [u8]) -> Option<usize> {

        match self.inc_size {
            Some(size) => Some(size),   // If size is already read, keep it
            None => {
                match self.stream.read_exact(&mut buffer[..SIZE_OF_MESSAGE_SIZE]) {
                    Ok(_) => {
                        let size = u16::from_le_bytes(buffer[..SIZE_OF_MESSAGE_SIZE].try_into().unwrap()) as usize;

                        if size <= MAXIMUM_MESSAGE_SIZE {
                            Some(size)
                        } else {
                            // Clear stream.
                            Self::clear_stream(&mut self.stream, buffer);
                            // Notify supervisor
                            self.send_message_client(ClientMessage::Error(ErrorWorker::IncomingMessageTooLarge));
                            None
                        }
                    },
                    Err(err) => {
                        match err.kind() {
                            std::io::ErrorKind::WouldBlock => None,
                            _ => {  // Client connection lost
                                self.handle_connection_lost();
                                None
                            },
                        } 
                    },
                }
            },
        }
        
        

    }


    /// Get incoming message
    #[inline]
    fn get_incoming_message(&mut self, buffer : &mut [u8]) -> Option<IN> {

        match self.stream.read_exact(buffer) {
        Ok(_) => match IN::deserialize(buffer) {
            Ok(message) => Some(message),
            Err(_) => {
                Self::clear_stream(&mut self.stream, buffer); // Clear stream
                self.send_message_client(ClientMessage::Error(ErrorWorker::IncomingMessageError));
                None

            },
        },
        Err(err) => {
            match err.kind() {
                std::io::ErrorKind::WouldBlock => None,
                _ => { // Client connection lost
                    self.handle_connection_lost();
                    None
                },
            } 
        },
    } 
    
    }



    /// Send message to server
    fn send(&mut self, msg : OUT, buffer : &mut [u8]) {

        match msg.serialize(buffer) {
            Ok(size) => {
                if size <= MAXIMUM_MESSAGE_SIZE {
                    // Get bytes of message size
                    let size_bytes = (size as u16).to_le_bytes();

                     // Send size
                    match self.stream.write_all(&size_bytes) {
                        Ok(_) => {
                            // Send message
                                match self.stream.write_all(&buffer[..size]) {
                                Ok(_) => {
                                    // Send message
                                },
                                Err(_) => self.handle_connection_lost(), // Connection lost
                            }
                        },
                        Err(_) => self.handle_connection_lost(), // Connection lost
                    }
                } else {
                    self.send_message_client(ClientMessage::Error(crate::client::error::ErrorWorker::OutgoingMessageTooLarge));
                }
            },
            Err(_) => self.send_message_client(ClientMessage::Error(crate::client::error::ErrorWorker::OutgoingSerializeError)),
        }

    }

    /// Handle worker connection lost
    #[inline]
    fn handle_connection_lost(&mut self) {
        self.send_message_client(ClientMessage::Error(super::error::ErrorWorker::ConnectionLost));
        self.status = WorkerStatus::Ended;
    }

    /// Clear a TcpStream with a buffer.
    #[inline]
    fn clear_stream(stream : &mut TcpStream, buffer : &mut [u8]) {

        // Clear read buffer
        'clear:
        loop {
            match stream.read(buffer){
                Ok(size) => {
                    if size == 0 {  // Nothing else to read
                        break 'clear;
                    }
                },
                Err(_) => break 'clear,
            }
        }

    }

    /// Send a client message to client.
    fn send_message_client(&mut self, msg : ClientMessage<IN>) {

        #[cfg(debug_assertions)]
        {
            match &msg { // Print error in debug mode
                ClientMessage::Error(err) => println!("Client Worker {:?}", err),
                _ => {},
            }
        }

        match self.channels.sdr_client.send(msg) {
            Ok(_) => {},    // Message send with success.
            Err(_) => { // Channel is closed, communication to client is lost, end worker.
                self.status = WorkerStatus::Ended;
            },
        }

    }

}