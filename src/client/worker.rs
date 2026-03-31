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

use std::net::{SocketAddr, TcpStream};

use crate::{MAXIMUM_MESSAGE_SIZE, Message, client::{Error, channel::WorkerChannel, message::{ClientMessage, WorkerMessage}, status::WorkerStatus}};


/// Client Worker thread.
pub struct Worker<IN : Message + Send + 'static,OUT : Message + Send + 'static> {

    /// TCP stream to server
    stream : TcpStream,

    /// Channels of worker thread
    channels : WorkerChannel<IN, OUT>,

    /// Status of worker thread
    status : WorkerStatus,

    /// Size of incoming message if any
    inc_size : Option<usize>

    
}

impl <IN : Message + Send + 'static,OUT : Message + Send + 'static> Worker<IN, OUT> {

    /// Create new worker from socket address and channels.
    pub fn new(addr : SocketAddr, channels : WorkerChannel<IN, OUT>) -> Result<Worker<IN, OUT>, Error> {

        match TcpStream::connect(addr) {
            Ok(stream) => {
                match stream.set_nonblocking(true) {
                    Ok(_) => match stream.set_nodelay(true) {
                        Ok(_) => {
                            Ok(Worker{ stream, channels, status: WorkerStatus::Starting, inc_size: None })
                        },
                        Err(err) => Err(Error::UnhandledIOError(err.kind())),
                    },
                    Err(err) => Err(Error::UnhandledIOError(err.kind())),
                }
            },
            Err(err) => {
                match err.kind() {
                    std::io::ErrorKind::InvalidInput |
                    std::io::ErrorKind::InvalidData => Err(Error::InvalidSocket),

                    std::io::ErrorKind::HostUnreachable |
                    std::io::ErrorKind::NetworkUnreachable |
                    std::io::ErrorKind::NotFound |
                    std::io::ErrorKind::AddrNotAvailable |
                    std::io::ErrorKind::NetworkDown => Err(Error::ServerNotFound),

                    std::io::ErrorKind::PermissionDenied |
                    std::io::ErrorKind::ConnectionRefused |
                    std::io::ErrorKind::ConnectionReset |
                    std::io::ErrorKind::ConnectionAborted |
                    std::io::ErrorKind::NotConnected => Err(Error::ConnectionRefused),                    
                    
                    _ => Err(Error::UnhandledIOError(err.kind())),
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
        self.message_client(ClientMessage::StatusChanged(WorkerStatus::Active));

        'worker:
        loop {
            match self.status {
                WorkerStatus::Starting | WorkerStatus::Active => {
                    match self.channels.rcv_worker.recv() {
                        Ok(message) => match message {
                            WorkerMessage::Receive => self.receive(&mut buffer),
                            WorkerMessage::Send(msg) => self.send(msg, &mut buffer),
                            WorkerMessage::Stop => self.status = WorkerStatus::Ended,
                        },
                        Err(_) => self.status = WorkerStatus::Ended,  // Channel is lost, end worker
                    }
                },
                WorkerStatus::Ended => break 'worker,
            }
        }

        // Shutdown stream
        match self.stream.shutdown(std::net::Shutdown::Both) {
            Ok(_) => {},
            Err(err) => self.message_client(ClientMessage::Error(Error::UnhandledIOError(err.kind()))),
        }


    }

    /// Receive message from server if any
    fn receive(&mut self, buffer : &mut [u8]) {
        todo!();

        // Tell server reception is done
        self.message_client(ClientMessage::ReceiveJobDone);
    }

    /// Send message to server
    fn send(&mut self, msg : OUT, buffer : &mut [u8]) {
        todo!()
    }

    /// Send a client message to client.
    fn message_client(&mut self, msg : ClientMessage<IN>) {

        #[cfg(debug_assertions)]
        {
            match &msg { // Print error in debug mode
                ClientMessage::Error(err) => println!("{:?}", err),
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