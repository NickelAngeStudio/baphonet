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
    io::{Read, Write},
    net::{Shutdown, TcpListener, TcpStream},
    sync::{Arc, Mutex, MutexGuard},
};

use crate::{
    MAXIMUM_MESSAGE_SIZE, Message, SIZE_OF_MESSAGE_SIZE, client,
    server::{
        ClientId,
        channel::WorkerChannel,
        client::{Client, Clients},
        error::ErrorUpdate,
        message::{
            IncomingMessage, OutgoingMessage, ServerMessage, SupervisorMessage,
            SupervisorWorkerMessage, WorkerMessage,
        },
        status::WorkerStatus,
    },
};

pub(crate) type WorkerId = usize;

/// Shortcut macro to fetch a client from id.
macro_rules! get_client_from_id {
    ($self:expr, $client:ident, $clients:expr, $client_id:expr, $($arg:tt)*) => {

        if ($client_id as usize) < $clients.len() {
            match $clients[$client_id as usize].lock(){
                Ok(mut $client) => match $client.as_mut() {
                    Some($client) => {
                        $($arg)*
                    },
                    None => $self.send_message_to_supervisor(SupervisorWorkerMessage::Error(ErrorUpdate::ClientNotFound($client_id))),    // Client not found
                },
                Err(_) => {
                    // TODO: Handle lock error #15
                    todo!()
                },
            }
        } else {
            $self.send_message_to_supervisor(SupervisorWorkerMessage::Error(ErrorUpdate::ClientNotFound($client_id)))
        }

    };


}

/// Worker that execute tasks.
pub(crate) struct Worker<IN: Message + Send, OUT: Message + Send> {
    /// Id of the worker
    worker_id: WorkerId,

    /// Maximum size of incoming message
    incoming_max_size: usize,

    /// Shared TCP listener
    listener: Arc<Mutex<TcpListener>>,

    /// Shared list of clients
    clients: Clients,

    /// Communication channels of the worker
    channels: WorkerChannel<IN, OUT>,

    /// Current status of worker
    status: WorkerStatus,
}

impl<IN: Message + Send, OUT: Message + Send> Worker<IN, OUT> {
    /// Create a new [`Worker`] from parameters.
    pub fn new(
        worker_id: WorkerId,
        incoming_max_size: usize,
        listener: Arc<Mutex<TcpListener>>,
        clients: Clients,
        channels: WorkerChannel<IN, OUT>,
    ) -> Worker<IN, OUT> {
        Worker {
            worker_id,
            incoming_max_size,
            listener,
            clients,
            status: WorkerStatus::Active,
            channels,
        }
    }

    /// Execute the worker routine
    pub fn execute(&mut self) {
        // Buffer to send / receive message
        let mut buffer = Vec::<u8>::with_capacity(MAXIMUM_MESSAGE_SIZE);
        buffer.resize(MAXIMUM_MESSAGE_SIZE, 0);

        'worker: loop {
            match self.status {
                WorkerStatus::Active => self.handle_worker_routine(&mut buffer),
                WorkerStatus::Ended => break 'worker,
            }
        }

        self.send_message_to_supervisor(SupervisorWorkerMessage::Finished(self.worker_id));
    }

    /// Handle worker active routine
    #[inline]
    fn handle_worker_routine(&mut self, buffer: &mut Vec<u8>) {
        // Get worker message while trying to release mutex ASAP
        let worker_message = {
            match self.channels.rcv_worker.lock() {
                Ok(rcv) => match rcv.recv() {
                    Ok(msg) => msg,
                    Err(_) => {
                        // Channel lost, break main
                        self.status = WorkerStatus::Ended;
                        return;
                    }
                },
                Err(_) => {
                    // Mutex error, close thread
                    self.status = WorkerStatus::Ended;
                    return;
                }
            }
        };

        match worker_message {
            WorkerMessage::Incoming => self.handle_worker_incoming(),
            WorkerMessage::Receive(client_id) => self.handle_worker_receive(client_id, buffer),
            WorkerMessage::Send(message) => self.handle_worker_send(buffer, message),
            WorkerMessage::Clear(client_id) => self.handle_worker_clear(client_id, buffer),
            WorkerMessage::Disconnect(client_id) => self.handle_worker_disconnect(client_id),
            WorkerMessage::End => self.status = WorkerStatus::Ended,
        }
    }

    /// Handle incoming connections
    #[inline]
    fn handle_worker_incoming(&mut self) {
        if self.is_server_full() {
            // Decline connections
            self.handle_worker_incoming_purge();
        } else {
            self.handle_worker_incoming_stream();
        }

        self.send_message_to_supervisor(SupervisorWorkerMessage::IncomingJobDone);
    }

    /// Purge incoming connections
    #[inline]
    fn handle_worker_incoming_purge(&mut self) {
        let listener = self.listener.clone();

        match listener.lock() {
            Ok(listener) => {
                'purge: loop {
                    for stream in listener.incoming() {
                        match stream {
                            Ok(stream) => {
                                // Immediately close connection
                                match stream.shutdown(std::net::Shutdown::Both) {
                                    Ok(_) => {}
                                    Err(_) => {}
                                }
                            }
                            Err(_) => break 'purge,
                        }
                    }
                }
            }
            Err(_) => todo!(), // TODO: Handle lock error #15
        }
    }

    /// Handle an incoming stream connection
    #[inline]
    fn handle_worker_incoming_stream(&mut self) {
        let listener = self.listener.clone();

        match listener.lock() {
            Ok(listener) => {
                for stream in listener.incoming() {
                    match stream {
                        Ok(tcp_stream) => {
                            // If those don't work, we prefer to crash instead since server won't be non-blocking anymore
                            tcp_stream.set_nonblocking(true).unwrap();
                            tcp_stream.set_nodelay(true).unwrap();

                            match self.fetch_new_client_id() {
                                Some(client_id) => {
                                    self.register_incoming_stream(client_id, tcp_stream)
                                }
                                None => break,
                            }
                        }
                        Err(_) => break,
                    }
                }
            }
            Err(_) => {
                // TODO: Handle lock error
                todo!()
            }
        }
    }

    /// Register incoming stream in clients list
    #[inline]
    fn register_incoming_stream(&mut self, client_id: ClientId, tcp_stream: TcpStream) {
        match tcp_stream.peer_addr() {
            Ok(_) => {
                let clients = self.clients.clone();
                let mut client = clients[client_id as usize].lock();
                match client.as_mut() {
                    Ok(client) => {
                        **client = Some(Client::new(tcp_stream));
                        self.send_message_to_supervisor(SupervisorWorkerMessage::Connected(
                            client_id,
                        ));
                    }
                    Err(_) => todo!(),
                }
            }
            Err(_) => {} // Skip client if can't peer address
        }
    }

    /// Handle receiving client message
    #[inline]
    fn handle_worker_receive(&mut self, client_id: ClientId, buffer: &mut Vec<u8>) {
        let clients = self.clients.clone();
        get_client_from_id! { self, client, clients, client_id,
            'receive:
            loop {
                // Fetch message size if any
                client.inc_msg_size = self.get_incoming_message_size(client, client_id, buffer);

                // Fetch message if any
                match client.inc_msg_size {
                    Some(size) => match self.get_incoming_message(client, client_id, &mut buffer[..size]) {
                        Some(incoming) => self.send_incoming_message_to_server(incoming),
                        None => break 'receive,
                    }
                    None => break 'receive,
                }
            }
        }

        // Tell supervisor task is finished
        self.send_message_to_supervisor(SupervisorWorkerMessage::ReceiveJobDone(client_id));
    }

    /// Get incoming message size.
    #[inline]
    fn get_incoming_message_size(
        &mut self,
        client: &mut Client,
        client_id: ClientId,
        buffer: &mut [u8],
    ) -> Option<usize> {
        match client.inc_msg_size {
            Some(size) => Some(size), // If size is already read, keep it
            None => {
                match client
                    .stream
                    .read_exact(&mut buffer[..SIZE_OF_MESSAGE_SIZE])
                {
                    Ok(_) => {
                        let size =
                            u16::from_le_bytes(buffer[..SIZE_OF_MESSAGE_SIZE].try_into().unwrap())
                                as usize;

                        if size <= MAXIMUM_MESSAGE_SIZE {
                            Some(size)
                        } else {
                            // Clear stream.
                            Self::clear_stream(&mut client.stream, buffer);
                            // Notify supervisor
                            self.send_message_to_supervisor(SupervisorWorkerMessage::Error(
                                ErrorUpdate::IncomingMessageTooLarge(client_id),
                            ));
                            None
                        }
                    }
                    Err(err) => {
                        match err.kind() {
                            std::io::ErrorKind::WouldBlock => None,
                            _ => {
                                // Client connection lost
                                self.handle_connection_lost(client, client_id);
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
        client: &mut Client,
        client_id: ClientId,
        buffer: &mut [u8],
    ) -> Option<IncomingMessage<IN>> {
        match client.stream.read_exact(buffer) {
            Ok(_) => {
                client.inc_msg_size = None; // Reset incoming size flag
                match IN::deserialize(buffer) {
                    Ok(message) => Some(IncomingMessage::new(client_id, message)),
                    Err(_) => {
                        Self::clear_stream(&mut client.stream, buffer); // Clear stream
                        self.send_message_to_supervisor(SupervisorWorkerMessage::Error(
                            ErrorUpdate::IncomingMessageError(client_id),
                        ));
                        None
                    }
                }
            }
            Err(err) => {
                match err.kind() {
                    std::io::ErrorKind::WouldBlock => None,
                    _ => {
                        // Client connection lost
                        self.handle_connection_lost(client, client_id);
                        None
                    }
                }
            }
        }
    }

    /// Handle sending message to clients
    #[inline]
    fn handle_worker_send(&mut self, buffer: &mut Vec<u8>, message: OutgoingMessage<OUT>) {
        match message.message.serialize(buffer) {
            Ok(size) => {
                if size > MAXIMUM_MESSAGE_SIZE {
                    self.send_message_to_supervisor(SupervisorWorkerMessage::Error(
                        ErrorUpdate::OutgoingMessageTooLarge,
                    ));
                } else {
                    let clients = self.clients.clone();

                    // Get bytes of message size
                    let size_bytes = (size as u16).to_le_bytes();

                    // For each destination
                    for client_id in message.destinations {
                        get_client_from_id! { self, client, clients, client_id,
                            // Send size
                            match client.stream.write_all(&size_bytes) {
                                Ok(_) => {
                                    // Send message
                                     match client.stream.write_all(&buffer[..size]) {
                                        Ok(_) => {
                                            // Send message
                                        },
                                        Err(_) => self.handle_connection_lost(client, client_id), // Connection lost
                                    }
                                },
                                Err(_) => self.handle_connection_lost(client, client_id), // Connection lost
                            }
                        }
                    }
                }
            }
            Err(_) => self.send_message_to_supervisor(SupervisorWorkerMessage::Error(
                ErrorUpdate::OutgoingMessageSerializeError,
            )),
        }
    }

    /// Handle clearing client stream buffer
    #[inline]
    fn handle_worker_clear(&mut self, client_id: ClientId, buffer: &mut Vec<u8>) {
        let clients = self.clients.clone();
        get_client_from_id! { self, client, clients, client_id, Self::clear_stream(&mut client.stream, buffer) }
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

    /// Handle disconnecting client
    #[inline]
    fn handle_connection_lost(&mut self, client: &mut Client, client_id: ClientId) {
        match client.stream.shutdown(Shutdown::Both) {
            Ok(_) => {}
            Err(_) => {}
        }

        // Notify supervisor of connection lost
        self.send_message_to_supervisor(SupervisorWorkerMessage::Error(
            ErrorUpdate::ConnectionLost(client_id),
        ));
    }

    /// Handle disconnecting client
    #[inline]
    fn handle_worker_disconnect(&mut self, client_id: ClientId) {
        let clients = self.clients.clone();
        get_client_from_id! { self, client, clients, client_id,
            match client.stream.shutdown(Shutdown::Both){
                Ok(_) => {},
                Err(_) => {},
            }
            self.send_message_to_supervisor(SupervisorWorkerMessage::Disconnected(client_id));

        }
    }

    /// Return true if server is full
    #[inline]
    fn is_server_full(&mut self) -> bool {
        self.fetch_new_client_id().is_none()
    }

    /// Find a free client id
    #[inline]
    fn fetch_new_client_id(&mut self) -> Option<ClientId> {
        let clients = self.clients.clone();

        let mut client_id: usize = 0;

        'find: loop {
            match clients[client_id].lock() {
                Ok(client) => {
                    if client.is_none() {
                        break 'find;
                    }
                }
                Err(_) => {
                    // TODO: Handle lock error
                    todo!()
                }
            }

            client_id += 1;
            if client_id >= clients.len() {
                return None;
            }
        }

        Some(client_id as ClientId)
    }

    /// Send a worker message to the supervisor thread.
    #[inline]
    fn send_message_to_supervisor(&mut self, message: SupervisorWorkerMessage) {
        match self
            .channels
            .sdr_supervisor
            .send(SupervisorMessage::FromWorker(message))
        {
            Ok(_) => {}
            Err(_) => self.status = WorkerStatus::Ended, // Channel lost, kill worker
        }
    }

    /// Send an incoming client message to server
    #[inline]
    fn send_incoming_message_to_server(&mut self, incoming: IncomingMessage<IN>) {
        match self
            .channels
            .sdr_server
            .send(ServerMessage::Incoming(incoming))
        {
            Ok(_) => {}
            Err(_) => self.status = WorkerStatus::Ended, // Channel lost, kill worker
        }
    }
}
