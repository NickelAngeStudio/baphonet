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

use std::{net::{TcpListener, TcpStream}, sync::{Arc, Mutex}};

use crate::{MAXIMUM_MESSAGE_SIZE, Message, server::{ClientId, channel::WorkerChannel, client::{Client, Clients}, message::{IncomingMessage, OutgoingMessage, ServerMessage, SupervisorMessage, SupervisorWorkerMessage, WorkerMessage}, status::WorkerStatus}};


pub(crate) type WorkerId = usize;

/// Worker that execute tasks.
pub(crate) struct Worker<IN : Message + Send,OUT : Message + Send> {
    /// Id of the worker
    worker_id : WorkerId,

    /// Shared TCP listener
    listener : Arc<Mutex<TcpListener>>,

    /// Shared list of clients
    clients : Clients,

    /// Communication channels of the worker
    channels : WorkerChannel<IN, OUT>,

    /// Current status of worker
    status : WorkerStatus,

}

impl<IN : Message + Send,OUT : Message + Send> Worker<IN, OUT> {
    /// Create a new [`Worker`] from parameters.
    pub fn new(worker_id : WorkerId, listener : Arc<Mutex<TcpListener>>, clients : Clients, channels : WorkerChannel<IN, OUT>) -> Worker<IN, OUT> {
        Worker { worker_id, listener, clients, status: WorkerStatus::Active, channels}
    }

    /// Execute the worker routine
    pub fn execute(&mut self) {
        // Buffer to send / receive message
        let mut buffer = Vec::<u8>::with_capacity(MAXIMUM_MESSAGE_SIZE);
        buffer.resize(MAXIMUM_MESSAGE_SIZE, 0);

        'worker:
        loop {
            match self.status {
                WorkerStatus::Active => self.handle_worker_routine(),
                WorkerStatus::Ended => break 'worker,
            }
        }

        self.send_message_to_supervisor(SupervisorWorkerMessage::Finished(self.worker_id));

    }

    /// Handle worker active routine 
    #[inline]
    fn handle_worker_routine(&mut self) {

        // Get worker message while trying to release mutex ASAP
        let worker_message = {
            match self.channels.rcv_worker.lock() {
                Ok(rcv) => match rcv.recv() {
                    Ok(msg) => msg,
                    Err(_) => { // Channel lost, break main
                        self.status = WorkerStatus::Ended;
                        return;
                    }
                },
                Err(_) => { // Mutex error, close thread
                    self.status = WorkerStatus::Ended;
                    return;
                } 
            }
        };

        match worker_message {
            WorkerMessage::Incoming => self.handle_worker_incoming(),
            WorkerMessage::Receive(client_id) => self.handle_worker_receive(client_id),
            WorkerMessage::Send(message) => self.handle_worker_send(message),
            WorkerMessage::Clear(client_id) => self.handle_worker_clear(client_id),
            WorkerMessage::Disconnect(client_id) => self.handle_worker_disconnect(client_id),
            WorkerMessage::End => self.status = WorkerStatus::Ended,
        }

    }

    /// Handle incoming connections
    #[inline]
    fn handle_worker_incoming(&mut self) {

        if self.is_server_full() {  // Decline connections
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
                for stream in listener.incoming() {
                    match stream {
                        Ok(stream) => {
                            match stream.shutdown(std::net::Shutdown::Both) {
                                Ok(_) => {},
                                Err(_) => {},
                            }
                        },
                        Err(_) => {},
                    }
                }
            },
            Err(_) => todo!(),  // TODO: Handle lock error #15
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

                            match self.fetch_new_client_id(){ 
                                Some(client_id) => self.register_incoming_stream(client_id, tcp_stream),
                                None => break,
                            }
                        },
                        Err(_) => break,
                    }
                }
            },
            Err(_) => {
                // TODO: Handle lock error
                todo!()
            }
        }


        

    }

    /// Register incoming stream in clients list
    #[inline]
    fn register_incoming_stream(&mut self, client_id : ClientId, tcp_stream : TcpStream) {

         match tcp_stream.peer_addr() {
            Ok(_) => {

                let clients = self.clients.clone();
                let mut client = clients[client_id as usize].lock();
                match client.as_mut() {
                    Ok(client) => {
                        **client = Some(Client::new(tcp_stream));
                        self.send_message_to_supervisor(SupervisorWorkerMessage::Connected(client_id));

                    },
                    Err(_) => todo!(),
                }

            },
            Err(_) => {},    // Skip client if can't peer address
        }               

    }

    /// Handle receiving client message
    #[inline]
    fn handle_worker_receive(&mut self, client_id : ClientId) {

    }

    /// Handle sending message to clients
    #[inline]
    fn handle_worker_send(&mut self, message : OutgoingMessage<OUT>) {

    }

    /// Handle clearing client stream buffer
    #[inline]
    fn handle_worker_clear(&mut self, client_id : ClientId) {

    }

    /// Handle disconnecting client
    #[inline]
    fn handle_worker_disconnect(&mut self, client_id : ClientId) {

    }

    
    /// Return true if server is full
    #[inline]
    fn is_server_full(&mut self) -> bool {

        self.fetch_new_client_id().is_none()

    }

    /// Find a free client id
    #[inline]
    fn fetch_new_client_id(&mut self) -> Option<ClientId>{
        
        let clients = self.clients.clone();

        let mut  client_id : usize = 0;

        'find:
        loop {
            match clients[client_id].lock() {
                Ok(client) => if client.is_none() {
                    break 'find;
                },
                Err(_) => {
                     // TODO: Handle lock error
                    todo!()
                },
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
    fn send_message_to_supervisor(&mut self, message : SupervisorWorkerMessage) {

        match self.channels.sdr_supervisor.send(SupervisorMessage::FromWorker(message)){
            Ok(_) => {},
            Err(_) => self.status = WorkerStatus::Ended,    // Channel lost, kill worker
        }

    }

    /// Send an incoming client message to server
    #[inline]
    fn send_incoming_message_to_server(&mut self, incoming : IncomingMessage<IN>) {

        match self.channels.sdr_server.send(ServerMessage::Incoming(incoming)) {
            Ok(_) => {},
            Err(_) => self.status = WorkerStatus::Ended,    // Channel lost, kill worker
        }
        
    }


}