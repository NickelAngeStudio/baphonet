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
};

use crate::{
    Message,
    server::{ClientId, error::ErrorUpdate, worker::WorkerId},
};

/// Message and updates sent to and received by server
#[derive(Debug, Clone)]
pub enum ServerUpdate {
    /// Server is now active
    Active,

    /// New client connected with Id and address
    ClientConnected(ClientId, SocketAddr),

    /// A client disconnected with Id
    ClientDisconnected(ClientId),

    /// An error occurred
    Error(ErrorUpdate),

    /// Server is currently full
    Full,

    /// Server is currently inactive
    Inactive,

    /// Supervisor has ended
    Ended,
}

/// Message sent to and received by supervisor
#[derive(Debug, Clone)]
pub enum SupervisorMessage {
    /// Message sent from server
    FromServer(SupervisorServerMessage),

    /// Message sent from worker
    FromWorker(SupervisorWorkerMessage),
}

/// Supervisor message sent from server
#[derive(Debug, Clone)]
pub enum SupervisorServerMessage {
    /// Give listener to supervisor
    Start(Arc<Mutex<TcpListener>>),

    /// Stop the supervisor
    Stop,

    /// Drop the supervisor thread
    End,
}

/// Supervisor message sent from worker
#[derive(Debug, Clone, Copy)]
pub enum SupervisorWorkerMessage {
    /// Client is now connected
    Connected(ClientId, SocketAddr),

    /// Worker finished incoming connection job
    IncomingJobDone,

    /// Worker finished receiving incoming message of client
    ReceiveJobDone(ClientId),

    /// Client connection closed
    Disconnected(ClientId),

    /// An error occurred
    Error(ErrorUpdate),

    /// Worker thread ended execution
    Finished(WorkerId),
}

/// Message sent to inactive worker
pub enum WorkerInactiveMessage {
    /// Start the worker
    Start(Arc<Mutex<TcpListener>>),

    /// Stop the worker, ending the thread
    Stop,

    /// Drop the worker thread
    End,
}

/// Message sent to and received by active worker
#[derive(Debug, Clone)]
pub enum WorkerActiveMessage<OUT: Message + Send> {
    /// Handle incoming connection to server
    Incoming,

    /// Receive message from client id
    Receive(ClientId),

    /// Send server message to clients
    Send(OutgoingMessage<OUT>),

    /// Disconnect client
    Disconnect(ClientId),

    /// Stop Client thread (set inactive)
    Stop,

    /// End client thread
    End,
}

/// Outgoing message sent by server to client.
#[derive(Debug, Clone)]
pub struct OutgoingMessage<OUT: Message + Send> {
    pub(crate) destinations: Vec<ClientId>,
    pub(crate) message: OUT,
}

impl<OUT: Message + Send> OutgoingMessage<OUT> {
    /// Wrap an outgoing message with destinations
    #[inline]
    pub fn new(client_id: ClientId, message: OUT) -> OutgoingMessage<OUT> {
        let destinations = vec![client_id];
        OutgoingMessage {
            destinations,
            message,
        }
    }

    /// Wrap an outgoing message with multiple destinations in a vector.
    #[inline]
    pub fn new_vec(destinations: &Vec<ClientId>, message: OUT) -> OutgoingMessage<OUT> {
        let destinations = destinations.clone();
        OutgoingMessage {
            destinations,
            message,
        }
    }
}

/// Message received by client
#[derive(Debug, Clone)]
pub struct IncomingMessage<IN: Message + Send> {
    pub client_id: ClientId,
    pub message: IN,
}

impl<IN: Message + Send> IncomingMessage<IN> {
    /// Create a new server incoming message
    pub fn new(client: ClientId, message: IN) -> IncomingMessage<IN> {
        IncomingMessage {
            client_id: client,
            message,
        }
    }
}
