/* 
Copyright (c) 2026  NickelAnge.Studio 
Email               mathieu.grenier@nickelange.studio
Git                 https://codeberg.org/NickelAngeStudio/baphonet

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

use crate::{Message, server::{ClientId, worker::WorkerId}};
use super::ServerError;

/// Message and updates sent to and received by server
pub enum ServerMessage<IN : Message> {

    /// Incoming message of a client
    Incoming(IncomingMessage<IN>),

    /// Server updates
    Update(ServerUpdate),


}

/// Possible server update
pub enum ServerUpdate {

    /// New client connected with Id
    ClientConnected(ClientId),

    /// A client disconnected with Id
    ClientDisconnected(ClientId),

    /// Lost connection to client id
    ClientConnectionLost(ClientId),

    /// An error occurred.
    Error(ServerError),

}

/// Message sent to and received by supervisor
pub enum SupervisorMessage {

    /// Message sent from server
    FromServer(SupervisorServerMessage),

    /// Message sent from worker
    FromWorker(SupervisorWorkerMessage),

}

/// Supervisor message sent from server
pub enum SupervisorServerMessage {

    /// Tell supervisor to execute jobs
    Execute,

    /// Pause the supervisor
    Pause,

    /// Resume the supervisor
    Resume,

    /// Stop the supervisor, ending threads    
    Stop


}

/// Supervisor message sent from worker
pub enum SupervisorWorkerMessage {

    /// Client is now connected
    Connected(ClientId),

    /// Worker finished incoming connection job
    IncomingDone,

    /// Worker finished receiving incoming message of client
    ReceiveDone(ClientId),

    /// Client connection closed
    ConnectionClosed(ClientId),

    /// Client connection lost
    ConnectionLost(ClientId),

    /// Worker thread ended execution
    Finished(WorkerId)

}

/// Message sent to and received by worker
pub enum WorkerMessage<OUT : Message> {

    /// Handle incoming connection to server
    Incoming,

    /// Receive message from client id
    Receive(ClientId),

    /// Send server message to clients
    Send(OUT),

    /// Resume a client, purging stream buffers
    Resume(ClientId),

    /// Disconnect client
    Disconnect(ClientId),

    /// End client thread
    End,
}

/// Outgoing message sent by server to client.
pub struct OutgoingMessage<OUT : Message> {
    destinations : Vec<ClientId>,
    message : OUT
}

impl<OUT: Message> OutgoingMessage<OUT> {
    /// Created a new server message around a CoreServerMessage
    #[inline]
    pub fn new(message : OUT) -> OutgoingMessage<OUT> {
        OutgoingMessage { destinations: Vec::new(), message }
    }

    /// Add a [`ClientId`] destination to message.
    #[inline]
    pub fn add_destination(&mut self, client_id : ClientId){
        self.destinations.push(client_id);
    }
}

/// Message received by client
pub struct IncomingMessage<IN : Message> {
    client : ClientId,
    message : IN
}
