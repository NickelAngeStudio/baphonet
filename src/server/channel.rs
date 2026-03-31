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

use std::sync::{Arc, Mutex, mpsc::{self, Receiver, Sender}};

use crate::{Message, server::message::{ServerMessage, SupervisorMessage, WorkerMessage}};

/// All server communication channels
pub(super) struct ServerChannel<IN : Message + Send,OUT : Message + Send> {

    /// Receive of server messages
    pub rcv_server : Receiver<ServerMessage<IN>>,

    // Sender channel for supervisor messages
    pub sdr_supervisor : Sender<SupervisorMessage>,

    // Sender channels for worker messages
    pub sdr_worker : Sender<WorkerMessage<OUT>>,
}


impl<IN : Message + Send,OUT : Message + Send> ServerChannel<IN, OUT> {

    /// Create communication channels used between theads
    /// 
    /// # Returns
    /// New [`ServerChannel`] instance with channels used.
    pub fn new() -> (ServerChannel<IN, OUT>, SupervisorChannel<IN, OUT>) {
        let (sdr_server, rcv_server) = mpsc::channel::<ServerMessage<IN>>();
        let (sdr_supervisor, rcv_supervisor) = mpsc::channel::<SupervisorMessage>();
        let (sdr_worker, rcv_worker) = mpsc::channel::<WorkerMessage<OUT>>();

        let sdr_supervisor_clone = sdr_supervisor.clone();
        let sdr_worker_clone = sdr_worker.clone();
        let server_channels = ServerChannel{ rcv_server, sdr_supervisor, sdr_worker };
        let super_channels = SupervisorChannel::new(sdr_server, sdr_supervisor_clone, rcv_supervisor, sdr_worker_clone, rcv_worker);

        (server_channels, super_channels)   
    }

}

/// Supervisor communication channels
pub(crate) struct SupervisorChannel<IN : Message + Send,OUT : Message + Send> {

    /// Channel Message sender to server
    pub sdr_server : Sender<ServerMessage<IN>>,

    /// Channel Message sender and receiver to supervisor
    pub sdr_supervisor : Sender<SupervisorMessage>,
    pub rcv_supervisor : Receiver<SupervisorMessage>,

     // Sender and receiver channels for worker messages
    pub sdr_worker : Sender<WorkerMessage<OUT>>,
    pub rcv_worker : Arc<Mutex<Receiver<WorkerMessage<OUT>>>>,

}

impl<IN : Message + Send,OUT : Message + Send> SupervisorChannel<IN,OUT> {
    pub fn new(sdr_server : Sender<ServerMessage<IN>>, sdr_supervisor : Sender<SupervisorMessage>, rcv_supervisor : Receiver<SupervisorMessage>,
        sdr_worker : Sender<WorkerMessage<OUT>>, rcv_worker : Receiver<WorkerMessage<OUT>>) -> SupervisorChannel<IN, OUT> {

            SupervisorChannel{ sdr_server, sdr_supervisor, rcv_supervisor, sdr_worker, 
                rcv_worker: Arc::new(Mutex::new(rcv_worker)) }

    }

}


/// Worker communication channels
pub(crate) struct WorkerChannel<IN : Message + Send,OUT : Message + Send> {

    /// Channel Message sender to server
    pub sdr_server : Sender<ServerMessage<IN>>,

    /// Channel Message sender and receiver to supervisor
    pub sdr_supervisor : Sender<SupervisorMessage>,

     // Receiver channels for worker messages
    pub rcv_worker : Arc<Mutex<Receiver<WorkerMessage<OUT>>>>,

}

impl<IN : Message + Send,OUT : Message + Send> WorkerChannel<IN,OUT> {

    pub fn new(sdr_server : Sender<ServerMessage<IN>>, sdr_supervisor : Sender<SupervisorMessage>, rcv_worker : Arc<Mutex<Receiver<WorkerMessage<OUT>>>>) -> WorkerChannel<IN, OUT> {
        WorkerChannel { sdr_server, sdr_supervisor, rcv_worker }
    }

}