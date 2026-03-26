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
pub(super) struct ServerChannel<IN : Message,OUT : Message> {

    // Sender and receiver channels for server messages
    pub sdr_server : Sender<ServerMessage<IN>>,
    pub rcv_server : Receiver<ServerMessage<IN>>,

    // Sender and receiver channels for supervisor messages
    pub sdr_supervisor : Sender<SupervisorMessage>,
    pub rcv_supervisor : Arc<Receiver<SupervisorMessage>>,

    // Sender and receiver channels for worker messages
    pub sdr_worker : Sender<WorkerMessage<OUT>>,
    pub rcv_worker : Arc<Mutex<Receiver<WorkerMessage<OUT>>>>,
}


impl<IN : Message,OUT : Message> ServerChannel<IN, OUT> {

    /// Create communication channels used between theads
    /// 
    /// # Returns
    /// New [`ServerChannel`] instance with channels used.
    pub fn new() -> ServerChannel<IN, OUT> {

        let (sdr_server, rcv_server) = mpsc::channel::<ServerMessage<IN>>();
        let (sdr_supervisor, rcv_supervisor) = mpsc::channel::<SupervisorMessage>();
        let (sdr_worker, rcv_worker) = mpsc::channel::<WorkerMessage<OUT>>();

        ServerChannel { sdr_server, rcv_server, sdr_supervisor, 
            rcv_supervisor: Arc::new(rcv_supervisor), sdr_worker, 
            rcv_worker: Arc::new(Mutex::new(rcv_worker)) }
    }

}