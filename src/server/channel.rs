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

/// Default capacity of the sender vec
const SENDER_VEC_CAPACITY: usize = 16;

use std::sync::{
    Arc, Mutex,
    mpsc::{self, Receiver, Sender},
};

use crate::{
    Message,
    server::{
        ServerStatus,
        message::{SenderMessage, ServerMessage, SupervisorMessage, WorkerMessage},
        sender::ServerMessageSender,
    },
};

/// All server communication channels
pub(super) struct ServerChannel<IN: Message + Send, OUT: Message + Send + 'static> {
    /// Receive of server messages
    pub rcv_server: Option<Receiver<ServerMessage<IN>>>,

    /// Channels of Server Sender
    pub sdr_sender: Vec<Sender<SenderMessage<OUT>>>,

    // Sender channel for supervisor messages
    pub sdr_supervisor: Option<Sender<SupervisorMessage>>,

    // Sender channels for worker messages
    pub sdr_worker: Option<Sender<WorkerMessage<OUT>>>,
}

impl<IN: Message + Send, OUT: Message + Send> ServerChannel<IN, OUT> {
    /// Create a new instance of server communication channels
    pub fn new() -> ServerChannel<IN, OUT> {
        ServerChannel {
            rcv_server: None,
            sdr_sender: Vec::with_capacity(SENDER_VEC_CAPACITY),
            sdr_supervisor: None,
            sdr_worker: None,
        }
    }

    /// Create and register a new [`ServerMessageSender`].
    ///
    /// # Returns
    /// A new [`ServerMessageSender`].
    pub fn sender_channel(&mut self, server_status: ServerStatus) -> ServerMessageSender<OUT> {
        let (sdr_sender, rcv_sender) = mpsc::channel::<SenderMessage<OUT>>();

        let mut sms = ServerMessageSender::new(rcv_sender, server_status);

        self.sdr_sender.push(sdr_sender); // Register sender
        match self.sdr_worker.as_ref() {
            Some(sender) => sms.sdr_worker = Some(sender.clone()),
            None => {}
        }

        sms
    }

    /// Create communication channels of supervisor
    ///
    /// # Returns
    /// New [`ServerChannel`] instance with channels used.
    pub fn supervisor_channels(&mut self) -> SupervisorChannel<IN, OUT> {
        let (sdr_server, rcv_server) = mpsc::channel::<ServerMessage<IN>>();
        let (sdr_supervisor, rcv_supervisor) = mpsc::channel::<SupervisorMessage>();
        let (sdr_worker, rcv_worker) = mpsc::channel::<WorkerMessage<OUT>>();

        let sdr_supervisor_clone = sdr_supervisor.clone();
        let sdr_worker_clone = sdr_worker.clone();

        self.rcv_server = Some(rcv_server);
        self.sdr_supervisor = Some(sdr_supervisor);
        self.sdr_worker = Some(sdr_worker);

        SupervisorChannel::new(
            sdr_server,
            sdr_supervisor_clone,
            rcv_supervisor,
            sdr_worker_clone,
            rcv_worker,
        )
    }

    /// Clear all channels except for active [`ServerMessageSender`]
    pub fn clear(&mut self) {
        self.rcv_server = None;
        self.sdr_supervisor = None;
        self.sdr_worker = None;

        for i in self.sdr_sender.len()..0 {
            match self.sdr_sender[i].send(SenderMessage::Ping) {
                Ok(_) => {}
                Err(_) => {
                    self.sdr_sender.swap_remove(i);
                }
            }
        }
    }
}

/// Supervisor communication channels
pub(crate) struct SupervisorChannel<IN: Message + Send, OUT: Message + Send> {
    /// Channel Message sender to server
    pub sdr_server: Sender<ServerMessage<IN>>,

    /// Channel Message sender and receiver to supervisor
    pub sdr_supervisor: Sender<SupervisorMessage>,
    pub rcv_supervisor: Receiver<SupervisorMessage>,

    // Sender and receiver channels for worker messages
    pub sdr_worker: Sender<WorkerMessage<OUT>>,
    pub rcv_worker: Arc<Mutex<Receiver<WorkerMessage<OUT>>>>,
}

impl<IN: Message + Send, OUT: Message + Send> SupervisorChannel<IN, OUT> {
    pub fn new(
        sdr_server: Sender<ServerMessage<IN>>,
        sdr_supervisor: Sender<SupervisorMessage>,
        rcv_supervisor: Receiver<SupervisorMessage>,
        sdr_worker: Sender<WorkerMessage<OUT>>,
        rcv_worker: Receiver<WorkerMessage<OUT>>,
    ) -> SupervisorChannel<IN, OUT> {
        SupervisorChannel {
            sdr_server,
            sdr_supervisor,
            rcv_supervisor,
            sdr_worker,
            rcv_worker: Arc::new(Mutex::new(rcv_worker)),
        }
    }
}

/// Worker communication channels
pub(crate) struct WorkerChannel<IN: Message + Send, OUT: Message + Send> {
    /// Channel Message sender to server
    pub sdr_server: Sender<ServerMessage<IN>>,

    /// Channel Message sender and receiver to supervisor
    pub sdr_supervisor: Sender<SupervisorMessage>,

    // Receiver channels for worker messages
    pub rcv_worker: Arc<Mutex<Receiver<WorkerMessage<OUT>>>>,
}

impl<IN: Message + Send, OUT: Message + Send> WorkerChannel<IN, OUT> {
    pub fn new(
        sdr_server: Sender<ServerMessage<IN>>,
        sdr_supervisor: Sender<SupervisorMessage>,
        rcv_worker: Arc<Mutex<Receiver<WorkerMessage<OUT>>>>,
    ) -> WorkerChannel<IN, OUT> {
        WorkerChannel {
            sdr_server,
            sdr_supervisor,
            rcv_worker,
        }
    }
}
