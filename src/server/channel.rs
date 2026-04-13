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

use std::sync::{
    Arc, Mutex,
    mpsc::{self, Receiver, Sender},
};

use crate::{
    Message,
    server::{
        message::{
            IncomingMessage, ServerUpdate, SupervisorMessage, WorkerActiveMessage,
            WorkerInactiveMessage,
        },
        transceiver::Transceiver,
    },
};

/// All server communication channels
pub(super) struct ServerChannel<IN: Message + Send + 'static, OUT: Message + Send + 'static> {
    /// Receive of server messages
    pub(crate) rcv_server: Receiver<ServerUpdate>,

    /// Sender channel for supervisor messages
    pub(crate) sdr_supervisor: Sender<SupervisorMessage>,

    /// Sender channels for worker messages
    pub(crate) sdr_worker: Sender<WorkerActiveMessage<OUT>>,

    /// Transceiver used to receive and sent messages.
    pub(crate) transceiver: Option<Transceiver<IN, OUT>>,
}

impl<IN: Message + Send, OUT: Message + Send> ServerChannel<IN, OUT> {
    /// Create both [`ServerChannel`] and [`SupervisorChannel`].
    pub(crate) fn create_server_supervisor_channels()
    -> (ServerChannel<IN, OUT>, SupervisorChannel<IN, OUT>) {
        let (sdr_server, rcv_server) = mpsc::channel::<ServerUpdate>();
        let (sdr_supervisor, rcv_supervisor) = mpsc::channel::<SupervisorMessage>();
        let (sdr_worker, rcv_worker) = mpsc::channel::<WorkerActiveMessage<OUT>>();
        let (sdr_incoming, rcv_incoming) = mpsc::channel::<IncomingMessage<IN>>();

        let sdr_supervisor_clone = sdr_supervisor.clone();
        let sdr_worker_clone = sdr_worker.clone();
        let sdr_worker_trscv = sdr_worker.clone();

        (
            ServerChannel {
                rcv_server,
                sdr_supervisor,
                sdr_worker,
                transceiver: Some(Transceiver::new(sdr_worker_trscv, rcv_incoming)),
            },
            SupervisorChannel {
                sdr_server,
                sdr_supervisor: sdr_supervisor_clone,
                rcv_supervisor,
                sdr_worker: sdr_worker_clone,
                rcv_worker: Arc::new(Mutex::new(rcv_worker)),
                sdr_incoming,
            },
        )
    }
}

/// Supervisor communication channels
pub(crate) struct SupervisorChannel<IN: Message + Send, OUT: Message + Send> {
    /// Channel Message sender to server
    pub sdr_server: Sender<ServerUpdate>,

    /// Channel Message sender and receiver to supervisor
    pub sdr_supervisor: Sender<SupervisorMessage>,
    pub rcv_supervisor: Receiver<SupervisorMessage>,

    // Sender and receiver channels for worker messages
    pub sdr_incoming: Sender<IncomingMessage<IN>>,
    pub sdr_worker: Sender<WorkerActiveMessage<OUT>>,
    pub rcv_worker: Arc<Mutex<Receiver<WorkerActiveMessage<OUT>>>>,
}

/// Worker communication channels
pub(crate) struct WorkerChannel<IN: Message + Send, OUT: Message + Send> {
    /// Channel Message sender to server
    pub sdr_incoming: Sender<IncomingMessage<IN>>,

    /// Channel Message sender and receiver to supervisor
    pub sdr_supervisor: Sender<SupervisorMessage>,

    // Unique receiver channel while inactive
    pub rcv_inactive: Receiver<WorkerInactiveMessage>,

    // Receiver channels for worker messages
    pub rcv_worker: Arc<Mutex<Receiver<WorkerActiveMessage<OUT>>>>,
}

impl<IN: Message + Send, OUT: Message + Send> WorkerChannel<IN, OUT> {
    pub fn new(
        sdr_incoming: Sender<IncomingMessage<IN>>,
        sdr_supervisor: Sender<SupervisorMessage>,
        rcv_inactive: Receiver<WorkerInactiveMessage>,
        rcv_worker: Arc<Mutex<Receiver<WorkerActiveMessage<OUT>>>>,
    ) -> WorkerChannel<IN, OUT> {
        WorkerChannel {
            sdr_incoming,
            sdr_supervisor,
            rcv_inactive,
            rcv_worker,
        }
    }
}
