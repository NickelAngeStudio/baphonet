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

use std::sync::mpsc::{self, Receiver, Sender};

use crate::{
    Message,
    client::{
        message::{ClientUpdate, WorkerMessage},
        transceiver::Transceiver,
    },
};

/// Channels used by [`Client`].
pub struct ClientChannel<IN: Message + Send + 'static, OUT: Message + Send + 'static> {
    /// Receiver of client messages
    pub rcv_update: Receiver<ClientUpdate>,

    // Sender channel for worker messages
    pub sdr_worker: Sender<WorkerMessage<OUT>>,

    /// Transceiver used to receive and send messages.
    pub(crate) transceiver: Option<Transceiver<IN, OUT>>,
}

impl<IN: Message + Send + 'static, OUT: Message + Send + 'static> ClientChannel<IN, OUT> {
    pub fn create_client_worker_channels() -> (ClientChannel<IN, OUT>, WorkerChannel<IN, OUT>) {
        let (sdr_update, rcv_update) = mpsc::channel::<ClientUpdate>();
        let (sdr_incoming, rcv_incoming) = mpsc::channel::<IN>();
        let (sdr_worker, rcv_worker) = mpsc::channel::<WorkerMessage<OUT>>();

        let sdr_worker_clone = sdr_worker.clone();
        let transceiver = Transceiver::new(rcv_incoming, sdr_worker.clone());
        (
            ClientChannel {
                rcv_update,
                sdr_worker: sdr_worker,
                transceiver: Some(transceiver),
            },
            WorkerChannel {
                sdr_update,
                rcv_worker,
                sdr_worker: sdr_worker_clone,
                sdr_incoming,
            },
        )
    }
}

/// Channels used by [`Worker`].
pub struct WorkerChannel<IN: Message + Send + 'static, OUT: Message + Send + 'static> {
    /// Sender of client update
    pub sdr_update: Sender<ClientUpdate>,

    /// Sender of message received
    pub sdr_incoming: Sender<IN>,

    // Receiver channel for worker messages
    pub rcv_worker: Receiver<WorkerMessage<OUT>>,

    // Sender channel for worker messages
    pub sdr_worker: Sender<WorkerMessage<OUT>>,
}
