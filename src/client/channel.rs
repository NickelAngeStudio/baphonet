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

/// Default capacity of the dispatcher vec
const DISPATCHER_VEC_CAPACITY: usize = 16;

use std::sync::mpsc::{self, Receiver, Sender};

use crate::{
    Message,
    client::{
        dispatcher::Dispatcher,
        message::{self, ClientMessage, DispatcherMessage, WorkerMessage},
        status::ClientStatus,
    },
};

/// Channels used by [`Client`].
pub struct ClientChannel<IN: Message + Send + 'static, OUT: Message + Send + 'static> {
    /// Receiver of client messages
    pub rcv_client: Option<Receiver<ClientMessage<IN>>>,

    /// Channels of client dispatcher
    pub sdr_dispatcher: Vec<Sender<DispatcherMessage<OUT>>>,

    // Sender channel for worker messages
    pub sdr_worker: Option<Sender<WorkerMessage<OUT>>>,
}

impl<IN: Message + Send + 'static, OUT: Message + Send + 'static> ClientChannel<IN, OUT> {
    /// Create a new instance of [`ClientChannel`].
    pub fn new() -> ClientChannel<IN, OUT> {
        ClientChannel {
            rcv_client: None,
            sdr_dispatcher: Vec::with_capacity(DISPATCHER_VEC_CAPACITY),
            sdr_worker: None,
        }
    }

    /// Send a [`DispatcherMessage`] to dispatchers
    pub fn send_message_to_dispatchers(&mut self, message: DispatcherMessage<OUT>) {
        match message {
            DispatcherMessage::Status(client_status) => {
                for sdr in &self.sdr_dispatcher {
                    match sdr.send(DispatcherMessage::Status(client_status.clone())) {
                        Ok(_) => {}
                        Err(_) => {}
                    }
                }
            }
            DispatcherMessage::Reference(sender) => {
                for sdr in &self.sdr_dispatcher {
                    match sdr.send(DispatcherMessage::Reference(sender.clone())) {
                        Ok(_) => {}
                        Err(_) => {}
                    }
                }
            }
            DispatcherMessage::Ping => {
                for sdr in &self.sdr_dispatcher {
                    match sdr.send(DispatcherMessage::Ping) {
                        Ok(_) => {}
                        Err(_) => {}
                    }
                }
            }
        }
    }

    /// Create communication channels of worker
    pub fn worker_channels(&mut self) -> WorkerChannel<IN, OUT> {
        let (sdr_client, rcv_client) = mpsc::channel::<ClientMessage<IN>>();
        let (sdr_worker, rcv_worker) = mpsc::channel::<WorkerMessage<OUT>>();

        self.rcv_client = Some(rcv_client);
        self.sdr_worker = Some(sdr_worker);

        WorkerChannel {
            sdr_client,
            rcv_worker,
        }
    }

    /// Create and register a new [`Dispatcher`].
    ///
    /// # Returns
    /// A new [`Dispatcher`].
    pub fn dispatcher(&mut self, client_status: ClientStatus) -> Dispatcher<OUT> {
        let (sdr_dispatcher, rcv_dispatcher) = mpsc::channel::<DispatcherMessage<OUT>>();

        let mut sms = Dispatcher::new(rcv_dispatcher, client_status);

        self.sdr_dispatcher.push(sdr_dispatcher); // Register sender
        match self.sdr_worker.as_ref() {
            Some(sender) => sms.sdr_worker = Some(sender.clone()),
            None => {}
        }

        sms
    }

    /// Clear all channels except for active [`Dispatcher`]
    pub fn clear(&mut self) {
        self.rcv_client = None;
        self.sdr_worker = None;

        for i in self.sdr_dispatcher.len()..0 {
            match self.sdr_dispatcher[i].send(DispatcherMessage::Ping) {
                Ok(_) => {}
                Err(_) => {
                    self.sdr_dispatcher.swap_remove(i);
                }
            }
        }
    }
}

/// Channels used by [`Worker`].
pub struct WorkerChannel<IN: Message + Send + 'static, OUT: Message + Send + 'static> {
    /// Sender of client messages
    pub sdr_client: Sender<ClientMessage<IN>>,

    // Receiver channel for worker messages
    pub rcv_worker: Receiver<WorkerMessage<OUT>>,
}
