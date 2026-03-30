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

use std::{marker::PhantomData, net::TcpListener, sync::{Arc, Mutex}};

use crate::{MAXIMUM_MESSAGE_SIZE, Message, client, server::{channel::{SupervisorChannel, WorkerChannel}, client::Clients, status::WorkerStatus}};


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
                WorkerStatus::Active => {}, // TODO
                WorkerStatus::Ended => break 'worker,
            }
        }

        // TODO:Tell supervisor worker ended

    }
}