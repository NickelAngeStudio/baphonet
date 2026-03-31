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

use crate::{Message, client::message::{ClientMessage, WorkerMessage}};

/// Channels used by [`Client`].
pub struct ClientChannel<IN : Message + Send + 'static, OUT : Message + Send + 'static>{
    /// Receiver of client messages
    pub rcv_client : Receiver<ClientMessage<IN>>,

    // Sender channel for worker messages
    pub sdr_worker : Sender<WorkerMessage<OUT>>,

}

/// Channels used by [`Worker`].
pub struct WorkerChannel<IN : Message + Send + 'static, OUT : Message + Send + 'static>{
    /// Sender of client messages
    pub sdr_client : Sender<ClientMessage<IN>>,

    // Receiver channel for worker messages
    pub rcv_worker : Receiver<WorkerMessage<OUT>>,

}

/// Create both [`ClientChannel`] and [`WorkerChannel`].
pub(crate) fn create_client_worker_channels<IN : Message + Send + 'static, OUT : Message + Send + 'static>() -> (ClientChannel<IN, OUT>, WorkerChannel<IN, OUT>){

    let (sdr_client, rcv_client) = mpsc::channel::<ClientMessage<IN>>();
    let (sdr_worker, rcv_worker) = mpsc::channel::<WorkerMessage<OUT>>();

    (ClientChannel{ rcv_client, sdr_worker }, WorkerChannel{ sdr_client, rcv_worker })

}