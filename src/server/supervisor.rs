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


use std::{net::TcpListener, sync::{Arc, Mutex}, thread::JoinHandle};

use crate::{Message, server::{channel::SupervisorChannel, client::Clients, status::SupervisorStatus, task::Tasks}};

/// Supervisor of worker threads
pub(crate) struct Supervisor<IN : Message, OUT : Message> {

    /// TcpListener used to manage connections
    listener : Arc<Mutex<TcpListener>>,

    /// Status of the supervisor thread
    status : SupervisorStatus,

    /// Count of worker for this supervisor
    worker_count : usize,

    /// Shared clients between threads
    clients : Clients,

    /// Shared server channels
    channels : SupervisorChannel<IN, OUT>,

    /// Tasks executed by workers
    tasks : Tasks,

    /// Worker thread handles
    workers : Vec<JoinHandle<()>>,
}

impl <IN : Message, OUT : Message> Supervisor<IN, OUT> {
    
}
