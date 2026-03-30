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


use std::{net::{SocketAddr, TcpListener}, sync::{Arc, Mutex}, thread::{self, JoinHandle}};

use crate::{Message, server::{Error, channel::{SupervisorChannel, WorkerChannel}, client::Clients, status::SupervisorStatus, task::Tasks, worker::Worker}};

/// Supervisor of worker threads
pub(crate) struct Supervisor<IN : Message + Send + 'static, OUT : Message + Send + 'static> {

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

impl <IN : Message + Send, OUT : Message + Send> Supervisor<IN, OUT> {
    /// Create a new instance of [`Supervisor`] from parameters.
    pub fn new(socket : SocketAddr, maximum_client : usize, worker_count : usize, clients : Clients, channels : SupervisorChannel<IN, OUT>) -> Result<Supervisor<IN, OUT>, Error> {

        // Try to create listener
        match Self::create_tcp_listener(socket) {
            Ok(listener) => {   // Listener created with success
                let listener = Arc::new(Mutex::new(listener));
                
                Ok(Supervisor { listener, worker_count, clients, channels, workers :  Vec::<JoinHandle<()>>::with_capacity(worker_count),
                    status: SupervisorStatus::Paused,
                    tasks: Tasks::new(maximum_client) })

            },
            Err(err) => Err(err),
        }
    }

     /// Execute the supervisor routine
    pub fn execute(&mut self) {

        // Create workers
        self.create_workers(self.worker_count, self.listener.clone(), self.clients.clone());

        // Set active
        

        'supervisor:
        loop {
            match self.status {
                SupervisorStatus::Active => {}, // TODO:
                SupervisorStatus::Paused => {}, //  TODO:
                SupervisorStatus::Ending => break 'supervisor,
            }
        }

    }


    /// Create the [`Supervisor`] workers
    #[inline]
    fn create_workers(&mut self, worker_count : usize, listener : Arc<Mutex<TcpListener>>, clients : Clients) {

        for id in 0..worker_count {
            let channels  = WorkerChannel::new(self.channels.sdr_server.clone(), 
                self.channels.sdr_supervisor.clone(), 
                self.channels.rcv_worker.clone());

            let mut worker = Worker::new(id, listener.clone(), clients.clone(), channels);
            self.workers.push( thread::spawn(move || {
                worker.execute();
            }));
        } 

    }

    /// Create the TcpListener from [`SocketAddr`].
    #[inline]
    fn create_tcp_listener(socket : SocketAddr) -> Result<TcpListener, Error> {

        match TcpListener::bind(socket){
            Ok(listener) => {
                match listener.set_nonblocking(true) {
                    Ok(_) => Ok(listener),
                    Err(_) => Err(Error::SetNonblockingFailed),
                }
            }, 
            Err(err) => {
                match err.kind() {
                    std::io::ErrorKind::AddrInUse => Err(Error::SocketAddressAlreadyUsed),
                    std::io::ErrorKind::InvalidInput => Err(Error::SocketInvalid),
                    _ => Err(Error::UnhandledIOError(err.kind())),
                }
                
            },
        }

    }
}
