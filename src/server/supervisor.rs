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

use std::{
    net::{SocketAddr, TcpListener},
    sync::{Arc, Mutex, mpsc::RecvTimeoutError},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use crate::{
    Message,
    server::{
        ClientId, ErrorServer, ErrorUpdate,
        channel::{SupervisorChannel, WorkerChannel},
        client::Clients,
        message::{
            ServerMessage, SupervisorMessage, SupervisorServerMessage, SupervisorUpdate,
            SupervisorWorkerMessage, WorkerMessage,
        },
        status::SupervisorStatus,
        task::{TaskStatus, Tasks},
        worker::{Worker, WorkerId},
    },
};

/// Milliseconds of wait time per worker.
const MS_JOIN_WAIT_DURATION_PER_WORKER: u64 = 50;

/// Supervisor of worker threads
pub(crate) struct Supervisor<IN: Message + Send + 'static, OUT: Message + Send + 'static> {
    /// TcpListener used to manage connections
    listener: Arc<Mutex<TcpListener>>,

    /// Status of the supervisor thread
    status: SupervisorStatus,

    /// Count of worker for this supervisor
    worker_count: usize,

    /// Shared clients between threads
    clients: Clients,

    /// Shared server channels
    channels: SupervisorChannel<IN, OUT>,

    /// Tasks executed by workers
    tasks: Tasks,

    /// Worker thread handles
    workers: Vec<JoinHandle<()>>,

    /// Supervisor last pool time
    last_pool: Instant,

    /// Supervisor pool rate duration in milliseconds
    pool_rate_duration: Duration,
}

impl<IN: Message + Send, OUT: Message + Send> Supervisor<IN, OUT> {
    /// Create a new instance of [`Supervisor`] from parameters.
    pub fn new(
        socket: SocketAddr,
        maximum_client: usize,
        worker_count: usize,
        pool_rate: u64,
        clients: Clients,
        channels: SupervisorChannel<IN, OUT>,
    ) -> Result<Supervisor<IN, OUT>, ErrorServer> {
        // Try to create listener
        match Self::create_tcp_listener(socket) {
            Ok(listener) => {
                // Listener created with success
                let listener = Arc::new(Mutex::new(listener));

                Ok(Supervisor {
                    listener,
                    worker_count,
                    clients,
                    channels,
                    workers: Vec::<JoinHandle<()>>::with_capacity(worker_count),
                    last_pool: Instant::now(),
                    pool_rate_duration: Duration::from_millis(1000 / pool_rate),
                    status: SupervisorStatus::Paused,
                    tasks: Tasks::new(maximum_client),
                })
            }
            Err(err) => Err(err),
        }
    }

    /// Execute the supervisor routine
    pub fn execute(&mut self) {
        // Create workers
        self.create_workers(
            self.worker_count,
            self.listener.clone(),
            self.clients.clone(),
        );

        // Set active
        self.status = SupervisorStatus::Active;
        self.send_update_to_server(SupervisorUpdate::Active);

        'supervisor: loop {
            match self.status {
                SupervisorStatus::Active | SupervisorStatus::Paused => {
                    if self.last_pool.elapsed() > self.pool_rate_duration {
                        self.handle_server_tasks(); // Do supervisor tasks
                    }
                    match self
                        .channels
                        .rcv_supervisor
                        .recv_timeout(self.pool_rate_duration)
                    {
                        Ok(message) => match message {
                            SupervisorMessage::FromServer(message) => {
                                self.handle_server_message(message)
                            }
                            SupervisorMessage::FromWorker(message) => {
                                self.handle_worker_message(message)
                            }
                        },
                        Err(err) => {
                            match err {
                                RecvTimeoutError::Timeout => self.handle_server_tasks(), // Do supervisor tasks
                                RecvTimeoutError::Disconnected => {
                                    self.status = SupervisorStatus::Ending
                                } // Channel lost, kill supervisor
                            }
                        }
                    }
                }
                SupervisorStatus::Ending => break 'supervisor,
            }
        }

        self.join_workers();
        self.send_update_to_server(SupervisorUpdate::Ended);
    }

    /// Handle message coming from the server
    #[inline]
    fn handle_server_message(&mut self, message: SupervisorServerMessage) {
        match message {
            SupervisorServerMessage::Pause => self.status = SupervisorStatus::Paused,
            SupervisorServerMessage::Resume => self.handle_server_message_resume(),
            SupervisorServerMessage::Stop => self.status = SupervisorStatus::Ending,
            SupervisorServerMessage::PoolRate(pool_rate) => {
                self.handle_server_message_pool_rate(pool_rate)
            }
        }
    }

    /// Handle the execute server message
    #[inline]
    fn handle_server_tasks(&mut self) {
        // Incoming connection task
        match self.tasks.incoming {
            TaskStatus::Ready => {
                self.send_message_to_worker(WorkerMessage::Incoming);
                self.tasks.incoming = TaskStatus::InProgress;
            }
            TaskStatus::InProgress => {}
        }

        // Client message reception tasks
        for client_id in 0..self.tasks.reception.len() {
            match self.tasks.reception[client_id].as_ref() {
                Some(task) => match task {
                    TaskStatus::Ready => {
                        self.send_message_to_worker(WorkerMessage::Receive(client_id as ClientId))
                    }
                    TaskStatus::InProgress => {}
                },
                None => {}
            }
        }

        // Register timestamp
        self.last_pool = Instant::now();
    }

    /// Handle the new pool rate server message
    #[inline]
    fn handle_server_message_pool_rate(&mut self, pool_rate: u64) {
        self.pool_rate_duration = Duration::from_millis(1000 / pool_rate);

        // Notify server of poolrate change
        self.send_update_to_server(SupervisorUpdate::PoolRate(pool_rate));
    }

    /// Handle the resume server message
    #[inline]
    fn handle_server_message_resume(&mut self) {
        // Clear all client buffer
        for client_id in 0..self.tasks.reception.len() {
            match self.tasks.reception[client_id].as_ref() {
                Some(task) => match task {
                    TaskStatus::Ready => {
                        self.send_message_to_worker(WorkerMessage::Clear(client_id as ClientId))
                    }
                    TaskStatus::InProgress => {}
                },
                None => {}
            }
        }
        // Set as active
        self.status = SupervisorStatus::Active;
    }

    /// Handle message coming from a worker
    #[inline]
    fn handle_worker_message(&mut self, message: SupervisorWorkerMessage) {
        match message {
            SupervisorWorkerMessage::Connected(client_id) => {
                self.handle_worker_message_connected(client_id)
            }
            SupervisorWorkerMessage::IncomingJobDone => self.tasks.incoming = TaskStatus::Ready,
            SupervisorWorkerMessage::ReceiveJobDone(client_id) => {
                self.handle_worker_message_receive_done(client_id)
            }
            SupervisorWorkerMessage::Disconnected(client_id) => {
                self.handle_worker_message_disconnected(client_id)
            }
            SupervisorWorkerMessage::Finished(worker_id) => {
                self.handle_worker_message_finished(worker_id)
            }
            SupervisorWorkerMessage::Error(error) => self.handle_worker_message_error(error),
        }
    }

    /// Handle Connected worker message
    #[inline]
    fn handle_worker_message_connected(&mut self, client_id: ClientId) {
        // Register task
        self.tasks.reception[client_id as usize] = Some(TaskStatus::Ready);

        // Notify server
        self.send_update_to_server(SupervisorUpdate::ClientConnected(client_id));
    }

    /// Handle received done worker message
    #[inline]
    fn handle_worker_message_receive_done(&mut self, client_id: ClientId) {
        self.tasks.reception[client_id as usize] = Some(TaskStatus::Ready);
    }

    /// Handle client disconnected message
    #[inline]
    fn handle_worker_message_disconnected(&mut self, client_id: ClientId) {
        self.remove_client_from_lists(client_id);

        // Notify server
        self.send_update_to_server(SupervisorUpdate::ClientDisconnected(client_id));
    }

    /// Handle worker client not found
    #[inline]
    fn handle_worker_message_error(&mut self, error: ErrorUpdate) {
        match &error {
            ErrorUpdate::ConnectionLost(client_id) => self.remove_client_from_lists(*client_id),
            _ => {}
        }

        // Notify server of error
        self.send_update_to_server(SupervisorUpdate::Error(error));
    }

    /// Handle worker message that it is finished
    #[inline]
    fn handle_worker_message_finished(&mut self, _worker_id: WorkerId) {
        // TODO: Determine if we recreate worker
    }

    /// Remove client from tasks and list
    #[inline]
    fn remove_client_from_lists(&mut self, client_id: ClientId) {
        // Remove from client list
        let clients = self.clients.clone();
        match clients[client_id as usize].lock() {
            Ok(mut client) => *client = None,
            Err(_) => {
                // TODO: Handle lock errors #15
            }
        }

        // Remove from works
        self.tasks.reception[client_id as usize] = None;
    }

    /// Join the [`Supervisor`] workers
    #[inline]
    fn join_workers(&mut self) {
        // Tell each worker to end
        for _ in 0..self.workers.len() {
            self.send_message_to_worker(WorkerMessage::End);
        }

        // Try to join
        let ts = Instant::now();
        let wait_duration =
            Duration::from_millis(self.worker_count as u64 * MS_JOIN_WAIT_DURATION_PER_WORKER);

        'workers: loop {
            match self.workers.pop() {
                Some(worker) => {
                    'join: loop {
                        if worker.is_finished() {
                            match worker.join() {
                                Ok(_) => {}
                                Err(_) => {
                                    #[cfg(debug_assertions)]
                                    {
                                        println!("Worker thread join failed!");
                                    }
                                }
                            }
                            break 'join;
                        }
                        if ts.elapsed() > wait_duration {
                            // Join took too long
                            #[cfg(debug_assertions)]
                            {
                                println!("join_client_threads : Thread join timeout!");
                            }
                            break 'workers;
                        }
                    }
                }
                None => break 'workers,
            }
        }
    }

    /// Create the [`Supervisor`] workers
    #[inline]
    fn create_workers(
        &mut self,
        worker_count: usize,
        listener: Arc<Mutex<TcpListener>>,
        clients: Clients,
    ) {
        for id in 0..worker_count {
            let channels = WorkerChannel::new(
                self.channels.sdr_server.clone(),
                self.channels.sdr_supervisor.clone(),
                self.channels.rcv_worker.clone(),
            );

            let mut worker = Worker::new(id, listener.clone(), clients.clone(), channels);
            self.workers.push(thread::spawn(move || {
                worker.execute();
            }));
        }
    }

    /// Create the TcpListener from [`SocketAddr`].
    #[inline]
    fn create_tcp_listener(socket: SocketAddr) -> Result<TcpListener, ErrorServer> {
        match TcpListener::bind(socket) {
            Ok(listener) => {
                // This should crash instead of having a blocking listener
                listener.set_nonblocking(true).unwrap();
                Ok(listener)
            }
            Err(err) => match err.kind() {
                std::io::ErrorKind::AddrInUse => Err(ErrorServer::SocketAddressAlreadyUsed),
                std::io::ErrorKind::InvalidInput => Err(ErrorServer::SocketInvalid),
                _ => Err(ErrorServer::UnhandledIOError(err.kind())),
            },
        }
    }

    /// Send a message to worker thread.
    #[inline]
    fn send_message_to_worker(&mut self, message: WorkerMessage<OUT>) {
        match self.channels.sdr_worker.send(message) {
            Ok(_) => {}
            Err(_) => self.status = SupervisorStatus::Ending, // Channel lost, kill supervisor
        }
    }

    /// Send [`SupervisorUpdate`] to the server.
    #[inline]
    fn send_update_to_server(&mut self, update: SupervisorUpdate) {
        match self.channels.sdr_server.send(ServerMessage::Update(update)) {
            Ok(_) => {}
            Err(_) => self.status = SupervisorStatus::Ending, // Channel lost, kill supervisor
        }
    }
}
