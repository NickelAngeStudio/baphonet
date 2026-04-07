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
    sync::{
        Arc, Mutex,
        mpsc::{self, RecvTimeoutError, Sender},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use crate::{
    Message,
    server::{
        ClientId,
        channel::{SupervisorChannel, WorkerChannel},
        client::Clients,
        error::{ErrorServer, ErrorUpdate},
        message::{
            ServerMessage, SupervisorMessage, SupervisorServerMessage, SupervisorUpdate,
            SupervisorWorkerMessage, WorkerActiveMessage, WorkerInactiveMessage,
        },
        status::Status,
        task::{TaskStatus, Tasks},
        worker::{Worker, WorkerId},
    },
};

/// Milliseconds of wait time per worker.
const MS_JOIN_WAIT_DURATION_PER_WORKER: u64 = 50;

/// Supervisor of worker threads
pub(crate) struct Supervisor<IN: Message + Send + 'static, OUT: Message + Send + 'static> {
    /// TcpListener used to manage connections
    listener: Option<Arc<Mutex<TcpListener>>>,

    /// Status of the supervisor thread
    status: Status,

    /// Shared clients between threads
    clients: Clients,

    /// Shared server channels
    channels: SupervisorChannel<IN, OUT>,

    /// Tasks executed by workers
    tasks: Tasks,

    /// Worker thread handles
    workers: Vec<SupervisorWorker>,

    /// Supervisor last pool time
    last_pool: Instant,

    /// Supervisor pool rate duration in milliseconds
    pool_rate_duration: Duration,
}

/// Worker channel and handles
pub struct SupervisorWorker {
    /// Sender for inactive worker.
    sdr_inactive: Sender<WorkerInactiveMessage>,

    /// Handle of the worker thread
    handle: JoinHandle<()>,
}

impl<IN: Message + Send, OUT: Message + Send> Supervisor<IN, OUT> {
    /// Create a new instance of [`Supervisor`] from parameters.
    pub fn new(
        maximum_client: usize,
        worker_count: usize,
        incoming_max_size: usize,
        pool_rate: u64,
        clients: Clients,
        channels: SupervisorChannel<IN, OUT>,
    ) -> Supervisor<IN, OUT> {
        // Create workers
        let workers =
            Self::create_workers(worker_count, incoming_max_size, clients.clone(), &channels);

        // Return supervisor
        Supervisor {
            listener: None,
            clients,
            channels,
            workers,

            last_pool: Instant::now(),
            pool_rate_duration: Duration::from_millis(1000 / pool_rate),
            status: Status::Inactive,
            tasks: Tasks::new(maximum_client),
        }
    }

    /// Execute the supervisor routine
    pub fn execute(&mut self) {
        // Supervisor inactive loop
        'inactive: loop {
            match self.status {
                Status::End => break 'inactive,
                _ => {
                    self.status = Status::Inactive; // Supervisor is inactive
                    match self.channels.rcv_supervisor.recv() {
                        // Wait message from server
                        Ok(msg) => match msg {
                            SupervisorMessage::FromServer(server_msg) => match server_msg {
                                SupervisorServerMessage::Start(listener) => self.active(listener),
                                SupervisorServerMessage::Stop => {} // Already stopped
                                SupervisorServerMessage::End => self.status = Status::End,
                            },
                            _ => {}
                        },
                        Err(_) => break 'inactive, // Server channel lost, end thread
                    }
                }
            }
        }

        // Join worker threads
        self.join_workers();
        self.send_update_to_server(SupervisorUpdate::Ended);
    }

    pub fn active(&mut self, listener: Arc<Mutex<TcpListener>>) {
        // Purge any remaining worker messages
        self.purge_rcv_worker_channel();

        // Signal all thread to start
        self.start_workers(listener.clone());
        self.listener = Some(listener);

        // Set active
        self.status = Status::Active;
        self.send_update_to_server(SupervisorUpdate::Active);

        'supervisor: loop {
            match self.status {
                Status::Active => {
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
                                    self.status = Status::End; // Channel lost, end thread
                                    break 'supervisor;
                                }
                            }
                        }
                    }
                }
                _ => break 'supervisor,
            }
        }

        // Tell worker to stop
        for _ in 0..self.workers.len() {
            self.send_message_to_worker(WorkerActiveMessage::Stop);
        }

        // Tell servr currently inactive
        self.send_update_to_server(SupervisorUpdate::Inactive);
    }

    /// Purge worker receive channel of any leftover message before
    /// starting server.
    #[inline]
    fn purge_rcv_worker_channel(&mut self) {
        match self.channels.rcv_worker.lock() {
            Ok(rcv) => 'purge: loop {
                match rcv.try_recv() {
                    Ok(_) => {}
                    Err(_) => break 'purge,
                }
            },
            Err(_) => {
                // Mutex error, close thread
                self.status = Status::End;
                return;
            }
        }
    }

    /// Handle message coming from the server
    #[inline]
    fn handle_server_message(&mut self, message: SupervisorServerMessage) {
        match message {
            SupervisorServerMessage::Start(_) => {} // Already started
            SupervisorServerMessage::Stop => self.status = Status::Stopping,
            SupervisorServerMessage::End => self.status = Status::End,
        }
    }

    /// Handle the execute server message
    #[inline]
    fn handle_server_tasks(&mut self) {
        // Incoming connection task
        match self.tasks.incoming {
            TaskStatus::Ready => {
                self.send_message_to_worker(WorkerActiveMessage::Incoming);
                self.tasks.incoming = TaskStatus::InProgress;
            }
            TaskStatus::InProgress => {}
        }

        // Client message reception tasks
        for client_id in 0..self.tasks.reception.len() {
            match self.tasks.reception[client_id].as_ref() {
                Some(task) => match task {
                    TaskStatus::Ready => self.send_message_to_worker(WorkerActiveMessage::Receive(
                        client_id as ClientId,
                    )),
                    TaskStatus::InProgress => {}
                },
                None => {}
            }
        }

        // Register timestamp
        self.last_pool = Instant::now();
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
            self.send_message_to_worker(WorkerActiveMessage::End);
        }
        for sw in &self.workers {
            match sw.sdr_inactive.send(WorkerInactiveMessage::End) {
                Ok(_) => {}
                Err(_) => {}
            }
        }

        // Try to join
        let ts = Instant::now();
        let wait_duration =
            Duration::from_millis(self.workers.len() as u64 * MS_JOIN_WAIT_DURATION_PER_WORKER);

        'workers: loop {
            match self.workers.pop() {
                Some(worker) => {
                    'join: loop {
                        if worker.handle.is_finished() {
                            match worker.handle.join() {
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

    /// Tell all workers to start
    #[inline]
    fn start_workers(&mut self, listener: Arc<Mutex<TcpListener>>) {
        for sw in &self.workers {
            match sw
                .sdr_inactive
                .send(WorkerInactiveMessage::Start(listener.clone()))
            {
                Ok(_) => {}
                Err(_) => self.status = Status::End, // Channel lost, kill thread
            }
        }
    }

    /// Create the [`Supervisor`] workers
    #[inline]
    fn create_workers(
        worker_count: usize,
        incoming_max_size: usize,
        clients: Clients,
        channels: &SupervisorChannel<IN, OUT>,
    ) -> Vec<SupervisorWorker> {
        let mut workers = Vec::<SupervisorWorker>::with_capacity(worker_count);

        for id in 0..worker_count {
            let (sdr_inactive, rcv_inactive) = mpsc::channel::<WorkerInactiveMessage>();

            let worker_channels = WorkerChannel::new(
                channels.sdr_server.clone(),
                channels.sdr_supervisor.clone(),
                rcv_inactive,
                channels.rcv_worker.clone(),
            );

            let mut worker = Worker::new(id, incoming_max_size, clients.clone(), worker_channels);

            workers.push(SupervisorWorker {
                sdr_inactive,
                handle: thread::spawn(move || {
                    worker.execute();
                }),
            });
        }

        workers
    }

    /// Send a message to worker thread.
    #[inline]
    fn send_message_to_worker(&mut self, message: WorkerActiveMessage<OUT>) {
        match self.channels.sdr_worker.send(message) {
            Ok(_) => {}
            Err(_) => self.status = Status::End, // Channel lost, kill supervisor
        }
    }

    /// Send [`SupervisorUpdate`] to the server.
    #[inline]
    fn send_update_to_server(&mut self, update: SupervisorUpdate) {
        match self.channels.sdr_server.send(ServerMessage::Update(update)) {
            Ok(_) => {}
            Err(_) => self.status = Status::End, // Channel lost, kill supervisor
        }
    }
}
