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

use std::sync::mpsc::{Receiver, Sender, TryRecvError};

use crate::{
    Message,
    server::{
        ClientId, ServerStatus,
        error::ErrorSender,
        message::{OutgoingMessage, SenderMessage, WorkerMessage},
    },
};

/// Server sender can send message to client
/// and can be shared to multiple threads.
pub struct ServerMessageSender<OUT: Message + Send + 'static> {
    /// Unique receiver channel for sender
    rcv_sender: Receiver<SenderMessage<OUT>>,

    /// Clone of Sender channel for worker message
    pub(crate) sdr_worker: Option<Sender<WorkerMessage<OUT>>>,

    /// Current server status
    server_status: ServerStatus,
}

impl<OUT: Message + Send + 'static> ServerMessageSender<OUT> {
    /// Create a new instance pf ServerSender with a message receiver.
    pub(crate) fn new(
        rcv_sender: Receiver<SenderMessage<OUT>>,
        server_status: ServerStatus,
    ) -> ServerMessageSender<OUT> {
        ServerMessageSender {
            rcv_sender,
            sdr_worker: None,
            server_status,
        }
    }

    /// Send message to one connected client
    ///
    /// Returns :
    /// - [`Result`]:
    ///     - Ok(()) if message was sent to thread.
    ///     - Err([`ErrorSender::Inactive`]) if server hasn't started
    ///     - Err([`ErrorSender::Paused`]) if server is paused.
    ///     - Err([`ErrorSender::Disconnected`]) if server was dropped.
    pub fn send(&mut self, client_id: ClientId, message: OUT) -> Result<(), ErrorSender> {
        match self.update() {
            // Update sender
            Ok(_) => match self.server_status {
                ServerStatus::Active => match self.sdr_worker.as_mut() {
                    Some(channel) => {
                        match channel.send(WorkerMessage::Send(OutgoingMessage::<OUT>::new(
                            client_id, message,
                        ))) {
                            Ok(_) => Ok(()), // Message was sent
                            Err(_) => {
                                // Remove channel
                                self.sdr_worker = None;
                                Err(ErrorSender::Inactive)
                            }
                        }
                    }
                    None => Err(ErrorSender::Inactive),
                },
                ServerStatus::Paused => Err(ErrorSender::Paused),
                _ => Err(ErrorSender::Inactive),
            },
            Err(err) => Err(err),
        }
    }

    /// Send message to multiple client with a vector of [`ClientId`].
    ///
    /// Returns :
    /// - [`Result`]:
    ///     - Ok(()) if message was sent to thread.
    ///     - Err([`ErrorSender::Inactive`]) if server hasn't started
    ///     - Err([`ErrorSender::Paused`]) if server is paused.
    ///     - Err([`ErrorSender::NoDestination`]) if no destination specified
    ///     - Err([`ErrorSender::Disconnected`]) if server was dropped.
    pub fn send_vec(
        &mut self,
        destinations: &Vec<ClientId>,
        message: OUT,
    ) -> Result<(), ErrorSender> {
        match self.update() {
            // Update sender
            Ok(_) => match self.server_status {
                ServerStatus::Active => match self.sdr_worker.as_mut() {
                    Some(channel) => {
                        if destinations.len() > 0 {
                            match channel.send(WorkerMessage::Send(
                                OutgoingMessage::<OUT>::new_vec(&destinations, message),
                            )) {
                                Ok(_) => Ok(()), // Message was sent
                                Err(_) => {
                                    // Remove channel
                                    self.sdr_worker = None;
                                    Err(ErrorSender::Inactive)
                                }
                            }
                        } else {
                            Err(ErrorSender::NoDestination)
                        }
                    }
                    None => Err(ErrorSender::Inactive),
                },
                ServerStatus::Paused => Err(ErrorSender::Paused),
                _ => Err(ErrorSender::Inactive),
            },
            Err(err) => Err(err),
        }
    }

    /// Receive message from server to update sender.
    ///
    /// # Returns
    /// - [`Result`]
    ///     - Ok(()) if updated with success.
    ///     - Err([`ErrorSender::Disconnected`]) if server was dropped.
    #[inline]
    fn update(&mut self) -> Result<(), ErrorSender> {
        'update: loop {
            match self.rcv_sender.try_recv() {
                Ok(message) => match message {
                    SenderMessage::Status(server_status) => {
                        match server_status {
                            // Remove sender if inactive
                            ServerStatus::Inactive | ServerStatus::Ending => self.sdr_worker = None,
                            _ => {}
                        }
                        self.server_status = server_status
                    }
                    SenderMessage::Reference(sender) => self.sdr_worker = Some(sender),
                    _ => {}
                },
                Err(err) => match err {
                    TryRecvError::Empty => break 'update,
                    TryRecvError::Disconnected => return Err(ErrorSender::Disconnected),
                },
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::mpsc::{self, Sender},
        thread,
        time::Duration,
    };

    use crate::{
        Message,
        server::{
            ClientId, ServerStatus,
            error::ErrorSender,
            message::{SenderMessage, WorkerMessage},
            sender::ServerMessageSender,
        },
    };

    /// Empty struct implementing Message trait
    struct TestMessage {}
    impl Message for TestMessage {
        fn serialize(&self, _buffer: &mut [u8]) -> Result<usize, ()> {
            todo!()
        }

        fn deserialize(_buffer: &[u8]) -> Result<Self, ()>
        where
            Self: Sized,
        {
            todo!()
        }
    }

    fn create_sms() -> (
        Sender<SenderMessage<TestMessage>>,
        ServerMessageSender<TestMessage>,
    ) {
        let (sdr_sender, rcv_sender) = mpsc::channel::<SenderMessage<TestMessage>>();

        let sms = ServerMessageSender::<TestMessage>::new(
            rcv_sender,
            crate::server::ServerStatus::Inactive,
        );

        (sdr_sender, sms)
    }

    #[test]
    fn server_sender_new() {
        let (_sdr_sender, sms) = create_sms();
        assert_eq!(sms.server_status, ServerStatus::Inactive);
        assert!(sms.sdr_worker.is_none());
    }

    /// Update and assert new status
    fn update_assert_status(
        sdr: &mut Sender<SenderMessage<TestMessage>>,
        sms: &mut ServerMessageSender<TestMessage>,
        status: ServerStatus,
    ) {
        sdr.send(SenderMessage::Status(status)).unwrap();

        // Wait for message
        thread::sleep(Duration::from_millis(10));

        sms.update().unwrap();
        assert_eq!(sms.server_status, status);
    }

    #[test]
    fn server_sender_update_status() {
        let (mut sdr_sender, mut sms) = create_sms();
        update_assert_status(&mut sdr_sender, &mut sms, ServerStatus::Active);
        update_assert_status(&mut sdr_sender, &mut sms, ServerStatus::Ending);
        update_assert_status(&mut sdr_sender, &mut sms, ServerStatus::Inactive);
        update_assert_status(&mut sdr_sender, &mut sms, ServerStatus::Paused);
        update_assert_status(&mut sdr_sender, &mut sms, ServerStatus::Starting);
    }

    #[test]
    fn server_sender_update_reference() {
        let (sdr_sender, mut sms) = create_sms();
        let (sdr_worker, _rcv_worker) = mpsc::channel::<WorkerMessage<TestMessage>>();

        sdr_sender
            .send(SenderMessage::Reference(sdr_worker))
            .unwrap();

        // Wait for message
        thread::sleep(Duration::from_millis(10));

        sms.update().unwrap();
        assert!(sms.sdr_worker.is_some());
    }

    #[test]
    fn server_sender_update_ping() {
        let (sdr_sender, mut sms) = create_sms();
        sdr_sender.send(SenderMessage::Ping).unwrap();

        // Wait for message
        thread::sleep(Duration::from_millis(10));
        sms.update().unwrap();
    }

    #[test]
    fn server_sender_send_ok() {
        let (sdr_sender, mut sms) = create_sms();
        let (sdr_worker, rcv_worker) = mpsc::channel::<WorkerMessage<TestMessage>>();

        sdr_sender
            .send(SenderMessage::Reference(sdr_worker))
            .unwrap();
        sdr_sender
            .send(SenderMessage::Status(ServerStatus::Active))
            .unwrap();

        thread::sleep(Duration::from_millis(10)); // Wait for message
        sms.update().unwrap();

        sms.send(32, TestMessage {}).unwrap();

        match rcv_worker.recv_timeout(Duration::from_millis(100)) {
            Ok(message) => match message {
                WorkerMessage::Send(outgoing_message) => {
                    assert_eq!(outgoing_message.destinations[0], 32);
                }
                _ => panic!("Wrong message sent!"),
            },
            Err(_) => panic!("Shouldn't be Err()!"),
        }
    }

    #[test]
    fn server_sender_send_vec_ok() {
        let (sdr_sender, mut sms) = create_sms();
        let (sdr_worker, rcv_worker) = mpsc::channel::<WorkerMessage<TestMessage>>();

        sdr_sender
            .send(SenderMessage::Reference(sdr_worker))
            .unwrap();
        sdr_sender
            .send(SenderMessage::Status(ServerStatus::Active))
            .unwrap();

        thread::sleep(Duration::from_millis(10)); // Wait for message
        sms.update().unwrap();

        let client_vec: Vec<ClientId> = vec![1, 2, 3, 4, 5, 6];
        sms.send_vec(&client_vec, TestMessage {}).unwrap();

        match rcv_worker.recv_timeout(Duration::from_millis(100)) {
            Ok(message) => match message {
                WorkerMessage::Send(outgoing_message) => {
                    assert_eq!(outgoing_message.destinations.len(), client_vec.len());
                    assert_eq!(outgoing_message.destinations, client_vec);
                }
                _ => panic!("Wrong message sent!"),
            },
            Err(_) => panic!("Shouldn't be Err()!"),
        }
    }

    #[test]
    fn server_sender_send_error_inactive() {
        let (_sdr_sender, mut sms) = create_sms();

        match sms.send(0, TestMessage {}) {
            Ok(_) => panic!("Shouldn't be Ok()!"),
            Err(err) => assert_eq!(err, ErrorSender::Inactive),
        }
    }

    #[test]
    fn server_sender_send_error_paused() {
        let (sdr_sender, mut sms) = create_sms();
        let (sdr_worker, _rcv_worker) = mpsc::channel::<WorkerMessage<TestMessage>>();

        sdr_sender
            .send(SenderMessage::Reference(sdr_worker))
            .unwrap();
        sdr_sender
            .send(SenderMessage::Status(ServerStatus::Paused))
            .unwrap();

        thread::sleep(Duration::from_millis(10)); // Wait for message

        match sms.send(0, TestMessage {}) {
            Ok(_) => panic!("Shouldn't be Ok()!"),
            Err(err) => assert_eq!(err, ErrorSender::Paused),
        }
    }

    #[test]
    fn server_sender_send_error_disconnected() {
        let mut _sms: Option<ServerMessageSender<TestMessage>> = None;

        {
            let (sdr_sender, rcv_sender) = mpsc::channel::<SenderMessage<TestMessage>>();
            let (sdr_worker, _rcv_worker) = mpsc::channel::<WorkerMessage<TestMessage>>();

            _sms = Some(ServerMessageSender::<TestMessage>::new(
                rcv_sender,
                crate::server::ServerStatus::Inactive,
            ));

            sdr_sender
                .send(SenderMessage::Reference(sdr_worker))
                .unwrap();
            sdr_sender
                .send(SenderMessage::Status(ServerStatus::Active))
                .unwrap();
        } // This will drop the sdr_sender

        match _sms.unwrap().send(0, TestMessage {}) {
            Ok(_) => panic!("Shouldn't be Ok()!"),
            Err(err) => assert_eq!(err, ErrorSender::Disconnected),
        }
    }

    #[test]
    fn server_sender_send_vec_error_inactive() {
        let (_sdr_sender, mut sms) = create_sms();

        let destinations: Vec<ClientId> = vec![1, 2, 3, 4, 5, 6];
        match sms.send_vec(&destinations, TestMessage {}) {
            Ok(_) => panic!("Shouldn't be Ok()!"),
            Err(err) => assert_eq!(err, ErrorSender::Inactive),
        }
    }

    #[test]
    fn server_sender_send_vec_error_paused() {
        let (sdr_sender, mut sms) = create_sms();
        let (sdr_worker, _rcv_worker) = mpsc::channel::<WorkerMessage<TestMessage>>();

        sdr_sender
            .send(SenderMessage::Reference(sdr_worker))
            .unwrap();
        sdr_sender
            .send(SenderMessage::Status(ServerStatus::Paused))
            .unwrap();

        thread::sleep(Duration::from_millis(10)); // Wait for message

        let destinations: Vec<ClientId> = vec![1, 2, 3, 4, 5, 6];
        match sms.send_vec(&destinations, TestMessage {}) {
            Ok(_) => panic!("Shouldn't be Ok()!"),
            Err(err) => assert_eq!(err, ErrorSender::Paused),
        }
    }

    #[test]
    fn server_sender_send_vec_error_disconnected() {
        let mut _sms: Option<ServerMessageSender<TestMessage>> = None;

        {
            let (sdr_sender, rcv_sender) = mpsc::channel::<SenderMessage<TestMessage>>();
            let (sdr_worker, _rcv_worker) = mpsc::channel::<WorkerMessage<TestMessage>>();

            _sms = Some(ServerMessageSender::<TestMessage>::new(
                rcv_sender,
                crate::server::ServerStatus::Inactive,
            ));

            sdr_sender
                .send(SenderMessage::Reference(sdr_worker))
                .unwrap();
            sdr_sender
                .send(SenderMessage::Status(ServerStatus::Active))
                .unwrap();
        } // This will drop the sdr_sender

        let destinations: Vec<ClientId> = vec![1, 2, 3, 4, 5, 6];
        match _sms.unwrap().send_vec(&destinations, TestMessage {}) {
            Ok(_) => panic!("Shouldn't be Ok()!"),
            Err(err) => assert_eq!(err, ErrorSender::Disconnected),
        }
    }

    #[test]
    fn server_sender_send_vec_error_no_destination() {
        let (sdr_sender, mut sms) = create_sms();
        let (sdr_worker, _rcv_worker) = mpsc::channel::<WorkerMessage<TestMessage>>();

        sdr_sender
            .send(SenderMessage::Reference(sdr_worker))
            .unwrap();
        sdr_sender
            .send(SenderMessage::Status(ServerStatus::Active))
            .unwrap();

        thread::sleep(Duration::from_millis(10)); // Wait for message

        let destinations: Vec<ClientId> = Vec::new();
        match sms.send_vec(&destinations, TestMessage {}) {
            Ok(_) => panic!("Shouldn't be Ok()!"),
            Err(err) => assert_eq!(err, ErrorSender::NoDestination),
        }
    }
}
