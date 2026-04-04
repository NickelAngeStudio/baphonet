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
    client::{
        error::ErrorDispatcher,
        message::{DispatcherMessage, WorkerMessage},
        status::ClientStatus,
    },
};

/// The client dispatcher can send message to server.
///
/// Multiple dispatcher can be created and shared among
/// threads.
pub struct Dispatcher<OUT: Message + Send + 'static> {
    /// Unique receiver channel for dispatcher
    rcv_dispatcher: Receiver<DispatcherMessage<OUT>>,

    /// Clone of Sender channel for worker message
    pub(crate) sdr_worker: Option<Sender<WorkerMessage<OUT>>>,

    /// Current client status
    client_status: ClientStatus,
}

impl<OUT: Message + Send + 'static> Dispatcher<OUT> {
    /// Create a new instance of [`Dispatcher`] with a message receiver.
    pub(crate) fn new(
        rcv_dispatcher: Receiver<DispatcherMessage<OUT>>,
        client_status: ClientStatus,
    ) -> Dispatcher<OUT> {
        Dispatcher {
            rcv_dispatcher: rcv_dispatcher,
            sdr_worker: None,
            client_status,
        }
    }

    /// Dispatch outgoing message to server.
    ///
    /// Returns :
    /// - [`Result`]:
    ///     - Ok(()) if message was sent.
    ///     - Err([`ErrorDispatcher::Disconnected`]) if client is not connected.
    ///     - Err([`ErrorDispatcher::ChannelDisconnected`]) if client was dropped.
    pub fn send(&mut self, message: OUT) -> Result<(), ErrorDispatcher> {
        match self.update() {
            Ok(_) => match self.client_status {
                ClientStatus::Connected => match self.sdr_worker.as_mut() {
                    Some(channel) => {
                        match channel.send(WorkerMessage::Send(message)) {
                            Ok(_) => Ok(()), // Message was sent
                            Err(_) => {
                                // Remove channel
                                self.sdr_worker = None;
                                Err(ErrorDispatcher::Disconnected)
                            }
                        }
                    }
                    None => Err(ErrorDispatcher::Disconnected),
                },
                _ => Err(ErrorDispatcher::Disconnected),
            },
            Err(err) => Err(err),
        }
    }

    /// Receive message from client to update dispatcher.
    ///
    /// # Returns
    /// - [`Result`]
    ///     - Ok(()) if updated with success.
    ///     - Err([`ErrorDispatcher::Disconnected`]) if client was dropped.
    #[inline]
    fn update(&mut self) -> Result<(), ErrorDispatcher> {
        'update: loop {
            match self.rcv_dispatcher.try_recv() {
                Ok(message) => match message {
                    DispatcherMessage::Status(client_status) => {
                        match client_status {
                            // Remove sender if inactive
                            ClientStatus::Disconnected | ClientStatus::Disconnecting => {
                                self.sdr_worker = None
                            }
                            _ => {}
                        }
                        self.client_status = client_status
                    }
                    DispatcherMessage::Reference(sender) => self.sdr_worker = Some(sender),
                    _ => {}
                },
                Err(err) => match err {
                    TryRecvError::Empty => break 'update,
                    TryRecvError::Disconnected => return Err(ErrorDispatcher::ChannelDisconnected),
                },
            }
        }

        Ok(())
    }

    /// Returns the status of the dispatcher.
    pub fn status(&mut self) -> ClientStatus {
        match self.update() {
            Ok(_) => {}
            Err(_) => {}
        }

        self.client_status
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
        client::{
            dispatcher::Dispatcher,
            error::ErrorDispatcher,
            message::{DispatcherMessage, WorkerMessage},
            status::ClientStatus,
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
        Sender<DispatcherMessage<TestMessage>>,
        Dispatcher<TestMessage>,
    ) {
        let (sdr_sender, rcv_dispatcher) = mpsc::channel::<DispatcherMessage<TestMessage>>();

        let sms = Dispatcher::<TestMessage>::new(rcv_dispatcher, ClientStatus::Disconnected);

        (sdr_sender, sms)
    }

    #[test]
    fn dispatcher_new() {
        let (_sdr_sender, sms) = create_sms();
        assert_eq!(sms.client_status, ClientStatus::Disconnected);
        assert!(sms.sdr_worker.is_none());
    }

    /// Update and assert new status
    fn update_assert_status(
        sdr: &mut Sender<DispatcherMessage<TestMessage>>,
        sms: &mut Dispatcher<TestMessage>,
        status: ClientStatus,
    ) {
        sdr.send(DispatcherMessage::Status(status)).unwrap();

        // Wait for message
        thread::sleep(Duration::from_millis(10));

        sms.update().unwrap();
        assert_eq!(sms.client_status, status);
    }

    #[test]
    fn dispatcher_update_status() {
        let (mut sdr_sender, mut sms) = create_sms();
        update_assert_status(&mut sdr_sender, &mut sms, ClientStatus::Connected);
        update_assert_status(&mut sdr_sender, &mut sms, ClientStatus::Connecting);
        update_assert_status(&mut sdr_sender, &mut sms, ClientStatus::Disconnected);
        update_assert_status(&mut sdr_sender, &mut sms, ClientStatus::Disconnecting);
    }

    #[test]
    fn dispatcher_update_reference() {
        let (sdr_sender, mut sms) = create_sms();
        let (sdr_worker, _rcv_worker) = mpsc::channel::<WorkerMessage<TestMessage>>();

        sdr_sender
            .send(DispatcherMessage::Reference(sdr_worker))
            .unwrap();

        // Wait for message
        thread::sleep(Duration::from_millis(10));

        sms.update().unwrap();
        assert!(sms.sdr_worker.is_some());
    }

    #[test]
    fn dispatcher_update_ping() {
        let (sdr_sender, mut sms) = create_sms();
        sdr_sender.send(DispatcherMessage::Ping).unwrap();

        // Wait for message
        thread::sleep(Duration::from_millis(10));
        sms.update().unwrap();
    }

    #[test]
    fn dispatcher_send_ok() {
        let (sdr_sender, mut sms) = create_sms();
        let (sdr_worker, rcv_worker) = mpsc::channel::<WorkerMessage<TestMessage>>();

        sdr_sender
            .send(DispatcherMessage::Reference(sdr_worker))
            .unwrap();
        sdr_sender
            .send(DispatcherMessage::Status(ClientStatus::Connected))
            .unwrap();

        thread::sleep(Duration::from_millis(10)); // Wait for message
        sms.update().unwrap();

        sms.send(TestMessage {}).unwrap();

        match rcv_worker.recv_timeout(Duration::from_millis(100)) {
            Ok(message) => match message {
                WorkerMessage::Send(_) => {}
                _ => panic!("Wrong message sent!"),
            },
            Err(_) => panic!("Shouldn't be Err()!"),
        }
    }

    #[test]
    fn dispatcher_send_error_disconnected() {
        let (_sdr_sender, mut sms) = create_sms();

        match sms.send(TestMessage {}) {
            Ok(_) => panic!("Shouldn't be Ok()!"),
            Err(err) => assert_eq!(err, ErrorDispatcher::Disconnected),
        }
    }

    #[test]
    fn dispatcher_send_error_channel_disconnected() {
        let mut _sms: Option<Dispatcher<TestMessage>> = None;

        {
            let (sdr_sender, rcv_dispatcher) = mpsc::channel::<DispatcherMessage<TestMessage>>();
            let (sdr_worker, _rcv_worker) = mpsc::channel::<WorkerMessage<TestMessage>>();

            _sms = Some(Dispatcher::<TestMessage>::new(
                rcv_dispatcher,
                ClientStatus::Disconnected,
            ));

            sdr_sender
                .send(DispatcherMessage::Reference(sdr_worker))
                .unwrap();
            sdr_sender
                .send(DispatcherMessage::Status(ClientStatus::Connected))
                .unwrap();
        } // This will drop the sdr_sender

        match _sms.unwrap().send(TestMessage {}) {
            Ok(_) => panic!("Shouldn't be Ok()!"),
            Err(err) => assert_eq!(err, ErrorDispatcher::ChannelDisconnected),
        }
    }
}
