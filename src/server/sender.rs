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
        ClientId, ErrorServer, ServerStatus,
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
    pub fn send_vec(&mut self, client_id: ClientId) {
        todo!()
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
    use std::sync::mpsc::{self, Sender};

    use crate::{
        Message,
        server::{ServerStatus, message::SenderMessage, sender::ServerMessageSender},
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

    #[test]
    fn server_sender_update_status() {
        todo!()
    }

    #[test]
    fn server_sender_update_reference() {
        todo!()
    }

    #[test]
    fn server_sender_update_ping() {
        todo!()
    }

    #[test]
    fn server_sender_send_ok() {
        todo!()
    }

    #[test]
    fn server_sender_send_vec_ok() {
        todo!()
    }

    #[test]
    fn server_sender_send_error_inactive() {
        todo!()
    }

    #[test]
    fn server_sender_send_error_paused() {
        todo!()
    }

    #[test]
    fn server_sender_send_error_disconnected() {
        todo!()
    }

    #[test]
    fn server_sender_send_vec_error_inactive() {
        todo!()
    }

    #[test]
    fn server_sender_send_vec_error_paused() {
        todo!()
    }

    #[test]
    fn server_sender_send_vec_error_disconnected() {
        todo!()
    }

    #[test]
    fn server_sender_send_vec_error_no_destination() {
        todo!()
    }
}
