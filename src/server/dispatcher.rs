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

use std::sync::mpsc::Sender;

use crate::{
    Message,
    server::{
        ClientId,
        error::ErrorDispatcher,
        message::{OutgoingMessage, WorkerActiveMessage},
    },
};

/// The server dispatcher can send message to client
/// and can be shared to multiple threads.
#[derive(Debug, Clone)]
pub struct Dispatcher<OUT: Message + Send + 'static> {
    /// Clone of Sender channel for worker message
    sdr_worker: Sender<WorkerActiveMessage<OUT>>,
}

impl<OUT: Message + Send + 'static> Dispatcher<OUT> {
    /// Create a new instance of [`Dispatcher`] with a message receiver.
    pub(crate) fn new(sdr_worker: Sender<WorkerActiveMessage<OUT>>) -> Dispatcher<OUT> {
        Dispatcher { sdr_worker }
    }

    /// Send message to one connected client
    ///
    /// Returns :
    /// - [`Result`]:
    ///     - Ok(()) if message was sent to thread.
    ///     - Err([`ErrorDispatcher::ChannelDisconnected`]) if server was dropped.
    pub fn send(&self, client_id: ClientId, message: OUT) -> Result<(), ErrorDispatcher> {
        match self
            .sdr_worker
            .send(WorkerActiveMessage::Send(OutgoingMessage::<OUT>::new(
                client_id, message,
            ))) {
            Ok(_) => Ok(()),
            Err(_) => Err(ErrorDispatcher::ChannelDisconnected),
        }
    }

    /// Send message to multiple client with a vector of [`ClientId`].
    ///
    /// Returns :
    /// - [`Result`]:
    ///     - Ok(()) if message was sent to thread.
    ///     - Err([`ErrorSender::NoDestination`]) if no destination specified
    ///     - Err([`ErrorDispatcher::ChannelDisconnected`]) if server was dropped.
    pub fn send_vec(
        &self,
        destinations: &Vec<ClientId>,
        message: OUT,
    ) -> Result<(), ErrorDispatcher> {
        if destinations.len() > 0 {
            match self
                .sdr_worker
                .send(WorkerActiveMessage::Send(OutgoingMessage::<OUT>::new_vec(
                    &destinations,
                    message,
                ))) {
                Ok(_) => Ok(()), // Message was sent
                Err(_) => Err(ErrorDispatcher::ChannelDisconnected),
            }
        } else {
            Err(ErrorDispatcher::NoDestination)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::mpsc::{self},
        time::Duration,
    };

    use crate::{
        Message,
        server::{
            ClientId, dispatcher::Dispatcher, error::ErrorDispatcher, message::WorkerActiveMessage,
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

    #[test]
    fn dispatcher_send_ok() {
        let (sdr_worker, rcv_worker) = mpsc::channel::<WorkerActiveMessage<TestMessage>>();
        let dispatcher = Dispatcher::new(sdr_worker);

        dispatcher.send(32, TestMessage {}).unwrap();

        match rcv_worker.recv_timeout(Duration::from_millis(100)) {
            Ok(message) => match message {
                WorkerActiveMessage::Send(outgoing_message) => {
                    assert_eq!(outgoing_message.destinations[0], 32);
                }
                _ => panic!("Wrong message sent!"),
            },
            Err(_) => panic!("Shouldn't be Err()!"),
        }
    }

    #[test]
    fn dispatcher_send_vec_ok() {
        let (sdr_worker, rcv_worker) = mpsc::channel::<WorkerActiveMessage<TestMessage>>();
        let dispatcher = Dispatcher::new(sdr_worker);

        let client_vec: Vec<ClientId> = vec![1, 2, 3, 4, 5, 6];
        dispatcher.send_vec(&client_vec, TestMessage {}).unwrap();

        match rcv_worker.recv_timeout(Duration::from_millis(100)) {
            Ok(message) => match message {
                WorkerActiveMessage::Send(outgoing_message) => {
                    assert_eq!(outgoing_message.destinations.len(), client_vec.len());
                    assert_eq!(outgoing_message.destinations, client_vec);
                }
                _ => panic!("Wrong message sent!"),
            },
            Err(_) => panic!("Shouldn't be Err()!"),
        }
    }

    #[test]
    fn dispatcher_send_vec_error_no_destination() {
        let (sdr_worker, _rcv_worker) = mpsc::channel::<WorkerActiveMessage<TestMessage>>();
        let dispatcher = Dispatcher::new(sdr_worker);

        let destinations: Vec<ClientId> = Vec::new();
        match dispatcher.send_vec(&destinations, TestMessage {}) {
            Ok(_) => panic!("Shouldn't be Ok()!"),
            Err(err) => assert_eq!(err, ErrorDispatcher::NoDestination),
        }
    }
}
