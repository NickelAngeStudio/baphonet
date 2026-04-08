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
    client::{error::ErrorTransceiver, message::WorkerMessage},
};

/// The client dispatcher can send message to server.
///
/// Multiple dispatcher can be created and shared among
/// threads.
pub struct Transmitter<OUT: Message + Send> {
    /// Clone of Sender channel for worker message
    pub(crate) sdr_worker: Sender<WorkerMessage<OUT>>,
}

impl<OUT: Message + Send + 'static> Clone for Transmitter<OUT> {
    fn clone(&self) -> Self {
        Self {
            sdr_worker: self.sdr_worker.clone(),
        }
    }
}

impl<OUT: Message + Send> Transmitter<OUT> {
    /// Create a new instance of [`Dispatcher`] with a message receiver.
    pub(crate) fn new(sdr_worker: Sender<WorkerMessage<OUT>>) -> Transmitter<OUT> {
        Transmitter { sdr_worker }
    }

    /// Dispatch outgoing message to server.
    ///
    /// Returns :
    /// - [`Result`]:
    ///     - Ok(()) if message was sent.
    ///     - Err([`ErrorTransceiver::ChannelDisconnected`]) if client was dropped.
    pub fn send(&self, message: OUT) -> Result<(), ErrorTransceiver> {
        match self.sdr_worker.send(WorkerMessage::Send(message)) {
            Ok(_) => Ok(()),
            Err(_) => Err(ErrorTransceiver::ChannelDisconnected),
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
        client::{message::WorkerMessage, transmitter::Transmitter},
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
    fn transmitter_send_ok() {
        let (sdr_worker, rcv_worker) = mpsc::channel::<WorkerMessage<TestMessage>>();
        let transmitter = Transmitter::new(sdr_worker);

        transmitter.send(TestMessage {}).unwrap();

        match rcv_worker.recv_timeout(Duration::from_millis(100)) {
            Ok(message) => match message {
                WorkerMessage::Send(_) => {}
                _ => panic!("Wrong message sent!"),
            },
            Err(_) => panic!("Shouldn't be Err()!"),
        }
    }
}
