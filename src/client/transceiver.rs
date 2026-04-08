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

use std::{
    sync::mpsc::{Receiver, RecvTimeoutError, Sender},
    time::Duration,
};

use crate::{
    Message,
    client::{error::ErrorTransceiver, message::WorkerMessage, transmitter::Transmitter},
};

/// Client transceiver used to receive and transmit messages from Server.
///
/// The Transmitter can be clone and shared with multiple thread
/// while the receiver can only be owned by one thread.
pub struct Transceiver<IN: Message + Send + 'static, OUT: Message + Send + 'static> {
    rcv_incoming: Receiver<IN>,
    transmitter: Transmitter<OUT>,
}

impl<IN: Message + Send + 'static, OUT: Message + Send + 'static> Transceiver<IN, OUT> {
    /// Create a new client [`Transceiver`].
    pub(crate) fn new(
        rcv_incoming: Receiver<IN>,
        sdr_worker: Sender<WorkerMessage<OUT>>,
    ) -> Transceiver<IN, OUT> {
        let transmitter = Transmitter::new(sdr_worker);
        Transceiver {
            rcv_incoming,
            transmitter,
        }
    }

    /// Receive an message from server without blocking the thread.
    ///
    /// # Returns
    /// - [`Option`]
    ///     - Some(`IN`) if any message
    ///     - None if no message.
    ///
    /// # Notes
    /// Will return [`None`] if channel is disconnected.
    pub fn receive(&self) -> Option<IN> {
        match self.rcv_incoming.try_recv() {
            Ok(message) => Some(message),
            Err(_) => None,
        }
    }

    /// Block the thread until it receive a message.
    ///
    /// # Returns
    /// - [`Result`]
    ///     - Ok([`IncomingMessage`]) if any message
    ///     - Err([`ErrorTransceiver::ChannelDisconnected`]) if channel is disconnected.
    pub fn receive_wait(&self) -> Result<IN, ErrorTransceiver> {
        match self.rcv_incoming.recv() {
            Ok(message) => Ok(message),
            Err(_) => Err(ErrorTransceiver::ChannelDisconnected),
        }
    }

    /// Block the thread until it receive a message
    /// or the duration expired.
    ///
    /// # Returns
    /// - [`Result`]
    ///     - Ok([`IncomingMessage`]) if any message
    ///     - Err([`ErrorTransceiver::Timeout`]) if channel timedout.
    ///     - Err([`ErrorTransceiver::ChannelDisconnected`]) if channel is disconnected.
    pub fn receive_timeout(&self, duration: Duration) -> Result<IN, ErrorTransceiver> {
        match self.rcv_incoming.recv_timeout(duration) {
            Ok(message) => Ok(message),
            Err(err) => match err {
                RecvTimeoutError::Timeout => Err(ErrorTransceiver::Timeout),
                RecvTimeoutError::Disconnected => Err(ErrorTransceiver::ChannelDisconnected),
            },
        }
    }

    /// Send message to server.
    ///
    /// Returns :
    /// - [`Result`]:
    ///     - Ok(()) if message was sent.
    ///     - Err([`ErrorTransceiver::ChannelDisconnected`]) if client was dropped.
    pub fn send(&self, message: OUT) -> Result<(), ErrorTransceiver> {
        self.transmitter.send(message)
    }

    /// Get a shareable reference of transmitter.
    ///
    /// Transmitter can be cloned and shared with other threads
    /// to send message to server.
    pub fn transmitter(&self) -> Transmitter<OUT> {
        self.transmitter.clone()
    }
}
