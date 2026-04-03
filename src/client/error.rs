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

/// Possible [`Client`](super::Client) error
#[derive(Debug, PartialEq)]
pub enum ErrorClient {
    /// Cant connect to given address. Server might be down.
    ServerNotFound,

    /// Client is already connected.
    AlreadyConnected,

    /// Socket given is invalid
    InvalidSocket,

    /// Happens when server refused connection (ie server is full).
    ConnectionRefused,

    /// Happens when client is disconnected.
    Disconnected,

    /// Unhandled IO error
    UnhandledIOError(std::io::ErrorKind),

    /// Unexpected error happened.
    UnexpectedError,

    /// Pool rate is below [`MINIMUM_POOL_RATE_PER_SECOND`](super::MINIMUM_POOL_RATE_PER_SECOND).
    PoolRateBelowMinimum,

    /// Pool rate is above [`MAXIMUM_POOL_RATE_PER_SECOND`](super::MAXIMUM_POOL_RATE_PER_SECOND).
    PoolRateAboveMaximum,

    /// Incoming message size is below [`MINIMUM_OUTGOING_SIZE`].
    OutgoingMessageSizeBelowMinimum,

    /// Incoming message size is above [`MAXIMUM_OUTGOING_SIZE`].
    OutgoingMessageSizeAboveMaximum,

    /// Error happened while trying to join worker thread
    CloseJoinError,

    /// Client took too much time closing connection.
    CloseTimeout,
}
/// Possible [`Worker`](super::Worker) error
#[derive(Debug, PartialEq)]
pub enum ErrorWorker {
    /// Outgoing message serialize failed
    OutgoingSerializeError,

    /// Outgoing message is larger than [`MAXIMUM_MESSAGE_SIZE`](super::super::MAXIMUM_MESSAGE_SIZE).
    OutgoingMessageTooLarge,

    /// Connection to server lost
    ConnectionLost,

    /// Incoming message from server is larger than [`MAXIMUM_MESSAGE_SIZE`](super::super::MAXIMUM_MESSAGE_SIZE).
    IncomingMessageTooLarge,

    /// An error occured while deserializing incoming message
    IncomingMessageError,
}
