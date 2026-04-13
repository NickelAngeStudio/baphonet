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

use crate::server::ClientId;

/// Error that happens to the server
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum ErrorServer {
    /// Server maximum client allowed is below [`SERVER_MINIMUM_CLIENT_CAP`](super::SERVER_MINIMUM_CLIENT_CAP).
    MaximumClientBelowMinimum,

    /// Server maximum client allowed is above [`SERVER_MAXIMUM_CLIENT_CAP`](super::SERVER_MAXIMUM_CLIENT_CAP).
    MaximumClientAboveMaximum,

    /// Server worker count is below [`SERVER_MINIMUM_WORKER_CAP`](super::SERVER_MINIMUM_WORKER_CAP).
    WorkerCountBelowMinimum,

    /// Server worker count is above maximum client allowed.
    WorkerCountAboveMaximum,

    /// Pool rate is below [`MINIMUM_POOL_RATE_PER_SECOND`](super::MINIMUM_POOL_RATE_PER_SECOND).
    PoolRateBelowMinimum,

    /// Pool rate is above [`MAXIMUM_POOL_RATE_PER_SECOND`](super::MAXIMUM_POOL_RATE_PER_SECOND).
    PoolRateAboveMaximum,

    /// Incoming message size is below [`MINIMUM_INCOMING_SIZE`].
    IncomingMessageSizeBelowMinimum,

    /// Incoming message size is above [`MAXIMUM_INCOMING_SIZE`].
    IncomingMessageSizeAboveMaximum,

    /// Outgoing message size is below [`OUTGOING_SIZE_MINIMUM`].
    OutgoingMessageSizeBelowMinimum,

    /// Outgoing message size is above [`OUTGOING_SIZE_MAXIMUM`].
    OutgoingMessageSizeAboveMaximum,

    /// Server is currently inactive
    Inactive,

    /// Server is already active
    AlreadyActive,

    /// Provided socket for start is invalid
    SocketInvalid,

    /// Provided socket address is already used by another process
    SocketAddressAlreadyUsed,

    /// Unexpected error happened
    UnexpectedError,

    /// An unhandled IO error occurred
    UnhandledIOError(std::io::ErrorKind),
}

/// Error given by the [`Dispatcher`].
#[derive(Debug, PartialEq, Clone)]
pub enum ErrorTransceiver {
    /// Channel are disconnected from the server (server dropped).
    ChannelDisconnected,

    /// Transceiver::receive_timeout() has expired.
    Timeout,

    /// Message has no destinations when using [`send_vec`].
    NoDestination,
}

/// Error given via ServerMessage::update()
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum ErrorUpdate {
    /// Client connection lost
    ConnectionLost(ClientId),

    /// Client not found
    ClientNotFound(ClientId),

    /// Outgoing message is bigger than maximum
    OutgoingMessageTooLarge,

    /// Outgoing message serialize ended in error
    OutgoingMessageSerializeError,

    /// Incoming message is too large.
    IncomingMessageTooLarge(ClientId),

    /// Incoming message deserialize ended in error
    IncomingMessageDeserializeError(ClientId),

    /// Sending a message to a client failed because TcpStreamBuffer is full and can't be emptied. (could be a busy client).
    TcpStreamBufferFull(ClientId),
}
