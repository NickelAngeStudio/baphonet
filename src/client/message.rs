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

use std::net::TcpStream;

use crate::{Message, client::error::ErrorWorker};

/// Updates sent to client from worker.
pub enum ClientUpdate {
    /// Client is now connected
    Connected,

    /// An error occurred from the worker thread
    Error(ErrorWorker),

    /// Client is now disconnected
    Disconnected,

    /// Client thread ended
    Ended,
}

/// Message sent to worker from client
pub enum WorkerMessage<OUT: Message + Send> {
    /// Connect to stream
    Connect(TcpStream),

    /// Send a message to the server
    Send(OUT),

    /// Stop the worker thread
    Stop,

    /// End the worker
    End,
}
