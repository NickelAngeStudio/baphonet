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

/// Status of a task
pub(crate) enum TaskStatus {
    /// Task is ready to be executed
    Ready,

    /// Task is currently in progress
    InProgress,
}

/// Task that are executed by workers.
pub(crate) struct Tasks {
    /// Incoming connection task
    pub incoming: TaskStatus,

    /// Reception of clients message.
    pub reception: Vec<Option<TaskStatus>>,
}

impl Tasks {
    /// Create a new tasks registry from maximum client.
    pub fn new(maximum_client: usize) -> Tasks {
        let mut reception = Vec::<Option<TaskStatus>>::with_capacity(maximum_client);
        reception.resize_with(maximum_client, || None);

        Tasks {
            incoming: TaskStatus::Ready,
            reception,
        }
    }
}
