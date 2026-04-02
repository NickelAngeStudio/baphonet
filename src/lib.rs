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

/// Bytes length of the size of messages.
pub(crate) const SIZE_OF_MESSAGE_SIZE: usize = size_of::<u16>();

/// Maximum message size is 65535 bytes (64ko).
///
/// Message bigger than that should be cut in smaller message.
pub const MAXIMUM_MESSAGE_SIZE: usize = u16::MAX as usize;

pub mod client;
pub mod server;

/// Message that are sent between server and client must implement this trait.
///
/// ```
///
/// ```
/// See `/tests/shared/message.rs` integration tests for usage with Tampon crate.
pub trait Message {
    /// Serialize the message into a provided [[u8]] buffer.
    ///
    /// # Returns
    /// - [`Result`]
    ///     - Ok([`usize`]) which contains the size of bytes serialized.
    ///     - Err(()) if any error happened while serializing (like a small buffer, etc...)
    ///
    /// # Panic
    /// Implementation could [`panic!`] if buffer length is too small.
    /// <br>Use buffer length of [`MAXIMUM_MESSAGE_SIZE`] before serializing and return Err(())
    /// if too small.
    ///
    /// Crate Tampon [`bytes_size`](https://docs.rs/tampon/latest/tampon/macro.bytes_size.html) can
    /// verify data length to compare to buffer.
    fn serialize(&self, buffer: &mut [u8]) -> Result<usize, ()>;

    /// Deserialize the message from a provided [[u8]] buffer.
    ///
    /// # Panic
    /// Implementation could [`panic!`] if buffer is incomplete and/
    /// or corrupt. <br>Verify buffer integrity and return Err(())
    /// accordingly.
    ///
    /// Crate Tampon [`deserialize_size`](https://docs.rs/tampon/latest/tampon/macro.deserialize_size.html)
    /// can verify buffer integrity.
    fn deserialize(buffer: &[u8]) -> Result<Self, ()>
    where
        Self: Sized;
}
