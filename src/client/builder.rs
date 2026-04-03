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

use crate::{
    Message,
    client::{
        Client, DEFAULT_OUTGOING_SIZE, DEFAULT_POOL_RATE_PER_SECOND, ErrorClient,
        MAXIMUM_OUTGOING_SIZE, MAXIMUM_POOL_RATE_PER_SECOND, MINIMUM_OUTGOING_SIZE,
        MINIMUM_POOL_RATE_PER_SECOND, status::ClientStatus,
    },
};

/// Builder helper used to create client.
pub struct ClientBuilder {
    /// Pool rate of the client
    pool_rate: u64,

    /// Maximum size of outgoing message
    outgoing_max_size: usize,
}

impl ClientBuilder {
    /// Create a new [`ClientBuilder`] instance.
    pub fn new() -> ClientBuilder {
        ClientBuilder {
            pool_rate: DEFAULT_POOL_RATE_PER_SECOND,
            outgoing_max_size: DEFAULT_OUTGOING_SIZE,
        }
    }

    /// Default pool rate of the client per second.
    /// Each pool look incoming server messages.
    ///
    /// Higher pool rate will reduce message larency and consume more
    /// resources.
    ///
    /// Value must be between [`MINIMUM_POOL_RATE_PER_SECOND`] and
    /// [`MAXIMUM_POOL_RATE_PER_SECOND`],
    ///
    /// Default is [`DEFAULT_POOL_RATE_PER_SECOND`].
    pub fn pool_rate(mut self, pool_rate: u64) -> ClientBuilder {
        self.pool_rate = pool_rate;
        self
    }

    /// Maximum size accepted to send to server.
    ///
    /// It is used to prevent malicious client from slowing
    /// the server with long message.
    ///
    /// Value must be between [`MINIMUM_OUTGOING_SIZE`] and
    /// [`MAXIMUM_OUTGOING_SIZE`].
    ///
    /// Default is [`DEFAULT_OUTGOING_SIZE`].
    pub fn outgoing_max_size(mut self, outgoing_max_size: usize) -> ClientBuilder {
        self.outgoing_max_size = outgoing_max_size;
        self
    }

    pub fn build<IN: Message + Send + 'static, OUT: Message + Send + 'static>(
        &self,
    ) -> Result<Client<IN, OUT>, ErrorClient> {
        if self.pool_rate < MINIMUM_POOL_RATE_PER_SECOND {
            return Err(ErrorClient::PoolRateBelowMinimum);
        }
        if self.pool_rate > MAXIMUM_POOL_RATE_PER_SECOND {
            return Err(ErrorClient::PoolRateAboveMaximum);
        }
        if self.outgoing_max_size < MINIMUM_OUTGOING_SIZE {
            return Err(ErrorClient::OutgoingMessageSizeBelowMinimum);
        }
        if self.outgoing_max_size > MAXIMUM_OUTGOING_SIZE {
            return Err(ErrorClient::OutgoingMessageSizeAboveMaximum);
        }

        Ok(Client {
            channels: None,
            worker_handle: None,
            status: ClientStatus::Disconnected,
            pool_rate: self.pool_rate,
            outgoing_max_size: self.outgoing_max_size,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        Message,
        client::{
            Client, DEFAULT_OUTGOING_SIZE, DEFAULT_POOL_RATE_PER_SECOND, ErrorClient,
            MAXIMUM_OUTGOING_SIZE, MAXIMUM_POOL_RATE_PER_SECOND, MINIMUM_OUTGOING_SIZE,
            MINIMUM_POOL_RATE_PER_SECOND, builder::ClientBuilder,
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

    /// Assert a client parameters from a builder
    fn assert_client_builder<IN: Message + Send + 'static, OUT: Message + Send + 'static>(
        client: &Client<IN, OUT>,
        builder: &ClientBuilder,
    ) {
        assert_eq!(client.pool_rate, builder.pool_rate);
        assert_eq!(client.outgoing_max_size, builder.outgoing_max_size);
    }

    #[test]
    fn client_builder_new_default() {
        let builder = ClientBuilder::new();
        assert_eq!(builder.pool_rate, DEFAULT_POOL_RATE_PER_SECOND);
        assert_eq!(builder.outgoing_max_size, DEFAULT_OUTGOING_SIZE);
        let client = builder.build::<TestMessage, TestMessage>().unwrap();
        assert_client_builder(&client, &builder);
    }

    #[test]
    fn client_builder_new_modified() {
        let pool_rate = MINIMUM_POOL_RATE_PER_SECOND + 1;
        let outgoing_max_size = MINIMUM_OUTGOING_SIZE + 1;
        let builder = ClientBuilder::new()
            .pool_rate(pool_rate)
            .outgoing_max_size(outgoing_max_size);
        assert_eq!(builder.pool_rate, pool_rate);
        assert_eq!(builder.outgoing_max_size, outgoing_max_size);
        let client = builder.build::<TestMessage, TestMessage>().unwrap();
        assert_client_builder(&client, &builder);
    }

    #[test]
    fn client_builder_err_poolrate_below_min() {
        let builder = ClientBuilder::new().pool_rate(MINIMUM_POOL_RATE_PER_SECOND - 1);
        match builder.build::<TestMessage, TestMessage>() {
            Ok(_) => panic!("Shouldn't be Ok()!"),
            Err(err) => assert_eq!(err, ErrorClient::PoolRateBelowMinimum),
        }
    }

    #[test]
    fn client_builder_err_poolrate_above_max() {
        let builder = ClientBuilder::new().pool_rate(MAXIMUM_POOL_RATE_PER_SECOND + 1);
        match builder.build::<TestMessage, TestMessage>() {
            Ok(_) => panic!("Shouldn't be Ok()!"),
            Err(err) => assert_eq!(err, ErrorClient::PoolRateAboveMaximum),
        }
    }

    #[test]
    fn client_builder_err_outgoing_max_below_min() {
        let builder = ClientBuilder::new().outgoing_max_size(MINIMUM_OUTGOING_SIZE - 1);
        match builder.build::<TestMessage, TestMessage>() {
            Ok(_) => panic!("Shouldn't be Ok()!"),
            Err(err) => assert_eq!(err, ErrorClient::OutgoingMessageSizeBelowMinimum),
        }
    }

    #[test]
    fn client_builder_err_outgoing_max_above_max() {
        let builder = ClientBuilder::new().outgoing_max_size(MAXIMUM_OUTGOING_SIZE + 1);
        match builder.build::<TestMessage, TestMessage>() {
            Ok(_) => panic!("Shouldn't be Ok()!"),
            Err(err) => assert_eq!(err, ErrorClient::OutgoingMessageSizeAboveMaximum),
        }
    }
}
