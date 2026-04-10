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

use crate::{
    Message,
    server::{
        ErrorServer, INCOMING_MESSAGE_SIZE, MAXIMUM_CLIENT_SIZE, OUTGOING_MESSAGE_SIZE,
        POOL_RATE_PER_SECOND, SERVER_WORKER_COUNT, Server,
    },
};

/// Builder helper used to create server.
pub struct ServerBuilder {
    /// Maximum client connection allowed
    pub(crate) maximum_client: usize,

    /// Count of worker threads allowed
    pub(crate) worker_count: usize,

    /// Pool rate of the server
    pub(crate) pool_rate: u64,

    /// Maximum size of incoming message
    pub(crate) incoming_max_size: usize,

    /// Maximum size of outgoing message
    pub(crate) outgoing_max_size: usize,
}

impl ServerBuilder {
    /// Create a new [`ServerBuilder`] instance.
    pub fn new() -> ServerBuilder {
        ServerBuilder {
            maximum_client: MAXIMUM_CLIENT_SIZE.default,
            worker_count: SERVER_WORKER_COUNT.default,
            pool_rate: POOL_RATE_PER_SECOND.default,
            incoming_max_size: INCOMING_MESSAGE_SIZE.default,
            outgoing_max_size: OUTGOING_MESSAGE_SIZE.default,
        }
    }

    /// Maximum client that can connect to server.
    ///
    /// Value must be between [`MINIMUM_CLIENT`] and [`MAXIMUM_CLIENT`].
    ///
    /// Default is [`DEFAULT_MAXIMUM_CLIENT`].
    pub fn maximum_client(mut self, max_client: usize) -> ServerBuilder {
        self.maximum_client = max_client;
        self
    }

    /// Number of worker used by the server.
    ///
    /// Higher count can handle bigger workload but consume
    /// more system resources.
    ///
    /// Value must be between [`MINIMUM_WORKER`] and [`ServerBuilder::maximum_client`].
    ///
    /// Default is [`DEFAULT_WORKER_COUNT`].
    pub fn worker(mut self, worker_count: usize) -> ServerBuilder {
        self.worker_count = worker_count;
        self
    }

    /// Pool rate of the supervisor per second.
    /// Each pool look for connection and receive incoming messages.
    ///
    /// Higher pool rate will reduce message latency and consume more
    /// resources.
    ///
    /// Value must be between [`MINIMUM_POOL_RATE_PER_SECOND`] and
    /// [`MAXIMUM_POOL_RATE_PER_SECOND`],
    ///
    /// Default is [`DEFAULT_POOL_RATE_PER_SECOND`].
    pub fn pool_rate(mut self, pool_rate: u64) -> ServerBuilder {
        self.pool_rate = pool_rate;
        self
    }

    /// Maximum size accepted from incoming client message.
    ///
    /// It is used to prevent malicious client from slowing
    /// the server with long message.
    ///
    /// Value must be between [`MINIMUM_INCOMING_SIZE`] and
    /// [`MAXIMUM_INCOMING_SIZE`].
    ///
    /// Default is [`DEFAULT_INCOMING_SIZE`].
    pub fn incoming_max_size(mut self, incoming_max_size: usize) -> ServerBuilder {
        self.incoming_max_size = incoming_max_size;
        self
    }

    /// Maximum size accepted for outgoing message.
    ///
    /// Value must be between [`OUTGOING_SIZE_MINIMUM`] and
    /// [`OUTGOING_SIZE_MAXIMUM`].
    ///
    /// Default is [`OUTGOING_SIZE_DEFAULT`].
    pub fn outgoing_max_size(mut self, outgoing_max_size: usize) -> ServerBuilder {
        self.outgoing_max_size = outgoing_max_size;
        self
    }

    /// Build a [`Server`] from types that implement [`Message`] trait.
    ///
    /// # Returns
    /// - [`Result`]
    ///     - Ok([`Server`]) on success.
    ///     - Err([`ErrorServer::MaximumClientBelowMinimum`]) if maximum client is below [`SERVER_MINIMUM_CLIENT_CAP`].
    ///     - Err([`ErrorServer::MaximumClientAboveMaximum`]) if maximum  client is above [`SERVER_MAXIMUM_CLIENT_CAP`].
    ///     - Err([`ErrorServer::WorkerCountBelowMinimum`]) if worker count is below [`SERVER_MINIMUM_WORKER_CAP`].
    ///     - Err([`ErrorServer::WorkerCountAboveMaximum`]) if worker count is above `maximum_client` parameter.
    ///     - Err([`ErrorServer::PoolRateBelowMinimum`]) if pool rate is below [`MINIMUM_POOL_RATE_PER_SECOND`].
    ///     - Err([`ErrorServer::PoolRateAboveMaximum`]) if pool rate is above [`MAXIMUM_POOL_RATE_PER_SECOND`].
    ///     - Err([`ErrorServer::IncomingMessageSizeBelowMinimum`]) if incoming maximum message size is below [`MINIMUM_INCOMING_SIZE`].
    ///     - Err([`ErrorServer::IncomingMessageSizeAboveMaximum`]) if incoming maximum message size is above [`MAXIMUM_INCOMING_SIZE`].
    ///     - Err([`ErrorServer::OutgoingMessageSizeBelowMinimum`]) if outgoing maximum message size is below [`OUTGOING_SIZE_MINIMUM`].
    ///     - Err([`ErrorServer::OutgoingMessageSizeAboveMaximum`]) if outgoing maximum message size is above [`OUTGOING_SIZE_MAXIMUM`].
    pub fn build<IN: Message + Send + 'static, OUT: Message + Send + 'static>(
        &self,
    ) -> Result<Server<IN, OUT>, ErrorServer> {
        match MAXIMUM_CLIENT_SIZE.compare(&self.maximum_client) {
            crate::ConstRangeCompare::InRange => {}
            crate::ConstRangeCompare::BelowMinimum => {
                return Err(ErrorServer::MaximumClientBelowMinimum);
            }
            crate::ConstRangeCompare::AboveMaximum => {
                return Err(ErrorServer::MaximumClientAboveMaximum);
            }
        }

        match SERVER_WORKER_COUNT.compare(&self.worker_count) {
            crate::ConstRangeCompare::InRange => {}
            crate::ConstRangeCompare::BelowMinimum => {
                return Err(ErrorServer::WorkerCountBelowMinimum);
            }
            crate::ConstRangeCompare::AboveMaximum => {
                return Err(ErrorServer::WorkerCountAboveMaximum);
            }
        }

        match POOL_RATE_PER_SECOND.compare(&self.pool_rate) {
            crate::ConstRangeCompare::InRange => {}
            crate::ConstRangeCompare::BelowMinimum => {
                return Err(ErrorServer::PoolRateBelowMinimum);
            }
            crate::ConstRangeCompare::AboveMaximum => {
                return Err(ErrorServer::PoolRateAboveMaximum);
            }
        }

        match INCOMING_MESSAGE_SIZE.compare(&self.incoming_max_size) {
            crate::ConstRangeCompare::InRange => {}
            crate::ConstRangeCompare::BelowMinimum => {
                return Err(ErrorServer::IncomingMessageSizeBelowMinimum);
            }
            crate::ConstRangeCompare::AboveMaximum => {
                return Err(ErrorServer::IncomingMessageSizeAboveMaximum);
            }
        }

        match OUTGOING_MESSAGE_SIZE.compare(&self.outgoing_max_size) {
            crate::ConstRangeCompare::InRange => {}
            crate::ConstRangeCompare::BelowMinimum => {
                return Err(ErrorServer::OutgoingMessageSizeBelowMinimum);
            }
            crate::ConstRangeCompare::AboveMaximum => {
                return Err(ErrorServer::OutgoingMessageSizeAboveMaximum);
            }
        }

        Ok(Server::<IN, OUT>::build(&self))
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        Message,
        server::{
            INCOMING_MESSAGE_SIZE, MAXIMUM_CLIENT_SIZE, OUTGOING_MESSAGE_SIZE,
            POOL_RATE_PER_SECOND, SERVER_WORKER_COUNT, Server, ServerBuilder, error::ErrorServer,
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

    /// Assert a server parameters from a builder
    fn assert_server_builder<IN: Message + Send + 'static, OUT: Message + Send + 'static>(
        server: &Server<IN, OUT>,
        builder: &ServerBuilder,
    ) {
        assert_eq!(server.worker_count, builder.worker_count);
    }

    #[test]
    fn server_builder_new_default() {
        let builder = ServerBuilder::new();
        assert_eq!(builder.maximum_client, MAXIMUM_CLIENT_SIZE.default);
        assert_eq!(builder.worker_count, SERVER_WORKER_COUNT.default);
        assert_eq!(builder.pool_rate, POOL_RATE_PER_SECOND.default);
        assert_eq!(builder.incoming_max_size, INCOMING_MESSAGE_SIZE.default);
        assert_eq!(builder.outgoing_max_size, OUTGOING_MESSAGE_SIZE.default);
        let server = builder.build::<TestMessage, TestMessage>().unwrap();
        assert_server_builder(&server, &builder);
    }

    #[test]
    fn server_builder_new_modified() {
        let max_client = MAXIMUM_CLIENT_SIZE.minimum + 1;
        let worker_count = SERVER_WORKER_COUNT.minimum + 1;
        let pool_rate = POOL_RATE_PER_SECOND.minimum + 1;
        let incoming_max_size = INCOMING_MESSAGE_SIZE.minimum + 1;
        let outgoing_max_size = OUTGOING_MESSAGE_SIZE.minimum + 1;
        let builder = ServerBuilder::new()
            .maximum_client(max_client)
            .worker(worker_count)
            .pool_rate(pool_rate)
            .incoming_max_size(incoming_max_size)
            .outgoing_max_size(outgoing_max_size);

        assert_eq!(builder.maximum_client, max_client);
        assert_eq!(builder.worker_count, worker_count);
        assert_eq!(builder.pool_rate, pool_rate);
        assert_eq!(builder.incoming_max_size, incoming_max_size);
        assert_eq!(builder.outgoing_max_size, outgoing_max_size);
        let server = builder.build::<TestMessage, TestMessage>().unwrap();
        assert_server_builder(&server, &builder);
    }

    #[test]
    fn server_builder_err_client_below_min() {
        let builder = ServerBuilder::new().maximum_client(MAXIMUM_CLIENT_SIZE.minimum - 1);
        match builder.build::<TestMessage, TestMessage>() {
            Ok(_) => panic!("Shouldn't be Ok()!"),
            Err(err) => assert_eq!(err, ErrorServer::MaximumClientBelowMinimum),
        }
    }

    #[test]
    fn server_builder_err_client_above_max() {
        let builder = ServerBuilder::new().maximum_client(MAXIMUM_CLIENT_SIZE.maximum + 1);
        match builder.build::<TestMessage, TestMessage>() {
            Ok(_) => panic!("Shouldn't be Ok()!"),
            Err(err) => assert_eq!(err, ErrorServer::MaximumClientAboveMaximum),
        }
    }

    #[test]
    fn server_builder_err_worker_below_min() {
        let builder = ServerBuilder::new().worker(SERVER_WORKER_COUNT.minimum - 1);
        match builder.build::<TestMessage, TestMessage>() {
            Ok(_) => panic!("Shouldn't be Ok()!"),
            Err(err) => assert_eq!(err, ErrorServer::WorkerCountBelowMinimum),
        }
    }

    #[test]
    fn server_builder_err_worker_above_max() {
        let builder = ServerBuilder::new().worker(SERVER_WORKER_COUNT.maximum + 1);
        match builder.build::<TestMessage, TestMessage>() {
            Ok(_) => panic!("Shouldn't be Ok()!"),
            Err(err) => assert_eq!(err, ErrorServer::WorkerCountAboveMaximum),
        }
    }

    #[test]
    fn server_builder_err_poolrate_below_min() {
        let builder = ServerBuilder::new().pool_rate(POOL_RATE_PER_SECOND.minimum - 1);
        match builder.build::<TestMessage, TestMessage>() {
            Ok(_) => panic!("Shouldn't be Ok()!"),
            Err(err) => assert_eq!(err, ErrorServer::PoolRateBelowMinimum),
        }
    }

    #[test]
    fn server_builder_err_poolrate_above_max() {
        let builder = ServerBuilder::new().pool_rate(POOL_RATE_PER_SECOND.maximum + 1);
        match builder.build::<TestMessage, TestMessage>() {
            Ok(_) => panic!("Shouldn't be Ok()!"),
            Err(err) => assert_eq!(err, ErrorServer::PoolRateAboveMaximum),
        }
    }

    #[test]
    fn server_builder_err_incoming_max_below_min() {
        let builder = ServerBuilder::new().incoming_max_size(INCOMING_MESSAGE_SIZE.minimum - 1);
        match builder.build::<TestMessage, TestMessage>() {
            Ok(_) => panic!("Shouldn't be Ok()!"),
            Err(err) => assert_eq!(err, ErrorServer::IncomingMessageSizeBelowMinimum),
        }
    }

    #[test]
    fn server_builder_err_incoming_max_above_max() {
        let builder = ServerBuilder::new().incoming_max_size(INCOMING_MESSAGE_SIZE.maximum + 1);
        match builder.build::<TestMessage, TestMessage>() {
            Ok(_) => panic!("Shouldn't be Ok()!"),
            Err(err) => assert_eq!(err, ErrorServer::IncomingMessageSizeAboveMaximum),
        }
    }

    #[test]
    fn server_builder_err_outgoing_max_below_min() {
        let builder = ServerBuilder::new().outgoing_max_size(OUTGOING_MESSAGE_SIZE.minimum - 1);
        match builder.build::<TestMessage, TestMessage>() {
            Ok(_) => panic!("Shouldn't be Ok()!"),
            Err(err) => assert_eq!(err, ErrorServer::OutgoingMessageSizeBelowMinimum),
        }
    }

    #[test]
    fn server_builder_err_outgoing_max_above_max() {
        let builder = ServerBuilder::new().outgoing_max_size(OUTGOING_MESSAGE_SIZE.maximum + 1);
        match builder.build::<TestMessage, TestMessage>() {
            Ok(_) => panic!("Shouldn't be Ok()!"),
            Err(err) => assert_eq!(err, ErrorServer::OutgoingMessageSizeAboveMaximum),
        }
    }
}
