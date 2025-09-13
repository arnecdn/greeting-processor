use std::str::Utf8Error;
use std::fmt::{Debug, Formatter};
use async_trait::async_trait;
use greeting_db_api::DbError;
use greeting_db_api::greeting_command::{GreetingCommandRepository, GreetingCommandRepositoryImpl};
use log::{error, info, warn};
use opentelemetry::{global};
use opentelemetry::propagation::Extractor;
use opentelemetry::trace::{Status, TraceContextExt, Tracer};
use rdkafka::{ClientConfig, ClientContext, Message, TopicPartitionList};
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer};
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::message::{BorrowedHeaders, BorrowedMessage, Headers};
use tracing::{instrument, span, Span};
use tracing_core::Level;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use crate::{Settings};

struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        info!("Committing offsets: {:?}", result);
    }
}

// A type alias with your custom consumer can be created for convenience.
type LoggingConsumer = StreamConsumer<CustomContext>;

#[derive(Debug)]
pub struct ConsumerError {
    err_msg: String,
}

impl From<DbError> for ConsumerError {
    fn from(value: DbError) -> Self {
        ConsumerError { err_msg: value.error_message }
    }
}
impl From<Utf8Error> for ConsumerError {
    fn from(value: Utf8Error) -> Self {
        ConsumerError { err_msg: value.to_string() }
    }
}

impl From<KafkaError> for ConsumerError {
    fn from(value: KafkaError) -> Self {
        ConsumerError { err_msg: value.to_string() }
    }
}

impl From<&str> for ConsumerError {
    fn from(value: &str) -> Self {
        ConsumerError { err_msg: value.to_string() }
    }
}
pub struct KafkaConsumer {
    // config: Settings,
    topic: String,
    consumer_group: String,
    // consumer: LoggingConsumer,
    repo: Box<GreetingCommandRepositoryImpl>,
    kafka_broker: String,
}

impl Debug for KafkaConsumer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "KafkaConsumer(topic) {}", &self.topic)
    }
}

impl KafkaConsumer {
    pub async fn new(settings: Settings, greeting_repo: Box<GreetingCommandRepositoryImpl>) -> Result<Self, ConsumerError> {
        Ok(Self {
            topic: settings.kafka.topic,
            consumer_group: settings.kafka.consumer_group,
            kafka_broker: settings.kafka.broker,
            repo: greeting_repo,
        })
    }

    #[instrument(name="greeting_rust_processor")]
    async fn store_message(&mut self, m: &BorrowedMessage<'_>) -> Result<(), ConsumerError> {
        let payload = match m.payload_view::<str>() {
            None => return Err(ConsumerError::from("No payload found")),
            Some(Err(e)) => return Err(ConsumerError::from(e)),
            Some(Ok(s)) => s,
        };

        let headers = match m.headers() {
            None => return Err(ConsumerError::from("No headers found")),
            Some(v) => v
        };
        let context = global::get_text_map_propagator(|propagator| {
            propagator.extract(&HeaderExtractor(headers))
        });

        let header_str = headers.iter().fold(String::new(), |a, h| -> String {
            format!("{},  Header {:#?}: {:?}", a, h.key, h.value)
        });

        let trace_id = format!("{}", context.span().span_context().trace_id());
        let span = Span::current();
        span.set_parent(context);
        let span_id = format!("{:?}", span.context().span().span_context().span_id());
        // let _enter = span.enter();
        // let mut span =
        //     global::tracer("consumer").start_with_context("consume_payload", &context);

        // span!( Level::INFO, "consume_payload", header_str, payload);

        info!("Consumed topic: {}, partition: {}, offset: {}, timestamp: {:?}, headers{:?},  payload: '{}'",
                    m.topic(), m.partition(), m.offset(), m.timestamp(), header_str, payload,);

        let pg_trace = greeting_db_api::greeting_pg_trace::PgTraceContext {trace_id:trace_id,parent_span_id:span_id};
        let msg:greeting_db_api::greeting_command::GreetingCmdDto = serde_json::from_str(&payload).unwrap();
        self.repo.store(pg_trace, msg.clone()).await.expect("Error");
        // span.set_status(Status::Ok);
        // span.end();

        Ok(())
    }
}

#[async_trait]
pub trait ConsumeTopics {
    async fn consume_and_store(&mut self) -> Result<(), ConsumerError>;
}


#[async_trait]
impl ConsumeTopics for KafkaConsumer {

    async fn consume_and_store(&mut self) -> Result<(), ConsumerError> {
        let consumer: LoggingConsumer = ClientConfig::new()
            .set("group.id", &self.consumer_group)
            .set("bootstrap.servers", &self.kafka_broker)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "false")
            .set_log_level(RDKafkaLogLevel::Debug)
            .create_with_context(CustomContext).expect("Failed creating consumer");

        consumer
            .subscribe(&[&self.topic])?;

        info!("Starting to subscriobe on topic: {}", &self.topic);

        loop {

            match &consumer.recv().await {
                Err(e) => warn!("Kafka error: {}", e),
                Ok(m) => {
                    self.store_message(m).await?;
                    consumer.commit_message(&m, CommitMode::Async)?;
                }
            };
        }
    }
}

pub struct HeaderExtractor<'a>(pub &'a BorrowedHeaders);

impl<'a> Extractor for HeaderExtractor<'a> {
    fn get(&self, key: &str) -> Option<&str> {
        for i in 0..self.0.count() {
            if let Ok(val) = self.0.get_as::<str>(i) {
                if val.key == key {
                    return val.value
                }
            }
        }
        None
    }

    fn keys(&self) -> Vec<&str> {
        self.0.iter().map(|kv| kv.key).collect::<Vec<_>>()
    }
}
