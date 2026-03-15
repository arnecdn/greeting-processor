use async_trait::async_trait;
use greeting_db_api::greeting_command::{GreetingCommandRepository, GreetingCommandRepositoryImpl};
use greeting_db_api::DbError;
use log::{info, warn};
use opentelemetry::global;
use opentelemetry::propagation::Extractor;
use opentelemetry::trace::TraceContextExt;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, StreamConsumer};
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::message::{BorrowedHeaders, BorrowedMessage, Headers};
use rdkafka::{ClientConfig, ClientContext, Message, TopicPartitionList};
use std::fmt::{Debug, Formatter};
use std::str::Utf8Error;
use tracing::{instrument, Span};
use crate::Settings;
use tracing_opentelemetry::OpenTelemetrySpanExt;

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
        ConsumerError {
            err_msg: value.error_message,
        }
    }
}
impl From<Utf8Error> for ConsumerError {
    fn from(value: Utf8Error) -> Self {
        ConsumerError {
            err_msg: value.to_string(),
        }
    }
}

impl From<KafkaError> for ConsumerError {
    fn from(value: KafkaError) -> Self {
        ConsumerError {
            err_msg: value.to_string(),
        }
    }
}

impl From<&str> for ConsumerError {
    fn from(value: &str) -> Self {
        ConsumerError {
            err_msg: value.to_string(),
        }
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
    pub async fn new(
        settings: Settings,
        greeting_repo: Box<GreetingCommandRepositoryImpl>,
    ) -> Result<Self, ConsumerError> {
        Ok(Self {
            topic: settings.kafka.topic,
            consumer_group: settings.kafka.consumer_group,
            kafka_broker: settings.kafka.broker,
            repo: greeting_repo,
        })
    }

    #[instrument(name = "greeting_rust_processor")]
    async fn store_message(&mut self, m: &BorrowedMessage<'_>) -> Result<(), ConsumerError> {
        let payload = match m.payload_view::<str>() {
            None => return Err(ConsumerError::from("No payload found")),
            Some(Err(e)) => return Err(ConsumerError::from(e)),
            Some(Ok(s)) => s,
        };

        let headers = match m.headers() {
            None => return Err(ConsumerError::from("No headers found")),
            Some(v) => v,
        };
        let context = global::get_text_map_propagator(|propagator| {
            propagator.extract(&HeaderExtractor(headers))
        });

        let header_str = headers.iter().fold(String::new(), |a, h| -> String {
            format!("{},  Header {:#?}: {:?}", a, h.key, h.value)
        });

        let span = Span::current();
        span.set_parent(context);
        let otel_ctx = span.context();
        let span_ref = otel_ctx.span();
        let sc = span_ref.span_context();
        let trace_id = format!("{}", sc.trace_id());
        let span_id = format!("{}", sc.span_id());

        info!("Consumed topic: {}, partition: {}, offset: {}, timestamp: {:?}, headers{:?},  payload: '{}'",
                    m.topic(), m.partition(), m.offset(), m.timestamp(), header_str, payload,);

        let pg_trace = greeting_db_api::greeting_pg_trace::PgTraceContext {
            trace_id: trace_id,
            parent_span_id: span_id,
        };
        let msg: greeting_db_api::greeting_command::GreetingCmdEntity =
            serde_json::from_str(&payload).unwrap();
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
            .create_with_context(CustomContext)
            .expect("Failed creating consumer");

        consumer.subscribe(&[&self.topic])?;

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
                    return val.value;
                }
            }
        }
        None
    }

    fn keys(&self) -> Vec<&str> {
        self.0.iter().map(|kv| kv.key).collect::<Vec<_>>()
    }
}
