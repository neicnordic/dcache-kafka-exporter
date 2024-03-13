use std::str;
use clap::Parser;
use std::error::Error;
use kafka::client::{KafkaClient, SecurityConfig};
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use openssl::ssl;
//use prometheus_exporter;

mod billing;
mod collector;

fn start_prometheus_exporter() {
    let binding = "127.0.0.1:19997".parse().unwrap();
    prometheus_exporter::start(binding).unwrap();
}

#[derive(Parser)]
struct Args {

    #[arg(long, value_delimiter = ',',
          default_values_t = ["localhost:9092".to_string()])]
    kafka_hosts: Vec<String>,

    #[arg(long = "kafka-ca", value_name = "PEM-FILE")]
    ca_path: Option<std::path::PathBuf>,

    #[arg(long = "client-key", value_name = "PEM-FILE")]
    key_path: Option<std::path::PathBuf>,

    #[arg(long = "client-cert", value_name = "PEM-FILE")]
    cert_path: Option<std::path::PathBuf>,

    #[arg(long, default_value = "billing")]
    kafka_topic: String,

    #[arg(long, default_value = "prometheus-billing-exporter")]
    kafka_group: String,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    let mut builder = ssl::SslConnector::builder(ssl::SslMethod::tls_client())?;
    if let Some(p) = args.cert_path {
        builder.set_certificate_file(p, ssl::SslFiletype::PEM)?;
    }
    if let Some(p) = args.key_path {
        builder.set_private_key_file(p, ssl::SslFiletype::PEM)?;
    }
    if let Some(p) = args.ca_path {
        builder.set_ca_file(p)?;
    }
    let ssl_connector = builder.build();

    let security_config = SecurityConfig::new(ssl_connector);
    let mut kafka_client = KafkaClient::new_secure(args.kafka_hosts, security_config);
        kafka_client.load_metadata_all().unwrap();
    let mut kafka_consumer = Consumer::from_client(kafka_client)
        .with_topic(args.kafka_topic)
        .with_fallback_offset(FetchOffset::Earliest)
        .with_group(args.kafka_group)
        .with_offset_storage(Some(GroupOffsetStorage::Kafka))
        .create()?;
    let mut collector = collector::Collector::new();
    start_prometheus_exporter();
    loop {
        for msgs in kafka_consumer.poll().unwrap().iter() {
            for msg in msgs.messages() {
                collector.process_message(str::from_utf8(msg.value)?);
            }
        }
    }
}
