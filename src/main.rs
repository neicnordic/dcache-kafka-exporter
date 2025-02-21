// This file is part of the dcache-kafka-exporter project.
// Copyright (C) 2024  Petter A. Urkedal
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use std::str;
use clap::Parser;
use std::error::Error;
use kafka::client::{KafkaClient, SecurityConfig};
use kafka::consumer::{Consumer, FetchOffset};
use openssl::ssl;

mod billing;
mod collector;
mod message_simplifier;

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

    #[arg(long, default_value = "dcache-kafka-exporter")]
    kafka_group: String,

    #[arg(long, default_value = "dcache_kafka_")]
    metric_prefix: String,

    #[arg(long, default_value = "127.0.0.1:19997")]
    listen: String,

    /// Enables the experimental *_message_count metric.
    #[arg(long)]
    enable_message_count: bool,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    env_logger::init();

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
        .with_fallback_offset(FetchOffset::Latest)
        .create()?;
    let mut collector =
        collector::Collector::new(args.metric_prefix, args.enable_message_count);
    let _exporter = prometheus_exporter::start(args.listen.parse().unwrap());
    loop {
        for msgs in kafka_consumer.poll().unwrap().iter() {
            for msg in msgs.messages() {
                collector.process_message(str::from_utf8(msg.value)?);
            }
        }
    }
}
