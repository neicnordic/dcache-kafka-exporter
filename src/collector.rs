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

use log::{warn};
use prometheus_exporter::{
    prometheus::core::{MetricVec, MetricVecBuilder},
    prometheus::{
        IntCounter, register_int_counter,
        IntCounterVec, register_int_counter_vec,
        HistogramVec, register_histogram_vec,
    }
};
use crate::billing::*;
use crate::message_simplifier::{MessageRewriteRules};

pub struct Collector {
    shortened_cell_names: Vec<String>,

    remove_count: IntCounterVec,
    remove_bytes: IntCounterVec,
    request_count: IntCounterVec,
    request_session_seconds: HistogramVec,
    restore_count: IntCounterVec,
    restore_bytes: IntCounterVec,
    restore_seconds: HistogramVec,
    store_count: IntCounterVec,
    store_bytes: IntCounterVec,
    store_seconds: HistogramVec,
    transfer_count: IntCounterVec,
    transfer_bytes: IntCounterVec,
    transfer_seconds: HistogramVec,
    transfer_mean_read_bandwidth_bytes_per_second: HistogramVec,
    transfer_mean_write_bandwidth_bytes_per_second: HistogramVec,
    message_count: IntCounterVec,
    unparsed_count: IntCounter,

    message_rewrite_rules: Option<MessageRewriteRules>,
}

// For Message::Remove and Message::Request
const REMOVE_REQUEST_LABELS : &[&str; 5] = &[
    "cell_name", "cell_domain", "cell_type",
    "status_code",
    "storage_info",
];

// For Message::Restore and Message::Store
const RESTORE_STORE_LABELS : &[&str; 8] = &[
    "cell_name", "cell_domain", "cell_type",
    "status_code",
    "storage_info",
    "hsm_instance", "hsm_provider", "hsm_type",
];

// For Message::Transfer
const TRANSFER_LABELS : &[&str; 6] = &[
    "cell_name", "cell_domain", "cell_type",
    "status_code",
    "direction",
    "storage_info",
];

// For any message with non-zero status code.
const MESSAGE_LABELS : &[&str; 5] = &[
    "cell_name", "cell_domain", "cell_type",
    "status_code",
    "status_msg",
];

// Buckets suitable for human presentation of durations which are typically
// around a minute or longer.  This is a precise geometrical sequence which
// aligns to 1 minute and 1 hour.
const LONG_DURATION_BUCKETS : [f64; 15] = [
    0.0010874632336580173,
    0.00425727462440863,
    0.016666666666666666,
    0.065247794019481067,
    0.2554364774645177,
    1.0,
    3.9148676411688634,
    15.32618864787106,
    60.0,
    234.89205847013176,
    919.57131887226399,
    3600.0,
    14093.523508207918,
    55174.279132335789,
    216000.0,
];

// Buckets suitable for short durations.  This is a precise geometrical
// sequence aligned to factors of 10.
const SHORT_DURATION_BUCKETS : [f64; 13] = [
    0.001,
    0.0031622776601683794,
    0.01,
    0.031622776601683791,
    0.10000000000000001,
    0.31622776601683794,
    1.0,
    3.1622776601683795,
    10.0,
    31.622776601683793,
    100.0,
    316.22776601683796,
    1000.0
];

const TRANSFER_RATE_BUCKETS : [f64; 15] = [
    10000.0,
    31622.77660168379,
    100000.0,
    316227.7660168379,
    1000000.0,
    3162277.660168379,
    10000000.0,
    31622776.60168379,
    100000000.0,
    316227766.0168379,
    1000000000.0,
    3162277660.168379,
    10000000000.0,
    31622776601.68379,
    100000000000.0,
];

impl Collector {
    pub fn new(
        metric_prefix: String,
        enable_message_count: bool,
        shortened_cell_names: Vec<String>
    ) -> Collector {
        Collector {
            shortened_cell_names: shortened_cell_names,

            remove_count: register_int_counter_vec!(
                metric_prefix.clone() + "remove_count",
                "The number of remove events seen.",
                REMOVE_REQUEST_LABELS).unwrap(),
            remove_bytes: register_int_counter_vec!(
                metric_prefix.clone() + "remove_bytes",
                "The accumulated size of removed files.",
                REMOVE_REQUEST_LABELS).unwrap(),

            request_count: register_int_counter_vec!(
                metric_prefix.clone() + "request_count",
                "The number of request events seen.",
                REMOVE_REQUEST_LABELS).unwrap(),
            request_session_seconds: register_histogram_vec!(
                metric_prefix.clone() + "request_session_duration",
                "A histogram of duration of request sessions.",
                REMOVE_REQUEST_LABELS,
                Vec::from(SHORT_DURATION_BUCKETS)).unwrap(),

            restore_count: register_int_counter_vec!(
                metric_prefix.clone() + "restore_count",
                "The number of restore events seen.",
                RESTORE_STORE_LABELS).unwrap(),
            restore_bytes: register_int_counter_vec!(
                metric_prefix.clone() + "restore_bytes",
                "The accumulated size of files attempted restored from tape.",
                RESTORE_STORE_LABELS).unwrap(),
            restore_seconds: register_histogram_vec!(
                metric_prefix.clone() + "restore_seconds",
                "A histogram of restore times.",
                RESTORE_STORE_LABELS,
                Vec::from(LONG_DURATION_BUCKETS)).unwrap(),

            store_count: register_int_counter_vec!(
                metric_prefix.clone() + "store_count",
                "The number of store events seen.",
                RESTORE_STORE_LABELS).unwrap(),
            store_bytes: register_int_counter_vec!(
                metric_prefix.clone() + "store_bytes",
                "The accumulated size of files attempted flushed to tape.",
                RESTORE_STORE_LABELS).unwrap(),
            store_seconds: register_histogram_vec!(
                metric_prefix.clone() + "store_seconds",
                "A histogram of store times.",
                RESTORE_STORE_LABELS,
                Vec::from(LONG_DURATION_BUCKETS)).unwrap(),

            transfer_count: register_int_counter_vec!(
                metric_prefix.clone() + "transfer_count",
                "The number of transfer events seen.",
                TRANSFER_LABELS).unwrap(),
            transfer_bytes: register_int_counter_vec!(
                metric_prefix.clone() + "transfer_bytes",
                "The number of bytes transferred, including from failed transfers.",
                TRANSFER_LABELS).unwrap(),
            transfer_seconds: register_histogram_vec!(
                metric_prefix.clone() + "transfer_seconds",
                "A histogram of transfer times.",
                TRANSFER_LABELS,
                Vec::from(LONG_DURATION_BUCKETS)).unwrap(),
            transfer_mean_read_bandwidth_bytes_per_second: register_histogram_vec!(
                metric_prefix.clone() + "transfer_mean_read_bandwidth_bytes_per_second",
                "A histogram of the mean read bandwidth for transfers.",
                TRANSFER_LABELS,
                Vec::from(TRANSFER_RATE_BUCKETS)).unwrap(),
            transfer_mean_write_bandwidth_bytes_per_second: register_histogram_vec!(
                metric_prefix.clone() + "transfer_mean_write_bandwidth_bytes_per_second",
                "A histogram of the mean write bandwidth for transfers.",
                TRANSFER_LABELS,
                Vec::from(TRANSFER_RATE_BUCKETS)).unwrap(),

            message_count: register_int_counter_vec!(
                metric_prefix.clone() + "message_count",
                "Status messages from any message type, simplified to reduce \
                 cardinality.",
                MESSAGE_LABELS).unwrap(),

            unparsed_count: register_int_counter!(
                metric_prefix.clone() + "unparsed_count",
                "The number of unparsed events.").unwrap(),

            message_rewrite_rules:
                if enable_message_count { Some(MessageRewriteRules::new()) }
                else { None },
        }
    }

    fn shorten_cell_name<'a>(&'a self, cell: &'a str) -> &'a str {
        return self.shortened_cell_names
            .iter().find_map(|prefix| {
                if prefix.len() < cell.len() && cell.starts_with(prefix)
                        && &cell[prefix.len() .. prefix.len() + 1] == "-" {
                    return Some(prefix.as_str())
                } else {
                    return None
                }
            })
            .unwrap_or(cell);
    }

    // Value projections corresponding to the above labels.
    fn proj<T : MetricVecBuilder>(&self, vec: &MetricVec<T>, index: &Message) -> T::M {
        match index {
            Message::Remove {cell, status, storage_info, ..} |
            Message::Request {cell, status, storage_info, ..} => {
                let storage_info: &str = match storage_info {
                    None => { "" }
                    Some(s) => { s.as_str() }
                };
                vec.with_label_values(&[
                    self.shorten_cell_name(cell.name.as_str()),
                    cell.domain.as_str(),
                    cell.type_.as_str(),
                    status.code.to_string().as_str(),
                    storage_info,
                ])
            }
            Message::Restore {cell, status, storage_info, hsm, ..} |
            Message::Store {cell, status, storage_info, hsm, ..} => {
                vec.with_label_values(&[
                    self.shorten_cell_name(cell.name.as_str()),
                    cell.domain.as_str(),
                    cell.type_.as_str(),
                    status.code.to_string().as_str(),
                    storage_info.as_str(),
                    hsm.instance.as_str(), hsm.provider.as_str(), hsm.type_.as_str(),
                ])
            }
            Message::Transfer {cell, status, direction, storage_info, ..} => {
                vec.with_label_values(&[
                    self.shorten_cell_name(&cell.name[..]),
                    &cell.domain[..],
                    &cell.type_[..],
                    status.code.to_string().as_str(),
                    &direction.to_string(),
                    storage_info.as_str(),
                ])
            }
        }
    }

    fn update_message_count_metric(&mut self, index: &Message) {
        let Some(rules) = &self.message_rewrite_rules else { return; };
        match index {
            Message::Remove {cell, status, ..} |
            Message::Request {cell, status, ..} |
            Message::Restore {cell, status, ..} |
            Message::Store {cell, status, ..} |
            Message::Transfer {cell, status, ..} => {
                if status.msg == "" { return; }
                let msg = rules.rewrite(&status.msg);
                self.message_count.with_label_values(&[
                    self.shorten_cell_name(&cell.name[..]),
                    &cell.domain[..],
                    &cell.type_[..],
                    status.code.to_string().as_str(),
                    &msg,
                ]).inc();
            }
        }
    }

    fn update_metrics(&mut self, msg: Message) {
        match msg {
            Message::Remove {file_size, ..} => {
                self.proj(&self.remove_count, &msg).inc();
                self.proj(&self.remove_bytes, &msg).inc_by(file_size);
            }
            Message::Request {session_duration, ..} => {
                self.proj(&self.request_count, &msg).inc();
                self.proj(&self.request_session_seconds, &msg).observe(session_duration as f64 / 1000.0);
            }
            Message::Restore {file_size, transfer_time, ..} => {
                self.proj(&self.restore_count, &msg).inc();
                self.proj(&self.restore_bytes, &msg).inc_by(file_size);
                self.proj(&self.restore_seconds, &msg).observe(transfer_time as f64 / 1000.0);
            }
            Message::Store {file_size, transfer_time, ..} => {
                self.proj(&self.store_count, &msg).inc();
                self.proj(&self.store_bytes, &msg).inc_by(file_size);
                self.proj(&self.store_seconds, &msg).observe(transfer_time as f64 / 1000.0);
            }
            Message::Transfer {transfer_size, transfer_time,
                               mean_read_bandwidth, mean_write_bandwidth, ..} => {
                self.proj(&self.transfer_count, &msg).inc();
                self.proj(&self.transfer_bytes, &msg).inc_by(transfer_size.unwrap_or(0));
                self.proj(&self.transfer_seconds, &msg).observe(transfer_time as f64 / 1000.0);
                if let Some(bandwidth) = mean_read_bandwidth {
                    self.proj(&self.transfer_mean_read_bandwidth_bytes_per_second, &msg)
                        .observe(bandwidth);
                }
                if let Some(bandwidth) = mean_write_bandwidth {
                    self.proj(&self.transfer_mean_write_bandwidth_bytes_per_second, &msg)
                        .observe(bandwidth);
                }
            }
        }
        self.update_message_count_metric(&msg);
    }

    pub fn process_message(&mut self, msg_str: &str) {
        match serde_json::from_str(msg_str) {
            Ok(msg) => {
                self.update_metrics(msg);
            }
            Err(error) => {
                warn!("Failed to parse JSON record {:?}: {:?}", msg_str, error);
                self.unparsed_count.inc();
            }
        }
    }
}
