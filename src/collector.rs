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

pub struct Collector {
    remove_count: IntCounterVec,
    remove_bytes: IntCounterVec,
    request_count: IntCounterVec,
    restore_count: IntCounterVec,
    restore_bytes: IntCounterVec,
    restore_seconds: HistogramVec,
    store_count: IntCounterVec,
    store_bytes: IntCounterVec,
    store_seconds: HistogramVec,
    transfer_count: IntCounterVec,
    transfer_bytes: IntCounterVec,
    transfer_seconds: HistogramVec,
    unparsed_count: IntCounter,
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
const TRANSFER_LABELS : &[&str; 5] = &[
    "cell_name", "cell_domain", "cell_type",
    "direction",
    "storage_info",
];

// Value projections corresponding to the above labels.
fn proj<T : MetricVecBuilder>(vec: &MetricVec<T>, index: &Message) -> T::M {
    match index {
        Message::Remove {cell, status, storage_info, ..} |
        Message::Request {cell, status, storage_info, ..} => {
            let storage_info: &str = match storage_info {
                None => { "" }
                Some(s) => { s.as_str() }
            };
            vec.with_label_values(&[
                cell.name.as_str(), cell.domain.as_str(), cell.type_.as_str(),
                status.code.to_string().as_str(),
                storage_info,
            ])
        }
        Message::Restore {cell, status, storage_info, hsm, ..} |
        Message::Store {cell, status, storage_info, hsm, ..} => {
            vec.with_label_values(&[
                cell.name.as_str(), cell.domain.as_str(), cell.type_.as_str(),
                status.code.to_string().as_str(),
                storage_info.as_str(),
                hsm.instance.as_str(), hsm.provider.as_str(), hsm.type_.as_str(),
            ])
        }
        Message::Transfer {cell, direction, storage_info, ..} => {
            vec.with_label_values(&[
                &cell.name[..], &cell.domain[..], &cell.type_[..],
                &direction.to_string(),
                storage_info.as_str(),
            ])
        }
    }
}

const DURATION_BUCKETS : [f64; 15] = [
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

impl Collector {
    pub fn new() -> Collector {
        Collector {
            remove_count: register_int_counter_vec!(
                "billing_remove_count",
                "The number of remove events seen.",
                REMOVE_REQUEST_LABELS).unwrap(),
            remove_bytes: register_int_counter_vec!(
                "billing_remove_bytes",
                "The accumulated size of removed files.",
                REMOVE_REQUEST_LABELS).unwrap(),

            request_count: register_int_counter_vec!(
                "billing_request_count",
                "The number of request events seen.",
                REMOVE_REQUEST_LABELS).unwrap(),

            restore_count: register_int_counter_vec!(
                "billing_restore_count",
                "The number of restore events seen.",
                RESTORE_STORE_LABELS).unwrap(),
            restore_bytes: register_int_counter_vec!(
                "billing_restore_bytes",
                "The accumulated size of files attempted restored from tape.",
                RESTORE_STORE_LABELS).unwrap(),
            restore_seconds: register_histogram_vec!(
                "billing_restore_seconds",
                "A histogram of restore times.",
                RESTORE_STORE_LABELS,
                Vec::from(DURATION_BUCKETS)).unwrap(),

            store_count: register_int_counter_vec!(
                "billing_store_count",
                "The number of store events seen.",
                RESTORE_STORE_LABELS).unwrap(),
            store_bytes: register_int_counter_vec!(
                "billing_store_bytes",
                "The accumulated size of files attempted flushed to tape.",
                RESTORE_STORE_LABELS).unwrap(),
            store_seconds: register_histogram_vec!(
                "billing_store_seconds",
                "A histogram of store times.",
                RESTORE_STORE_LABELS,
                Vec::from(DURATION_BUCKETS)).unwrap(),

            transfer_count: register_int_counter_vec!(
                "billing_transfer_count",
                "The number of transfer events seen.",
                TRANSFER_LABELS).unwrap(),
            transfer_bytes: register_int_counter_vec!(
                "billing_transfer_bytes",
                "The number of bytes transferred, including from failed transfers.",
                TRANSFER_LABELS).unwrap(),
            transfer_seconds: register_histogram_vec!(
                "billing_transfer_seconds",
                "A histogram of transfer times.",
                TRANSFER_LABELS,
                Vec::from(DURATION_BUCKETS)).unwrap(),

            unparsed_count: register_int_counter!(
                "billing_unparsed_count",
                "The number of unparsed events.").unwrap(),
        }
    }

    fn update_metrics(&mut self, msg: Message) {
        match msg {
            Message::Remove {file_size, ..} => {
                proj(&self.remove_count, &msg).inc();
                proj(&self.remove_bytes, &msg).inc_by(file_size);
            }
            Message::Request {..} => {
                proj(&self.request_count, &msg).inc();
            }
            Message::Restore {file_size, transfer_time, ..} => {
                proj(&self.restore_count, &msg).inc();
                proj(&self.restore_bytes, &msg).inc_by(file_size);
                proj(&self.restore_seconds, &msg).observe(transfer_time as f64 / 1000.0);
            }
            Message::Store {file_size, transfer_time, ..} => {
                proj(&self.store_count, &msg).inc();
                proj(&self.store_bytes, &msg).inc_by(file_size);
                proj(&self.store_seconds, &msg).observe(transfer_time as f64 / 1000.0);
            }
            Message::Transfer {transfer_size, transfer_time, ..} => {
                proj(&self.transfer_count, &msg).inc();
                proj(&self.transfer_bytes, &msg).inc_by(transfer_size);
                proj(&self.transfer_seconds, &msg).observe(transfer_time as f64 / 1000.0);
            }
        }
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
