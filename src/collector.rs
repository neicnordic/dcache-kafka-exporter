use log::{error, warn};
use prometheus_exporter::{
    prometheus::core::{MetricVec, MetricVecBuilder},
    prometheus::{
        IntCounter, IntCounterVec, register_int_counter, register_int_counter_vec
    }
};
use crate::billing::*;

pub struct Collector {
    remove_count: IntCounterVec,
    remove_bytes: IntCounterVec,
    request_count: IntCounterVec,
    restore_count: IntCounterVec,
    restore_bytes: IntCounterVec,
    store_count: IntCounterVec,
    store_bytes: IntCounterVec,
    transfer_count: IntCounterVec,
    transfer_bytes: IntCounterVec,
    unparsed_count: IntCounter,
}

pub trait MetricIndex {
    fn project<T : MetricVecBuilder>(vec: &MetricVec<T>, index: &Self) -> T::M;
}

// Remove and Request
const REMOVE_REQUEST_LABELS : &[&str; 5] = &[
    "cell_name", "cell_domain", "cell_type",
    "status_code",
    "storage_info",
];
impl MetricIndex for (Cell, Status, Option<String>) {
    fn project<T : MetricVecBuilder>(vec: &MetricVec<T>, index: &Self) -> T::M {
        let (cell, status, storage_info) = index;
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
}

// Restore and Store
const RESTORE_STORE_LABELS : &[&str; 8] = &[
    "cell_name", "cell_domain", "cell_type",
    "status_code",
    "storage_info",
    "hsm_instance", "hsm_provider", "hsm_type",
];
impl MetricIndex for (Cell, Status, String, Hsm) {
    fn project<T : MetricVecBuilder>(vec: &MetricVec<T>, index: &Self) -> T::M {
        let (cell, status, storage_info, hsm) = index;
        vec.with_label_values(&[
            cell.name.as_str(), cell.domain.as_str(), cell.type_.as_str(),
            status.code.to_string().as_str(),
            storage_info.as_str(),
            hsm.instance.as_str(), hsm.provider.as_str(), hsm.type_.as_str(),
        ])
    }
}

// Transfer
const TRANSFER_LABELS : &[&str; 5] = &[
    "cell_name", "cell_domain", "cell_type",
    "direction",
    "storage_info",
];
impl MetricIndex for (Cell, Direction, String) {
    fn project<T : MetricVecBuilder>(vec: &MetricVec<T>, index: &Self) -> T::M {
        let (cell, direction, storage_info) = index;
        vec.with_label_values(&[
            &cell.name[..], &cell.domain[..], &cell.type_[..],
            &direction.to_string(),
            storage_info.as_str(),
        ])
    }
}


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

            store_count: register_int_counter_vec!(
                "billing_store_count",
                "The number of store events seen.",
                RESTORE_STORE_LABELS).unwrap(),
            store_bytes: register_int_counter_vec!(
                "billing_store_bytes",
                "The accumulated size of files attempted flushed to tape.",
                RESTORE_STORE_LABELS).unwrap(),

            transfer_count: register_int_counter_vec!(
                "billing_transfer_count",
                "The number of transfer events seen.",
                TRANSFER_LABELS).unwrap(),
            transfer_bytes: register_int_counter_vec!(
                "billing_transfer_bytes",
                "The number of bytes transferred, including from failed transfers.",
                TRANSFER_LABELS).unwrap(),

            unparsed_count: register_int_counter!(
                "billing_unparsed_count",
                "The number of unparsed events.").unwrap(),
        }
    }

    pub fn process_message(&mut self, msg_str: &str) {
        let msg = serde_json::from_str(msg_str);
        match msg {
            Ok(Message::Remove {cell, status, storage_info, file_size, ..}) => {
                let index = (cell, status, storage_info);
                MetricIndex::project(&self.remove_count, &index).inc();
                MetricIndex::project(&self.remove_bytes, &index).inc_by(file_size);
            }
            Ok(Message::Request {cell, status, storage_info, ..}) => {
                let index = (cell, status, storage_info);
                MetricIndex::project(&self.request_count, &index).inc();
            }
            Ok(Message::Restore {cell, status, storage_info, hsm, file_size, ..}) => {
                let index = (cell, status, storage_info, hsm);
                MetricIndex::project(&self.restore_count, &index).inc();
                MetricIndex::project(&self.restore_bytes, &index).inc_by(file_size);
            }
            Ok(Message::Store {cell, status, storage_info, hsm, file_size, ..}) => {
                let index = (cell, status, storage_info, hsm);
                MetricIndex::project(&self.store_count, &index).inc();
                MetricIndex::project(&self.store_bytes, &index).inc_by(file_size);
            }
            Ok(Message::Transfer {cell, direction, storage_info, transfer_size, ..}) => {
                let index = (cell, direction, storage_info);
                MetricIndex::project(&self.transfer_count, &index).inc();
                MetricIndex::project(&self.transfer_bytes, &index).inc_by(transfer_size);
            }
            Ok(Message::Unparsed) => {
                warn!("Unrecognized billing record {:?}", msg_str);
                self.unparsed_count.inc();
            }
            Err(error) => {
                error!("Failed to parse JSON record {:?}: {:?}", msg_str, error);
                self.unparsed_count.inc();
            }
        }
    }
}
