use prometheus_exporter::{
    prometheus::core::{MetricVec, MetricVecBuilder},
    prometheus::{
        IntCounter, IntCounterVec, register_int_counter, register_int_counter_vec
    }
};
use crate::billing::*;

pub struct Collector {
    remove_count: IntCounterVec,
    request_count: IntCounterVec,
    restore_count: IntCounterVec,
    store_count: IntCounterVec,
    transfer_count: IntCounterVec,
    unparsed_count: IntCounter,

    transferred_bytes: IntCounterVec,
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
                "billing_remove_count", "The number of remove events seen.",
                REMOVE_REQUEST_LABELS).unwrap(),
            request_count: register_int_counter_vec!(
                "billing_request_count", "The number of request events seen.",
                REMOVE_REQUEST_LABELS).unwrap(),
            restore_count: register_int_counter_vec!(
                "billing_restore_count", "The number of restore events seen.",
                RESTORE_STORE_LABELS).unwrap(),
            store_count: register_int_counter_vec!(
                "billing_store_count", "The number of store events seen.",
                RESTORE_STORE_LABELS).unwrap(),
            transfer_count: register_int_counter_vec!(
                "billing_transfer_count", "The number of transfer events seen.",
                TRANSFER_LABELS).unwrap(),
            unparsed_count: register_int_counter!(
                "billing_unparsed_count", "The number of unparsed events.")
                .unwrap(),
            transferred_bytes: register_int_counter_vec!(
                "billing_transferred_bytes", "Bytes transferred.",
                TRANSFER_LABELS).unwrap(),
        }
    }

    pub fn process_message(&mut self, msg_str: &str) {
        let msg : Message = serde_json::from_str(msg_str).unwrap_or_else(|error| {
            panic!("Failed to parse JSON record %{:?}: %{:?}", msg_str, error)
        });
        match msg {
            Message::Remove {cell, status, storage_info, ..} => {
                let index = (cell, status, storage_info);
                MetricIndex::project(&self.remove_count, &index).inc();
            }
            Message::Request {cell, status, storage_info, ..} => {
                let index = (cell, status, storage_info);
                MetricIndex::project(&self.request_count, &index).inc();
            }
            Message::Restore {cell, status, storage_info, hsm, ..} => {
                let index = (cell, status, storage_info, hsm);
                MetricIndex::project(&self.restore_count, &index).inc();
            }
            Message::Store {cell, status, storage_info, hsm, ..} => {
                let index = (cell, status, storage_info, hsm);
                MetricIndex::project(&self.store_count, &index).inc();
            }
            Message::Transfer {cell, direction, storage_info, transfer_size, ..} => {
                let index = (cell, direction, storage_info);
                MetricIndex::project(&self.transfer_count, &index).inc();
                MetricIndex::project(&self.transferred_bytes, &index).inc_by(transfer_size);
            }
            Message::Unparsed => {
                println!("Unrecognized billing record {:?}", msg_str);
                self.unparsed_count.inc();
            }
        }
    }
}
