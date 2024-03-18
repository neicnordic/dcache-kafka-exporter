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

impl MetricIndex for (Cell, Status) {
    fn project<T : MetricVecBuilder>(vec: &MetricVec<T>, index: &Self) -> T::M {
        let (cell, status) = index;
        vec.with_label_values(
            &[cell.name.as_str(), cell.domain.as_str(), cell.type_.as_str(),
              status.code.to_string().as_str()])
    }
}

const NONTRANSFER_LABELS : &[&str; 4] =
    &["cell_name", "cell_domain", "cell_type", "status_code"];
const TRANSFER_LABELS : &[&str; 4] =
    &["cell_name", "cell_domain", "cell_type", "direction"];

impl Collector {
    pub fn new() -> Collector {
        Collector {
            remove_count: register_int_counter_vec!(
                "billing_remove_count", "The number of remove events seen.",
                NONTRANSFER_LABELS).unwrap(),
            request_count: register_int_counter_vec!(
                "billing_request_count", "The number of request events seen.",
                NONTRANSFER_LABELS).unwrap(),
            restore_count: register_int_counter_vec!(
                "billing_restore_count", "The number of restore events seen.",
                NONTRANSFER_LABELS).unwrap(),
            store_count: register_int_counter_vec!(
                "billing_store_count", "The number of store events seen.",
                NONTRANSFER_LABELS).unwrap(),
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

    pub fn process_message<'a>(&mut self, msg_str: &str) {
        let msg : Message = serde_json::from_str(msg_str).unwrap_or_else(|error| {
            panic!("Failed to parse JSON record %{:?}: %{:?}", msg_str, error)
        });
        match msg {
            Message::Remove {cell, status, ..} => {
                MetricIndex::project(&self.remove_count, &(cell, status)).inc();
            }
            Message::Request {cell, status, ..} => {
                MetricIndex::project(&self.request_count, &(cell, status)).inc();
            }
            Message::Restore {cell, status, ..} => {
                MetricIndex::project(&self.restore_count, &(cell, status)).inc();
            }
            Message::Store {cell, status, ..} => {
                MetricIndex::project(&self.store_count, &(cell, status)).inc();
            }
            Message::Transfer {cell, direction, transfer_size, ..} => {
                let vs = [
                    &cell.name[..], &cell.domain[..], &cell.type_[..],
                    &direction.to_string(),
                ];
                self.transfer_count.with_label_values(&vs).inc();
                self.transferred_bytes.with_label_values(&vs).inc_by(transfer_size);
            }
            Message::Unparsed => {
                println!("Unrecognized billing record {:?}", msg_str);
                self.unparsed_count.inc();
            }
        }
    }
}
