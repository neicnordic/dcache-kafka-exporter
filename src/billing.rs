use serde::{Deserialize, Serialize};

// #[derive(Debug, Deserialize, Serialize)]
// #[serde(rename_all = "lowercase")]
// pub enum IsWrite {Read, Write}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProtocolInfo {
    pub host: String,
    pub port: i32,
    pub protocol: String, // FIXME
    pub version_major: i32,
    pub version_minor: i32,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Hsm {
    pub instance: String,
    pub provider: String,
    #[serde(rename = "type")]
    pub type_: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Status {
    pub code: i32,
    pub msg: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Cell {
    #[serde(rename = "cellDomain")]
    pub domain: String,
    #[serde(rename = "cellName")]
    pub name: String,
    #[serde(rename = "cellType")]
    pub type_: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", tag = "msgType")]
pub enum MoverInfo {

    #[serde(rename_all = "camelCase")]
    Transfer {
        #[serde(flatten)]
        cell: Cell,
        date: String, // FIXME
        is_p2p: bool,
        is_write: String,
        local_endpoint: String,
        mean_read_bandwidth: Option<f64>,
        protocol_info: ProtocolInfo,
        queuing_time: i32,
        read_active: Option<String>,
        read_idle: Option<String>,
        session: String,
        status: Status,
        transfer_path: String,
        transfer_size: u64,
        transfer_time: i32,
        version: String,
    },

    #[serde(other)]
    Unparsed,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", tag = "msgType")]
pub enum Message {

    #[serde(rename_all = "camelCase")]
    Remove {
        billing_path: String,
        #[serde(flatten)]
        cell: Cell,
        file_size: i64,
        pnfsid: String,
        queuing_time: i32,
        session: String,
        status: Status,
        storage_info: Option<String>,
        subject: Vec<String>,
        transaction: Option<String>,
    },

    #[serde(rename_all = "camelCase")]
    Request {
        billing_path: String,
        #[serde(flatten)]
        cell: Cell,
        client: String,
        client_chain: String,
        file_size: i64,
        #[serde(rename = "mappedGID")]
        mapped_gid: i32,
        #[serde(rename = "mappedUID")]
        mapped_uid: i32,
        mover_info: Option<MoverInfo>,
        owner: Option<String>,
        pnfsid: String,
        queuing_time: i32,
        session: String,
        session_duration: i32,
        status: Status,
        storage_info: String,
        subject: Vec<String>,
        transfer_path: String,
    },

    #[serde(rename_all = "camelCase")]
    Restore {
        billing_path: String,
        #[serde(flatten)]
        cell: Cell,
        date: String,
        file_size: i64,
        hsm: Hsm,
        locations: Vec<String>,
        pnfsid: String,
        queuing_time: i32,
        session: String,
        status: Status,
        storage_info: String,
        transaction: String,
        transfer_time: i32,
        version: String,
    },

    #[serde(rename_all = "camelCase")]
    Store {
	billing_path: String,
        #[serde(flatten)]
        cell: Cell,
	date: String, // FIXME
	file_size: i64,
        hsm: Hsm,
        locations: Vec<String>,
        status: Status,
        queuing_time: i32,
        transfer_time: i32,
        session: String,
        storage_info: String,
        pnfsid: String,
        trasaction: String,
    },

    #[serde(rename_all = "camelCase")]
    Transfer {
	billing_path: String,
        #[serde(flatten)]
        cell: Cell,
	date: String, // FIXME
	file_size: i64,
	initiator: String,
	is_p2p: bool,
	is_write: String,
	local_endpoint: Option<String>,
	mean_read_bandwidth: Option<f64>,
	pnfsid: String,
        protocol_info: ProtocolInfo,
	queuing_time: i64,
	read_active: Option<String>,
        session: String,
        transfer_time: i32,
        storage_info: String,
        transfer_size: u64,
        transfer_path: String,
        write_active: Option<String>,
        subject: Vec<String>,
    },

    #[serde(other)]
    Unparsed,
}
