use std::fmt;
use serde::{de, Deserialize};

#[derive(Debug)]
pub enum Direction {Read, Write, P2p}

impl fmt::Display for Direction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}",
            match self {
                Direction::Read => {"read"}
                Direction::Write => {"write"}
                Direction::P2p => {"p2p"}
            }
        )
    }
}

#[derive(Deserialize)]
#[serde(field_identifier, rename_all = "camelCase")]
enum DirectionField {IsP2p, IsWrite}

impl<'de> Deserialize<'de> for Direction {

    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: de::Deserializer<'de>
    {
        struct DirectionVisitor;
        impl<'de> de::Visitor<'de> for DirectionVisitor {

            type Value = Direction;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("isP2p and isWrite attributes")
            }

            fn visit_map<V>(self, mut map: V) -> Result<Direction, V::Error>
                where V: de::MapAccess<'de>
            {
                let mut is_p2p = None;
                let mut is_write = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        DirectionField::IsP2p => {
                            is_p2p = Some(map.next_value()?);
                        }
                        DirectionField::IsWrite => {
                            is_write = Some(map.next_value()?);
                        }
                    }
                }
                match (is_p2p, is_write) {
                    (Some(true), Some("read")) => { Ok(Direction::P2p) }
                    (Some(false), Some("read")) => { Ok(Direction::Read) }
                    (Some(false), Some("write")) => { Ok(Direction::Write) }
                    _ => {
                        Err(de::Error::custom("Unexpected isP2p or isWrite."))
                    }
                }
            }
        }
        const FIELDS: &[&str] = &["isP2p", "isWrite"];
        deserializer.deserialize_struct("Direction", FIELDS, DirectionVisitor)
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProtocolInfo {
    pub host: String,
    pub port: i32,
    pub protocol: String, // FIXME
    pub version_major: i32,
    pub version_minor: i32,
}

#[derive(Debug, Deserialize)]
pub struct Hsm {
    pub instance: String,
    pub provider: String,
    #[serde(rename = "type")]
    pub type_: String,
}

#[derive(Debug, Deserialize)]
pub struct Status {
    pub code: i32,
    pub msg: String,
}

#[derive(Debug, Deserialize)]
pub struct Cell {
    #[serde(rename = "cellDomain")]
    pub domain: String,
    #[serde(rename = "cellName")]
    pub name: String,
    #[serde(rename = "cellType")]
    pub type_: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", tag = "msgType")]
pub enum MoverInfo {

    #[serde(rename_all = "camelCase")]
    Transfer {
        #[serde(flatten)]
        cell: Cell,
        date: String, // FIXME
        #[serde(flatten)]
        direction: Direction,
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

#[derive(Debug, Deserialize)]
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
        storage_info: Option<String>, // present for pools, absent for doors
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
        pnfsid: Option<String>,
        queuing_time: i32,
        session: String,
        session_duration: i32,
        status: Status,
        storage_info: Option<String>, // may be missing when status.code != 0
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
        transaction: String,
    },

    #[serde(rename_all = "camelCase")]
    Transfer {
        billing_path: String,
        #[serde(flatten)]
        cell: Cell,
        date: String, // FIXME
        file_size: i64,
        initiator: String,
        #[serde(flatten)]
        direction: Direction,
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
