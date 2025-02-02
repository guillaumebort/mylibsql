use std::path::Path;

use libsql::replication::FrameNo;

pub struct Snapshot {
    pub path: Box<dyn AsRef<Path> + Send + 'static>,
    pub last_frame_no: Option<FrameNo>,
}
