use std::path::Path;

use bytes::Bytes;
use tokio::fs::{create_dir_all, File};
use tokio::io::{self, AsyncWriteExt};

pub async fn ensure_file_on_disk(
    path: &Path,
    content: &Bytes,
) -> Result<(), io::Error> {
    create_dir_all(path.parent().unwrap()).await?;
    let try_open = File::open(&path).await;
    if let Ok(_) = try_open {
        return Ok(());
    }
    let mut file = File::create(path).await?;
    file.write_all(&content).await?;
    Ok(())
}
