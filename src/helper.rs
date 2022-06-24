use std::path::Path;

use anyhow::{Context, Result};

pub fn create_dir_all_exists_ok<T>(path: T) -> Result<()>
where
    T: AsRef<Path>,
{
    std::fs::create_dir_all(&path)
        .or_else(|e| {
            if e.kind() == std::io::ErrorKind::AlreadyExists {
                Ok(())
            } else {
                Err(e)
            }
        })
        .with_context(|| {
            format!(
                "Failed to create dir at {}",
                path.as_ref().to_string_lossy()
            )
        })?;
    Ok(())
}
