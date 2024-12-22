// implementation of the SectorManager struct
use crate::sectors_manager_public::{self, SectorsManager};
use crate::{SectorVec, StatusCode};
use crate::domain::Configuration;
use core::{sync, time};
use std::path::{Path, PathBuf};
use sha2::{Sha256, Digest};
use tokio::{fs::rename, io::{AsyncWriteExt, AsyncReadExt}};
use std::io::IoSlice;

const TMP_SUFFIX: &str = "_tmp";
const META_EXTENSION: &str = "meta";

pub struct BasicSectorsManager {
    config_storage_dir: PathBuf,
}

fn get_sector_dir(storage_dir: PathBuf, idx: u64) -> PathBuf {
    storage_dir.join(idx.to_string())
}

fn get_sector_data_path(sector_dir: PathBuf) -> PathBuf {
    sector_dir.join("data")
}

fn get_tmp_path(file_path: PathBuf) -> PathBuf {
    let mut tmp_file = file_path.clone();
    let old_extension = file_path.extension();
    let new_extension = old_extension.and_then(|s| s.to_str()).unwrap_or("");
    tmp_file.set_extension(format!("{}{}", new_extension, TMP_SUFFIX));
    tmp_file
}

fn get_metadata_path(
        sector_dir: &PathBuf, timestamp: &u64, write_rank: &u8) -> PathBuf {
    let custom_name = format!("{}_{}.meta", write_rank, timestamp);
    sector_dir.join(custom_name)
}

fn calculate_checksum(sector: &Vec<u8>) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(&sector);
    hasher.finalize().to_vec()
}

async fn sync_path(dir: &PathBuf) {
    tokio::fs::File::open(dir).await.unwrap().sync_data().await.unwrap();
}

async fn safe_write(dir_path: &PathBuf, path: &PathBuf, data: Vec<&Vec<u8>>){
    let mut should_sync_dir = false;
    let mut write_file = if !path.exists() {
        should_sync_dir = true;
        tokio::fs::File::create(&path).await.unwrap()
    } else {
        tokio::fs::File::open(&path).await.unwrap()
    };
    for d in data {
        write_file.write_all(&d).await.unwrap();
    }
    write_file.sync_all().await.unwrap();
    sync_path(&path).await;

    if should_sync_dir {
        sync_path(dir_path).await;
    }
}

fn decode_metadata(metadata: &PathBuf) -> (u64, u8) {
    let string_metadata = metadata.file_stem().unwrap().to_str().unwrap();
    let metadata_parts: Vec<&str> = string_metadata.split('_').collect();
    let timestamp = metadata_parts[1].parse::<u64>().unwrap();
    let write_rank = metadata_parts[0].parse::<u8>().unwrap();
    (timestamp, write_rank)
}

async fn safe_delete(dir_path: &PathBuf, path: &PathBuf) {
    tokio::fs::remove_file(&path).await.unwrap();
    sync_path(&dir_path).await;
}

async fn recover_dir(dir_path: &PathBuf) {
    let mut tmp_file = None;
    let mut tmp_metadata = None;
    let mut meata_data_files: Vec<PathBuf> = Vec::new();
    let mut files = tokio::fs::read_dir(&dir_path).await.unwrap();
    while let Some(entry) = files.next_entry().await.unwrap() {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str())
            == Some(format!("{}{}", META_EXTENSION, TMP_SUFFIX).as_str())
        {
            tmp_metadata = Some(path);
        }
        else if path.ends_with(TMP_SUFFIX){
            tmp_file = Some(path);
        }
        else if path.extension().and_then(|s| s.to_str()) == Some(META_EXTENSION) {
            meata_data_files.push(path);
        }
    }
    if tmp_file.is_none() {
        // Everything is fine
        return;
    }
    else if tmp_file.is_some() && tmp_metadata.is_none() {
        // We are at a stage where all data has been correctly overwritten
        // we should only delete old meta and tmp file
        safe_delete(&dir_path, &tmp_file.unwrap()).await;
        let max_timestamp = meata_data_files.iter().map(|f| {
            let (timestamp, _) = decode_metadata(f);
            timestamp
        }).max().unwrap();
        for meta_file in meata_data_files {
            let (timestamp, _) = decode_metadata(&meta_file);
            if timestamp != max_timestamp {
                safe_delete(&dir_path, &meta_file).await;
            }
        }
        return;
    }

    let tmp_file = tmp_file.unwrap();
    let mut data_file = tokio::fs::File::open(&tmp_file).await.unwrap();
    let mut data = Vec::new();
    data_file.read_to_end(&mut data).await.unwrap();
    // Check if file content length is data size + checksum size
    if data.len() != 4096 + 32 {
        safe_delete(&dir_path, &tmp_file).await;
        return;
    }
    let pure_data = &data[0..4096].to_vec();
    let checksum_data = &data[4096..];
    let checksum = calculate_checksum(&pure_data);
    if checksum != checksum_data {
        // Tmp file is corrupted
        safe_delete(&dir_path, &tmp_file).await;
        return;
    }
    
    // We have to recover from tmp file.
    let data_file_path = get_sector_data_path(dir_path.clone());
    safe_write(dir_path, &data_file_path, vec![&pure_data]).await;

    let tmp_metadata = tmp_metadata.unwrap();
    let (timestamp, write_rank) = decode_metadata(&tmp_metadata.clone());
    rename(tmp_metadata, 
        get_metadata_path(&dir_path, &timestamp, &write_rank)
        ).await.unwrap();
    sync_path(dir_path).await;

    safe_delete(&dir_path, &tmp_file).await;
}

#[async_trait::async_trait]
impl SectorsManager for BasicSectorsManager {
    
    async fn read_data(&self, idx: u64) -> SectorVec {
        let sector_dir = get_sector_dir(self.config_storage_dir.clone(), idx);
        let sector_data_path = get_sector_data_path(sector_dir);
        let sector_data = tokio::fs::read(sector_data_path).await.unwrap();
        SectorVec(sector_data)
    }

    async fn read_metadata(&self, idx: u64) -> (u64, u8) {
        let sector_dir = get_sector_dir(self.config_storage_dir.clone(), idx);
        let mut meta_files = tokio::fs::read_dir(&sector_dir).await.unwrap();
        let mut metadata = None;
        while let Some(entry) = meta_files.next_entry().await.unwrap() {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("meta") {
                metadata = Some(path);
                break;
            }
        }
        decode_metadata(&metadata.unwrap())
    }

    async fn write(&self, idx: u64, sector: &(SectorVec, u64, u8)) {
        if sector.0.0.len() != 4096 {
            panic!("SectorVec must have 4096 bytes");
        }
        let (data_vec, timestamp, write_rank) = sector;
        let data = &data_vec.0;

        let sector_dir = get_sector_dir(self.config_storage_dir.clone(), idx);
        let sector_data_path = get_sector_data_path(sector_dir.clone());
        let tmp_path = get_tmp_path(sector_data_path.clone());
        let new_metadata = get_metadata_path(&sector_dir, timestamp, write_rank);
        let tmp_new_metadata = get_tmp_path(new_metadata.clone());
        let data_checksum = calculate_checksum(&data);


        if !sector_dir.exists() {
            tokio::fs::create_dir(&sector_dir).await.unwrap();
            sync_path(&self.config_storage_dir).await;
        }
        
        let mut meta_file: Option<PathBuf> = None;
        let mut meta_files = tokio::fs::read_dir(&sector_dir).await.unwrap();
        while let Some(entry) = meta_files.next_entry().await.unwrap() {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("meta") {
                meta_file = Some(path);
                break;
            }
        }

        safe_write(&sector_dir, &tmp_new_metadata, vec![&Vec::new()]).await;

        safe_write(&sector_dir, &tmp_path, vec![&data, &data_checksum]).await;

        safe_write(&sector_dir, &sector_data_path, vec![&data]).await;
        
        rename(tmp_new_metadata, new_metadata).await.unwrap();

        sync_path(&sector_dir).await;

        if let Some(meta_file) = meta_file {
            safe_delete(&sector_dir, &meta_file).await;
        }

        safe_delete(&sector_dir, &tmp_path).await;
    }

}

impl BasicSectorsManager {
    pub(crate) async fn new(storage_dir: &PathBuf) -> BasicSectorsManager {
        let storage = BasicSectorsManager {
            config_storage_dir: storage_dir.clone(),
        };
        storage.full_recovery().await.unwrap();
        storage
    }

    async fn full_recovery(&self) -> Result<(), ()> {
        if !self.config_storage_dir.exists() {
            tokio::fs::create_dir(&self.config_storage_dir).await.unwrap();
        }
        let storage_dir = self.config_storage_dir.clone();
        let mut sectors = tokio::fs::read_dir(&storage_dir).await.unwrap();
        while let Some(entry) = sectors.next_entry().await.unwrap() {
            let sector_dir = entry.path();
            recover_dir(&sector_dir).await;
        }
        Ok(())
    }
}
