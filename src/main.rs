use anyhow::Result;
use axum::{
    body::Bytes,
    extract::{path::Path, DefaultBodyLimit, Query, State},
    http::{
        header::{self, HeaderMap, HeaderName},
        StatusCode,
    },
    response::IntoResponse,
    routing::{get, post, put},
    Json, Router,
};
use bincode::Options;
use clap::{Parser, Subcommand};
use fastbloom::BloomFilter;
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    io::{Cursor, Read},
    path::PathBuf,
    sync::Arc,
};
use tokio::fs;
use url::Url;
use uuid::Uuid;

#[derive(Debug, Parser)]
#[command(name = "fairing")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Server {
        object_store_url: Url,
    },
    #[command(arg_required_else_help = true)]
    Deploy {
        #[arg(long)]
        create_site: bool,

        #[arg(long)]
        stage: bool,

        site_name: String,
        remote: String,
        path: PathBuf,
    },
    #[command(arg_required_else_help = true)]
    Activate {
        site_name: String,
        deployment_id: Uuid,
        remote: String,
    },
}

struct AppState {
    object_store: Arc<dyn ObjectStore>,
    base_path: object_store::path::Path,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let args = Cli::parse();

    match args.command {
        Commands::Server { object_store_url } => {
            let (object_store, base_path) = object_store::parse_url(&object_store_url)?;

            let app_state = Arc::new(AppState {
                object_store: object_store.into(),
                base_path,
            });

            let app = Router::new()
                .route("/sites/:site_name/current/", get(get_file))
                .route("/sites/:site_name/current/*path", get(get_file))
                .route("/sites/:site_name/:deployment_id/", get(get_file))
                .route("/sites/:site_name/:deployment_id/*path", get(get_file))
                .route("/api/v1/deployments", post(create_deployment))
                .route("/api/v1/files", put(create_file))
                .route("/api/v1/sites", put(create_site))
                .route("/api/v1/sites/:site_name", put(update_site))
                .with_state(app_state)
                .layer(DefaultBodyLimit::max(256 * 2_usize.pow(20)));

            let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
            axum::serve(listener, app).await.unwrap();
        }
        Commands::Deploy {
            create_site,
            stage,
            site_name,
            remote,
            path: base_path,
        } => {
            let mut paths = vec![base_path.clone()];
            let mut found_files = vec![];

            while let Some(path) = paths.pop() {
                let mut dir = fs::read_dir(&path).await?;

                while let Some(entry) = dir.next_entry().await? {
                    let file_type = entry.file_type().await?;

                    if file_type.is_dir() {
                        paths.push(entry.path());
                    }

                    if file_type.is_file() {
                        let entry_path = entry.path();
                        let relative_path = entry_path.strip_prefix(&base_path).unwrap().to_owned();

                        let extension = relative_path
                            .extension()
                            .and_then(|ext| ext.to_str())
                            .unwrap_or("");

                        // https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/MIME_types/Common_types
                        const TEXT: &str = "text";
                        const IMAGE: &str = "image";
                        const FONT: &str = "font";
                        const APPLICATION: &str = "application";
                        let content_type = match extension {
                            // text
                            "html" | "htm" => (TEXT, "html"),
                            "css" => (TEXT, "css"),
                            "csv" => (TEXT, "csv"),
                            "ics" => (TEXT, "calendar"),
                            "md" => (TEXT, "markdown"),
                            // image
                            "apng" => (IMAGE, "apng"),
                            "avif" => (IMAGE, "avif"),
                            "bmp" => (IMAGE, "bmp"),
                            "gif" => (IMAGE, "gif"),
                            "ico" => (IMAGE, "vnd.microsoft.icon"),
                            "jpeg" | "jpg" => (IMAGE, "jpeg"),
                            "svg" => (IMAGE, "svg+xml"),
                            "tiff" | "tif" => (IMAGE, "tiff"),
                            "webp" => (IMAGE, "webp"),
                            // font
                            "otf" => (FONT, "otf"),
                            "ttf" => (FONT, "ttf"),
                            "woff" => (FONT, "woff"),
                            "woff2" => (FONT, "woff2"),
                            // application
                            "js" => (APPLICATION, "javascript"),
                            "wasm" => (APPLICATION, "wasm"),
                            "xml" => (APPLICATION, "xml"),
                            "pdf" => (APPLICATION, "pdf"),
                            _ => (APPLICATION, "octet-stream"),
                        };

                        let data = fs::read(entry.path()).await?;

                        let mut metadata = FileMetadata {
                            headers: vec![(
                                "content-type".to_owned(),
                                format!("{}/{}", content_type.0, content_type.1),
                            )],
                            variants: vec![],
                        };
                        let mut variants_data = vec![];

                        let is_compressible = {
                            let sample_data_len = data.len().min(1024);

                            let mut ratio_buf = vec![];
                            flate2::read::GzEncoder::new(
                                &data[..sample_data_len],
                                flate2::Compression::new(6),
                            )
                            .read_to_end(&mut ratio_buf)?;

                            let ratio = sample_data_len as f64 / ratio_buf.len() as f64;

                            // Check if the gzip ratio is good enough to justify the additional
                            // computation.
                            ratio > 1.2
                        };

                        // gzip
                        if is_compressible {
                            let mut gzip_data = vec![];
                            flate2::read::GzEncoder::new(&data[..], flate2::Compression::best())
                                .read_to_end(&mut gzip_data)?;

                            // println!("gzip {relative_path:?} {:.2} {}/{}", data.len() as f64 / gzip_data.len() as f64, data.len(), gzip_data.len());

                            if gzip_data.len() < data.len() {
                                metadata.variants.push(FileMetadataVariant {
                                    len: gzip_data.len() as u64,
                                    headers: vec![(
                                        "content-encoding".to_string(),
                                        "gzip".to_string(),
                                    )],
                                });
                                variants_data.push(gzip_data);
                            }
                        }

                        // zstd
                        if is_compressible && data.len() > 512 * 1024 {
                            let zstd_data = zstd::encode_all(&data[..], 14)?;

                            // println!("zstd {relative_path:?} {:.2} {}/{}", data.len() as f64 / zstd_data.len() as f64, data.len(), zstd_data.len());

                            if zstd_data.len() < data.len() {
                                metadata.variants.push(FileMetadataVariant {
                                    len: zstd_data.len() as u64,
                                    headers: vec![(
                                        "content-encoding".to_string(),
                                        "zstd".to_string(),
                                    )],
                                });
                                variants_data.push(zstd_data);
                            }
                        }

                        if variants_data.is_empty() {
                            metadata.variants.push(FileMetadataVariant {
                                len: data.len() as u64,
                                headers: vec![],
                            });
                            variants_data.push(data);
                        }

                        let mut data_with_header = vec![1];

                        let bincode_options = bincode::options().allow_trailing_bytes();
                        bincode_options.serialize_into(
                            &mut data_with_header,
                            &FileMetadataFormat::V1Alpha1 {
                                headers: metadata.headers,
                                variants: metadata.variants,
                            },
                        )?;

                        for variant_data in variants_data {
                            data_with_header.extend_from_slice(&variant_data);
                        }

                        let hash = blake3::hash(&data_with_header);

                        let mut web_path = String::new();

                        for part in relative_path.iter() {
                            web_path.push_str("/");
                            web_path.push_str(part.to_str().unwrap());
                        }

                        let index_path = web_path
                            .strip_suffix("/index.html")
                            .or_else(|| web_path.strip_suffix("/index.htm"));

                        if let Some(index_path) = index_path {
                            if !index_path.is_empty() {
                                // todo: add a redirect here
                            }

                            found_files.push((
                                format!("{index_path}/"),
                                hash.to_hex().to_string(),
                                data_with_header.clone(),
                            ));
                        }

                        found_files.push((web_path, hash.to_hex().to_string(), data_with_header));
                    }
                }
            }

            println!("Found {} files to upload.", found_files.len());

            let client = reqwest::Client::new();

            if create_site {
                client
                    .put(format!("{remote}/api/v1/sites"))
                    .json(&CreateSite {
                        name: site_name.clone(),
                    })
                    .send()
                    .await?
                    .error_for_status()
                    .unwrap();
            }

            let deployment = client
                .post(format!("{remote}/api/v1/deployments"))
                .json(&CreateDeployment {
                    site_name: site_name.clone(),
                    files: found_files
                        .iter()
                        .map(|(path, hash, _)| CreateDeploymentFiles {
                            path: path.clone(),
                            hash: hash.clone(),
                        })
                        .collect(),
                    ignore_paths: vec![],
                })
                .send()
                .await?
                .json::<Deployment>()
                .await?;

            let upload_limit = Arc::new(tokio::sync::Semaphore::new(4));
            let mut upload_tasks = vec![];

            for file_to_upload in deployment.files_to_upload {
                let (_, _, data_with_header) = found_files
                    .iter()
                    .find(|(path, _, _)| path == &file_to_upload)
                    .unwrap();

                let upload_limit = upload_limit.clone();

                let res = client
                    .put(format!("{remote}/api/v1/files"))
                    .query(&CreateFileQuery {
                        site_name: site_name.clone(),
                        deployment_id: deployment.id,
                        path: file_to_upload,
                    })
                    .body(data_with_header.clone())
                    .send();

                let task = tokio::spawn(async move {
                    let upload_permit = upload_limit.acquire().await.unwrap();

                    let res = res.await;

                    drop(upload_permit);

                    res
                });

                upload_tasks.push(task);
            }

            for upload_task in upload_tasks {
                upload_task.await??;
            }

            let update_site = if stage {
                UpdateSite {
                    finalize_deployment_id: Some(deployment.id),
                    current_deployment_id: None,
                }
            } else {
                UpdateSite {
                    finalize_deployment_id: Some(deployment.id),
                    current_deployment_id: Some(deployment.id),
                }
            };

            client
                .put(format!("{remote}/api/v1/sites/{site_name}"))
                .json(&update_site)
                .send()
                .await?
                .error_for_status()
                .unwrap();

            if stage {
                println!(
                    "Staged deployment ({}) to {site_name}.",
                    deployment.id.hyphenated()
                );
            } else {
                println!("Deployed ({}) to {site_name}.", deployment.id.hyphenated());
            }
        }
        Commands::Activate {
            site_name,
            deployment_id,
            remote,
        } => {
            let client = reqwest::Client::new();

            client
                .put(format!("{remote}/api/v1/sites/{site_name}"))
                .json(&UpdateSite {
                    finalize_deployment_id: None,
                    current_deployment_id: Some(deployment_id),
                })
                .send()
                .await?
                .error_for_status()
                .unwrap();

            println!("Deployed ({}) to {site_name}.", deployment_id.hyphenated());
        }
    }

    Ok(())
}

#[derive(Debug, Deserialize, Serialize)]
enum FileMetadataFormat {
    V1Alpha1 {
        headers: Vec<(String, String)>,
        variants: Vec<FileMetadataVariant>,
    },
}

#[derive(Debug)]
struct FileMetadata {
    headers: Vec<(String, String)>,
    variants: Vec<FileMetadataVariant>,
}

#[derive(Debug, Deserialize, Serialize)]
struct FileMetadataVariant {
    len: u64,
    headers: Vec<(String, String)>,
}

#[derive(Debug, Deserialize)]
struct GetFilePath {
    site_name: String,
    #[serde(default)]
    deployment_id: Option<Uuid>,
    #[serde(default)]
    path: String,
}

async fn get_file(
    State(state): State<Arc<AppState>>,
    req_headers: HeaderMap,
    Path(path): Path<GetFilePath>,
) -> impl IntoResponse {
    let mut headers = HeaderMap::new();
    headers.insert(header::X_CONTENT_TYPE_OPTIONS, "nosniff".parse().unwrap());

    let mut full_path = String::with_capacity(path.path.len() + 1);
    full_path.push_str("/");
    full_path.push_str(&path.path);

    let site_path = state
        .base_path
        .child("sites")
        .child(path.site_name.as_str());
    let site_metadata_path = site_path.child("metadata.bc");

    let site_metadata: SiteMetadataFormat = match state.object_store.get(&site_metadata_path).await
    {
        Ok(site_metadata_obj) => {
            let site_metadata_raw = site_metadata_obj.bytes().await.unwrap();
            bincode::options().deserialize(&site_metadata_raw).unwrap()
        }
        Err(object_store::Error::NotFound { .. }) => {
            headers.insert(header::CONTENT_TYPE, "text/plain".parse().unwrap());
            return (StatusCode::NOT_FOUND, headers, b"site not found".to_vec());
        }
        Err(err) => {
            tracing::error!("get_file: metadata {err:?}");

            headers.insert(header::CONTENT_TYPE, "text/plain".parse().unwrap());
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                headers,
                b"internal error".to_vec(),
            );
        }
    };

    let site_metadata = match site_metadata {
        SiteMetadataFormat::V1Alpha1 {
            current_deployment_id,
            finalized_deployment_ids,
            bloom_filter_seed,
            bloom_filter,
        } => SiteMetadata {
            current_deployment_id,
            finalized_deployment_ids,
            bloom_filter_seed,
            bloom_filter,
        },
    };

    let path_hash = blake3::hash(full_path.as_bytes());
    let path_hash_hex = path_hash.to_hex();

    let deployment_id = if let Some(deployment_id) = path.deployment_id {
        if site_metadata
            .finalized_deployment_ids
            .contains(&deployment_id)
        {
            deployment_id
        } else {
            headers.insert(header::CONTENT_TYPE, "text/plain".parse().unwrap());
            return (
                StatusCode::NOT_FOUND,
                headers,
                b"deployment not found or not finalized".to_vec(),
            );
        }
    } else if let Some(current_deployment_id) = site_metadata.current_deployment_id {
        current_deployment_id
    } else {
        headers.insert(header::CONTENT_TYPE, "text/plain".parse().unwrap());
        return (
            StatusCode::NOT_FOUND,
            headers,
            b"site has no current deployment".to_vec(),
        );
    };

    let deployment_index = site_metadata
        .finalized_deployment_ids
        .iter()
        .position(|&v| v == deployment_id)
        .unwrap();

    let bloom_filter = BloomFilter::from_vec(site_metadata.bloom_filter)
        .seed(&site_metadata.bloom_filter_seed)
        .hashes(20);

    let mut file_raw = None;

    for deployment_id in &site_metadata.finalized_deployment_ids[deployment_index..] {
        if !bloom_filter.contains(&(deployment_id, path_hash)) {
            continue;
        }

        let file_path = site_path
            .child("deployments")
            .child(deployment_id.to_string())
            .child("files")
            .child(&path_hash_hex[..2])
            .child(&path_hash_hex[2..]);

        match state.object_store.get(&file_path).await {
            Ok(file_obj) => {
                file_raw = Some(file_obj.bytes().await.unwrap().to_vec());
                break;
            }
            Err(object_store::Error::NotFound { .. }) => (),
            Err(err) => {
                tracing::error!("get_file: file {err:?}");

                headers.insert(header::CONTENT_TYPE, "text/plain".parse().unwrap());
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    headers,
                    b"internal error".to_vec(),
                );
            }
        };
    }

    let Some(file_raw) = file_raw else {
        headers.insert(header::CONTENT_TYPE, "text/plain".parse().unwrap());
        return (StatusCode::NOT_FOUND, headers, b"not found".to_vec());
    };

    let mut file_cursor = Cursor::new(&file_raw[1..]);

    let file_header: FileMetadataFormat = bincode::options()
        .allow_trailing_bytes()
        .deserialize_from(&mut file_cursor)
        .unwrap();

    let header_length = 1 + file_cursor.position() as usize;

    let file_header = match file_header {
        FileMetadataFormat::V1Alpha1 { headers, variants } => FileMetadata { headers, variants },
    };

    for (header_name, header_value) in file_header.headers.iter() {
        headers.insert(
            HeaderName::try_from(header_name).unwrap(),
            header_value.parse().unwrap(),
        );
    }

    let largest_file_size = file_header
        .variants
        .iter()
        .map(|variant| variant.len)
        .max()
        .unwrap();

    let accept_encoding = req_headers
        .get(header::ACCEPT_ENCODING)
        .and_then(|v| v.to_str().ok());

    let mut variant_ratings = file_header
        .variants
        .iter()
        .scan(0u64, |offset, variant| {
            let variant_offset = *offset;
            *offset += variant.len;
            Some((variant_offset, variant))
        })
        .map(|(offset, variant)| {
            let size_preference = ((variant.len as f64 / largest_file_size as f64) * 100.0) as u32;

            let content_encoding_header = variant
                .headers
                .iter()
                .find(|(name, _value)| name.eq_ignore_ascii_case("content-encoding"));

            if let Some((_, content_encoding)) = content_encoding_header {
                let accepts_encoding = accept_encoding
                    .and_then(|accept_encoding| {
                        accept_encoding
                            .split(",")
                            .map(|v| v.trim())
                            .find(|v| v.eq_ignore_ascii_case(content_encoding))
                    })
                    .is_some();

                if accepts_encoding {
                    (size_preference, offset, variant)
                } else {
                    (100 + size_preference, offset, variant)
                }
            } else {
                (100 + size_preference, offset, variant)
            }
        })
        .collect::<Vec<_>>();

    variant_ratings.sort_by_key(|&(rating, _, _)| rating);

    let &(_, offset, variant) = variant_ratings.first().unwrap();

    for (header_name, header_value) in variant.headers.iter() {
        headers.insert(
            HeaderName::try_from(header_name).unwrap(),
            header_value.parse().unwrap(),
        );
    }

    (
        StatusCode::OK,
        headers,
        file_raw[header_length + offset as usize
            ..header_length + offset as usize + variant.len as usize]
            .to_owned(),
    )
}

async fn create_deployment(
    State(state): State<Arc<AppState>>,
    Json(create_deployment): Json<CreateDeployment>,
) -> (StatusCode, Json<Deployment>) {
    use arrow::array::{
        cast::downcast_array, ArrayRef, BooleanArray, FixedSizeBinaryArray, RecordBatch,
        StringArray,
    };
    use futures::TryStreamExt;
    use parquet::{
        arrow::{
            arrow_reader::{ArrowPredicateFn, RowFilter},
            async_reader::ParquetObjectReader,
            async_writer::AsyncArrowWriter,
            ParquetRecordBatchStreamBuilder, ProjectionMask,
        },
        basic::Compression,
        file::properties::WriterProperties,
        format::SortingColumn,
    };

    let site_path = state
        .base_path
        .child("sites")
        .child(create_deployment.site_name.as_str());
    let site_metadata_path = site_path.child("metadata.bc");

    let bincode_options = bincode::options();

    let site_metadata = match state.object_store.get(&site_metadata_path).await {
        Ok(site_metadata_obj) => {
            let site_metadata_raw = site_metadata_obj.bytes().await.unwrap();
            let site_metadata: SiteMetadataFormat =
                bincode_options.deserialize(&site_metadata_raw).unwrap();

            site_metadata
        }
        Err(object_store::Error::NotFound { .. }) => {
            panic!();
            // return StatusCode::NOT_FOUND;
        }
        Err(err) => {
            tracing::error!("get_file: metadata {err:?}");
            panic!();
            // return StatusCode::INTERNAL_SERVER_ERROR;
        }
    };

    let site_metadata = match site_metadata {
        SiteMetadataFormat::V1Alpha1 {
            current_deployment_id,
            finalized_deployment_ids,
            bloom_filter_seed,
            bloom_filter,
        } => SiteMetadata {
            current_deployment_id,
            finalized_deployment_ids,
            bloom_filter_seed,
            bloom_filter,
        },
    };

    let mut files_to_upload = create_deployment
        .files
        .iter()
        .map(|file| {
            (
                file.path.clone(),
                blake3::Hash::from_hex(&file.hash).unwrap(),
            )
        })
        .collect::<HashMap<_, _>>();

    let bloom_filter = BloomFilter::from_vec(site_metadata.bloom_filter)
        .seed(&site_metadata.bloom_filter_seed)
        .hashes(20);

    let _base_deployment_id = site_metadata.finalized_deployment_ids.first().cloned();

    for old_deployment_id in site_metadata.finalized_deployment_ids.iter() {
        let found_files = create_deployment
            .files
            .iter()
            .filter_map(|file| {
                let path_hash = blake3::hash(&file.path.as_bytes());

                if bloom_filter.contains(&(old_deployment_id, path_hash)) {
                    Some(file.path.clone())
                } else {
                    None
                }
            })
            .collect::<HashSet<_>>();

        if found_files.is_empty() {
            continue;
        }

        let deployment_dir_path = site_path
            .child("deployments")
            .child(old_deployment_id.to_string());

        let deployment_files_path = deployment_dir_path.child("files.parquet");

        let deployment_files_meta = state
            .object_store
            .head(&deployment_files_path)
            .await
            .unwrap();

        let reader = ParquetObjectReader::new(state.object_store.clone(), deployment_files_meta);
        let builder = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .unwrap()
            .with_batch_size(1024);

        let file_metadata = builder.metadata().file_metadata();

        /*
        let scalar = StringArray::from(
            found_files
                .iter()
                .map(|file| file.path.clone())
                .collect::<Vec<_>>(),
        );
        */

        let filter = ArrowPredicateFn::new(
            ProjectionMask::roots(file_metadata.schema_descr(), [0]),
            move |record_batch| {
                let path_array: StringArray = downcast_array(record_batch.column(0));
                Ok(BooleanArray::from_unary(&path_array, |path| {
                    found_files.contains(path)
                }))
            },
        );

        let mask = ProjectionMask::roots(file_metadata.schema_descr(), [0, 1]);
        let builder = builder
            .with_projection(mask)
            .with_row_filter(RowFilter::new(vec![Box::new(filter)]));

        let mut stream = builder.build().unwrap();

        while let Some(file) = stream.try_next().await.unwrap() {
            let path_array: StringArray = downcast_array(file.column(0));
            let hash_array: FixedSizeBinaryArray = downcast_array(file.column(1));

            for (path, hash) in path_array.iter().zip(hash_array.iter()) {
                let (Some(path), Some(hash)) = (path, hash) else {
                    continue;
                };

                let Some(new_hash) = files_to_upload.get(path) else {
                    continue;
                };

                if new_hash.as_bytes() == hash {
                    files_to_upload.remove(path);
                }
            }
        }
    }

    let files_to_upload = files_to_upload.into_iter().collect::<Vec<_>>();

    let deployment_id = Uuid::now_v7();

    let deployment_dir_path = site_path
        .child("deployments")
        .child(deployment_id.to_string());

    let deployment_path = deployment_dir_path.child("metadata.json");

    let deployment_files_path = deployment_dir_path.child("files.parquet");

    // TODO: store the base id
    let deployment_stage_meta = serde_json::to_vec(&create_deployment).unwrap();

    // println!("writing {} bytes json", deployment_stage_meta.len());

    // metadata.json must be created first to avoid data races.
    state
        .object_store
        .put_opts(
            &deployment_path,
            deployment_stage_meta.into(),
            object_store::PutOptions {
                mode: object_store::PutMode::Create,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let mut files = create_deployment.files;
    files.sort_by(|a, b| a.path.cmp(&b.path));

    let sample_path = Arc::new(StringArray::new_null(0)) as ArrayRef;
    let sample_hash = Arc::new(FixedSizeBinaryArray::new_null(32, 0)) as ArrayRef;
    let sample =
        RecordBatch::try_from_iter([("path", sample_path), ("hash", sample_hash)]).unwrap();

    let writer_props = WriterProperties::builder()
        .set_compression(Compression::LZ4_RAW)
        .set_sorting_columns(Some(vec![SortingColumn::new(0, false, false)]))
        .build();

    let mut buffer = Vec::new();
    let mut writer =
        AsyncArrowWriter::try_new(&mut buffer, sample.schema(), Some(writer_props)).unwrap();

    for chunk in files_to_upload.chunks(1024) {
        let paths = Arc::new(StringArray::from_iter_values(
            chunk.iter().map(|(path, _hash)| path.clone()),
        )) as ArrayRef;

        let hashes = Arc::new(
            FixedSizeBinaryArray::try_from_iter(
                chunk.iter().map(|(_path, hash)| hash.as_bytes().to_vec()),
            )
            .unwrap(),
        ) as ArrayRef;

        let batch = RecordBatch::try_from_iter([("path", paths), ("hash", hashes)]).unwrap();

        writer.write(&batch).await.unwrap();
    }

    writer.close().await.unwrap();

    // println!("writing {} bytes parquet", buffer.len());

    state
        .object_store
        .put(&deployment_files_path, buffer.into())
        .await
        .unwrap();

    let deployment = Deployment {
        id: deployment_id,
        files_to_upload: files_to_upload
            .into_iter()
            .map(|(path, _hash)| path)
            .collect(),
    };

    (StatusCode::CREATED, Json(deployment))
}

async fn create_file(
    State(state): State<Arc<AppState>>,
    Query(query): Query<CreateFileQuery>,
    body: Bytes,
) -> StatusCode {
    use arrow::{
        array::{cast::downcast_array, FixedSizeBinaryArray, Scalar, StringArray},
        compute::kernels::cmp::eq,
    };
    use futures::TryStreamExt;
    use parquet::arrow::{
        arrow_reader::{ArrowPredicateFn, RowFilter},
        async_reader::ParquetObjectReader,
        ParquetRecordBatchStreamBuilder, ProjectionMask,
    };

    let deployment_dir_path = state
        .base_path
        .child("sites")
        .child(query.site_name.as_str())
        .child("deployments")
        .child(query.deployment_id.to_string());

    let deployment_files_path = deployment_dir_path.child("files.parquet");

    let deployment_files_meta = state
        .object_store
        .head(&deployment_files_path)
        .await
        .unwrap();

    let reader = ParquetObjectReader::new(state.object_store.clone(), deployment_files_meta);
    let builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .unwrap()
        .with_batch_size(1024);

    let file_metadata = builder.metadata().file_metadata();

    let scalar = StringArray::from(vec![query.path.clone()]);
    let filter = ArrowPredicateFn::new(
        ProjectionMask::roots(file_metadata.schema_descr(), [0]),
        move |record_batch| eq(record_batch.column(0), &Scalar::new(&scalar)),
    );

    let mask = ProjectionMask::roots(file_metadata.schema_descr(), [1]);
    let builder = builder
        .with_projection(mask)
        .with_row_filter(RowFilter::new(vec![Box::new(filter)]));

    let stream = builder.build().unwrap();
    let results = stream.try_collect::<Vec<_>>().await.unwrap();

    let Some(file) = results.first() else {
        return StatusCode::BAD_REQUEST;
    };

    let hash_array: FixedSizeBinaryArray = downcast_array(file.column(0));
    let file_hash = hash_array.value(0);

    let hash = blake3::hash(&body);

    if hash.as_bytes() != file_hash {
        return StatusCode::BAD_REQUEST;
    }

    let path_hash = blake3::hash(&query.path.as_bytes());
    let path_hash_hex = path_hash.to_hex();

    let file_path = deployment_dir_path
        .child("files")
        .child(&path_hash_hex[..2])
        .child(&path_hash_hex[2..]);

    state
        .object_store
        .put(&file_path, body.into())
        .await
        .unwrap();

    StatusCode::CREATED
}

async fn create_site(
    State(state): State<Arc<AppState>>,
    Json(create_site): Json<CreateSite>,
) -> StatusCode {
    let site_metadata_path = state
        .base_path
        .child("sites")
        .child(create_site.name.as_str())
        .child("metadata.bc");

    let site_metadata = SiteMetadataFormat::V1Alpha1 {
        current_deployment_id: None,
        finalized_deployment_ids: vec![],
        bloom_filter_seed: Uuid::new_v4().as_u128(),
        bloom_filter: vec![0u64; 512 * 1024 / 8],
    };

    let site_metadata_raw = bincode::options().serialize(&site_metadata).unwrap();

    let res = state
        .object_store
        .put_opts(
            &site_metadata_path,
            site_metadata_raw.into(),
            object_store::PutOptions {
                mode: object_store::PutMode::Create,
                ..Default::default()
            },
        )
        .await;

    match res {
        Ok(_) => StatusCode::CREATED,
        Err(object_store::Error::AlreadyExists { .. }) => StatusCode::OK,
        Err(err) => {
            tracing::error!("create_site: {err:?}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

async fn update_site(
    State(state): State<Arc<AppState>>,
    Path(site_name): Path<String>,
    Json(update_site): Json<UpdateSite>,
) -> StatusCode {
    use arrow::array::{cast::downcast_array, StringArray};
    use futures::TryStreamExt;
    use parquet::arrow::{
        async_reader::ParquetObjectReader, ParquetRecordBatchStreamBuilder, ProjectionMask,
    };

    let site_path = state.base_path.child("sites").child(site_name.as_str());
    let site_metadata_path = site_path.child("metadata.bc");

    let bincode_options = bincode::options();

    let (site_metadata, site_metadata_obj_meta) =
        match state.object_store.get(&site_metadata_path).await {
            Ok(site_metadata_obj) => {
                let site_metadata_obj_meta = object_store::UpdateVersion {
                    e_tag: site_metadata_obj.meta.e_tag.clone(),
                    version: site_metadata_obj.meta.version.clone(),
                };

                let site_metadata_raw = site_metadata_obj.bytes().await.unwrap();
                let site_metadata: SiteMetadataFormat =
                    bincode_options.deserialize(&site_metadata_raw).unwrap();

                (site_metadata, site_metadata_obj_meta)
            }
            Err(object_store::Error::NotFound { .. }) => {
                return StatusCode::NOT_FOUND;
            }
            Err(err) => {
                tracing::error!("get_file: metadata {err:?}");
                return StatusCode::INTERNAL_SERVER_ERROR;
            }
        };

    let mut site_metadata = match site_metadata {
        SiteMetadataFormat::V1Alpha1 {
            current_deployment_id,
            finalized_deployment_ids,
            bloom_filter_seed,
            bloom_filter,
        } => SiteMetadata {
            current_deployment_id,
            finalized_deployment_ids,
            bloom_filter_seed,
            bloom_filter,
        },
    };

    if let Some(finalize_deployment_id) = update_site.finalize_deployment_id {
        let deployment_dir_path = site_path
            .child("deployments")
            .child(finalize_deployment_id.to_string());

        let deployment_metadata_path = deployment_dir_path.child("metadata.json");

        let deployment_metadata_obj = state
            .object_store
            .get(&deployment_metadata_path)
            .await
            .unwrap();

        let deployment_metadata = deployment_metadata_obj.bytes().await.unwrap();

        let _deployment_metadata: CreateDeployment =
            serde_json::from_slice(&deployment_metadata).unwrap();

        // todo: verify if the deployment is stale.

        let deployment_files_path = deployment_dir_path.child("files.parquet");

        let deployment_files_meta = state
            .object_store
            .head(&deployment_files_path)
            .await
            .unwrap();

        let reader = ParquetObjectReader::new(state.object_store.clone(), deployment_files_meta);
        let builder = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .unwrap()
            .with_batch_size(1024);

        let file_metadata = builder.metadata().file_metadata();

        let mask = ProjectionMask::roots(file_metadata.schema_descr(), [0]);
        let builder = builder.with_projection(mask);

        let mut stream = builder.build().unwrap();

        let mut bloom_filter = BloomFilter::from_vec(site_metadata.bloom_filter)
            .seed(&site_metadata.bloom_filter_seed)
            .hashes(20);

        while let Some(file) = stream.try_next().await.unwrap() {
            let path_array: StringArray = downcast_array(file.column(0));

            for path in path_array.iter().filter_map(|path| path) {
                let path_hash = blake3::hash(&path.as_bytes());
                let path_hash_hex = path_hash.to_hex();

                let file_path = deployment_dir_path
                    .child("files")
                    .child(&path_hash_hex[..2])
                    .child(&path_hash_hex[2..]);

                match state.object_store.head(&file_path).await {
                    Ok(_) => (),
                    Err(object_store::Error::NotFound { .. }) => {
                        return StatusCode::BAD_REQUEST;
                    }
                    Err(err) => {
                        tracing::error!("get_file: metadata {err:?}");
                        return StatusCode::INTERNAL_SERVER_ERROR;
                    }
                };

                bloom_filter.insert(&(finalize_deployment_id, path_hash));
            }
        }

        site_metadata
            .finalized_deployment_ids
            .push(finalize_deployment_id);
        site_metadata.finalized_deployment_ids.rotate_right(1);
        site_metadata.bloom_filter = bloom_filter.as_slice().to_vec();
    }

    if let Some(current_deployment_id) = update_site.current_deployment_id {
        if site_metadata
            .finalized_deployment_ids
            .contains(&current_deployment_id)
        {
            site_metadata.current_deployment_id = Some(current_deployment_id);
        } else {
            return StatusCode::BAD_REQUEST;
        }
    }

    let site_metadata_raw = bincode_options
        .serialize(&SiteMetadataFormat::V1Alpha1 {
            current_deployment_id: site_metadata.current_deployment_id,
            finalized_deployment_ids: site_metadata.finalized_deployment_ids,
            bloom_filter_seed: site_metadata.bloom_filter_seed,
            bloom_filter: site_metadata.bloom_filter,
        })
        .unwrap();

    state
        .object_store
        .put_opts(
            &site_metadata_path,
            site_metadata_raw.into(),
            object_store::PutOptions {
                mode: object_store::PutMode::Update(site_metadata_obj_meta),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    StatusCode::OK
}

#[derive(Debug, Deserialize, Serialize)]
struct CreateDeployment {
    site_name: String,
    files: Vec<CreateDeploymentFiles>,

    #[serde(default)]
    ignore_paths: Vec<()>,
}

#[derive(Debug, Deserialize, Serialize)]
struct CreateDeploymentFiles {
    path: String,
    hash: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct Deployment {
    id: Uuid,
    files_to_upload: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
enum DeploymentMetadataFormat {
    V1Alpha1 {
        id: Uuid,
        base_deployment_id: Option<Uuid>,
    },
}

#[derive(Debug, Deserialize, Serialize)]
struct CreateFileQuery {
    site_name: String,
    deployment_id: Uuid,
    path: String,
}

#[derive(Debug, Deserialize, Serialize)]
enum SiteMetadataFormat {
    V1Alpha1 {
        current_deployment_id: Option<Uuid>,
        finalized_deployment_ids: Vec<Uuid>,
        bloom_filter_seed: u128,
        bloom_filter: Vec<u64>,
    },
}

#[derive(Debug)]
struct SiteMetadata {
    current_deployment_id: Option<Uuid>,
    finalized_deployment_ids: Vec<Uuid>,
    bloom_filter_seed: u128,
    bloom_filter: Vec<u64>,
}

#[derive(Debug, Deserialize, Serialize)]
struct CreateSite {
    name: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct UpdateSite {
    #[serde(default)]
    finalize_deployment_id: Option<Uuid>,

    #[serde(default)]
    current_deployment_id: Option<Uuid>,
}
