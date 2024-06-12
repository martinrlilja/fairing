use anyhow::Result;
use axum::{
    body::Bytes,
    extract::{path::Path, DefaultBodyLimit, Query},
    http::{
        header::{self, HeaderMap},
        StatusCode,
    },
    response::IntoResponse,
    routing::{get, post, put},
    Json, Router,
};
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use std::{io::ErrorKind, path::PathBuf};
use tokio::fs;
use uuid::Uuid;

#[derive(Debug, Parser)]
#[command(name = "fairing")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Server,
    #[command(arg_required_else_help = true)]
    Push {
        site_name: String,
        remote: String,
        path: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let args = Cli::parse();

    match args.command {
        Commands::Server => {
            let app = Router::new()
                .route("/sites/:site_name/current/", get(get_file))
                .route("/sites/:site_name/current/*path", get(get_file))
                .route("/sites/:site_name/:deployment_id/", get(get_file))
                .route("/sites/:site_name/:deployment_id/*path", get(get_file))
                .route("/api/v1/deployments", post(create_deployment))
                .route("/api/v1/files", post(create_file))
                .route("/api/v1/sites/:site_name", put(update_site))
                .layer(DefaultBodyLimit::max(256 * 2_usize.pow(20)));

            let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
            axum::serve(listener, app).await.unwrap();
        }
        Commands::Push {
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
                        let data = fs::read(entry.path()).await?;

                        let hash = blake3::hash(&data);

                        let entry_path = entry.path();
                        let mut relative_path =
                            entry_path.strip_prefix(&base_path).unwrap().to_owned();

                        if relative_path.ends_with("index.html")
                            || relative_path.ends_with("index.htm")
                        {
                            relative_path = relative_path.parent().unwrap().to_owned();
                        }

                        let mut web_path = String::new();

                        for part in relative_path.iter() {
                            web_path.push_str("/");
                            web_path.push_str(part.to_str().unwrap());
                        }

                        if web_path.is_empty() {
                            web_path.push_str("/");
                        }

                        found_files.push((web_path, entry.path(), hash.to_hex().to_string()));
                    }
                }
            }

            let client = reqwest::Client::new();

            let deployment = client
                .post(format!("{remote}/api/v1/deployments"))
                .json(&CreateDeployment {
                    site_name: site_name.clone(),
                    files: found_files
                        .iter()
                        .map(|(path, _, hash)| CreateDeploymentFiles {
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

            for file_to_upload in deployment.files_to_upload {
                let (_, fs_path, _) = found_files
                    .iter()
                    .find(|(path, _, _)| path == &file_to_upload)
                    .unwrap();

                client
                    .post(format!("{remote}/api/v1/files"))
                    .query(&CreateFileQuery {
                        site_name: site_name.clone(),
                        deployment_id: deployment.id,
                        path: file_to_upload,
                    })
                    .body(fs::read(&fs_path).await.unwrap())
                    .send()
                    .await?
                    .error_for_status()
                    .unwrap();
            }

            client
                .put(format!("{remote}/api/v1/sites/{site_name}"))
                .json(&UpdateSite {
                    finalize_deployment_id: Some(deployment.id),
                    current_deployment_id: Some(deployment.id),
                })
                .send()
                .await?
                .error_for_status()
                .unwrap();

            println!("Deployed {}", deployment.id.hyphenated());
        }
    }

    Ok(())
}

#[derive(Debug, Deserialize)]
struct GetFilePath {
    site_name: String,
    #[serde(default)]
    deployment_id: Option<Uuid>,
    #[serde(default)]
    path: String,
}

async fn get_file(Path(path): Path<GetFilePath>) -> impl IntoResponse {
    let mut headers = HeaderMap::new();

    let mut full_path = String::with_capacity(path.path.len() + 1);
    full_path.push_str("/");
    full_path.push_str(&path.path);

    let site_path = {
        let mut site_path = PathBuf::from(".data/sites");
        site_path.push(&path.site_name);
        site_path
    };

    let metadata_path = {
        let mut metadata_path = site_path.clone();
        metadata_path.push("metadata.json");
        metadata_path
    };

    let site_metadata: SiteMetadata = if fs::try_exists(&metadata_path).await.unwrap() {
        serde_json::from_slice(&fs::read(&metadata_path).await.unwrap()).unwrap()
    } else {
        headers.insert(header::CONTENT_TYPE, "text/plain".parse().unwrap());
        return (StatusCode::NOT_FOUND, headers, b"site not found".to_vec());
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

    let mut file_path = site_path.clone();
    file_path.push("files");
    file_path.push(deployment_id.to_string());
    file_path.push(&path_hash_hex[..2]);
    file_path.push(&path_hash_hex[2..]);

    let bytes = fs::read(&file_path).await.unwrap();

    let (_, ext) = path.path.rsplit_once(".").unwrap_or(("", ""));

    let mime = match ext {
        "html" | "htm" | "" => "text/html",
        "jpeg" | "jpg" => "image/jpeg",
        _ => "application/octet-stream",
    };

    headers.insert(header::CONTENT_TYPE, mime.parse().unwrap());

    (StatusCode::OK, headers, bytes)
}

async fn create_deployment(
    Json(create_deployment): Json<CreateDeployment>,
) -> (StatusCode, Json<Deployment>) {
    let deployment = Deployment {
        id: Uuid::now_v7(),
        files_to_upload: create_deployment
            .files
            .iter()
            .map(|file| file.path.clone())
            .collect(),
    };

    let mut stage_dir_path = PathBuf::from(".data/sites");
    stage_dir_path.push(&create_deployment.site_name);
    stage_dir_path.push("stage");

    fs::create_dir_all(&stage_dir_path).await.unwrap();

    let mut stage_path = stage_dir_path.clone();
    stage_path.push(format!("{}.json", deployment.id.to_string()));

    let deployment_stage_meta = serde_json::to_vec(&create_deployment).unwrap();

    fs::write(&stage_path, deployment_stage_meta).await.unwrap();

    (StatusCode::CREATED, Json(deployment))
}

async fn create_file(Query(query): Query<CreateFileQuery>, body: Bytes) -> StatusCode {
    let site_path = {
        let mut site_path = PathBuf::from(".data/sites");
        site_path.push(&query.site_name);
        site_path
    };

    let stage_path = {
        let mut stage_path = site_path.clone();
        stage_path.push("stage");
        stage_path.push(format!("{}.json", query.deployment_id.to_string()));
        stage_path
    };

    let deployment_stage_meta: CreateDeployment =
        serde_json::from_slice(&fs::read(stage_path).await.unwrap()).unwrap();

    let Some(file) = deployment_stage_meta
        .files
        .iter()
        .find(|f| f.path == query.path)
    else {
        return StatusCode::BAD_REQUEST;
    };

    let hash = blake3::hash(&body);

    if hash.to_hex().to_string() != file.hash {
        return StatusCode::BAD_REQUEST;
    }

    let path_hash = blake3::hash(&query.path.as_bytes());
    let path_hash_hex = path_hash.to_hex();

    let file_dir_path = {
        let mut file_dir_path = site_path.clone();
        file_dir_path.push("files");
        file_dir_path.push(query.deployment_id.to_string());
        file_dir_path.push(&path_hash_hex[..2]);
        file_dir_path
    };

    fs::create_dir_all(&file_dir_path).await.unwrap();

    let file_path = {
        let mut file_path = file_dir_path.clone();
        file_path.push(&path_hash_hex[2..]);
        file_path
    };

    let tmp_file_path = file_path.with_extension(".tmp");

    fs::write(&tmp_file_path, body).await.unwrap();

    fs::rename(&tmp_file_path, &file_path).await.unwrap();

    StatusCode::CREATED
}

async fn update_site(
    Path(site_name): Path<String>,
    Json(update_site): Json<UpdateSite>,
) -> StatusCode {
    let site_path = {
        let mut site_path = PathBuf::from(".data/sites");
        site_path.push(&site_name);
        site_path
    };

    let metadata_path = {
        let mut metadata_path = site_path.clone();
        metadata_path.push("metadata.json");
        metadata_path
    };

    let mut site_metadata = match fs::read(&metadata_path).await {
        Ok(metadata_raw) => serde_json::from_slice(&metadata_raw).unwrap(),
        Err(err) if err.kind() == ErrorKind::NotFound => SiteMetadata {
            current_deployment_id: None,
            finalized_deployment_ids: vec![],
        },
        Err(err) => panic!("{err:?}"),
    };

    if let Some(finalize_deployment_id) = update_site.finalize_deployment_id {
        let stage_path = {
            let mut stage_path = site_path.clone();
            stage_path.push("stage");
            stage_path.push(format!("{}.json", finalize_deployment_id.to_string()));
            stage_path
        };

        let deployment_stage_meta: CreateDeployment =
            serde_json::from_slice(&fs::read(stage_path).await.unwrap()).unwrap();

        for file in deployment_stage_meta.files.iter() {
            let path_hash = blake3::hash(&file.path.as_bytes());
            let path_hash_hex = path_hash.to_hex();

            let file_path = {
                let mut file_path = site_path.clone();
                file_path.push("files");
                file_path.push(finalize_deployment_id.to_string());
                file_path.push(&path_hash_hex[..2]);
                file_path.push(&path_hash_hex[2..]);
                file_path
            };

            if !fs::try_exists(&file_path).await.unwrap() {
                return StatusCode::BAD_REQUEST;
            }
        }

        site_metadata
            .finalized_deployment_ids
            .push(finalize_deployment_id);
        site_metadata.finalized_deployment_ids.rotate_right(1);
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

    let site_metadata_raw = serde_json::to_vec(&site_metadata).unwrap();
    fs::write(&metadata_path, &site_metadata_raw).await.unwrap();

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
struct CreateFileQuery {
    site_name: String,
    deployment_id: Uuid,
    path: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct SiteMetadata {
    current_deployment_id: Option<Uuid>,
    finalized_deployment_ids: Vec<Uuid>,
}

#[derive(Debug, Deserialize, Serialize)]
struct UpdateSite {
    #[serde(default)]
    finalize_deployment_id: Option<Uuid>,

    #[serde(default)]
    current_deployment_id: Option<Uuid>,
}
