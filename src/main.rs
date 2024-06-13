use anyhow::Result;
use axum::{
    body::Bytes,
    extract::{path::Path, DefaultBodyLimit, Query, State},
    http::{
        header::{self, HeaderMap},
        StatusCode,
    },
    response::IntoResponse,
    routing::{get, post, put},
    Json, Router,
};
use clap::{Parser, Subcommand};
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, sync::Arc};
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
    object_store: Box<dyn ObjectStore>,
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
                object_store,
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
                    .put(format!("{remote}/api/v1/files"))
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
    Path(path): Path<GetFilePath>,
) -> impl IntoResponse {
    let mut headers = HeaderMap::new();

    let mut full_path = String::with_capacity(path.path.len() + 1);
    full_path.push_str("/");
    full_path.push_str(&path.path);

    let site_path = state
        .base_path
        .child("sites")
        .child(path.site_name.as_str());
    let site_metadata_path = site_path.child("metadata.json");

    let site_metadata: SiteMetadata = match state.object_store.get(&site_metadata_path).await {
        Ok(site_metadata_obj) => {
            let site_metadata_raw = site_metadata_obj.bytes().await.unwrap();
            serde_json::from_slice(&site_metadata_raw).unwrap()
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

    let file_path = site_path
        .child("deployments")
        .child(deployment_id.to_string())
        .child("files")
        .child(&path_hash_hex[..2])
        .child(&path_hash_hex[2..]);

    let (_, ext) = path.path.rsplit_once(".").unwrap_or(("", ""));

    let mime = match ext {
        "html" | "htm" | "" => "text/html",
        "jpeg" | "jpg" => "image/jpeg",
        _ => "application/octet-stream",
    };

    headers.insert(header::CONTENT_TYPE, mime.parse().unwrap());

    let file_obj = state.object_store.get(&file_path).await.unwrap();
    let file_raw = file_obj.bytes().await.unwrap().to_vec();

    (StatusCode::OK, headers, file_raw)
}

async fn create_deployment(
    State(state): State<Arc<AppState>>,
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

    let deployment_path = state
        .base_path
        .child("sites")
        .child(create_deployment.site_name.as_str())
        .child("deployments")
        .child(deployment.id.to_string())
        .child("metadata.json");

    let deployment_stage_meta = serde_json::to_vec(&create_deployment).unwrap();

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

    (StatusCode::CREATED, Json(deployment))
}

async fn create_file(
    State(state): State<Arc<AppState>>,
    Query(query): Query<CreateFileQuery>,
    body: Bytes,
) -> StatusCode {
    let deployment_dir_path = state
        .base_path
        .child("sites")
        .child(query.site_name.as_str())
        .child("deployments")
        .child(query.deployment_id.to_string());

    let deployment_path = deployment_dir_path.child("metadata.json");

    let deployment_stage_meta_obj = state.object_store.get(&deployment_path).await.unwrap();
    let deployment_stage_meta: CreateDeployment =
        serde_json::from_slice(&deployment_stage_meta_obj.bytes().await.unwrap()).unwrap();

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
        .child("metadata.json");

    let site_metadata = SiteMetadata {
        current_deployment_id: None,
        finalized_deployment_ids: vec![],
    };

    let site_metadata_raw = serde_json::to_vec(&site_metadata).unwrap();

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
    let site_path = state.base_path.child("sites").child(site_name.as_str());
    let site_metadata_path = site_path.child("metadata.json");

    let (mut site_metadata, site_metadata_obj_meta) =
        match state.object_store.get(&site_metadata_path).await {
            Ok(site_metadata_obj) => {
                let site_metadata_obj_meta = object_store::UpdateVersion {
                    e_tag: site_metadata_obj.meta.e_tag.clone(),
                    version: site_metadata_obj.meta.version.clone(),
                };

                let site_metadata_raw = site_metadata_obj.bytes().await.unwrap();
                let site_metadata: SiteMetadata =
                    serde_json::from_slice(&site_metadata_raw).unwrap();

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

    if let Some(finalize_deployment_id) = update_site.finalize_deployment_id {
        let deployment_path = site_path
            .child("deployments")
            .child(finalize_deployment_id.to_string());
        let deployment_metadata_path = deployment_path.child("metadata.json");

        let deployment_metadata_obj = state
            .object_store
            .get(&deployment_metadata_path)
            .await
            .unwrap();

        let deployment_metadata = deployment_metadata_obj.bytes().await.unwrap();

        let deployment_metadata: CreateDeployment =
            serde_json::from_slice(&deployment_metadata).unwrap();

        for file in deployment_metadata.files.iter() {
            let path_hash = blake3::hash(&file.path.as_bytes());
            let path_hash_hex = path_hash.to_hex();

            let file_path = deployment_path
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
