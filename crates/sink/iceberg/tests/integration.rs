//! Integration tests for `IcebergSink` against a real REST catalog backed by
//! MinIO as the S3 object store.
//!
//! # Docker requirement
//!
//! These tests require Docker (matching the kafka / postgres-cdc convention).
//! Two containers are started and connected to the same bridge network:
//!
//! 1. **MinIO** (`minio/minio:latest`) — provides S3-compatible object storage.
//!    Started with the `server /data` command; the `warehouse` bucket is
//!    pre-created via the MinIO client container or by the REST catalog
//!    image's bucket-auto-create behaviour.
//!
//! 2. **iceberg-rest** (`tabulario/iceberg-rest:latest`) — the Apache Iceberg
//!    REST catalog reference implementation. Configured to write table data
//!    to MinIO via the S3FileIO. The two containers share a Docker bridge
//!    network so the catalog can reach MinIO by container name.
//!
//! The host connects to MinIO and the REST catalog via their forwarded ports.
//! S3 properties for the iceberg-rust client use the host-mapped MinIO port
//! so data files are reachable from the test process.
//!
//! # Environment variables
//!
//! No environment variables need to be set. The tests skip automatically when
//! Docker is unavailable (testcontainers will panic or time out if the Docker
//! daemon is not running — see note below).
//!
//! # NOTE for CI
//!
//! If the `tabulario/iceberg-rest` image does not auto-create the `warehouse`
//! bucket on MinIO, bucket creation must be performed manually before starting
//! the REST catalog (e.g. via a `mc` exec in the MinIO container). The
//! `CATALOG_WAREHOUSE_URI` env on newer `tabulario/iceberg-rest` images
//! supports auto-bucket-create; if not, add a MinIO SDK bucket-create step
//! after starting the MinIO container. This is documented here as a NOTE so
//! CI can extend the test without touching the test logic.

use std::collections::HashMap;
use std::time::Duration;

use faucet_core::Sink;
use faucet_sink_iceberg::{IcebergSink, IcebergSinkConfig};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder,
};
use serde_json::json;
use testcontainers::{ContainerAsync, GenericImage, ImageExt, runners::AsyncRunner};

// ── Constants ─────────────────────────────────────────────────────────────────

/// Docker network name shared by MinIO and the iceberg-rest catalog.
const NETWORK: &str = "iceberg-test-net";

/// MinIO container name — used by the REST catalog to reach MinIO inside the
/// Docker network.
const MINIO_CONTAINER_NAME: &str = "minio-test";

const MINIO_ROOT_USER: &str = "minioadmin";
const MINIO_ROOT_PASSWORD: &str = "minioadmin";
const MINIO_BUCKET: &str = "warehouse";
const MINIO_REGION: &str = "us-east-1";

/// Internal MinIO port inside the Docker network.
const MINIO_INTERNAL_PORT: u16 = 9000;

// ── Container helpers ─────────────────────────────────────────────────────────

/// Start a MinIO container on `NETWORK`, return the container handle and the
/// host-mapped port (for use by the test process to create the bucket and for
/// the iceberg-rust S3 client).
async fn start_minio() -> (ContainerAsync<GenericImage>, u16) {
    let image = GenericImage::new("minio/minio", "latest")
        .with_network(NETWORK)
        .with_container_name(MINIO_CONTAINER_NAME)
        .with_env_var("MINIO_ROOT_USER", MINIO_ROOT_USER)
        .with_env_var("MINIO_ROOT_PASSWORD", MINIO_ROOT_PASSWORD)
        // MinIO exposes the API on 9000; the console lives on 9001 (not needed).
        .with_env_var("MINIO_DEFAULT_BUCKETS", MINIO_BUCKET)
        .with_cmd(["server", "/data"])
        .with_startup_timeout(Duration::from_secs(60))
        // Wait for the MinIO "ready" log line.
        .with_ready_conditions(vec![testcontainers::core::WaitFor::message_on_stdout(
            "S3-API:",
        )]);

    let container = image.start().await.expect("MinIO container start");
    let host_port = container
        .get_host_port_ipv4(MINIO_INTERNAL_PORT)
        .await
        .expect("MinIO host port");

    (container, host_port)
}

/// Start the Iceberg REST catalog container on `NETWORK`, wired to reach MinIO
/// by container name (`MINIO_CONTAINER_NAME`) inside the shared network.
///
/// Returns the container handle and the host-mapped port for the REST catalog
/// (default internal port: 8181).
///
/// # NOTE for CI
///
/// The `tabulario/iceberg-rest` image uses the Iceberg REST catalog reference
/// implementation. It reads `CATALOG_WAREHOUSE` and the `AWS_*` / `CATALOG_S3_*`
/// env vars to configure S3FileIO. The image is expected to auto-create the
/// `warehouse` bucket on MinIO if it does not already exist; if the image
/// version used in CI does not support this, a bucket-create step must be
/// added before this function is called (e.g. `aws s3 mb s3://warehouse` via
/// an exec in the MinIO container, using the localstack-aws-cli image or
/// similar).
async fn start_iceberg_rest(minio_host_port: u16) -> (ContainerAsync<GenericImage>, u16) {
    // Inside the Docker network, the REST catalog reaches MinIO by its
    // container name. From the host, the test uses `minio_host_port`.
    let minio_internal_url = format!("http://{MINIO_CONTAINER_NAME}:{MINIO_INTERNAL_PORT}");

    let image = GenericImage::new("tabulario/iceberg-rest", "latest")
        .with_network(NETWORK)
        // S3 warehouse root — all table data goes under s3://warehouse/.
        .with_env_var("CATALOG_WAREHOUSE", format!("s3://{MINIO_BUCKET}/"))
        // Use the S3FileIO implementation for object storage.
        .with_env_var("CATALOG_IO__IMPL", "org.apache.iceberg.aws.s3.S3FileIO")
        // Point S3FileIO at MinIO inside the shared network.
        .with_env_var("CATALOG_S3_ENDPOINT", &minio_internal_url)
        .with_env_var("CATALOG_S3_PATH__STYLE__ACCESS", "true")
        // AWS credentials for MinIO (MinIO ignores region; set to satisfy the SDK).
        .with_env_var("AWS_ACCESS_KEY_ID", MINIO_ROOT_USER)
        .with_env_var("AWS_SECRET_ACCESS_KEY", MINIO_ROOT_PASSWORD)
        .with_env_var("AWS_REGION", MINIO_REGION)
        // Suppress the catalog's auto-discovery of AWS credentials from EC2
        // metadata (irrelevant in a local test container, but can slow startup).
        .with_env_var("AWS_EC2_METADATA_DISABLED", "true")
        // Suppress warning: the catalog runs as root in the container.
        .with_env_var(
            "CATALOG_CATALOG__IMPL",
            "org.apache.iceberg.rest.RESTCatalog",
        )
        .with_startup_timeout(Duration::from_secs(90))
        // Wait for the REST catalog's "Started" log line.
        .with_ready_conditions(vec![testcontainers::core::WaitFor::message_on_stdout(
            "REST catalog server started",
        )]);

    // Silence unused-variable warning; `minio_host_port` is only used by the
    // test process (not the container) but is passed here for clarity.
    let _ = minio_host_port;

    let container = image.start().await.expect("iceberg-rest container start");
    let host_port = container
        .get_host_port_ipv4(8181)
        .await
        .expect("iceberg-rest host port");

    (container, host_port)
}

/// Build an `IcebergSinkConfig` pointing at the REST catalog on `rest_port`
/// with the S3 FileIO configured to use MinIO on `minio_port` (both are
/// host-side ports mapped by testcontainers).
fn sink_config(rest_port: u16, minio_port: u16, table: &str) -> IcebergSinkConfig {
    let catalog_uri = format!("http://127.0.0.1:{rest_port}");
    let minio_endpoint = format!("http://127.0.0.1:{minio_port}");

    serde_json::from_value(json!({
        "catalog": {
            "type": "rest",
            "uri": catalog_uri,
            "warehouse": format!("s3://{MINIO_BUCKET}/"),
            // iceberg-rust reads these keys from the `properties` map and
            // passes them to the FileIO builder.  The exact key names are the
            // `iceberg::io::storage::config::s3` constants:
            //   S3_ENDPOINT              = "s3.endpoint"
            //   S3_ACCESS_KEY_ID         = "s3.access-key-id"
            //   S3_SECRET_ACCESS_KEY     = "s3.secret-access-key"
            //   S3_REGION                = "s3.region"
            //   S3_PATH_STYLE_ACCESS     = "s3.path-style-access"
            //   S3_DISABLE_EC2_METADATA  = "s3.disable-ec2-metadata"
            //   S3_DISABLE_CONFIG_LOAD   = "s3.disable-config-load"
            "properties": {
                "s3.endpoint": minio_endpoint,
                "s3.access-key-id": MINIO_ROOT_USER,
                "s3.secret-access-key": MINIO_ROOT_PASSWORD,
                "s3.region": MINIO_REGION,
                "s3.path-style-access": "true",
                // Prevent the SDK from trying to reach EC2/ECS metadata or
                // reading ~/.aws/credentials inside the test process.
                "s3.disable-ec2-metadata": "true",
                "s3.disable-config-load": "true"
            }
        },
        "namespace": ["db"],
        "table": table,
        "create_if_missing": true,
        // batch_size = 0 → single chunk per write_batch call.
        "batch_size": 0
    }))
    .expect("sink config parse")
}

/// Open a read-only `RestCatalog` for post-write assertions.
///
/// Uses the same host-side ports as the test. The catalog only needs to reach
/// the REST server (HTTP), not MinIO directly (table metadata is in the REST
/// catalog; data files are in MinIO but we assert on snapshot count, not file
/// content).
async fn open_reader_catalog(rest_port: u16, minio_port: u16) -> iceberg_catalog_rest::RestCatalog {
    let catalog_uri = format!("http://127.0.0.1:{rest_port}");
    let minio_endpoint = format!("http://127.0.0.1:{minio_port}");

    let mut props = HashMap::new();
    props.insert(REST_CATALOG_PROP_URI.to_string(), catalog_uri);
    props.insert(
        REST_CATALOG_PROP_WAREHOUSE.to_string(),
        format!("s3://{MINIO_BUCKET}/"),
    );
    // S3 FileIO properties so the catalog can resolve file-IO for metadata ops.
    props.insert("s3.endpoint".to_string(), minio_endpoint);
    props.insert("s3.access-key-id".to_string(), MINIO_ROOT_USER.to_string());
    props.insert(
        "s3.secret-access-key".to_string(),
        MINIO_ROOT_PASSWORD.to_string(),
    );
    props.insert("s3.region".to_string(), MINIO_REGION.to_string());
    props.insert("s3.path-style-access".to_string(), "true".to_string());
    props.insert("s3.disable-ec2-metadata".to_string(), "true".to_string());
    props.insert("s3.disable-config-load".to_string(), "true".to_string());

    RestCatalogBuilder::default()
        .load("reader", props)
        .await
        .expect("reader catalog open")
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// Core integration test: write one batch → flush → assert exactly ONE snapshot.
/// Then write a second batch → flush → assert TWO snapshots and a fresh snapshot ID.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_and_flush_creates_iceberg_snapshots() {
    // Start infrastructure.
    let (_minio, minio_port) = start_minio().await;
    let (_rest, rest_port) = start_iceberg_rest(minio_port).await;

    // Give the REST catalog a moment to register its namespace defaults.
    // (The ready-condition log line fires after the HTTP server is bound, so
    // in practice no extra wait is needed — but CI may need adjustment here.)

    // ── Write batch 1 ─────────────────────────────────────────────────────────

    let cfg = sink_config(rest_port, minio_port, "events");
    let sink = IcebergSink::new(cfg).await.expect("IcebergSink::new");

    // 1 000 records: {"id": 0..999, "name": "n0".."n999"}
    let records: Vec<serde_json::Value> = (0u64..1_000)
        .map(|i| json!({ "id": i, "name": format!("n{i}") }))
        .collect();

    let written = sink.write_batch(&records).await.expect("write_batch 1");
    assert_eq!(written, 1_000, "expected 1 000 rows written");

    sink.flush().await.expect("flush 1");

    // ── Assert snapshot 1 ─────────────────────────────────────────────────────

    let reader = open_reader_catalog(rest_port, minio_port).await;
    let ns = NamespaceIdent::from_strs(["db"]).expect("namespace ident");
    let tid = TableIdent::new(ns.clone(), "events".to_string());

    let table1 = reader
        .load_table(&tid)
        .await
        .expect("load_table after flush 1");
    let meta1 = table1.metadata();

    let snapshot_count_1 = meta1.snapshots().count();
    assert_eq!(
        snapshot_count_1, 1,
        "expected exactly 1 snapshot after first flush, got {snapshot_count_1}"
    );
    let snap1_id = meta1
        .current_snapshot_id()
        .expect("current_snapshot_id must be set after first flush");

    // ── Write batch 2 ─────────────────────────────────────────────────────────

    let records2: Vec<serde_json::Value> = (1_000u64..2_000)
        .map(|i| json!({ "id": i, "name": format!("n{i}") }))
        .collect();

    let written2 = sink.write_batch(&records2).await.expect("write_batch 2");
    assert_eq!(written2, 1_000, "expected 1 000 rows written in batch 2");

    sink.flush().await.expect("flush 2");

    // ── Assert snapshot 2 ─────────────────────────────────────────────────────

    let table2 = reader
        .load_table(&tid)
        .await
        .expect("load_table after flush 2");
    let meta2 = table2.metadata();

    let snapshot_count_2 = meta2.snapshots().count();
    assert_eq!(
        snapshot_count_2, 2,
        "expected exactly 2 snapshots after second flush, got {snapshot_count_2}"
    );

    let snap2_id = meta2
        .current_snapshot_id()
        .expect("current_snapshot_id must be set after second flush");

    assert_ne!(
        snap1_id, snap2_id,
        "second flush must produce a new snapshot id (got {snap1_id} twice)"
    );

    // ── Empty flush is a no-op (no new snapshot) ───────────────────────────────

    sink.flush().await.expect("flush 3 (empty)");

    let table3 = reader
        .load_table(&tid)
        .await
        .expect("load_table after empty flush");
    assert_eq!(
        table3.metadata().snapshots().count(),
        2,
        "empty flush must NOT create a third snapshot"
    );
}
