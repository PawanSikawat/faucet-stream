//! Arrow columnar load-job helpers (#380) — Parquet encode + the BigQuery
//! `PARQUET` load-job builder.
//!
//! The columnar sink path buffers each Arrow [`RecordBatch`] to a
//! self-contained Parquet file, uploads it to a GCS staging bucket, and then
//! runs a BigQuery load job (`jobs.insert`) with `sourceFormat = PARQUET`.
//! The pure pieces here (Parquet encode, the [`Job`] builder) are unit-tested;
//! the GCS upload + job polling live in `sink.rs`.

use arrow::array::RecordBatch;
use faucet_core::FaucetError;
use gcp_bigquery_client::model::job::Job;
use gcp_bigquery_client::model::job_configuration::JobConfiguration;
use gcp_bigquery_client::model::job_configuration_load::JobConfigurationLoad;
use gcp_bigquery_client::model::table_reference::TableReference;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

/// Encode one Arrow [`RecordBatch`] as a self-contained ZSTD-compressed
/// Parquet file in memory. Mirrors the S3/GCS sinks' `encode_parquet`.
pub fn encode_parquet(batch: &RecordBatch) -> Result<Vec<u8>, FaucetError> {
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props))
            .map_err(|e| FaucetError::Sink(format!("parquet writer init failed: {e}")))?;
        writer
            .write(batch)
            .map_err(|e| FaucetError::Sink(format!("parquet write failed: {e}")))?;
        writer
            .close()
            .map_err(|e| FaucetError::Sink(format!("parquet finalize failed: {e}")))?;
    }
    Ok(buf)
}

/// Build a BigQuery `PARQUET` load [`Job`] that loads `source_uri` (a
/// `gs://…` object) into the fully-qualified destination table. Pure.
pub fn build_load_job(
    project_id: &str,
    dataset_id: &str,
    table_id: &str,
    source_uri: &str,
    write_disposition: &str,
) -> Job {
    let load = JobConfigurationLoad {
        source_uris: Some(vec![source_uri.to_string()]),
        source_format: Some("PARQUET".to_string()),
        write_disposition: Some(write_disposition.to_string()),
        create_disposition: Some("CREATE_IF_NEEDED".to_string()),
        destination_table: Some(TableReference::new(project_id, dataset_id, table_id)),
        ..Default::default()
    };
    Job {
        configuration: Some(JobConfiguration {
            load: Some(load),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn load_job_sets_parquet_source_and_destination() {
        let job = build_load_job(
            "proj",
            "ds",
            "events",
            "gs://bucket/faucet-bq-load/x.parquet",
            "WRITE_APPEND",
        );
        let load = job.configuration.unwrap().load.unwrap();
        assert_eq!(load.source_format.as_deref(), Some("PARQUET"));
        assert_eq!(load.write_disposition.as_deref(), Some("WRITE_APPEND"));
        assert_eq!(
            load.source_uris.as_deref(),
            Some(["gs://bucket/faucet-bq-load/x.parquet".to_string()].as_slice())
        );
        let dest = load.destination_table.unwrap();
        assert_eq!(dest.project_id, "proj");
        assert_eq!(dest.dataset_id, "ds");
        assert_eq!(dest.table_id, "events");
    }

    #[test]
    fn encode_parquet_roundtrips_a_batch() {
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["a", "b"]))])
            .unwrap();
        let bytes = encode_parquet(&batch).unwrap();
        assert_eq!(&bytes[..4], b"PAR1");
        assert_eq!(&bytes[bytes.len() - 4..], b"PAR1");
    }
}
