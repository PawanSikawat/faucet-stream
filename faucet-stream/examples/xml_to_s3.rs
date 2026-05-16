//! XML/SOAP API → AWS S3 (JSONL files).
//!
//! Each batch from the source is written as a JSONL object under
//! `s3://<bucket>/<prefix>/<uuid>.jsonl`. Auth comes from the standard AWS
//! credential chain (env vars, profile, IMDS).
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example xml_to_s3 \
//!     --features "source-xml sink-s3"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::s3::{S3Sink, S3SinkConfig};
use faucet_stream::source::xml::{XmlStream, XmlStreamConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = XmlStream::new(XmlStreamConfig::new(
        "https://soap.example.com",
        "/InventoryService/Items",
    ));

    let sink = S3Sink::new(S3SinkConfig::new("my-data-lake")).await?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!(
        "wrote {} records to s3://my-data-lake/",
        result.records_written
    );
    Ok(())
}
