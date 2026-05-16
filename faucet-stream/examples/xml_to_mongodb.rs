//! XML/SOAP API → MongoDB collection.
//!
//! Required: a MongoDB instance reachable at the connection URI.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example xml_to_mongodb \
//!     --features "source-xml sink-mongodb"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::mongodb::{MongoSink, MongoSinkConfig};
use faucet_stream::source::xml::{XmlStream, XmlStreamConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = XmlStream::new(XmlStreamConfig::new(
        "https://feeds.example.com",
        "/catalog.xml",
    ));

    let sink = MongoSink::new(MongoSinkConfig::new(
        "mongodb://localhost:27017",
        "warehouse",
        "catalog_items",
    ))
    .await?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!(
        "inserted {} catalog items into MongoDB",
        result.records_written
    );
    Ok(())
}
