// Temporary spike module — compile-only proof of the iceberg 0.9.1 write path.
// Removed in Task 3 when the real sink modules replace this file.
//
// Arrow major: iceberg 0.9.1 uses arrow 57 (specifically 57.3.1).
//
// Verified API surface:
//
// REST catalog:
//   RestCatalogBuilder::default()
//       .load("name", HashMap::from([(REST_CATALOG_PROP_URI.to_string(), "..."),
//                                    (REST_CATALOG_PROP_WAREHOUSE.to_string(), "...")]))
//       .await?
//   -- `RestCatalogConfig` is pub(crate) in iceberg-catalog-rest-0.9.1; there is no
//      public struct-level builder. The only public API is `RestCatalogBuilder`.
//
// arrow-json 57 schema inference + RecordBatch construction:
//   arrow_json::reader::infer_json_schema_from_iterator(values.iter().map(|v| Ok(v)))
//   let mut decoder = arrow_json::ReaderBuilder::new(schema).build_decoder()?;
//   decoder.serialize(&[value, ...])?;
//   let batch: Option<RecordBatch> = decoder.flush()?;
//
// iceberg schema from arrow schema (auto-assigns field IDs — correct for inferred schemas):
//   iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(&arrow_schema) -> Result<Schema>
//
// TableCreation builder:
//   TableCreation::builder().name(String).schema(Schema).build()
//
// Catalog::create_table / load_table / table_exists:
//   catalog.create_table(&NamespaceIdent, TableCreation).await -> Result<Table>
//   catalog.load_table(&TableIdent).await -> Result<Table>
//   catalog.table_exists(&TableIdent).await -> Result<bool>
//
// Writer pipeline (iceberg::writer module paths):
//   use iceberg::writer::file_writer::location_generator::{DefaultLocationGenerator, DefaultFileNameGenerator};
//   use iceberg::writer::file_writer::{ParquetWriterBuilder, rolling_writer::RollingFileWriterBuilder};
//   use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
//   use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
//
//   DefaultLocationGenerator::new(table.metadata().clone()) -> Result<DefaultLocationGenerator>
//   DefaultFileNameGenerator::new(prefix: String, suffix: Option<String>, format: DataFileFormat)
//   ParquetWriterBuilder::new(WriterProperties, iceberg_schema_ref: SchemaRef)
//     -- Note: takes iceberg::spec::SchemaRef, NOT arrow SchemaRef!
//   RollingFileWriterBuilder::new(inner_builder, target_file_size: usize, file_io: FileIO,
//                                  location_gen, file_name_gen)
//   DataFileWriterBuilder::new(rolling_writer_builder)
//   let mut writer = data_file_writer_builder.build(None).await?;
//   writer.write(record_batch).await?;
//   let data_files: Vec<DataFile> = writer.close().await?;
//
// Transaction / fast_append / commit:
//   use iceberg::transaction::{ApplyTransactionAction, Transaction};
//   let tx = Transaction::new(&table);
//   let action = tx.fast_append().add_data_files(data_files);
//   let tx = action.apply(tx)?;   -- ApplyTransactionAction::apply()
//   let _updated_table = tx.commit(&catalog).await?;
//   -- Transaction::commit takes &dyn Catalog (not Arc)

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;

use arrow_json::ReaderBuilder;
use arrow_json::reader::infer_json_schema_from_iterator;
use iceberg::CatalogBuilder;
use iceberg::spec::DataFileFormat;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_rest::{REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder};
use parquet::file::properties::WriterProperties;
use serde_json::Value;

pub(crate) async fn _spike() -> Result<(), Box<dyn std::error::Error>> {
    // ── 1. Build a REST catalog ──────────────────────────────────────────────
    // `RestCatalogConfig` is pub(crate); the only public entry point is
    // `RestCatalogBuilder::default().load(name, props).await`.
    let catalog = RestCatalogBuilder::default()
        .load(
            "spike",
            HashMap::from([
                (
                    REST_CATALOG_PROP_URI.to_string(),
                    "http://localhost:8181".to_string(),
                ),
                (
                    REST_CATALOG_PROP_WAREHOUSE.to_string(),
                    "s3://warehouse".to_string(),
                ),
            ]),
        )
        .await?;

    // ── 2. JSON → arrow SchemaRef + RecordBatch (arrow-json 57 decoder API) ─
    let sample_values: Vec<Value> = vec![
        serde_json::json!({"id": 1, "name": "Alice"}),
        serde_json::json!({"id": 2, "name": "Bob"}),
    ];

    // Infer arrow schema from an iterator of &Value.
    // The iterator item type is Result<&Value, ArrowError>; map Ok to satisfy the bound.
    let arrow_schema = Arc::new(infer_json_schema_from_iterator(
        sample_values.iter().map(Ok::<&Value, arrow::error::ArrowError>),
    )?);

    // Build a RecordBatch via the Decoder (arrow-json 57):
    //   ReaderBuilder::new(schema).build_decoder()? -> Decoder
    //   decoder.serialize(&[Value, ...])? -> ()
    //   decoder.flush()? -> Option<RecordBatch>
    let mut decoder = ReaderBuilder::new(arrow_schema.clone()).build_decoder()?;
    decoder.serialize(&sample_values)?;
    let record_batch = decoder.flush()?.expect("expected a RecordBatch from decoder");

    // ── 3. Arrow schema → Iceberg schema (auto-assigns field IDs) ───────────
    // `arrow_schema_to_schema` requires field IDs in Arrow metadata;
    // `arrow_schema_to_schema_auto_assign_ids` assigns them automatically —
    // the correct choice for inferred schemas that don't originate from an
    // existing Iceberg table.
    let iceberg_schema =
        iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(&arrow_schema)?;

    // ── 4. Create (or load) a table ──────────────────────────────────────────
    let namespace = NamespaceIdent::from_strs(["analytics", "events"])?;
    let table_ident = TableIdent::new(namespace.clone(), "spike_table".to_string());

    let table = if catalog.table_exists(&table_ident).await? {
        catalog.load_table(&table_ident).await?
    } else {
        // TableCreation uses a TypedBuilder:
        //   .name(String)  .schema(Schema)  .build()
        // Optional: .location(String) .partition_spec(UnboundPartitionSpec)
        //           .sort_order(SortOrder) .properties([(k,v),...])
        let creation = TableCreation::builder()
            .name("spike_table".to_string())
            .schema(iceberg_schema)
            .build();
        catalog.create_table(&namespace, creation).await?
    };

    // ── 5. Writer pipeline ───────────────────────────────────────────────────
    // DefaultLocationGenerator::new(TableMetadata) -> Result<DefaultLocationGenerator>
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone())?;

    // DefaultFileNameGenerator::new(prefix: String, suffix: Option<String>, format: DataFileFormat)
    let file_name_generator = DefaultFileNameGenerator::new(
        "spike".to_string(),
        None,
        DataFileFormat::Parquet,
    );

    // ParquetWriterBuilder::new(WriterProperties, iceberg::spec::SchemaRef)
    // Takes the iceberg schema reference from the table (NOT an arrow SchemaRef).
    let parquet_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::default(),
        table.metadata().current_schema().clone(),
    );

    // RollingFileWriterBuilder::new(inner_builder, target_file_size: usize, FileIO, loc_gen, name_gen)
    let target_file_size: usize = 256 * 1024 * 1024; // 256 MiB
    let rolling_writer_builder = RollingFileWriterBuilder::new(
        parquet_writer_builder,
        target_file_size,
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );

    // DataFileWriterBuilder::new(rolling_writer_builder)
    // .build(Option<PartitionKey>) -> IcebergWriter (via IcebergWriterBuilder::build)
    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);
    let mut writer = data_file_writer_builder.build(None).await?;

    // IcebergWriter::write(RecordBatch) and close() -> Vec<DataFile>
    writer.write(record_batch).await?;
    let data_files = writer.close().await?;

    // ── 6. Commit: Transaction → fast_append → add_data_files → apply → commit ──
    // Transaction::new(&Table) — takes a reference, clones internally.
    // fast_append() -> FastAppendAction
    // .add_data_files(IntoIterator<Item=DataFile>) -> FastAppendAction (builder style)
    // ApplyTransactionAction::apply(tx: Transaction) -> Result<Transaction>
    // Transaction::commit(&dyn Catalog) -> Result<Table>
    let tx = Transaction::new(&table);
    let action = tx.fast_append().add_data_files(data_files);
    let tx = action.apply(tx)?;
    let _updated_table = tx.commit(&catalog).await?;

    Ok(())
}
