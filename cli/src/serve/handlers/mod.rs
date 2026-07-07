//! serve HTTP handlers.
pub mod audit;
#[cfg(feature = "catalog")]
pub mod catalog;
pub mod dlq;
pub mod doctor;
pub mod health;
pub mod logs;
pub mod runs;
pub mod schemas;
