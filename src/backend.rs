use anyhow::Result;
use chrono::{DateTime, Datelike, Utc};
use log::{error, info};
use polars::prelude::*;
use rand::Rng;
use serde::Serialize;
use std::error::Error;

#[derive(Debug, Serialize, Clone)]
pub struct Trip {
    tpep_pickup_datetime: DateTime<Utc>,
    tpep_dropoff_datetime: DateTime<Utc>,
    trip_distance: f64,
    fare_amount: f64,
}

pub async fn get_trips(from_ms: i64, n_results: i64) -> Result<Vec<Trip>, Box<dyn Error>> {
    let (year, month) = get_year_and_month(from_ms);
    info!("Extracted year: {}, month: {}", year, month);
    println!("Extracted year: {}, month: {}", year, month);

    info!(
        "Downloaded parquet file for year: {}, month: {}",
        year, month
    );
    let file_path = download_parquet_file(year, month).await?;
    let trips = get_trips_from_file(&file_path, from_ms, n_results)?;

    info!("Return {} trips", trips.len());
    Ok(trips)
}

fn get_year_and_month(from_ms: i64) -> (i32, i32) {
    let datetime = DateTime::<Utc>::from_timestamp(from_ms / 1000, 0).unwrap();
    (datetime.year(), datetime.month() as i32)
}

pub async fn download_parquet_file(year: i32, month: i32) -> Result<String> {
    let url = format!(
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{}-{:02}.parquet",
        year, month
    );
    let file_path = format!("yellow_tripdata_{}-{:02}.parquet", year, month);
    if tokio::fs::try_exists(&file_path).await? {
        println!("File {} already exists", &file_path);
        return Ok(file_path);
    }
    println!("Downloading file from {}", &url);
    let response = reqwest::get(&url).await?;
    if response.status().is_success() {
        let bytes = response.bytes().await?;
        tokio::fs::write(&file_path, bytes).await?;
        println!("File {} downloaded successfully", &file_path);
    } else {
        println!("Failed to download file");
        error!("Failed to download file");
    }
    Ok(file_path)
}

fn get_trips_from_file(file_path: &str, from_ms: i64, n_results: i64) -> Result<Vec<Trip>> {
    let df = LazyFrame::scan_parquet(file_path, Default::default())?
        .select([
            col("tpep_pickup_datetime"),
            col("tpep_dropoff_datetime"),
            col("trip_distance"),
            col("fare_amount"),
        ])
        .filter(col("tpep_pickup_datetime").gt_eq(lit(from_ms * 1_000_000)))
        .sort("tpep_pickup_datetime", Default::default())
        .limit(n_results as u32)
        .collect()?;

    let pickup_series = df
        .column("tpep_pickup_datetime")?
        .datetime()
        .expect("pickup datetime column should be datetime type");

    let dropoff_series = df
        .column("tpep_pickup_datetime")?
        .datetime()
        .expect("pickup datetime column should be datetime type");

    let distance_series = df
        .column("trip_distance")?
        .f64()
        .expect("distance column should be f64 type");

    let fare_series = df
        .column("fare_amount")?
        .f64()
        .expect("fare column should be f64 type");

    let trips = (0..df.height())
        .map(|i| Trip {
            tpep_pickup_datetime: DateTime::<Utc>::from_timestamp_nanos(
                pickup_series.get(i).unwrap(),
            ),
            tpep_dropoff_datetime: DateTime::<Utc>::from_timestamp_nanos(
                dropoff_series.get(i).unwrap(),
            ),
            trip_distance: distance_series.get(i).unwrap(),
            fare_amount: fare_series.get(i).unwrap(),
        })
        .collect();
    Ok(trips)
}

pub async fn get_fake_trips(from_ms: i64, n_results: i64) -> Result<Vec<Trip>, Box<dyn Error>> {
    let mut rng = rand::thread_rng();

    let trips = (0..n_results)
        .map(|_| {
            let random_seconds = rng.gen_range(0..60);
            let pickup_time =
                DateTime::<Utc>::from_timestamp(from_ms / 1000 + random_seconds, 0).unwrap();
            let dropoff_time = DateTime::<Utc>::from_timestamp(
                from_ms / 1000 + random_seconds + rng.gen_range(300..3600),
                0,
            )
            .unwrap();

            Trip {
                tpep_pickup_datetime: pickup_time,
                tpep_dropoff_datetime: dropoff_time,
                trip_distance: rng.gen_range(0.5..20.0),
                fare_amount: rng.gen_range(2.5..100.0),
            }
        })
        .collect();
    Ok(trips)
}
