use chrono::{DateTime, Datelike, Utc};
use rand::Rng;
use serde::Serialize;
use std::error::Error;

#[derive(Debug, Serialize)]
pub struct Trip {
    tpep_pickup_datetime: DateTime<Utc>,
    tpep_dropoff_datetime: DateTime<Utc>,
    trip_distance: f64,
    fare_amount: f64,
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
