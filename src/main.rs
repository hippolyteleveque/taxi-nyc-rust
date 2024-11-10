use actix_web::{web, App, HttpResponse, HttpServer};
use backend::get_trips;
use serde_json;
use serde::Deserialize;

mod backend;

async fn health() -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({"status": "healthy"}))
}

#[derive(Deserialize)]
struct TripQuery {
    from_ms: i64,
    n_results: i64,
}

async fn trips(query: web::Query<TripQuery>) -> HttpResponse {
    println!("{}", query.from_ms);
    match get_trips(query.from_ms, query.n_results).await {
        Ok(trips) => HttpResponse::Ok().json(serde_json::json!({"trips": trips})),
        Err(e) => {
            HttpResponse::InternalServerError().json(serde_json::json!({"error": e.to_string()}))
        }
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    println!("Starting server...");
    let _ = HttpServer::new(|| {
        App::new()
            .wrap(actix_web::middleware::Logger::default())
            .route("/health", web::get().to(health))
            .route("/trips", web::get().to(trips))
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await;
    Ok(())
}
