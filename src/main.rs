use actix_web::{web, App, HttpResponse, HttpServer};
use serde_json;

async fn health() -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({"status": "healthy"}))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    println!("Starting server...");
    HttpServer::new(|| App::new().route("/health", web::get().to(health)))
        .bind(("127.0.0.1", 8080))?
        .run()
        .await;
    Ok(())
}
