use axum:: {
    extract::{Path,State},
    routing::{get,post},
    Json,Router,
};

use chrono::Utc;
use hyper::StatusCode;
use serde::{Deserialize,Serialize};

use std::{
    collections::HashMap, net::SocketAddr, sync::Arc, time::Duration
};

use tokio::{sync::{Mutex,mpsc}};
use tokio::net::TcpListener;

use rand::Rng;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct JobRequest {
    job_type: String,
    payload: serde_json::Value,
    idempotency_key: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
enum JobStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
    Compensated,
}

#[derive(Clone, Debug, Serialize,serde::Deserialize)]
struct JobInfo {
    job_id: String,
    status: JobStatus,
    attempts: u32,
    last_error: Option<String>,
    started_at:Option<String>,
    completed_at:Option<String>,
}

#[derive(Clone)]
struct JobRequestInternal {
    job_id: String,
    req: JobRequest,
    attempts:u32,
}

struct AppState {
    jobs: Mutex<HashMap<String, JobInfo>>,
    idempotency: Mutex<HashMap<String, String>>, // key -> jobId
    tx: mpsc::Sender<JobRequestInternal>,
}

#[derive(Debug, Serialize)]
struct JobResponse {
    #[serde(rename = "jobId")]
    job_id: String,
}





#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    // Configure workers + queue
    let num_workers = 4;
    let queue_capacity = 10;
    let state = setup_state(num_workers, queue_capacity);

    // Build router
    let app = Router::new()
        .route("/v1/jobs", post(submit_job))
        .route("/v1/jobs/{id}", get(get_job_status))
        .with_state(state);

    // Run server
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    tracing::info!(" Listening on http://{}", addr);

    let listener = TcpListener::bind("127.0.0.1:3000")
            .await
            .expect("Failed to bind to address");

    axum::serve(listener,app.into_make_service())
        .await
        .unwrap();


}


async fn worker_loop_single(job: JobRequestInternal, state: Arc<AppState>) {
    let mut job = job;

    {
        let mut jobs = state.jobs.lock().await;
        if let Some(info) = jobs.get_mut(&job.job_id) {
            info.status = JobStatus::Running;
            info.started_at = Some(Utc::now().to_string());
        }
    }

    let mut success = false;
    let max_attempts = 3;

    while job.attempts < max_attempts {
        job.attempts += 1;

        match execute_job(&job).await {
            Ok(_) => {
                let mut jobs = state.jobs.lock().await;
                if let Some(info) = jobs.get_mut(&job.job_id) {
                    info.status = JobStatus::Succeeded;
                    info.attempts = job.attempts;
                    info.completed_at = Some(Utc::now().to_string());
                }
                success = true;
                break;
            }
            Err(err) => {
                let mut jobs = state.jobs.lock().await;
                if let Some(info) = jobs.get_mut(&job.job_id) {
                    info.status = JobStatus::Running;
                    info.attempts = job.attempts;
                    info.last_error = Some(err.clone());
                }

                let backoff_ms = (2u64.pow(job.attempts) * 100) // exponential backupff
                    + rand::rng().random_range(0..100); //jitter
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
            }
        }
    }

    if !success {
        match compensate_job(&job).await {
            Ok(_) => {
                let mut jobs = state.jobs.lock().await;
                if let Some(info) = jobs.get_mut(&job.job_id) {
                    info.status = JobStatus::Compensated;
                    info.completed_at = Some(Utc::now().to_string());
                }
            }
            Err(err) => {
                let mut jobs = state.jobs.lock().await;
                if let Some(info) = jobs.get_mut(&job.job_id) {
                    info.status = JobStatus::Compensated;
                    info.last_error = Some(format!("Compensation failed: {}", err));
                    info.completed_at = Some(Utc::now().to_string());
                }
            }
        }
    }
}


fn setup_state(num_workers: usize, queue_capacity: usize) -> Arc<AppState> {
    let (tx, rx) = mpsc::channel::<JobRequestInternal>(queue_capacity);

    let state = Arc::new(AppState {
        jobs: Mutex::new(HashMap::new()),
        idempotency: Mutex::new(HashMap::new()),
        tx,
    });

    // Spawn workers
    let rx = Arc::new(Mutex::new(rx));
    for _ in 0..num_workers {
        let state_cloned = Arc::clone(&state);
        let rx_cloned = Arc::clone(&rx);
        tokio::spawn(async move {
            loop {
                let job_opt = {
                    let mut rx_guard = rx_cloned.lock().await;
                    rx_guard.recv().await
                };

                if let Some(job) = job_opt {
                    worker_loop_single(job, state_cloned.clone()).await;
                } else {
                    break; // channel closed
                }
            }
        });
    }

    state
}



async fn submit_job(
    State(state): State<Arc<AppState>>,
    Json(req): Json<JobRequest>,
) -> Result<Json<JobResponse>, (StatusCode, String)> {
    // Check idempotency key
    if let Some(key) = &req.idempotency_key {
        let idem = state.idempotency.lock().await;
        if let Some(existing) = idem.get(key) {
            println!("Returning existing job for idempotency key: {}", key);
            return Ok(Json(JobResponse { job_id: existing.clone() }));
        }
    }

    // Create new job
    let job_id = uuid::Uuid::new_v4().to_string();
    let internal = JobRequestInternal {
        job_id: job_id.clone(),
        req: req.clone(),
        attempts: 0,
    };

    // Insert JobInfo into state
    {
        let mut jobs = state.jobs.lock().await;
        jobs.insert(
            job_id.clone(),
            JobInfo {
                job_id: job_id.clone(),
                status: JobStatus::Queued,
                attempts: 0,
                last_error: None,
                started_at: None,
                completed_at: None,
            },
        );
    }

    // Try to enqueue job
    match state.tx.try_send(internal) {
        Ok(_) => {
            if let Some(key) = &req.idempotency_key {
                let mut idem = state.idempotency.lock().await;
                idem.insert(key.clone(), job_id.clone());
            }
            println!("Enqueued job: {}", job_id);
            Ok(Json(JobResponse { job_id }))
        }
        Err(mpsc::error::TrySendError::Full(_)) => {
            let mut jobs = state.jobs.lock().await;
            jobs.remove(&job_id);
            Err((
                StatusCode::TOO_MANY_REQUESTS,
                "Queue full, try again later".to_string(),
            ))
        }
        Err(_) => {
            let mut jobs = state.jobs.lock().await;
            jobs.remove(&job_id);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to enqueue job".to_string(),
            ))
        }
    }
}

async fn get_job_status(
    Path(job_id): Path<String>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<JobInfo>, StatusCode> {
    let jobs = state.jobs.lock().await;
    if let Some(info) = jobs.get(&job_id) {
        Ok(Json(info.clone()))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}








/// Undo side effects if a job failed after max retries.
/// Return Ok if compensation worked, Err if it failed.
async fn compensate_job(job: &JobRequestInternal) -> Result<(), String> {
    match job.req.job_type.as_str() {
        "send_email" => {
            println!("Compensating email: deleting draft for payload {}", job.req.payload);
            Ok(())
        }
        "generate_report" => {
            println!(" Compensating report: cleaning up partial report {}", job.req.payload);
            Ok(())
        }
        "send_email_fail" => { // need to impulate a fail
            println!(" Compensating email forced fail: cleaning up partial report {}", job.req.payload);
            Ok(())
        }
        other => Err(format!("No compensation available for {}", other)),
    }
}

/// Run the actual job work. Return Ok if successful, Err with reason otherwise.
async fn execute_job(job: &JobRequestInternal) -> Result<(), String> {
    match job.req.job_type.as_str() {
        // Always fail → triggers retries → compensation
        "send_email_fail" => Err("forced failure".into()),

        // Succeeds (example of a different type)
        "generate_report" => Ok(()),

        // Optional: a real email that might succeed in your app
        "send_email" => Ok(()),

        other => Err(format!("Unknown job type: {}", other)),
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Request, StatusCode},
        Router,
    };
    use tower::ServiceExt; // for oneshot
    use http_body_util::BodyExt; // for .collect()
    use bytes::Bytes;
    use serde_json::json;


    async fn body_bytes(resp: axum::response::Response) -> Bytes {
        resp.into_body().collect().await.unwrap().to_bytes()
    }

    const NUM_WORKERS: usize = 2;
    const QUEUE_CAPACITY: usize = 5;

  #[tokio::test]
async fn test_retry_then_compensation() { // Satisfies compensation test
    println!("Running test here");
    let state = setup_state(NUM_WORKERS, QUEUE_CAPACITY);
    let app = Router::new()
        .route("/v1/jobs", axum::routing::post(submit_job))
        .route("/v1/jobs/{id}", axum::routing::get(get_job_status))
        .with_state(state.clone());

    // Use the deterministic failing type
    let req = JobRequest {
        job_type: "send_email_fail".to_string(),
        payload: serde_json::json!({"to":"user@example.com"}),
        idempotency_key: None,
    };

    let response = app.clone().oneshot(
        Request::builder()
            .method("POST")
            .uri("/v1/jobs")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(&req).unwrap()))
            .unwrap(),
    ).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let job_id: String = serde_json::from_slice::<serde_json::Value>(&body)
        .unwrap()["jobId"]
        .as_str()
        .unwrap()
        .to_string();

    // Wait long enough for 3 attempts + backoffs (~< 2s worst case in our backoff)
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let response = app.clone().oneshot(
        Request::builder()
            .method("GET")
            .uri(format!("/v1/jobs/{}", job_id))
            .body(Body::empty())
            .unwrap(),
    ).await.unwrap();

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let job: JobInfo = serde_json::from_slice(&body).unwrap();

    assert_eq!(job.status, JobStatus::Compensated);
    assert_eq!(job.attempts, 3);
}

   

    #[tokio::test]
    async fn test_idempotency_key() { // test same idempotency key from two jobs.
        let state = setup_state(NUM_WORKERS, QUEUE_CAPACITY);
        let app = Router::new()
            .route("/v1/jobs", axum::routing::post(submit_job))
            .route("/v1/jobs/{id}", axum::routing::get(get_job_status))
            .with_state(state.clone());

        let req = JobRequest {
            job_type: "generate_report".to_string(),
            payload: json!({"kind":"weekly"}),
            idempotency_key: Some("abc123".to_string()),
        };

        let res1 = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/jobs")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let body1 = body_bytes(res1).await;
        let job_id1: String = serde_json::from_slice::<serde_json::Value>(&body1)
            .unwrap()["jobId"]
            .as_str()
            .unwrap()
            .to_string();

        // Submit second time with same idempotency key
        let res2 = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/jobs")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let body2 = body_bytes(res2).await;
        let job_id2: String = serde_json::from_slice::<serde_json::Value>(&body2)
            .unwrap()["jobId"]
            .as_str()
            .unwrap()
            .to_string();

        assert_eq!(job_id1, job_id2);
    }


    // Creates to many request and will fail with a 429
    #[tokio::test]
    async fn test_backpressure_returns_429() { // test back pressure works
        let state = setup_state(NUM_WORKERS, QUEUE_CAPACITY);
        let app = Router::new()
            .route("/v1/jobs", axum::routing::post(submit_job))
            .with_state(state.clone());

        let mut statuses = vec![];

        // Submit more jobs than queue capacity
        for i in 0..(QUEUE_CAPACITY + 2) {
            let req = JobRequest {
                job_type: "generate_report".to_string(),
                payload: json!({"i": i}),
                idempotency_key: None,
            };

            let res = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/jobs")
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_string(&req).unwrap()))
                        .unwrap(),
                )
                .await
                .unwrap();

            statuses.push(res.status());
        }

        for x in statuses.clone() {
            println!("{}",x.as_str());
        }

        // Ensure at least one 429 returned
        assert!(statuses.contains(&StatusCode::TOO_MANY_REQUESTS));
    }
}
