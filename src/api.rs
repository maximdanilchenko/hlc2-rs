use actix_web::http::header;
use actix_web::{HttpResponse, Json, Path, Query, State,
                Responder, Result, HttpRequest};
use validator::Validate;
use actix_web::dev::AsyncResult;


use crate::schemas::*;
use crate::database::AppState;

pub fn filter(query: Query<Filters>, state: State<AppState>) -> HttpResponse {
    match query.validate() {
        Ok(_) => {
            let database = state.database.read();
            match database.filter(query.into_inner()) {
                Err(()) => HttpResponse::BadRequest().finish(),
                Ok(val) => HttpResponse::Ok().json(val),
            }
        }
        Err(_) => HttpResponse::BadRequest().finish(),
    }
}

pub fn group(query: Query<Group>, state: State<AppState>) -> HttpResponse {
    match query.validate() {
        Ok(_) => {
            let database = state.database.read();
            match database.group(query.into_inner()) {
                Err(status) => HttpResponse::build(status).content_type("application/json")
                    .header(header::CONNECTION, "keep-alive")
                    .header(header::SERVER, "highload")
                    .finish(),
                Ok(val) => HttpResponse::Ok().json(val),
            }
        }
        Err(_) => HttpResponse::BadRequest().content_type("application/json")
            .header(header::CONNECTION, "keep-alive")
            .header(header::SERVER, "highload")
            .finish(),
    }
}

pub fn suggest(query: Query<SuggestRecommend>, path: Path<(u32, )>, state: State<AppState>) -> HttpResponse {
    match query.validate() {
        Ok(_) => {
            let database = state.database.read();
            match database.suggest(path.0, query.into_inner()) {
                Err(status) => HttpResponse::build(status).content_type("application/json")
                    .header(header::CONNECTION, "keep-alive")
                    .header(header::SERVER, "highload")
                    .finish(),
                Ok(val) => HttpResponse::Ok().json(val),
            }
        }
        Err(_) => HttpResponse::BadRequest().content_type("application/json")
            .header(header::CONNECTION, "keep-alive")
            .header(header::SERVER, "highload")
            .finish(),
    }
}

pub fn recommend(query: Query<SuggestRecommend>, path: Path<(u32, )>, state: State<AppState>) -> HttpResponse {
    match query.validate() {
        Ok(_) => {
            let database = state.database.read();
            match database.recommend(path.0, query.into_inner()) {
                Err(status) => HttpResponse::build(status).content_type("application/json")
                    .header(header::CONNECTION, "keep-alive")
                    .header(header::SERVER, "highload")
                    .finish(),
                Ok(val) => HttpResponse::Ok().json(val),
            }
        }
        Err(_) => HttpResponse::BadRequest().content_type("application/json")
            .header(header::CONNECTION, "keep-alive")
            .header(header::SERVER, "highload")
            .finish(),
    }
}

pub fn likes(req: HttpRequest<AppState>, item: Json<LikesRequest>, state: State<AppState>) -> Result<AsyncResult<HttpResponse>> {
    let mut database = state.database.write();
    database.update_likes(item.0, req)
}

pub fn update(req: HttpRequest<AppState>, item: Json<AccountOptional>, path: Path<(u32, )>, state: State<AppState>) -> Result<AsyncResult<HttpResponse>> {
    match item.0.validate() {
        Ok(_) => {
            let mut database = state.database.write();
            database.update_account(path.0, item.0, req)
        }
        Err(_) => Ok(HttpResponse::BadRequest().content_type("application/json")
            .header(header::CONNECTION, "keep-alive")
            .header(header::SERVER, "highload")
            .finish().respond_to(&req)?),
    }
}

pub fn new(req: HttpRequest<AppState>, item: Json<AccountFull>, state: State<AppState>) -> Result<AsyncResult<HttpResponse>> {
    match item.0.validate() {
        Ok(_) => {
            let mut database = state.database.write();
            database.insert(item.0, req)
        }
        Err(_) => Ok(HttpResponse::BadRequest().content_type("application/json")
            .header(header::CONNECTION, "keep-alive")
            .header(header::SERVER, "highload")
            .finish().respond_to(&req)?),
    }
}