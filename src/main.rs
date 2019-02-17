extern crate actix_web;
extern crate serde;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate serde_json;
extern crate zip;
#[macro_use]
extern crate validator_derive;
extern crate chrono;
extern crate indexmap;
extern crate itertools;
extern crate validator;
extern crate radix_trie;
extern crate flat_map;
extern crate smallvec;
extern crate smallset;
extern crate parking_lot;
extern crate hashbrown;

use std::env;
use std::sync::{Arc};

use actix_web::http::{header, Method};
use actix_web::{error, server, App, HttpResponse};
use parking_lot::RwLock;

mod api;
mod database;
mod schemas;


fn main() {
    let prod = env::var("PROD").is_ok();
    let addr = if prod { "0.0.0.0:80" } else { "0.0.0.0:8010" };
    let data_file = if prod {
        "/tmp/data/data.zip"
    } else {
        "data.zip"
    };

    let sys = actix::System::new("accounts");
    let database = if prod {
        Arc::new(RwLock::new(database::DataBase::from_file_in_place(data_file)))
    } else {
        Arc::new(RwLock::new(database::DataBase::from_file(data_file)))
    };
    println!("Database ready!");

    server::new(move || {
        App::with_state(database::AppState {
            database: database.clone(),
        })
            .prefix("/accounts")
            .resource("/filter/", |r| {
                r.method(Method::GET).with_config(api::filter, |(cfg, _)| {
                    cfg.error_handler(|err, _| {
                        error::InternalError::from_response(
                            err,
                            HttpResponse::BadRequest()
                                .content_type("application/json")
                                .header(header::CONNECTION, "keep-alive")
                                .header(header::SERVER, "highload")
                                .finish(),
                        )
                            .into()
                    });
                })
            })
            .resource("/group/", |r|
                r.method(Method::GET).with_config(api::group, |(cfg, _)| {
                    cfg.error_handler(|err, _| {
                        error::InternalError::from_response(
                            err,
                            HttpResponse::BadRequest()
                                .content_type("application/json")
                                .header(header::CONNECTION, "keep-alive")
                                .header(header::SERVER, "highload")
                                .finish(),
                        )
                            .into()
                    });
                }))
            .resource("/{id}/recommend/", |r|
                r.method(Method::GET).with_config(api::recommend, |(cfg, _, _)| {
                    cfg.error_handler(|err, _| {
                        error::InternalError::from_response(
                            err,
                            HttpResponse::BadRequest()
                                .content_type("application/json")
                                .header(header::CONNECTION, "keep-alive")
                                .header(header::SERVER, "highload")
                                .finish(),
                        )
                            .into()
                    });
                }))
            .resource("/{id}/suggest/", |r|
                r.method(Method::GET).with_config(api::suggest, |(cfg, _, _)| {
                    cfg.error_handler(|err, _| {
                        error::InternalError::from_response(
                            err,
                            HttpResponse::BadRequest()
                                .content_type("application/json")
                                .header(header::CONNECTION, "keep-alive")
                                .header(header::SERVER, "highload")
                                .finish(),
                        )
                            .into()
                    });
                }))
            .resource("/new/", |r| r.method(Method::POST).with(api::new))
            .resource("/likes/", |r| r.method(Method::POST).with(api::likes))
            .resource("/{id}/", |r| r.method(Method::POST).with(api::update))
            .default_resource(|r| {
                r.f(|_| HttpResponse::NotFound()
                    .content_type("application/json")
                    .header(header::CONNECTION, "keep-alive")
                    .header(header::SERVER, "highload")
                    .finish()
                );
            })
    })
        .keep_alive(1800)
        .bind(addr)
        .unwrap()
        .shutdown_timeout(1)
        .start();
    println!("Started http server: {}", addr);
    let _ = sys.run();
    println!("Stopped http server");
}

// 1697M without liked
// 2110M with liked
// 1752M with groups and without liked
// 1537M with groups and without liked and with u16 instead of usize
// - 1951M with groups and with liked(Vec) and with u16 instead of usize - плохо на третьей фазе
// 1942M with groups and with liked(Vec) and with u16 instead of usize and with fnv emails
// 2103M with groups and with liked(hashbrown::HashSet) and with u16 instead of usize
// 1547M without accs
// 1865M without interests
// 1869M with optimized timestamps
// - 1869M with optimized timestamps - все еще плохо на третьей фазе
// 1853M without joined index
// 1637M with likes as Vecs
// 1832M with likes as Vecs with ts in liked

// interests - 86
// accs - 404

// 2844M with stupid groups index (len 11915127)
// 3088M with clever groups index Oo
