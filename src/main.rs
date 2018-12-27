extern crate actix_web;
extern crate serde;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate serde_json;
extern crate zip;
#[macro_use]
extern crate validator_derive;
extern crate validator;
extern crate sys_info;
extern crate chrono;

use std::collections::{HashMap, HashSet, BTreeMap, BTreeSet};
use std::env;
use std::fs::File;
use std::io::{BufReader, Read};
use std::sync::Arc;
use std::sync::RwLock;

use actix_web::{App, HttpRequest, HttpResponse, Json, Path, Query,
                server, State};
use actix_web::http::{Method, StatusCode};
use validator::Validate;

use chrono::{Utc, Datelike, TimeZone};


struct AppState {
    database: Arc<RwLock<DataBase>>
}

struct DataBase {
    interests: Vec<String>,
    countries: Vec<String>,
    cities: Vec<String>,
    accounts: BTreeMap<u32, Account>,
    emails: HashSet<String>,
    hash_sex: HashMap<Sex, BTreeSet<u32>>,
    hash_status: HashMap<Status, BTreeSet<u32>>,

}

impl DataBase {
    fn new() -> DataBase {
        let mut hash_sex = HashMap::new();
        hash_sex.insert(Sex::F, BTreeSet::new());
        hash_sex.insert(Sex::M, BTreeSet::new());
        let mut hash_status = HashMap::new();
        hash_status.insert(Status::Free, BTreeSet::new());
        hash_status.insert(Status::Muted, BTreeSet::new());
        hash_status.insert(Status::AllHard, BTreeSet::new());
        DataBase {
            accounts: BTreeMap::new(),
            interests: Vec::new(),
            countries: Vec::new(),
            cities: Vec::new(),
            emails: HashSet::new(),
            hash_sex,
            hash_status,

        }
    }

    fn from_file(path: &str) -> DataBase {
        let mut database = DataBase::new();

        let file = File::open(path).unwrap();
        let reader = BufReader::new(file);

        let mut zip = zip::ZipArchive::new(reader).unwrap();
        let mut index = 1;
        loop {
            let file_name = format!("accounts_{}.json", index);

            let mut data_file = match zip.by_name(file_name.as_str()) {
                Ok(f) => f,
                Err(_) => break,
            };

            let mut data = String::new();
            data_file.read_to_string(&mut data).unwrap();

            let mut accounts: HashMap<String, Vec<AccountFull>> = serde_json::from_str(&data).unwrap();

            for account in accounts.remove("accounts").unwrap() {
                match database.insert(account, false) {
                    Err(_) => println!("Error while inserting"),
                    Ok(_) => ()
                }
            }

            println!("Ready file: {:?}", file_name);
            database.print_len();
            memory_info();

            index += 1;
        };
        database
    }

    fn print_len(&self) {
        println!("Accounts total num: {}", self.accounts.len());
        println!("Interests total num: {}", self.interests.len());
        println!("Countries total num: {}", self.countries.len());
        println!("Cities total num: {}", self.cities.len());
        println!("Emails total num: {}", self.emails.len());
    }

    fn validate_email(&self, email: &String) -> Result<(), ()> {
        match self.emails.contains(email) {
            true => Err(()),
            false => Ok(())
        }
    }


    fn validate_country(&mut self, row: Option<String>) -> Result<Option<usize>, ()> {
        match row {
            Some(s) => {
                match self.countries.binary_search(&s) {
                    Ok(index) => Ok(Some(index)),
                    Err(_) => {
//                        self.countries.push(s);
                        Ok(Some(self.countries.len()))
                    }
                }
            }
            None => Ok(None)
        }
    }

    fn validate_city(&mut self, row: Option<String>) -> Result<Option<usize>, ()> {
        match row {
            Some(s) => {
                match self.cities.binary_search(&s) {
                    Ok(index) => Ok(Some(index)),
                    Err(_) => {
//                        self.cities.push(s);
                        Ok(Some(self.cities.len()))
                    }
                }
            }
            None => Ok(None)
        }
    }

    fn validate_interests(&mut self, row: Vec<String>) -> Result<Vec<usize>, ()> {
        let mut interests: Vec<usize> = Vec::new();
        for elem in row {
            if elem.len() > 100 { return Err(()); }
            interests.push(match self.interests.binary_search(&elem) {
                Ok(index) => index,
                Err(_) => {
//                    self.interests.push(elem);
                    self.interests.len()
                }
            })
        }
        Ok(interests)
    }

    fn update_account(&mut self, uid: u32, account: AccountOptional) -> Result<u32, StatusCode> {
        let mut new_account = match self.accounts.get(&uid) {
            None => return Err(StatusCode::NOT_FOUND),
            Some(val) => val.clone()
        };

        match account.email {
            Some(val) => match self.validate_email(&val) {
                Ok(_) => new_account.email = val,
                Err(_) => { if new_account.email != val { return Err(StatusCode::BAD_REQUEST); } }
            }
            None => ()
        };

        match account.sex {
            None => (),
            Some(val) => new_account.sex = val
        };

        match account.status {
            None => (),
            Some(val) => new_account.status = val
        };


        match account.country {
            Some(val) => match self.validate_country(Some(val)) {
                Ok(val) => new_account.country = val,
                Err(_) => return Err(StatusCode::BAD_REQUEST)
            }
            None => ()
        };


        match account.city {
            Some(val) => match self.validate_city(Some(val)) {
                Ok(val) => new_account.city = val,
                Err(_) => return Err(StatusCode::BAD_REQUEST)
            }
            None => ()
        };

        match account.interests {
            Some(val) => match self.validate_interests(val) {
                Ok(val) => new_account.interests.extend(val),
                Err(_) => return Err(StatusCode::BAD_REQUEST)
            },
            None => ()
        };


        match account.fname {
            None => (),
            Some(val) => new_account.fname = Some(val)
        }

        match account.sname {
            None => (),
            Some(val) => new_account.sname = Some(val)
        }

        match account.phone {
            None => (),
            Some(val) => new_account.phone = Some(val)
        }

        match account.birth {
            None => (),
            Some(val) => new_account.birth = val
        }

        match account.joined {
            None => (),
            Some(val) => new_account.joined = val
        }

        match account.premium {
            None => (),
            Some(val) => new_account.premium = Some(val)
        }

        match account.likes {
            None => (),
            Some(val) => new_account.likes.extend(val)
        }

        self.emails.insert(new_account.email.clone());

        self.accounts.insert(uid, new_account);

        Ok(uid)
    }

    fn insert(&mut self, account: AccountFull, with_unique: bool) -> Result<u32, StatusCode> {
        let uid = account.id;

        if with_unique {
            if let true = self.accounts.contains_key(&uid) {
                return Err(StatusCode::BAD_REQUEST);
            };

            if let Err(_) = self.validate_email(&account.email) {
                return Err(StatusCode::BAD_REQUEST);
            };
        }

        let country = match self.validate_country(account.country) {
            Ok(val) => val,
            Err(_) => return Err(StatusCode::BAD_REQUEST)
        };
        let city = match self.validate_city(account.city) {
            Ok(val) => val,
            Err(_) => return Err(StatusCode::BAD_REQUEST)
        };
        let interests = match account.interests {
            None => Vec::new(),
            Some(val) => match self.validate_interests(val) {
                Ok(val) => val,
                Err(_) => return Err(StatusCode::BAD_REQUEST)
            }
        };

        let likes = match account.likes {
            None => Vec::new(),
            Some(val) => val
        };

        (*self.hash_status.get_mut(&account.status).unwrap()).insert(uid);
        (*self.hash_sex.get_mut(&account.sex).unwrap()).insert(uid);

        self.emails.insert(account.email.clone());

        self.accounts.insert(uid, Account {
            email: account.email,
            fname: account.fname,
            sname: account.sname,
            phone: account.phone,
            sex: account.sex,
            birth: account.birth,
            country,
            city,
            joined: account.joined,
            status: account.status,
            interests,
            premium: account.premium,
            likes,
        });
        Ok(uid)
    }

    fn update_likes(&mut self, likes: LikesRequest) -> Result<(), StatusCode> {
        let mut ids: HashSet<u32> = HashSet::new();

        for like in likes.likes.iter() {
            ids.insert(like.likee);
            ids.insert(like.liker);
        }
        for id in ids {
            if let false = self.accounts.contains_key(&id) {
                return Err(StatusCode::BAD_REQUEST);
            }
        }

        for like in likes.likes.iter() {
            self.accounts.get_mut(&like.liker).unwrap().likes.push(
                Likes { id: like.likee, ts: like.ts })
        }

        Ok(())
    }

    fn filter(&self, filters: Filters) -> Result<serde_json::Value, ()> {

        match &filters.email_domain {
        Some(val) => {return Ok(json!({"accounts": []}))},
        None => ()
        };
        match &filters.email_lt {
            Some(val) => {return Ok(json!({"accounts": []}))},
            None => ()
        };
        match &filters.email_gt {
            Some(val) => {return Ok(json!({"accounts": []}))},
            None => ()
        };
        match &filters.fname_any {
            Some(val) => {return Ok(json!({"accounts": []}))},
            None => ()
        };
        match &filters.sname_starts {
            Some(val) => {return Ok(json!({"accounts": []}))},
            None => ()
        };
        match &filters.phone_code {
            Some(val) => {return Ok(json!({"accounts": []}))},
            None => ()
        };
        match &filters.country_eq {
            Some(val) => {return Ok(json!({"accounts": []}))},
            None => ()
        };
        match &filters.country_null {
            Some(val) => {return Ok(json!({"accounts": []}))},
            None => ()
        };
        match &filters.city_eq {
            Some(val) => {return Ok(json!({"accounts": []}))},
            None => ()
        };
        match &filters.city_any {
            Some(val) => {return Ok(json!({"accounts": []}))},
            None => ()
        };
        match &filters.city_null {
            Some(val) => {return Ok(json!({"accounts": []}))},
            None => ()
        };
        match &filters.birth_lt {
            Some(val) => {return Ok(json!({"accounts": []}))},
            None => ()
        };
        match &filters.birth_gt {
            Some(val) => {return Ok(json!({"accounts": []}))},
            None => ()
        };
        match &filters.interests_contains {
            Some(val) => {return Ok(json!({"accounts": []}))},
            None => ()
        };
        match &filters.interests_any {
            Some(val) => {return Ok(json!({"accounts": []}))},
            None => ()
        };
        match &filters.likes_contains {
            Some(val) => {return Ok(json!({"accounts": []}))},
            None => ()
        };
        match &filters.premium_now {
            Some(val) => {return Ok(json!({"accounts": []}))},
            None => ()
        };
        match &filters.premium_null {
            Some(val) => {return Ok(json!({"accounts": []}))},
            None => ()
        };



        let end_ids: BTreeMap<&u32, &Account> = self.accounts
            .iter()
            .rev()
            .filter(|(_, acc)| filter_one(&filters, acc))
            .take(filters.limit as usize)
            .collect();

        let mut result = Vec::new();

        for (id, acc) in end_ids.iter().rev() {
            let mut elem = HashMap::new();
            elem.insert("id", json!(id));
            elem.insert("email", json!(acc.email));

            if let Some(_) = filters.sex_eq {
                elem.insert("sex", json!(acc.sex));
            };

            match filters.status_eq {
                Some(_) => { elem.insert("status", json!(acc.status)); }
                None => {
                    match filters.status_neq {
                        Some(_) => { elem.insert("status", json!(acc.status)); }
                        None => ()
                    };
                }
            };

            match filters.birth_year {
                Some(_) => { elem.insert("birth", json!(acc.birth)); }
                None => {}
            };

            match filters.fname_null {
                Some(val) => { if val == 0 {elem.insert("fname", json!(acc.fname)); }}
                None => {
                    match filters.fname_eq {
                        Some(_) => { elem.insert("fname", json!(acc.fname)); }
                        None => ()
                    };
                }
            };

            match filters.sname_null {
                Some(val) => { if val == 0 {elem.insert("sname", json!(acc.sname)); }}
                None => {
                    match filters.sname_eq {
                        Some(_) => { elem.insert("sname", json!(acc.sname)); }
                        None => ()
                    };
                }
            };

            match filters.phone_null {
                Some(val) => { if val == 0 {elem.insert("phone", json!(acc.phone)); }}
                None => {}
            };


            result.push(elem);
        };


        Ok(json!({"accounts": result}))
    }
}

fn filter_one(filters: &Filters, account: &Account) -> bool {

    match &filters.status_eq {
        None => (),
        Some(val) => {
            if account.status != *val { return false; }
        }
    };
    match &filters.status_neq {
        None => (),
        Some(val) => {
            if account.status == *val { return false; }
        }
    };
    match &filters.sex_eq {
        None => (),
        Some(val) => {
            if account.sex != *val { return false; }
        }
    };
    match &filters.birth_year {
        None => (),
        Some(val) => {
            if Utc.timestamp(account.birth, 0).year() != *val { return false; }
        }
    };
    match &filters.fname_null {
        None => (),
        Some(val) => {
            match val {
                1 => { if let Some(_) = account.fname { return false; } }
                0 => { if let None = account.fname { return false; } }
                _ => ()
            }
        }
    };
    match &filters.fname_eq {
        None => (),
        Some(val) => {
            if let Some(name) = &account.fname {
                if name != val { return false; }
            } else {
                return false;
            }
        }
    };
    match &filters.phone_null {
        None => (),
        Some(val) => {
            match val {
                1 => { if let Some(_) = account.phone { return false; } }
                0 => { if let None = account.phone { return false; } }
                _ => ()
            }
        }
    };
    match &filters.sname_null {
        None => (),
        Some(val) => {
            match val {
                1 => { if let Some(_) = account.sname { return false; } }
                0 => { if let None = account.sname { return false; } }
                _ => ()
            }
        }
    };
    match &filters.sname_eq {
        None => (),
        Some(val) => {
            if let Some(name) = &account.sname {
                if name != val { return false; }
            } else {
                return false;
            }
        }
    };
//    match &filters.email_domain {
//        Some(val) => (),
//        None => ()
//    };
//    match &filters.email_lt {
//        Some(val) => (),
//        None => ()
//    };
//    match &filters.email_gt {
//        Some(val) => (),
//        None => ()
//    };
//    match &filters.fname_any {
//        Some(val) => (),
//        None => ()
//    };
//    match &filters.sname_starts {
//        Some(val) => (),
//        None => ()
//    };
//    match &filters.phone_code {
//        Some(val) => (),
//        None => ()
//    };
//    match &filters.country_eq {
//        Some(val) => (),
//        None => ()
//    };
//    match &filters.country_null {
//        Some(val) => (),
//        None => ()
//    };
//    match &filters.city_eq {
//        Some(val) => (),
//        None => ()
//    };
//    match &filters.city_any {
//        Some(val) => (),
//        None => ()
//    };
//    match &filters.city_null {
//        Some(val) => (),
//        None => ()
//    };
//    match &filters.birth_lt {
//        Some(val) => (),
//        None => ()
//    };
//    match &filters.birth_gt {
//        Some(val) => (),
//        None => ()
//    };
//    match &filters.interests_contains {
//        Some(val) => (),
//        None => ()
//    };
//    match &filters.interests_any {
//        Some(val) => (),
//        None => ()
//    };
//    match &filters.likes_contains {
//        Some(val) => (),
//        None => ()
//    };
//    match &filters.premium_now {
//        Some(val) => (),
//        None => ()
//    };
//    match &filters.premium_null {
//        Some(val) => (),
//        None => ()
//    };
    true
}

#[derive(Debug, Clone)]
#[derive(PartialEq, Eq, Hash, Serialize, Deserialize)]
enum Sex {
    #[serde(rename = "m")]
    M,
    #[serde(rename = "f")]
    F,
}

#[derive(Debug, Clone)]
#[derive(PartialEq, Eq, Hash, Serialize, Deserialize)]
enum Status {
    #[serde(rename = "свободны")]
    Free,
    #[serde(rename = "заняты")]
    Muted,
    #[serde(rename = "всё сложно")]
    AllHard,
}

#[derive(Debug, Clone)]
struct Account {
    email: String,
    fname: Option<String>,
    sname: Option<String>,
    phone: Option<String>,
    sex: Sex,
    birth: i64,
    country: Option<usize>,
    city: Option<usize>,
    joined: i64,
    status: Status,
    interests: Vec<usize>,
    premium: Option<Premium>,
    likes: Vec<Likes>,
}

#[serde(deny_unknown_fields)]
#[derive(Debug, Validate, Serialize, Deserialize)]
struct AccountFull {
    id: u32,
    #[validate(contains = "@")]
    #[validate(length(max = "100"))]
    email: String,
    #[validate(length(max = "50"))]
    fname: Option<String>,
    #[validate(length(max = "50"))]
    sname: Option<String>,
    #[validate(length(max = "16"))]
    phone: Option<String>,
    sex: Sex,
    birth: i64,
    #[validate(length(max = "50"))]
    country: Option<String>,
    #[validate(length(max = "50"))]
    city: Option<String>,
    joined: i64,
    status: Status,
    interests: Option<Vec<String>>,
    premium: Option<Premium>,
    likes: Option<Vec<Likes>>,
}

#[serde(deny_unknown_fields)]
#[derive(Debug, Validate, Serialize, Deserialize)]
struct AccountOptional {
    #[validate(contains = "@")]
    #[validate(length(max = "100"))]
    email: Option<String>,
    #[validate(length(max = "50"))]
    fname: Option<String>,
    #[validate(length(max = "50"))]
    sname: Option<String>,
    #[validate(length(max = "16"))]
    phone: Option<String>,
    sex: Option<Sex>,
    birth: Option<i64>,
    #[validate(length(max = "50"))]
    country: Option<String>,
    #[validate(length(max = "50"))]
    city: Option<String>,
    joined: Option<i64>,
    status: Option<Status>,
    interests: Option<Vec<String>>,
    premium: Option<Premium>,
    likes: Option<Vec<Likes>>,
}

#[derive(Debug, Clone)]
#[derive(Serialize, Deserialize)]
struct Premium {
    start: i64,
    finish: i64,
}

#[derive(Debug, Clone)]
#[derive(Serialize, Deserialize)]
struct Likes {
    id: u32,
    ts: i64,
}

#[derive(Debug, Clone)]
#[derive(Serialize, Deserialize)]
struct LikerLikee {
    liker: u32,
    likee: u32,
    ts: i64,
}

#[derive(Debug, Clone)]
#[derive(Serialize, Deserialize)]
struct LikesRequest {
    likes: Vec<LikerLikee>
}

#[serde(deny_unknown_fields)]
#[derive(Debug, Validate, Serialize, Deserialize)]
struct Filters {
    limit: u32,
    sex_eq: Option<Sex>,
    email_domain: Option<String>,
    email_lt: Option<String>,
    email_gt: Option<String>,
    status_eq: Option<Status>,
    status_neq: Option<Status>,
    fname_eq: Option<String>,
    fname_any: Option<String>,
    #[validate(range(min = "0", max = "1"))]
    fname_null: Option<u8>,
    sname_eq: Option<String>,
    sname_starts: Option<String>,
    #[validate(range(min = "0", max = "1"))]
    sname_null: Option<u8>,
    phone_code: Option<u16>,
    #[validate(range(min = "0", max = "1"))]
    phone_null: Option<u8>,
    country_eq: Option<String>,
    country_null: Option<u8>,
    city_eq: Option<String>,
    city_any: Option<String>,
    #[validate(range(min = "0", max = "1"))]
    city_null: Option<u8>,
    birth_lt: Option<i64>,
    birth_gt: Option<i64>,
    birth_year: Option<i32>,
    interests_contains: Option<String>,
    interests_any: Option<String>,
    likes_contains: Option<String>,
    premium_now: Option<i64>,
    #[validate(range(min = "0", max = "1"))]
    premium_null: Option<u8>,
}

fn index(req: &HttpRequest<AppState>) -> HttpResponse {
    HttpResponse::build(StatusCode::OK)
        .content_type("application/json")
        .body("{\"accounts\": []}")
}


fn filter(query: Query<Filters>, state: State<AppState>) -> HttpResponse {
    match query.validate() {
        Ok(_) => {
            let database = state.database.read().unwrap();
            match database.filter(query.into_inner()) {
                Err(()) => HttpResponse::BadRequest().finish(),
                Ok(val) => HttpResponse::Ok().json(val)
            }
        }
        Err(_) => HttpResponse::build(StatusCode::BAD_REQUEST).finish()
    }
}


fn likes(item: Json<LikesRequest>, state: State<AppState>) -> HttpResponse {
    let mut database = state.database.write().unwrap();
    match database.update_likes(item.0) {
        Ok(_) => HttpResponse::Accepted()
            .content_type("application/json")
            .body("{}"),
        Err(status) => HttpResponse::build(status).finish()
    }
}

fn update(item: Json<AccountOptional>, path: Path<(u32, )>, state: State<AppState>) -> HttpResponse {
    match item.0.validate() {
        Ok(_) => {
            let mut database = state.database.write().unwrap();
            match database.update_account(path.0, item.0) {
                Ok(_) => HttpResponse::Accepted()
                    .content_type("application/json")
                    .body("{}"),
                Err(status) => HttpResponse::build(status).finish()
            }
        }
        Err(_) => HttpResponse::BadRequest().finish()
    }
}

fn new(item: Json<AccountFull>, state: State<AppState>) -> HttpResponse {
    match item.0.validate() {
        Ok(_) => {
            let mut database = state.database.write().unwrap();
            match database.insert(item.0, true) {
                Ok(_) => HttpResponse::Created()
                    .content_type("application/json")
                    .body("{}"),
                Err(status) => HttpResponse::build(status).finish()
            }
        }
        Err(_) => HttpResponse::BadRequest().finish()
    }
}

fn main() {
    let prod = env::var("PROD").is_ok();
    let addr = if prod { "0.0.0.0:80" } else { "0.0.0.0:8010" };
    let data_file = if prod { "/tmp/data/data.zip" } else { "data.zip" };

    let sys = actix::System::new("accounts");
    let database = Arc::new(RwLock::new(DataBase::from_file(data_file)));
    println!("Database ready!");
    server::new(move || {
        App::with_state(AppState { database: database.clone() })
            .prefix("/accounts")
            .resource("/filter/", |r|
                r.method(Method::GET).with(filter))
            .resource("/group/", |r|
                r.method(Method::GET).f(index))
            .resource("/{id}/recommend/", |r|
                r.method(Method::GET).f(index))
            .resource("/{id}/suggest/", |r|
                r.method(Method::GET).f(index))
            .resource("/new/", |r|
                r.method(Method::POST).with(new))
            .resource("/likes/", |r|
                r.method(Method::POST).with(likes))
            .resource("/{id}/", |r|
                r.method(Method::POST).with(update))
    }).keep_alive(120)
        .workers(8)
        .bind(addr)
        .unwrap()
        .shutdown_timeout(1)
        .start();
    println!("Started http server: {}", addr);
    memory_info();
    let _ = sys.run();
    println!("Stopped http server");
    memory_info();
}


fn memory_info() {
    match sys_info::mem_info() {
        Ok(val) => println!("{:?}", val),
        Err(e) => println!("{:?}", e)
    };
}
