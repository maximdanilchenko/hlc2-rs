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
use std::ops::Bound::Excluded;

use actix_web::{App, HttpRequest, HttpResponse, Json, Path, Query,
                server, State};
use actix_web::http::{Method, StatusCode};
use validator::Validate;
use itertools::Itertools;

use chrono::{Utc, Datelike, TimeZone};


struct AppState {
    database: Arc<RwLock<DataBase>>
}

struct DataBase {
    interests: BTreeMap<String, BTreeSet<u32>>,
    countries: HashMap<String, BTreeSet<u32>>,
    cities: HashMap<String, BTreeSet<u32>>,
    accounts: BTreeMap<u32, Account>,
    emails: BTreeMap<String, u32>,
    hash_sex: HashMap<Sex, BTreeSet<u32>>,
    hash_status: HashMap<Status, BTreeSet<u32>>,
    hash_codes: HashMap<u16, BTreeSet<u32>>,
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
            interests: BTreeMap::new(),
            countries: HashMap::new(),
            cities: HashMap::new(),
            emails: BTreeMap::new(),
            hash_sex,
            hash_status,
            hash_codes: HashMap::new(),

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

    fn insert_to_codes(&mut self, uid: &u32, phone: String) {
//        self.countries.entry(country).or_insert(BTreeSet::new()).insert(uid.clone());
    }

    fn delete_from_codes(&mut self, uid: &u32, phone: String) {
//        self.countries.entry(country).and_modify(|set|
//            { set.remove(uid); }
//        );
    }

    fn insert_to_country(&mut self, uid: &u32, country: String) {
        self.countries.entry(country).or_insert(BTreeSet::new()).insert(uid.clone());
    }

    fn delete_from_country(&mut self, uid: &u32, country: String) {
        self.countries.entry(country).and_modify(|set|
            { set.remove(uid); }
        );
    }

    fn insert_to_city(&mut self, uid: &u32, city: String) {
        self.cities.entry(city).or_insert(BTreeSet::new()).insert(uid.clone());
    }

    fn delete_from_city(&mut self, uid: &u32, city: String) {
        self.cities.entry(city).and_modify(|set|
            { set.remove(uid); }
        );
    }

    fn insert_to_interests(&mut self, uid: &u32, interests: Vec<String>) {
        for elem in interests {
            self.interests.entry(elem).or_insert(BTreeSet::new()).insert(uid.clone());
        }
    }

    fn update_account(&mut self, uid: u32, account: AccountOptional) -> Result<u32, StatusCode> {
        let mut new_account = match self.accounts.get_mut(&uid) {
            Some(account) => account,
            None => return Err(StatusCode::NOT_FOUND)
        };

        if let Some(email) = account.email {
            match self.emails.get(&email) {
                None => (),
                Some(other_uid) => {
                    if *other_uid != uid {
                        return Err(StatusCode::BAD_REQUEST);
                    };
                }
            };
            new_account.email = email.clone();
            self.emails.insert(email, uid);
        };

        if let Some(sex) = account.sex {
            new_account.sex = sex;
        };

        if let Some(status) = account.status {
            new_account.status = status;
        };

        if let Some(fname) = account.fname {
            new_account.fname = Some(fname);
        };

        if let Some(sname) = account.sname {
            new_account.sname = Some(sname);
        };

        if let Some(phone) = account.phone {
            new_account.phone = Some(phone);
        };

        if let Some(birth) = account.birth {
            new_account.birth = birth;
        };

        if let Some(joined) = account.joined {
            new_account.joined = joined;
        };

        if let Some(premium) = account.premium {
            new_account.premium = Some(premium);
        };

        if let Some(likes) = account.likes {
            new_account.likes.extend(likes);
        };

        if let Some(city) = account.city {
            self.delete_from_city(&uid, city.clone());
            self.insert_to_city(&uid, city);
        };

        if let Some(country) = account.country {
            self.delete_from_country(&uid, country.clone());
            self.insert_to_country(&uid, country);
        };

        if let Some(interests) = account.interests {
            self.insert_to_interests(&uid, interests);
        };

        Ok(uid)
    }

    fn insert(&mut self, account: AccountFull, with_unique: bool) -> Result<u32, StatusCode> {
        let uid = account.id;

        if with_unique {
            if let true = self.accounts.contains_key(&uid) {
                return Err(StatusCode::BAD_REQUEST);
            };

            if self.emails.contains_key(&account.email) {
                return Err(StatusCode::BAD_REQUEST);
            };
        }

        let likes = match account.likes {
            None => Vec::new(),
            Some(val) => val
        };

        let city = match account.city {
            Some(city) => {
                self.insert_to_city(&uid, city.clone());
                Some(city)
            }
            None => None
        };

        let country = match account.country {
            Some(country) => {
                self.insert_to_country(&uid, country.clone());
                Some(country)
            }
            None => None
        };

        if let Some(val) = account.interests {
            self.insert_to_interests(&uid, val);
        };

        (*self.hash_status.get_mut(&account.status).unwrap()).insert(uid);
        (*self.hash_sex.get_mut(&account.sex).unwrap()).insert(uid);

        self.emails.insert(account.email.clone(), uid);

        self.accounts.insert(uid, Account {
            email: account.email,
            fname: account.fname,
            sname: account.sname,
            phone: account.phone,
            sex: account.sex,
            birth: account.birth,
            joined: account.joined,
            status: account.status,
            premium: account.premium,
            country,
            city,
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
        let mut ids_filter = BTreeSet::new();

        for (filter, data) in [
            (&filters.city_eq, &self.cities),
            (&filters.country_eq, &self.countries),
        ].iter() {
            match filter {
                Some(key) => {
                    match data.get(key) {
                        Some(vec) => match ids_filter.is_empty() {
                            true => ids_filter.extend(vec.iter()),
                            false => {
                                for elem in &ids_filter {
                                    if !vec.contains(elem) {
                                        ids_filter.remove(elem);
                                    };
                                };
                                if ids_filter.is_empty() {
                                    return Ok(json!({"accounts": []}));
                                };
                            }
                        },
                        None => return Ok(json!({"accounts": []}))
                    }
                }
                None => ()
            };
        };
        if let Some(status) = &filters.status_eq {
            match self.hash_status.get(status) {
                Some(vec) => match ids_filter.is_empty() {
                    true => ids_filter.extend(vec.iter()),
                    false => {
                        for elem in &ids_filter {
                            if !vec.contains(elem) {
                                ids_filter.remove(elem);
                            };
                        };
                        if ids_filter.is_empty() {
                            return Ok(json!({"accounts": []}));
                        };
                    }
                },
                None => return Ok(json!({"accounts": []}))
            }
        };
        if let Some(status) = &filters.status_neq {
            for val in [Status::Free, Status::Muted, Status::AllHard].iter() {
                if status != val {
                    match self.hash_status.get(val) {
                        Some(vec) => match ids_filter.is_empty() {
                            true => ids_filter.extend(vec.iter()),
                            false => {
                                for elem in &ids_filter {
                                    if !vec.contains(elem) {
                                        ids_filter.remove(elem);
                                    };
                                };
                                if ids_filter.is_empty() {
                                    return Ok(json!({"accounts": []}));
                                };
                            }
                        },
                        None => return Ok(json!({"accounts": []}))
                    }
                }
            }
        };
        if let Some(sex) = &filters.sex_eq {
            match self.hash_sex.get(sex) {
                Some(vec) => match ids_filter.is_empty() {
                    true => ids_filter.extend(vec.iter()),
                    false => {
                        for elem in &ids_filter {
                            if !vec.contains(elem) {
                                ids_filter.remove(elem);
                            };
                        };
                        if ids_filter.is_empty() {
                            return Ok(json!({"accounts": []}));
                        };
                    }
                },
                None => return Ok(json!({"accounts": []}))
            }
        };

        let end_ids: BTreeMap<&u32, &Account> = if ids_filter.is_empty() {
            self.accounts
                .iter()
                .rev()
                .filter(|(_, acc)| filter_one(&filters, acc))
                .take(filters.limit as usize)
                .collect()
        } else {
            ids_filter
                .iter()
                .rev()
                .map(|k| (k, self.accounts.get(&k).unwrap()))
                .filter(|(_, acc)| filter_one(&filters, acc))
                .take(filters.limit as usize)
                .collect()
        };

        let mut result = Vec::new();

        for (id, acc) in end_ids.iter().rev() {
            let mut elem = HashMap::new();
            elem.insert("id", json!(id));
            elem.insert("email", json!(acc.email));

            if let Some(_) = filters.sex_eq {
                elem.insert("sex", json!(acc.sex));
            };

            if let Some(_) = filters.status_eq {
                elem.insert("status", json!(acc.status));
            } else if let Some(_) = filters.status_neq {
                elem.insert("status", json!(acc.status));
            };

            if let Some(_) = filters.birth_year {
                elem.insert("birth", json!(acc.birth));
            } else if let Some(_) = filters.birth_gt {
                elem.insert("birth", json!(acc.birth));
            } else if let Some(_) = filters.birth_lt {
                elem.insert("birth", json!(acc.birth));
            };

            if let Some(_) = filters.fname_eq {
                elem.insert("fname", json!(acc.fname));
            } else if let Some(val) = filters.fname_null {
                if val == 0 {
                    elem.insert("fname", json!(acc.fname));
                };
            };

            if let Some(_) = filters.sname_eq {
                elem.insert("sname", json!(acc.sname));
            } else if let Some(val) = filters.sname_null {
                if val == 0 {
                    elem.insert("sname", json!(acc.sname));
                };
            };

            if let Some(val) = filters.phone_null {
                if val == 0 {
                    elem.insert("phone", json!(acc.phone));
                };
            };

            if let Some(_) = filters.city_any {
                elem.insert("city", json!(acc.city));
            } else if let Some(_) = filters.city_eq {
                elem.insert("city", json!(acc.city));
            } else if let Some(val) = filters.city_null {
                if val == 0 {
                    elem.insert("city", json!(acc.city));
                };
            };

            if let Some(_) = filters.country_eq {
                elem.insert("country", json!(acc.country));
            } else if let Some(val) = filters.country_null {
                if val == 0 {
                    elem.insert("country", json!(acc.country));
                };
            };


            result.push(elem);
        };


        Ok(json!({"accounts": result}))
    }
}

fn filter_one(filters: &Filters, account: &Account) -> bool {
//    match &filters.status_eq {
//        None => (),
//        Some(val) => {
//            if account.status != *val { return false; }
//        }
//    };
//    match &filters.status_neq {
//        None => (),
//        Some(val) => {
//            if account.status == *val { return false; }
//        }
//    };
//    match &filters.sex_eq {
//        None => (),
//        Some(val) => {
//            if account.sex != *val { return false; }
//        }
//    };
    match &filters.birth_year {
        None => (),
        Some(val) => {
            if Utc.timestamp(account.birth, 0).year() != *val { return false; }
        }
    };
    match &filters.birth_gt {
        None => (),
        Some(val) => {
            if account.birth <= *val { return false; }
        }
    };
    match &filters.birth_lt {
        None => (),
        Some(val) => {
            if account.birth >= *val { return false; }
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
    country: Option<String>,
    city: Option<String>,
    joined: i64,
    status: Status,
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
    // +
    sex_eq: Option<Sex>,
    // +
    email_domain: Option<String>,
    //
    email_lt: Option<String>,
    //
    email_gt: Option<String>,
    //
    status_eq: Option<Status>,
    // +
    status_neq: Option<Status>,
    // +
    fname_eq: Option<String>,
    // +
    fname_any: Option<String>,
    //
    #[validate(range(min = "0", max = "1"))]
    fname_null: Option<u8>,
    // +
    sname_eq: Option<String>,
    // +
    sname_starts: Option<String>,
    //
    #[validate(range(min = "0", max = "1"))]
    sname_null: Option<u8>,
    // +
    phone_code: Option<u16>,
    //
    #[validate(range(min = "0", max = "1"))]
    phone_null: Option<u8>,
    //
    country_eq: Option<String>,
    // +
    country_null: Option<u8>,
    //
    city_eq: Option<String>,
    // +
    city_any: Option<String>,
    //
    #[validate(range(min = "0", max = "1"))]
    city_null: Option<u8>,
    //
    birth_lt: Option<i64>,
    // +
    birth_gt: Option<i64>,
    // +
    birth_year: Option<i32>,
    // +
    interests_contains: Option<String>,
    //
    interests_any: Option<String>,
    //
    likes_contains: Option<String>,
    //
    premium_now: Option<i64>,
    //
    #[validate(range(min = "0", max = "1"))]
    premium_null: Option<u8>,
    //
    query_id: Option<i64>,
}

fn index(_req: &HttpRequest<AppState>) -> HttpResponse {
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
