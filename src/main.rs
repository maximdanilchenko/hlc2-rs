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

use itertools::Itertools;

use std::collections::HashMap;
use std::collections::HashSet;
use std::env;
use std::fs::File;
use std::io::{BufReader, Read};
use std::sync::Arc;
use std::sync::RwLock;

use actix_web::{App, HttpRequest, HttpResponse, Json, Path, Query,
                server, State};
use actix_web::http::{Method, StatusCode};
use validator::Validate;


struct AppState {
    database: Arc<RwLock<DataBase>>
}

struct DataBase {
    interests: Vec<String>,
    countries: Vec<String>,
    cities: Vec<String>,
    accounts: HashMap<u32, Account>,
    emails: HashSet<String>,
    hash_sex: HashMap<Sex, HashSet<u32>>,
    hash_status: HashMap<Status, HashSet<u32>>,

}

impl DataBase {
    fn new() -> DataBase {
        let mut hash_sex = HashMap::new();
        hash_sex.insert(Sex::F, HashSet::new());
        hash_sex.insert(Sex::M, HashSet::new());
        let mut hash_status = HashMap::new();
        hash_status.insert(Status::Free, HashSet::new());
        hash_status.insert(Status::Muted, HashSet::new());
        hash_status.insert(Status::AllHard, HashSet::new());
        DataBase {
            accounts: HashMap::new(),
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
        let mut ids: HashSet<u32> = HashSet::new();

        let mut add_fname = false;
        let mut add_sname = false;
        let mut add_phone = false;
        let mut add_sex = false;
        let mut add_birth = false;
        let mut add_country = false;
        let mut add_city = false;
        let mut add_joined = false;
        let mut add_status = false;
        let mut add_interests = false;
        let mut add_premium = false;
        let mut add_likes = false;

        // Status
        match filters.status_eq {
            None => (),
            Some(val) => {
                ids.extend(&self.hash_status[&val]);
                add_status = true;
            }
        };

        match filters.status_neq {
            None => (),
            Some(val) => {
                for elem in [Status::AllHard, Status::Muted, Status::Free].iter() {
                    if *elem != val {
                        ids.extend(&self.hash_status[elem])
                    }
                };
                add_status = true;
            }
        };

        //Sex
        match filters.sex_eq {
            None => (),
            Some(val) => {
                match ids.is_empty() {
                    true => ids.extend(&self.hash_sex[&val]),
                    false => {
                        ids = ids.intersection(&self.hash_sex[&val])
                            .cloned()
                            .collect();
                    }
                };
                add_sex = true;
            }
        };


        let ids_vec: Vec<u32> = match ids.is_empty() {
            true => self.accounts.keys()
                .map(|k| k.clone()).into_iter().sorted_by(|a, b| b.cmp(a))
                .take(filters.limit as usize)
                .collect(),
            false => ids.into_iter().sorted_by(|a, b| b.cmp(a))
                .take(filters.limit as usize)
                .collect(), //iteration on accounts with another filters
        };

        let mut result = Vec::new();

        for id in &ids_vec {
            let account = self.accounts.get(id).unwrap();
            let mut elem = HashMap::new();
            elem.insert("id", json!(id));
            elem.insert("email", json!(account.email));

            if add_sex {
                elem.insert("sex", json!(account.sex));
            }

            if add_status {
                elem.insert("status", json!(account.status));
            }


            result.push(elem);
        };


        Ok(json!({"accounts": result}))
    }
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

#[derive(Debug, Serialize, Deserialize)]
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
    fname_null: Option<u8>,
    sname_eq: Option<String>,
    sname_starts: Option<String>,
    sname_null: Option<u8>,
    phone_code: Option<u16>,
    phone_null: Option<u8>,
    country_eq: Option<String>,
    country_null: Option<u8>,
    city_eq: Option<String>,
    city_any: Option<String>,
    city_null: Option<u8>,
    birth_lt: Option<i64>,
    birth_gt: Option<i64>,
    birth_year: Option<u16>,
    interests_contains: Option<String>,
    interests_any: Option<String>,
    likes_contains: Option<String>,
    premium_now: Option<i64>,
    premium_null: Option<u8>,
}

fn index(req: &HttpRequest<AppState>) -> HttpResponse {
    HttpResponse::build(StatusCode::OK)
        .content_type("application/json")
        .body("{\"accounts\": []}")
}


fn filter(query: Query<Filters>, state: State<AppState>) -> HttpResponse {
//    println!("Query: {:?}", query);
    let database = state.database.read().unwrap();
    match database.filter(query.into_inner()) {
        Err(()) => HttpResponse::build(StatusCode::BAD_REQUEST).finish(),
        Ok(val) => HttpResponse::Ok().json(val)
    }
}


fn likes(item: Json<LikesRequest>, state: State<AppState>) -> HttpResponse {
    let mut database = state.database.write().unwrap();
    match database.update_likes(item.0) {
        Ok(_) => HttpResponse::build(StatusCode::ACCEPTED)
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
                Ok(_) => HttpResponse::build(StatusCode::ACCEPTED)
                    .content_type("application/json")
                    .body("{}"),
                Err(status) => HttpResponse::build(status).finish()
            }
        }
        Err(e) => HttpResponse::build(StatusCode::BAD_REQUEST).finish()
    }
}

fn new(item: Json<AccountFull>, state: State<AppState>) -> HttpResponse {
    match item.0.validate() {
        Ok(_) => {
            let mut database = state.database.write().unwrap();
            match database.insert(item.0, true) {
                Ok(_) => HttpResponse::build(StatusCode::CREATED)
                    .content_type("application/json")
                    .body("{}"),
                Err(status) => HttpResponse::build(status).finish()
            }
        }
        Err(e) => HttpResponse::build(StatusCode::BAD_REQUEST).finish()
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
                r.method(Method::GET).f(index))
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
    println!("MemInfo:");
    match sys_info::mem_info() {
        Ok(val) => println!("{:?}", val),
        Err(e) => println!("{:?}", e)
    };
}
