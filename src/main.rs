extern crate actix_web;
extern crate zip;

use std::io::{BufReader, Read};
use std::fs::File;
use std::collections::HashMap;
use std::collections::HashSet;
use std::env;
use std::sync::Arc;
use std::sync::Mutex;

use actix_web::{Path, State, server, App, HttpRequest, HttpResponse,
                Json, Query};
use actix_web::http::{Method, StatusCode};
#[macro_use]
extern crate serde_derive;


struct AppState {
    database: Arc<Mutex<DataBase>>
}

struct DataBase {
    interests: Vec<String>,
    countries: Vec<String>,
    cities: Vec<String>,
    accounts: HashMap<u32, Account>,
    emails: HashSet<String>,
}

impl DataBase {
    fn new() -> DataBase {
        DataBase {
            interests: Vec::new(),
            countries: Vec::new(),
            cities: Vec::new(),
            accounts: HashMap::new(),
            emails: HashSet::new()
        }
    }

    fn from_file(path: &str) -> DataBase{
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

            index += 1;
        };
        database

    }

    fn print_len(&self) {
        println!("Accounts num: {}", self.accounts.len());
    }

    fn validate_email(&self, email: &String) -> Result<(), ()>{
        match email.find('@') {
            None => Err(()),
            Some(_) => match self.emails.contains(email) {
                true => Err(()),
                false => Ok(())
            }
        }
    }

    fn validate_sex(&self, row: &str) -> Result<Sex, ()> {
        match row {
            "f" => Ok(Sex::F),
            "m" => Ok(Sex::M),
            _ => Err(())
        }
    }

    fn validate_status(&self, row: &str) -> Result<Status, ()> {
        match row {
            "свободны" => Ok(Status::Free),
            "заняты" => Ok(Status::Muted),
            "всё сложно" => Ok(Status::AllHard),
            _ => Err(())
        }
    }

    fn validate_country(&mut self, row: Option<String>) -> Result<Option<usize>, ()>{
        match row {
            Some(s) => {
                if s.len() > 50 {return Err(())}
                match self.countries.binary_search(&s) {
                    Ok(index) => Ok(Some(index)),
                    Err(_) => {
                        self.countries.push(s);
                        Ok(Some(self.countries.len()))
                    }
                }
            },
            None => Ok(None)
        }
    }

    fn validate_city(&mut self, row: Option<String>) -> Result<Option<usize>, ()>{
        match row {
            Some(s) => {
                if s.len() > 50 {return Err(())}
                match self.cities.binary_search(&s) {
                    Ok(index) => Ok(Some(index)),
                    Err(_) => {
                        self.cities.push(s);
                        Ok(Some(self.cities.len()))
                    }
                }
            },
            None => Ok(None)
        }
    }

    fn validate_interests(&mut self, row: Vec<String>) -> Result<Vec<usize>, ()>{
        let mut interests: Vec<usize> = Vec::new();
        for elem in row {
            if elem.len() > 100 {return Err(())}
            interests.push(match self.interests.binary_search(&elem) {
                Ok(index) => index,
                Err(_) => {
                        self.interests.push(elem);
                        self.interests.len()
                    }
            })
        }
        Ok(interests)
    }

    fn update_account(&mut self, uid: u32, account: AccountOptional) -> Result<u32, StatusCode> {

        let mut new_account = match self.accounts.get(&uid){
            None => return Err(StatusCode::NOT_FOUND),
            Some(val) => val.clone()
        };

        match account.email {
            Some(val) => match self.validate_email(&val) {
                Ok(_) => new_account.email = val,
                Err(_) => {if new_account.email != val {return Err(StatusCode::BAD_REQUEST)}}
            }
            None => ()
        };

        match account.sex {
            Some(val) => match self.validate_sex(val.as_str()) {
                Ok(val) => new_account.sex = val,
                Err(_) => return Err(StatusCode::BAD_REQUEST)
            },
            None => ()
        };

        match account.status {
            Some(val) => match self.validate_status(val.as_str()) {
                Ok(val) => new_account.status = val,
                Err(_) => return Err(StatusCode::BAD_REQUEST)
            }
            None => ()
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
            Some(val) => match self.validate_interests(val){
                Ok(val) => new_account.interests.extend(val),
                Err(_) => return Err(StatusCode::BAD_REQUEST)
            },
            None => ()
        };

        self.emails.insert(new_account.email.clone());

        self.accounts.insert(uid, new_account);

        Ok(uid)
    }

    fn insert(&mut self, account: AccountFull, with_unique: bool) -> Result<u32, StatusCode> {
        let uid = account.id;

        if with_unique {
            if let true = self.accounts.contains_key(&uid) {
                return Err(StatusCode::BAD_REQUEST)
            };

            if let Err(_) = self.validate_email(&account.email) {
                return Err(StatusCode::BAD_REQUEST)
            };
        }

        let sex = match self.validate_sex(account.sex.as_str()) {
            Ok(val) => val,
            Err(_) => return Err(StatusCode::BAD_REQUEST)
        };
        let status = match self.validate_status(account.status.as_str()) {
            Ok(val) => val,
            Err(_) => return Err(StatusCode::BAD_REQUEST)
        };
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
            Some(val) => match self.validate_interests(val){
                Ok(val) => val,
                Err(_) => return Err(StatusCode::BAD_REQUEST)
            }
        };

        let likes = match account.likes {
            None => Vec::new(),
            Some(val) => val
        };

        self.emails.insert(account.email.clone());

        self.accounts.insert(uid ,Account{
            email: account.email,
            fname: account.fname,
            sname: account.sname,
            phone: account.phone,
            sex,
            birth: account.birth,
            country,
            city,
            joined: account.joined,
            status,
            interests,
            premium: account.premium,
            likes
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
                return Err(StatusCode::BAD_REQUEST)
            }
        }

        for like in likes.likes.iter() {
            self.accounts.get_mut(&like.liker).unwrap().likes.push(
                Likes{id: like.likee, ts: like.ts})
        }

        Ok(())
    }

    fn filter(&self, filters: Filters) {

    }
}

#[derive(Debug, Clone)]
enum Sex { M, F }

#[derive(Debug, Clone)]
enum Status { Free, Muted, AllHard }

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
    likes: Vec<Likes>
}

#[derive(Debug, Serialize, Deserialize)]
struct AccountFull {
    id: u32,
    email: String,
    fname: Option<String>,
    sname: Option<String>,
    phone: Option<String>,
    sex: String,
    birth: i64,
    country: Option<String>,
    city: Option<String>,
    joined: i64,
    status: String,
    interests: Option<Vec<String>>,
    premium: Option<Premium>,
    likes: Option<Vec<Likes>>
}

#[derive(Debug, Serialize, Deserialize)]
struct AccountOptional {
    email: Option<String>,
    fname: Option<String>,
    sname: Option<String>,
    phone: Option<String>,
    sex: Option<String>,
    birth: Option<i64>,
    country: Option<String>,
    city: Option<String>,
    joined: Option<i64>,
    status: Option<String>,
    interests: Option<Vec<String>>,
    premium: Option<Premium>,
    likes: Option<Vec<Likes>>
}

#[derive(Debug, Clone)]
#[derive(Serialize, Deserialize)]
struct Premium {
    start: i64,
    finish: i64
}

#[derive(Debug, Clone)]
#[derive(Serialize, Deserialize)]
struct Likes {
    id: u32,
    ts: i64
}

#[derive(Debug, Clone)]
#[derive(Serialize, Deserialize)]
struct LikerLikee {
    liker: u32,
    likee: u32,
    ts: i64
}

#[derive(Debug, Clone)]
#[derive(Serialize, Deserialize)]
struct LikesRequest {
    likes: Vec<LikerLikee>
}

#[derive(Debug, Serialize, Deserialize)]
struct Filters {
    sex_eq: Option<String>,
    email_domain: Option<String>,
    email_lt: Option<String>,
    email_gt: Option<String>,
    status_eq: Option<String>,
    status_neq: Option<String>,
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
    birth_year: Option<i64>,
    premium_now: Option<i64>,
    premium_null: Option<u8>,
}

fn index(req: &HttpRequest<AppState>) -> HttpResponse {
    HttpResponse::build(StatusCode::OK)
        .content_type("application/json")
        .body("{}")
}


fn filter(query: Query<Filters>, state: State<AppState>) -> HttpResponse {
//    println!("Query: {:?}", query);
    HttpResponse::build(StatusCode::OK)
        .content_type("application/json")
        .body("{}")
}


fn likes(item: Json<LikesRequest>, state: State<AppState>) -> HttpResponse {
    let mut database = state.database.lock().unwrap();
    match database.update_likes(item.0) {
        Ok(_) => HttpResponse::build(StatusCode::ACCEPTED)
        .content_type("application/json")
        .body("{}"),
        Err(status) => HttpResponse::build(status).finish()
    }
}

fn update(item: Json<AccountOptional>, path: Path<(u32,)>, state: State<AppState>) -> HttpResponse {
    let mut database = state.database.lock().unwrap();
    match database.update_account(path.0, item.0) {
        Ok(_) => HttpResponse::build(StatusCode::ACCEPTED)
            .content_type("application/json")
            .body("{}"),
        Err(status) => HttpResponse::build(status).finish()
    }
}

fn new(item: Json<AccountFull>, state: State<AppState>) -> HttpResponse  {
    let mut database = state.database.lock().unwrap();
    match database.insert(item.0, true) {
        Ok(_) => HttpResponse::build(StatusCode::CREATED)
            .content_type("application/json")
            .body("{}"),
        Err(status) => HttpResponse::build(status).finish()
    }
}

fn main() {
    let prod = env::var("PROD").is_ok();
    let addr = if prod {"0.0.0.0:80"} else {"0.0.0.0:8010"};
    let data_file = if prod {"/tmp/data/data.zip"} else {"data.zip"};

    let sys = actix::System::new("accounts");
    let database = Arc::new(Mutex::new(DataBase::from_file(data_file)));
    println!("Database ready!");
    server::new(move || {
        App::with_state(AppState{database: database.clone()})
            .prefix("/accounts")
            .resource("/filter/", |r| r.method(Method::GET).with(filter))
            .resource("/group/", |r| r.method(Method::GET).f(index))
            .resource("/{id}/recommend/", |r| r.method(Method::GET).f(index))
            .resource("/{id}/suggest/", |r| r.method(Method::GET).f(index))
            .resource("/new/", |r| r.method(Method::POST).with(new))
            .resource("/likes/", |r| r.method(Method::POST).with(likes))
            .resource("/{id}/", |r| r.method(Method::POST).with(update))
    }).keep_alive(120)
        .bind(addr)
        .unwrap()
        .shutdown_timeout(1)
        .start();
    println!("Started http server: {}", addr);
    let _ = sys.run();
}
