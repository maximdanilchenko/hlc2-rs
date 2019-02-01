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
extern crate sys_info;
extern crate validator;
extern crate radix_trie;
extern crate flat_map;
extern crate smallvec;
extern crate smallset;
extern crate parking_lot;
extern crate hashbrown;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::env;
use std::io::{BufReader, BufRead, Read};
use std::fs::File;
use std::sync::{Arc, Mutex};

use std::thread;
use std::time::Instant;
use std::mem;
use std::cmp::Ordering::Equal;

use actix_web::http::{header, Method, StatusCode};
use actix_web::{error, server, App, HttpResponse, Json, Path, Query, State,
                Responder, Result, HttpRequest};
use itertools::Itertools;
use validator::Validate;
use futures::Future;
use actix_web::dev::AsyncResult;

use chrono::{TimeZone, Utc, Local, Datelike};
use indexmap::{IndexMap, IndexSet};

use smallvec::SmallVec;
use smallset::SmallSet;

use parking_lot::RwLock;

struct AppState {
    database: Arc<RwLock<DataBase>>,
}

type GroupKey = (Option<u16>, Option<u16>, Option<u16>, Sex, Status, Option<u32>);


struct DataBase {
    accounts: BTreeMap<u32, Account>,
    interests: IndexMap<String, BTreeSet<u32>>,
    countries: IndexMap<String, BTreeSet<u32>>,
    cities: IndexMap<String, BTreeSet<u32>>,
    emails: hashbrown::HashSet<String>,
    phone_codes: IndexSet<String>,
    email_domains: IndexSet<String>,
    fnames: IndexMap<String, BTreeSet<u32>>,
    snames: IndexMap<String, BTreeSet<u32>>,

    birth_index: hashbrown::HashMap<u32, BTreeSet<u32>>,
    joined_index: hashbrown::HashMap<u32, BTreeSet<u32>>,
    snames_index: BTreeSet<String>,

    likes: hashbrown::HashMap<u32, Vec<(u32, u32)>>,

    liked: hashbrown::HashMap<u32, Vec<u32>>,

    groups: hashbrown::HashMap<Vec<GroupKeys>, hashbrown::HashMap<GroupKey, u32>>,

    given_time: Option<u32>,

    premium_now: BTreeSet<u32>,

    post_phase: u32,

    raiting: bool,
}

impl DataBase {
    fn new() -> DataBase {
        let mut groups = hashbrown::HashMap::new();

        groups.insert(vec![GroupKeys::Sex, GroupKeys::Status], hashbrown::HashMap::new());
        groups.insert(vec![GroupKeys::City, GroupKeys::Sex, GroupKeys::Status], hashbrown::HashMap::new());
        groups.insert(vec![GroupKeys::Country, GroupKeys::Sex, GroupKeys::Status], hashbrown::HashMap::new());
        groups.insert(vec![GroupKeys::City, GroupKeys::Country, GroupKeys::Sex, GroupKeys::Status], hashbrown::HashMap::new());
        groups.insert(vec![GroupKeys::Sex, GroupKeys::Status, GroupKeys::Joined], hashbrown::HashMap::new());
        groups.insert(vec![GroupKeys::City, GroupKeys::Sex, GroupKeys::Status, GroupKeys::Joined], hashbrown::HashMap::new());
        groups.insert(vec![GroupKeys::Country, GroupKeys::Sex, GroupKeys::Status, GroupKeys::Joined], hashbrown::HashMap::new());
        groups.insert(vec![GroupKeys::City, GroupKeys::Country, GroupKeys::Sex, GroupKeys::Status, GroupKeys::Joined], hashbrown::HashMap::new());

        groups.insert(vec![GroupKeys::Interests, GroupKeys::Sex, GroupKeys::Status], hashbrown::HashMap::new());
        groups.insert(vec![GroupKeys::City, GroupKeys::Interests, GroupKeys::Sex, GroupKeys::Status], hashbrown::HashMap::new());
        groups.insert(vec![GroupKeys::Country, GroupKeys::Interests, GroupKeys::Sex, GroupKeys::Status], hashbrown::HashMap::new());
        groups.insert(vec![GroupKeys::City, GroupKeys::Country, GroupKeys::Interests, GroupKeys::Sex, GroupKeys::Status], hashbrown::HashMap::new());
        groups.insert(vec![GroupKeys::Interests, GroupKeys::Sex, GroupKeys::Status, GroupKeys::Joined], hashbrown::HashMap::new());
        groups.insert(vec![GroupKeys::City, GroupKeys::Interests, GroupKeys::Sex, GroupKeys::Status, GroupKeys::Joined], hashbrown::HashMap::new());
        groups.insert(vec![GroupKeys::Country, GroupKeys::Interests, GroupKeys::Sex, GroupKeys::Status, GroupKeys::Joined], hashbrown::HashMap::new());
        groups.insert(vec![GroupKeys::City, GroupKeys::Country, GroupKeys::Interests, GroupKeys::Sex, GroupKeys::Status, GroupKeys::Joined], hashbrown::HashMap::new());

        DataBase {
            accounts: BTreeMap::new(),
            interests: IndexMap::new(),
            countries: IndexMap::default(),
            cities: IndexMap::default(),
            emails: hashbrown::HashSet::default(),
            phone_codes: IndexSet::default(),
            email_domains: IndexSet::default(),
            fnames: IndexMap::default(),
            snames: IndexMap::default(),

            snames_index: BTreeSet::new(),
            birth_index: hashbrown::HashMap::default(),
            joined_index: hashbrown::HashMap::default(),

            likes: hashbrown::HashMap::new(),
            liked: hashbrown::HashMap::new(),

            groups,

            given_time: None,

            premium_now: BTreeSet::new(),

            post_phase: 0,
            raiting: false,
        }
    }

    fn from_file(path: &'static str) -> DataBase {
        println!("{:?}", mem::size_of::<Account>());

        let now = Instant::now();

        let mut database = DataBase::new();

        let opts = File::open("options.txt");
        if let Ok(f) = opts {
            let file = BufReader::new(&f);
            let mut lines = vec![];
            for line in file.lines() {
                let l = line.unwrap();
                println!("{}", l);
                lines.push(l);
            }
            database.given_time = Some(lines[0].parse().unwrap());
        }
        let accounts_arc: Arc<Mutex<Vec<AccountFull>>> =
            Arc::new(Mutex::new(Vec::with_capacity(1300000)));
        {
            let mut handles = vec![];

            for thread_num in 1..7 {
                let accounts_clone = Arc::clone(&accounts_arc);

                let handle = thread::spawn(move || {
                    let file = File::open(path).unwrap();
                    let reader = BufReader::new(file);
                    let mut zip = zip::ZipArchive::new(reader).unwrap();
                    for file_num in 0..100 {
                        let index = thread_num + (file_num * 6);

                        let file_name = format!("accounts_{}.json", index);
                        let mut accounts: HashMap<String, Vec<AccountFull>>;

                        let mut data_file = match zip.by_name(file_name.as_str()) {
                            Err(_) => break,
                            Ok(f) => f,
                        };
                        println!(
                            "Starting file: {:?}. Elapsed {} secs from start.",
                            file_name,
                            now.elapsed().as_secs()
                        );

                        let mut data = String::new();
                        data_file.read_to_string(&mut data).unwrap();

                        accounts = serde_json::from_str(data.as_ref()).unwrap();
                        let mut accounts_cache = accounts_clone.lock().unwrap();
                        accounts_cache.extend(accounts.remove("accounts").unwrap());
                        println!(
                            "Ready file: {:?}. Accs capacity: {} Elapsed {} secs from start.",
                            file_name,
                            accounts_cache.capacity(),
                            now.elapsed().as_secs()
                        );
                    }
                });
                handles.push(handle);
            }

            for handle in handles {
                handle.join().unwrap();
            }
        }

        println!(
            "Starting inserting! Elapsed {} secs from start.",
            now.elapsed().as_secs()
        );
        let mut accs = accounts_arc.lock().unwrap();
        for index in 0..1000 {
            for account in accs.drain(0..10000) {
                match database.insert_sync(account) {
                    Err(_) => println!("Error while inserting"),
                    Ok(_) => (),
                }
            }
            accs.shrink_to_fit();

            database.likes.shrink_to_fit();

            println!("Vector capacity: {}", accs.capacity());
            println!(
                "Inserted from {} to {}. Elapsed {} secs from start",
                index * 10000,
                index * 10000 + 10000,
                now.elapsed().as_secs()
            );
            database.print_len();
            memory_info();
            if accs.is_empty() {
                break;
            }
        }

        println!("Done! Elapsed {} secs from start.", now.elapsed().as_secs());
        database.print_len();
        database.joined_index.shrink_to_fit();
        database.birth_index.shrink_to_fit();
        database.emails.shrink_to_fit();
        database.likes.shrink_to_fit();
        database.liked.shrink_to_fit();

        if database.accounts.len() > 1000000 {
            database.raiting = true;
        }

        database
    }

    fn from_file_in_place(path: &'static str) -> DataBase {
        println!("{:?}", mem::size_of::<Account>());

        let mut database = DataBase::new();

        let opts = File::open("/tmp/data/options.txt");
        if let Ok(f) = opts {
            let file = BufReader::new(&f);
            let mut lines = vec![];
            for line in file.lines() {
                let l = line.unwrap();
                println!("{}", l);
                lines.push(l);
            }
            database.given_time = Some(lines[0].parse().unwrap());
        }


        let now = Instant::now();

        let file = File::open(path).unwrap();
        let reader = BufReader::new(file);
        let mut zip = zip::ZipArchive::new(reader).unwrap();
        for file_num in 1..200 {
            let file_name = format!("accounts_{}.json", file_num);
            let mut accounts: HashMap<String, Vec<AccountFull>>;

            let mut data_file = match zip.by_name(file_name.as_str()) {
                Err(_) => break,
                Ok(f) => f,
            };
            println!(
                "Starting file: {:?}. Elapsed {} secs from start.",
                file_name,
                now.elapsed().as_secs()
            );

            let mut data = String::new();
            data_file.read_to_string(&mut data).unwrap();

            accounts = serde_json::from_str(data.as_ref()).unwrap();
            for account in accounts.remove("accounts").unwrap() {
                database.insert_sync(account).unwrap();
            }
            database.likes.shrink_to_fit();
        }

        println!("Done! Elapsed {} secs from start.", now.elapsed().as_secs());
        database.print_len();

        database.joined_index.shrink_to_fit();
        database.birth_index.shrink_to_fit();
        database.emails.shrink_to_fit();
        database.likes.shrink_to_fit();
        database.liked.shrink_to_fit();

        database
    }

    fn print_len(&self) {
        println!(
            "Accs: {}, countries: {}, cities: {}, emails: {}, \
             interests: {}(cap: {}), codes: {}, doms: {}, fnames: {}, \
             snames: {}, likes: {}, liked: {}, snames_index: {}, \
             birth_index: {}, joined_index: {}, groups_map: {}, premium_now: {}",
            self.accounts.len(),
            self.countries.len(),
            self.cities.len(),
            self.emails.len(),
            self.interests.len(),
            self.interests.capacity(),
            self.phone_codes.len(),
            self.email_domains.len(),
            self.fnames.len(),
            self.snames.len(),
            self.likes.len(),
            self.liked.len(),
            self.snames_index.len(),
            self.birth_index.len(),
            self.joined_index.len(),
            self.groups.values().map(|i| i.len()).sum::<usize>(),
            self.premium_now.len(),
        );
    }

    fn subtract_from_group(&mut self, acc: &Account, joined: Option<u32>, birth: Option<u32>) {
        self.groups.entry(vec![GroupKeys::Sex, GroupKeys::Status])
            .and_modify(|index| {
                index
                    .entry(
                        (None, None, None, acc.sex.clone(), acc.status.clone(), None)
                    ).and_modify(|v| *v -= 1);
            });
        self.groups.entry(vec![GroupKeys::City, GroupKeys::Sex, GroupKeys::Status])
            .and_modify(|index| {
                index
                    .entry(
                        (acc.city, None, None, acc.sex.clone(), acc.status.clone(), None)
                    ).and_modify(|v| *v -= 1);
            });
        self.groups.entry(vec![GroupKeys::Country, GroupKeys::Sex, GroupKeys::Status])
            .and_modify(|index| {
                index
                    .entry(
                        (None, acc.country, None, acc.sex.clone(), acc.status.clone(), None)
                    ).and_modify(|v| *v -= 1);
            });
        self.groups.entry(vec![GroupKeys::City, GroupKeys::Country, GroupKeys::Sex, GroupKeys::Status])
            .and_modify(|index| {
                index
                    .entry(
                        (acc.city, acc.country, None, acc.sex.clone(), acc.status.clone(), None)
                    ).and_modify(|v| *v -= 1);
            });
        self.groups.entry(vec![GroupKeys::Sex, GroupKeys::Status, GroupKeys::Joined])
            .and_modify(|index| {
                index
                    .entry(
                        (None, None, None, acc.sex.clone(), acc.status.clone(), joined)
                    ).and_modify(|v| *v -= 1);
            });
        self.groups.entry(vec![GroupKeys::City, GroupKeys::Sex, GroupKeys::Status, GroupKeys::Joined])
            .and_modify(|index| {
                index
                    .entry(
                        (acc.city, None, None, acc.sex.clone(), acc.status.clone(), joined)
                    ).and_modify(|v| *v -= 1);
            });
        self.groups.entry(vec![GroupKeys::Country, GroupKeys::Sex, GroupKeys::Status, GroupKeys::Joined])
            .and_modify(|index| {
                index
                    .entry(
                        (None, acc.country, None, acc.sex.clone(), acc.status.clone(), joined)
                    ).and_modify(|v| *v -= 1);
            });
        self.groups.entry(vec![GroupKeys::City, GroupKeys::Country, GroupKeys::Sex, GroupKeys::Status, GroupKeys::Joined])
            .and_modify(|index| {
                index
                    .entry(
                        (acc.city, acc.country, None, acc.sex.clone(), acc.status.clone(), joined)
                    ).and_modify(|v| *v -= 1);
            });

        for interest in &acc.interests {
            self.groups.entry(vec![GroupKeys::Interests, GroupKeys::Sex, GroupKeys::Status])
                .and_modify(|index| {
                    index
                        .entry(
                            (None, None, Some(interest.clone()), acc.sex.clone(), acc.status.clone(), None)
                        ).and_modify(|v| *v -= 1);
                });
            self.groups.entry(vec![GroupKeys::City, GroupKeys::Interests, GroupKeys::Sex, GroupKeys::Status])
                .and_modify(|index| {
                    index
                        .entry(
                            (acc.city, None, Some(interest.clone()), acc.sex.clone(), acc.status.clone(), None)
                        ).and_modify(|v| *v -= 1);
                });
            self.groups.entry(vec![GroupKeys::Country, GroupKeys::Interests, GroupKeys::Sex, GroupKeys::Status])
                .and_modify(|index| {
                    index
                        .entry(
                            (None, acc.country, Some(interest.clone()), acc.sex.clone(), acc.status.clone(), None)
                        ).and_modify(|v| *v -= 1);
                });
            self.groups.entry(vec![GroupKeys::City, GroupKeys::Country, GroupKeys::Interests, GroupKeys::Sex, GroupKeys::Status])
                .and_modify(|index| {
                    index
                        .entry(
                            (acc.city, acc.country, Some(interest.clone()), acc.sex.clone(), acc.status.clone(), None)
                        ).and_modify(|v| *v -= 1);
                });
            self.groups.entry(vec![GroupKeys::Interests, GroupKeys::Sex, GroupKeys::Status, GroupKeys::Joined])
                .and_modify(|index| {
                    index
                        .entry(
                            (None, None, Some(interest.clone()), acc.sex.clone(), acc.status.clone(), joined)
                        ).and_modify(|v| *v -= 1);
                });
            self.groups.entry(vec![GroupKeys::City, GroupKeys::Interests, GroupKeys::Sex, GroupKeys::Status, GroupKeys::Joined])
                .and_modify(|index| {
                    index
                        .entry(
                            (acc.city, None, Some(interest.clone()), acc.sex.clone(), acc.status.clone(), joined)
                        ).and_modify(|v| *v -= 1);
                });
            self.groups.entry(vec![GroupKeys::Country, GroupKeys::Interests, GroupKeys::Sex, GroupKeys::Status, GroupKeys::Joined])
                .and_modify(|index| {
                    index
                        .entry(
                            (None, acc.country, Some(interest.clone()), acc.sex.clone(), acc.status.clone(), joined)
                        ).and_modify(|v| *v -= 1);
                });
            self.groups.entry(vec![GroupKeys::City, GroupKeys::Country, GroupKeys::Interests, GroupKeys::Sex, GroupKeys::Status, GroupKeys::Joined])
                .and_modify(|index| {
                    index
                        .entry(
                            (acc.city, acc.country, Some(interest.clone()), acc.sex.clone(), acc.status.clone(), joined)
                        ).and_modify(|v| *v -= 1);
                });
        }
    }

    fn add_to_group(&mut self, acc: &Account, joined: Option<u32>, birth: Option<u32>) {
        self.groups.entry(vec![GroupKeys::Sex, GroupKeys::Status])
            .and_modify(|index|
                *index
                    .entry(
                        (None, None, None, acc.sex.clone(), acc.status.clone(), None)
                    ).or_insert(0) += 1);
        self.groups.entry(vec![GroupKeys::City, GroupKeys::Sex, GroupKeys::Status])
            .and_modify(|index|
                *index
                    .entry(
                        (acc.city, None, None, acc.sex.clone(), acc.status.clone(), None)
                    ).or_insert(0) += 1);
        self.groups.entry(vec![GroupKeys::Country, GroupKeys::Sex, GroupKeys::Status])
            .and_modify(|index|
                *index
                    .entry(
                        (None, acc.country, None, acc.sex.clone(), acc.status.clone(), None)
                    ).or_insert(0) += 1);
        self.groups.entry(vec![GroupKeys::City, GroupKeys::Country, GroupKeys::Sex, GroupKeys::Status])
            .and_modify(|index|
                *index
                    .entry(
                        (acc.city, acc.country, None, acc.sex.clone(), acc.status.clone(), None)
                    ).or_insert(0) += 1);
        self.groups.entry(vec![GroupKeys::Sex, GroupKeys::Status, GroupKeys::Joined])
            .and_modify(|index|
                *index
                    .entry(
                        (None, None, None, acc.sex.clone(), acc.status.clone(), joined)
                    ).or_insert(0) += 1);
        self.groups.entry(vec![GroupKeys::City, GroupKeys::Sex, GroupKeys::Status, GroupKeys::Joined])
            .and_modify(|index|
                *index
                    .entry(
                        (acc.city, None, None, acc.sex.clone(), acc.status.clone(), joined)
                    ).or_insert(0) += 1);
        self.groups.entry(vec![GroupKeys::Country, GroupKeys::Sex, GroupKeys::Status, GroupKeys::Joined])
            .and_modify(|index|
                *index
                    .entry(
                        (None, acc.country, None, acc.sex.clone(), acc.status.clone(), joined)
                    ).or_insert(0) += 1);
        self.groups.entry(vec![GroupKeys::City, GroupKeys::Country, GroupKeys::Sex, GroupKeys::Status, GroupKeys::Joined])
            .and_modify(|index|
                *index
                    .entry(
                        (acc.city, acc.country, None, acc.sex.clone(), acc.status.clone(), joined)
                    ).or_insert(0) += 1);

        for interest in &acc.interests {
            self.groups.entry(vec![GroupKeys::Interests, GroupKeys::Sex, GroupKeys::Status])
                .and_modify(|index|
                    *index
                        .entry(
                            (None, None, Some(interest.clone()), acc.sex.clone(), acc.status.clone(), None)
                        ).or_insert(0) += 1);
            self.groups.entry(vec![GroupKeys::City, GroupKeys::Interests, GroupKeys::Sex, GroupKeys::Status])
                .and_modify(|index|
                    *index
                        .entry(
                            (acc.city, None, Some(interest.clone()), acc.sex.clone(), acc.status.clone(), None)
                        ).or_insert(0) += 1);
            self.groups.entry(vec![GroupKeys::Country, GroupKeys::Interests, GroupKeys::Sex, GroupKeys::Status])
                .and_modify(|index|
                    *index
                        .entry(
                            (None, acc.country, Some(interest.clone()), acc.sex.clone(), acc.status.clone(), None)
                        ).or_insert(0) += 1);
            self.groups.entry(vec![GroupKeys::City, GroupKeys::Country, GroupKeys::Interests, GroupKeys::Sex, GroupKeys::Status])
                .and_modify(|index|
                    *index
                        .entry(
                            (acc.city, acc.country, Some(interest.clone()), acc.sex.clone(), acc.status.clone(), None)
                        ).or_insert(0) += 1);
            self.groups.entry(vec![GroupKeys::Interests, GroupKeys::Sex, GroupKeys::Status, GroupKeys::Joined])
                .and_modify(|index|
                    *index
                        .entry(
                            (None, None, Some(interest.clone()), acc.sex.clone(), acc.status.clone(), joined)
                        ).or_insert(0) += 1);
            self.groups.entry(vec![GroupKeys::City, GroupKeys::Interests, GroupKeys::Sex, GroupKeys::Status, GroupKeys::Joined])
                .and_modify(|index|
                    *index
                        .entry(
                            (acc.city, None, Some(interest.clone()), acc.sex.clone(), acc.status.clone(), joined)
                        ).or_insert(0) += 1);
            self.groups.entry(vec![GroupKeys::Country, GroupKeys::Interests, GroupKeys::Sex, GroupKeys::Status, GroupKeys::Joined])
                .and_modify(|index|
                    *index
                        .entry(
                            (None, acc.country, Some(interest.clone()), acc.sex.clone(), acc.status.clone(), joined)
                        ).or_insert(0) += 1);
            self.groups.entry(vec![GroupKeys::City, GroupKeys::Country, GroupKeys::Interests, GroupKeys::Sex, GroupKeys::Status, GroupKeys::Joined])
                .and_modify(|index|
                    *index
                        .entry(
                            (acc.city, acc.country, Some(interest.clone()), acc.sex.clone(), acc.status.clone(), joined)
                        ).or_insert(0) += 1);
        }
    }

    fn insert_to_domains(&mut self, mail: String, uid: u32) -> u16 {
        let vec: Vec<&str> = mail.split('@').collect();
        if let Some(domain) = vec.get(1) {
            self.email_domains.insert_full(domain.to_string()).0 as u16
        } else {
            0
        }
    }

    fn insert_to_interests(&mut self, interest: String, uid: u32) -> u16 {
        self.interests
            .entry(interest.clone())
            .or_insert(BTreeSet::new())
            .insert(uid);
        self.interests.get_full(&interest).unwrap().0 as u16
    }

    fn update_account(&mut self, uid: u32, account: AccountOptional, req: HttpRequest<AppState>) -> Result<AsyncResult<HttpResponse>> {
        let mut new_account = match self.accounts.get_mut(&uid) {
            Some(account) => account,
            None => return Ok(HttpResponse::build(StatusCode::NOT_FOUND)
                .content_type("application/json")
                .header(header::CONNECTION, "keep-alive")
                .header(header::SERVER, "highload")
                .finish().respond_to(&req)?),
        };
        let old_account = new_account.clone();
        let (new_account, joined_year, birth_year) = {
            if let Some(email) = account.email {
                if self.emails.contains(&email) {
                    if email != new_account.email {
                        return Ok(HttpResponse::build(StatusCode::BAD_REQUEST)
                            .content_type("application/json")
                            .header(header::CONNECTION, "keep-alive")
                            .header(header::SERVER, "highload")
                            .finish().respond_to(&req)?);
                    }
                } else {
                    let vec: Vec<&str> = email.split('@').collect();
                    new_account.email_domain = match vec.get(1) {
                        Some(domain) => {
                            self.email_domains.insert_full(domain.to_string()).0 as u16
                        }
                        None => 0,
                    };
                    new_account.email = email.clone();
                    self.emails.insert(email);
                }
            }

            if let Some(sex) = account.sex {
                new_account.sex = sex;
            };

            if let Some(status) = account.status {
                new_account.status = status;
            };

            if let Some(phone) = account.phone {
                new_account.phone_code = match phone.get(2..5) {
                    Some(phone_code) => Some({
                        self.phone_codes.insert_full(phone_code.to_string()).0 as u16
                    }),
                    None => None,
                };
                new_account.phone = Some(phone);
            };

            let birth_year = if let Some(birth) = account.birth {
                let year = Utc.timestamp(birth as i64, 0).year() as u32;
                self.birth_index
                    .entry(year)
                    .and_modify(|v| { v.remove(&uid); });
                self.birth_index
                    .entry(year)
                    .or_insert(BTreeSet::new())
                    .insert(uid);
                new_account.birth = birth;
                year
            } else {
                Utc.timestamp(new_account.birth as i64, 0).year() as u32
            };

            let joined_year = if let Some(joined) = account.joined {
                let joined_year = Utc.timestamp(joined as i64, 0).year() as u32;
                self.joined_index
                    .entry(joined_year)
                    .and_modify(|v| { v.remove(&uid); });
                self.joined_index
                    .entry(joined_year)
                    .or_insert(BTreeSet::new())
                    .insert(uid);
                new_account.joined = joined;
                joined_year
            } else {
                Utc.timestamp(new_account.joined as i64, 0).year() as u32
            };

            if let Some(premium) = account.premium {
                if let Some(now) = self.given_time {
                    if premium.start <= now && premium.finish >= now {
                        self.premium_now.insert(uid);
                    } else {
                        self.premium_now.remove(&uid);
                    }
                }
                new_account.premium = Some(premium);
            };

            if let Some(likes) = account.likes {
                let liked = self.liked
                    .entry(uid)
                    .or_insert(Vec::new());
                liked.clear();
                for like in likes {
                    self.likes.entry(like.id)
                        .or_insert(Vec::new())
                        .push((uid, like.ts));
                    liked.push(like.id);
                }
            };

            if let Some(country) = account.country {
                new_account.country = Some({
                    if let Some(old_country) = new_account.country {
                        if let Some((_, v)) = self.countries.get_index_mut(old_country as usize) {
                            v.remove(&uid);
                        }
                    }
                    self.countries
                        .entry(country.to_string())
                        .or_insert(BTreeSet::new())
                        .insert(uid);
                    self.countries.get_full(&country.to_string()).unwrap().0 as u16
                });
            };

            if let Some(city) = account.city {
                new_account.city = Some({
                    if let Some(old_city) = new_account.city {
                        if let Some((_, v)) = self.cities.get_index_mut(old_city as usize) {
                            v.remove(&uid);
                        }
                    }
                    self.cities
                        .entry(city.to_string())
                        .or_insert(BTreeSet::new())
                        .insert(uid);
                    self.cities.get_full(&city.to_string()).unwrap().0 as u16
                });
            };

            if let Some(sname) = account.sname {
                new_account.sname = Some({
                    if let Some(old_sname) = new_account.sname {
                        if let Some((_, v)) = self.snames.get_index_mut(old_sname as usize) {
                            v.remove(&uid);
                        }
                    }
                    self.snames
                        .entry(sname.to_string())
                        .or_insert(BTreeSet::new())
                        .insert(uid);
                    self.snames_index.insert(sname.clone());
                    self.snames.get_full(&sname.to_string()).unwrap().0 as u16
                });
            };

            if let Some(fname) = account.fname {
                new_account.fname = Some({
                    if let Some(old_fname) = new_account.fname {
                        if let Some((_, v)) = self.fnames.get_index_mut(old_fname as usize) {
                            v.remove(&uid);
                        }
                    }
                    self.fnames
                        .entry(fname.to_string())
                        .or_insert(BTreeSet::new())
                        .insert(uid);
                    self.fnames.get_full(&fname.to_string()).unwrap().0 as u16
                });
            };

            if let Some(interests) = account.interests {
                for elem in &new_account.interests {
                    if let Some((_, v)) = self.interests.get_index_mut(elem.clone() as usize) {
                        v.remove(&uid);
                    }
                }
                new_account.interests.clear();
                for elem in interests {
                    let index = {
                        self.interests
                            .entry(elem.to_string())
                            .or_insert(BTreeSet::new())
                            .insert(uid);
                        self.interests.get_full(&elem.to_string()).unwrap().0 as u16
                    };
                    new_account.interests.push(index);
                }
            };
            (new_account.clone(), joined_year, birth_year)
        };
        Ok(HttpResponse::Accepted()
            .content_type("application/json")
            .body("{}").respond_to(&req).and_then(|r| {
            self.subtract_from_group(&old_account, Some(joined_year), Some(birth_year));
            self.add_to_group(&new_account, Some(joined_year), Some(birth_year));
            Ok(r)
        })?)
    }

    fn insert(&mut self, account: AccountFull, req: HttpRequest<AppState>) -> Result<AsyncResult<HttpResponse>> {
        if self.post_phase == 0 {
            self.post_phase = 1;
        }

        let uid = account.id;

        if let true = self.accounts.contains_key(&uid) {
            return Ok(HttpResponse::build(StatusCode::BAD_REQUEST)
                .content_type("application/json")
                .header(header::CONNECTION, "keep-alive")
                .header(header::SERVER, "highload")
                .finish().respond_to(&req)?);
        };

        if self.emails.contains(&account.email) {
            return Ok(HttpResponse::build(StatusCode::BAD_REQUEST)
                .content_type("application/json")
                .header(header::CONNECTION, "keep-alive")
                .header(header::SERVER, "highload")
                .finish().respond_to(&req)?);
        };


        Ok(HttpResponse::Created()
            .content_type("application/json")
            .body("{}").respond_to(&req).and_then(|r| {
            if let Some(likes) = account.likes {
                for like in likes {
                    self.likes.entry(like.id)
                        .or_insert(Vec::new())
                        .push((uid, like.ts));
                    self.liked.entry(uid)
                        .or_insert(Vec::new())
                        .push(like.id);
                }
            }

            let city = match account.city {
                Some(city) => Some({
                    self.cities
                        .entry(city.to_string())
                        .or_insert(BTreeSet::new())
                        .insert(uid);
                    self.cities.get_full(&city.to_string()).unwrap().0 as u16
                }),
                None => {
                    None
                }
            };

            let sname = match account.sname {
                Some(sname) => Some({
                    self.snames
                        .entry(sname.to_string())
                        .or_insert(BTreeSet::new())
                        .insert(uid);
                    self.snames_index.insert(sname.clone());
                    self.snames.get_full(&sname.to_string()).unwrap().0 as u16
                }),
                None => {
                    None
                }
            };

            let fname = match account.fname {
                Some(fname) => Some({
                    self.fnames
                        .entry(fname.to_string())
                        .or_insert(BTreeSet::new())
                        .insert(uid);
                    self.fnames.get_full(&fname.to_string()).unwrap().0 as u16
                }),
                None => {
                    None
                }
            };

            let country = match account.country {
                Some(country) => Some({
                    self.countries
                        .entry(country.to_string())
                        .or_insert(BTreeSet::new())
                        .insert(uid);
                    self.countries.get_full(&country.to_string()).unwrap().0 as u16
                }),
                None => {
                    None
                }
            };

            let phone_code = match &account.phone {
                Some(phone) => match phone.get(2..5) {
                    Some(phone_code) => Some({
                        self.phone_codes.insert_full(phone_code.to_string()).0 as u16
                    }),
                    None => None,
                },
                None => {
                    None
                }
            };

            let interests: Vec<u16> = match account.interests {
                Some(vec) => vec
                    .into_iter()
                    .map(|interest| self.insert_to_interests(interest, uid))
                    .collect(),
                None => Vec::new(),
            };

            self.emails.insert(account.email.clone());
            let email_domain = self.insert_to_domains(account.email.clone(), uid);

            if let Some(premium) = &account.premium {
                if let Some(now) = self.given_time {
                    if premium.start <= now && premium.finish >= now {
                        self.premium_now.insert(uid);
                    } else {
                        self.premium_now.remove(&uid);
                    }
                }
            }

            let birth_year = Utc.timestamp(account.birth as i64, 0).year() as u32;
            self.birth_index
                .entry(birth_year)
                .or_insert(BTreeSet::new())
                .insert(uid);

            let joined_year = Utc.timestamp(account.joined as i64, 0).year() as u32;
            self.joined_index
                .entry(joined_year)
                .or_insert(BTreeSet::new())
                .insert(uid);

            let new_account = Account {
                email: account.email,
                fname,
                sname,
                phone: account.phone,
                sex: account.sex,
                birth: account.birth,
                joined: account.joined,
                status: account.status,
                premium: account.premium,
                country,
                city,
                interests,
                phone_code,
                email_domain,
            };
            self.add_to_group(&new_account, Some(joined_year), Some(birth_year));
            self.accounts.insert(
                uid,
                new_account,
            );
            Ok(r)
        })?)
    }

    fn insert_sync(&mut self, account: AccountFull) -> Result<()> {
        if self.post_phase == 0 {
            self.post_phase = 1;
        }

        let uid = account.id;

        if let Some(likes) = account.likes {
            for like in likes {
                self.likes.entry(like.id)
                    .or_insert(Vec::new())
                    .push((uid, like.ts));
                self.liked.entry(uid)
                    .or_insert(Vec::new())
                    .push(like.id);
            }
        }

        let city = match account.city {
            Some(city) => Some({
                self.cities
                    .entry(city.to_string())
                    .or_insert(BTreeSet::new())
                    .insert(uid);
                self.cities.get_full(&city.to_string()).unwrap().0 as u16
            }),
            None => {
                None
            }
        };

        let sname = match account.sname {
            Some(sname) => Some({
                self.snames
                    .entry(sname.to_string())
                    .or_insert(BTreeSet::new())
                    .insert(uid);
                self.snames_index.insert(sname.clone());
                self.snames.get_full(&sname.to_string()).unwrap().0 as u16
            }),
            None => {
                None
            }
        };

        let fname = match account.fname {
            Some(fname) => Some({
                self.fnames
                    .entry(fname.to_string())
                    .or_insert(BTreeSet::new())
                    .insert(uid);
                self.fnames.get_full(&fname.to_string()).unwrap().0 as u16
            }),
            None => {
                None
            }
        };

        let country = match account.country {
            Some(country) => Some({
                self.countries
                    .entry(country.to_string())
                    .or_insert(BTreeSet::new())
                    .insert(uid);
                self.countries.get_full(&country.to_string()).unwrap().0 as u16
            }),
            None => {
                None
            }
        };

        let phone_code = match &account.phone {
            Some(phone) => match phone.get(2..5) {
                Some(phone_code) => Some({
                    self.phone_codes.insert_full(phone_code.to_string()).0 as u16
                }),
                None => None,
            },
            None => {
                None
            }
        };

        let interests: Vec<u16> = match account.interests {
            Some(vec) => vec
                .into_iter()
                .map(|interest| self.insert_to_interests(interest, uid))
                .collect(),
            None => Vec::new(),
        };

        self.emails.insert(account.email.clone());
        let email_domain = self.insert_to_domains(account.email.clone(), uid);

        if let Some(premium) = &account.premium {
            if let Some(now) = self.given_time {
                if premium.start <= now && premium.finish >= now {
                    self.premium_now.insert(uid);
                } else {
                    self.premium_now.remove(&uid);
                }
            }
        }

        let birth_year = Utc.timestamp(account.birth as i64, 0).year() as u32;
        self.birth_index
            .entry(birth_year)
            .or_insert(BTreeSet::new())
            .insert(uid);

        let joined_year = Utc.timestamp(account.joined as i64, 0).year() as u32;
        self.joined_index
            .entry(joined_year)
            .or_insert(BTreeSet::new())
            .insert(uid);

        let new_account = Account {
            email: account.email,
            fname,
            sname,
            phone: account.phone,
            sex: account.sex,
            birth: account.birth,
            joined: account.joined,
            status: account.status,
            premium: account.premium,
            country,
            city,
            interests,
            phone_code,
            email_domain,
        };
        self.add_to_group(&new_account, Some(joined_year), Some(birth_year));
        self.accounts.insert(
            uid,
            new_account,
        );
        Ok(())
    }

    fn update_likes(&mut self, likes: LikesRequest, req: HttpRequest<AppState>) -> Result<AsyncResult<HttpResponse>> {
        for like in likes.likes.iter() {
            if !self.accounts.contains_key(&like.likee) || !self.accounts.contains_key(&like.liker) {
                return Ok(HttpResponse::build(StatusCode::BAD_REQUEST)
                    .content_type("application/json")
                    .header(header::CONNECTION, "keep-alive")
                    .header(header::SERVER, "highload")
                    .finish().respond_to(&req)?);
            }
        }
        Ok(HttpResponse::Accepted()
            .content_type("application/json")
            .body("{}").respond_to(&req).and_then(|r| {
            for like in likes.likes.iter() {
                self.likes.entry(like.likee)
                    .or_insert(Vec::new())
                    .push((like.liker, like.ts));
                self.liked.entry(like.liker)
                    .or_insert(Vec::new())
                    .push(like.likee);
            }
            Ok(r)
        })?)
    }

    fn filter(&self, filters: Filters) -> Result<serde_json::Value, ()> {
        let end_ids: BTreeMap<&u32, &Account> = {
            // ФИЛЬТРЫ ЧЕРЕЗ ИНДЕКСЫ

            let mut indexes = SmallVec::<[&BTreeSet<u32>; 4]>::new();
            let mut indexes_options = Vec::new();
            let mut indexes_likes: Vec<BTreeSet<&u32>> = Vec::new();
            let mut filters_fns = SmallVec::<[Box<Fn(&Account) -> bool>; 4]>::new();

            if let Some(val) = &filters.phone_code {
                if let Some((number, _)) = self.phone_codes.get_full(val) {
                    let number = Some(number as u16);
                    filters_fns.push(Box::new(move |acc: &Account| acc.phone_code == number));
                } else {
                    return Ok(json!({"accounts": []}));
                }
            }

            if let Some(val) = &filters.email_domain {
                if let Some((number, _)) = self.email_domains.get_full(val) {
                    let number = number as u16;
                    filters_fns.push(Box::new(move |acc: &Account| acc.email_domain == number));
                } else {
                    return Ok(json!({"accounts": []}));
                }
            }

            if let Some(val) = &filters.sname_eq {
                if !indexes.is_empty() {
                    if let Some((number, _, _)) = self.snames.get_full(val) {
                        let number = Some(number as u16);
                        filters_fns.push(Box::new(move |acc: &Account| acc.sname == number));
                    } else {
                        return Ok(json!({"accounts": []}));
                    }
                } else {
                    if let Some(res) = self.snames.get(val) {
                        indexes.push(res);
                    } else {
                        return Ok(json!({"accounts": []}));
                    }
                }
            }

            if let Some(val) = &filters.city_eq {
                if !indexes.is_empty() {
                    if let Some((number, _, _)) = self.cities.get_full(val) {
                        let number = Some(number as u16);
                        filters_fns.push(Box::new(move |acc: &Account| acc.city == number));
                    } else {
                        return Ok(json!({"accounts": []}));
                    }
                } else {
                    if let Some(res) = self.cities.get(val) {
                        indexes.push(res);
                    } else {
                        return Ok(json!({"accounts": []}));
                    }
                }
            }

            if let Some(val) = &filters.fname_eq {
                if !indexes.is_empty() {
                    if let Some((number, _, _)) = self.fnames.get_full(val) {
                        let number = Some(number as u16);
                        filters_fns.push(Box::new(move |acc: &Account| acc.fname == number));
                    } else {
                        return Ok(json!({"accounts": []}));
                    }
                } else {
                    if let Some(res) = self.fnames.get(val) {
                        indexes.push(res);
                    } else {
                        return Ok(json!({"accounts": []}));
                    }
                }
            }

            if let Some(val) = &filters.country_eq {
                if !indexes.is_empty() {
                    if let Some((number, _, _)) = self.countries.get_full(val) {
                        let number = Some(number as u16);
                        filters_fns.push(Box::new(move |acc: &Account| acc.country == number));
                    } else {
                        return Ok(json!({"accounts": []}));
                    }
                } else {
                    if let Some(res) = self.countries.get(val) {
                        indexes.push(res);
                    } else {
                        return Ok(json!({"accounts": []}));
                    }
                }
            }

            if let Some(val) = &filters.birth_year {
                if let Some(res) = self.birth_index.get(val) {
                    indexes.push(res);
                } else {
                    return Ok(json!({"accounts": []}));
                }
            }

            if let Some(val) = &filters.interests_contains {
                let mut res = vec![];
                for elem in val.split(',') {
                    if let Some(interests) = self.interests.get(elem) {
                        res.push(interests);
                    } else {
                        return Ok(json!({"accounts": []}));
                    }
                }

                if res.is_empty() {
                    return Ok(json!({"accounts": []}));
                } else {
                    indexes.extend(res.into_iter());
                };
            }

            if let Some(val) = &filters.likes_contains {
                for elem in val.split(',') {
                    let liked_id = match elem.parse::<u32>() {
                        Ok(r) => r,
                        Err(_) => return Err(())
                    };
                    if let Some(likes) = self.likes.get(&liked_id) {
                        indexes_likes.push(likes.iter().map(|(like, _)| like).collect());
                    } else {
                        return Ok(json!({"accounts": []}));
                    }
                }

                if indexes_likes.is_empty() {
                    return Ok(json!({"accounts": []}));
                }
            }


            if let Some(val) = &filters.interests_any {
                let mut to_add = vec![];
                for elem in val.split(',') {
                    if let Some(values) = self.interests.get(elem) {
                        to_add.push(values)
                    }
                }

                if to_add.is_empty() {
                    return Ok(json!({"accounts": []}));
                } else {
                    if to_add.len() == 1 {
                        indexes.push(to_add.pop().unwrap());
                    } else {
                        indexes_options = to_add;
                    }
                };
            }


            if let Some(_) = &filters.premium_now {
                if self.given_time.is_some() {
                    indexes.push(&self.premium_now);
                } else {
                    let now = Local::now().timestamp() as u32;
                    filters_fns.push(Box::new(move |acc: &Account| {
                        if let Some(val) = &acc.premium {
                            (val.start <= now) && (val.finish >= now)
                        } else {
                            false
                        }
                    }));
                }
            };

            // ФИЛЬТРЫ ЧЕРЕЗ ПЕРЕБОР

            match &filters.city_null {
                Some(1) => {
                    filters_fns.push(Box::new(move |acc: &Account| acc.city.is_none()));
                }
                Some(0) => {
                    filters_fns.push(Box::new(move |acc: &Account| acc.city.is_some()));
                }
                _ => (),
            }

            match &filters.country_null {
                Some(1) => {
                    filters_fns.push(Box::new(move |acc: &Account| acc.country.is_none()));
                }
                Some(0) => {
                    filters_fns.push(Box::new(move |acc: &Account| acc.country.is_some()));
                }
                _ => (),
            }

            match &filters.phone_null {
                Some(1) => {
                    filters_fns.push(Box::new(move |acc: &Account| acc.phone.is_none()));
                }
                Some(0) => {
                    filters_fns.push(Box::new(move |acc: &Account| acc.phone.is_some()));
                }
                _ => (),
            }

            match &filters.premium_null {
                Some(1) => {
                    filters_fns.push(Box::new(move |acc: &Account| acc.premium.is_none()));
                }
                Some(0) => {
                    filters_fns.push(Box::new(move |acc: &Account| acc.premium.is_some()));
                }
                _ => (),
            }

            match &filters.sname_null {
                Some(1) => {
                    filters_fns.push(Box::new(move |acc: &Account| acc.sname.is_none()));
                }
                Some(0) => {
                    filters_fns.push(Box::new(move |acc: &Account| acc.sname.is_some()));
                }
                _ => (),
            }

            match &filters.fname_null {
                Some(1) => {
                    filters_fns.push(Box::new(move |acc: &Account| acc.fname.is_none()));
                }
                Some(0) => {
                    filters_fns.push(Box::new(move |acc: &Account| acc.fname.is_some()));
                }
                _ => (),
            }

            if let Some(val) = &filters.status_eq {
                filters_fns.push(Box::new(move |acc: &Account| &acc.status == val));
            } else if let Some(val) = &filters.status_neq {
                filters_fns.push(Box::new(move |acc: &Account| &acc.status != val));
            };

            if let Some(val) = &filters.sex_eq {
                filters_fns.push(Box::new(move |acc: &Account| &acc.sex == val));
            };

            if let Some(val) = &filters.birth_gt {
                filters_fns.push(Box::new(move |acc: &Account| &acc.birth > val));
            } else if let Some(val) = &filters.birth_lt {
                filters_fns.push(Box::new(move |acc: &Account| &acc.birth < val));
            };

            if let Some(val) = &filters.email_gt {
                let searcher = val.as_str();
                filters_fns.push(Box::new(move |acc: &Account| &*acc.email > searcher));
            } else if let Some(val) = &filters.email_lt {
                let searcher = val.as_str();
                filters_fns.push(Box::new(move |acc: &Account| &*acc.email < searcher));
            };

            if let Some(val) = &filters.city_any {
                let mut varians: SmallSet<[Option<u16>; 5]> = SmallSet::new();
                for elem in val.split(',') {
                    if let Some((index, _, _)) = self.cities.get_full(elem) {
                        varians.insert(Some(index as u16));
                    }
                }

                if varians.len() == 0 {
                    return Ok(json!({"accounts": []}));
                } else {
                    if varians.len() == 1 {
                        let var = varians.iter().next().unwrap().clone();
                        filters_fns.push(Box::new(move |acc: &Account| var == acc.city));
                    } else {
                        filters_fns.push(Box::new(move |acc: &Account| varians.contains(&acc.city)));
                    }
                }
            }

            if let Some(val) = &filters.fname_any {
                let mut varians: SmallSet<[Option<u16>; 5]> = SmallSet::new();
                for elem in val.split(',') {
                    if let Some((index, _, _)) = self.fnames.get_full(elem) {
                        varians.insert(Some(index as u16));
                    }
                }

                if varians.len() == 0 {
                    return Ok(json!({"accounts": []}));
                } else {
                    if varians.len() == 1 {
                        let var = varians.iter().next().unwrap().clone();
                        filters_fns.push(Box::new(move |acc: &Account| var == acc.fname));
                    } else {
                        filters_fns.push(Box::new(move |acc: &Account| varians.contains(&acc.fname)));
                    }
                }
            }

            if let Some(val) = &filters.sname_starts {
                let mut varians: SmallSet<[Option<u16>; 5]> = SmallSet::new();
                for sname in self.snames_index.range::<String, _>(val..).take_while(|k| k.starts_with(val)) {
                    if let Some((index, _, _)) = self.snames.get_full(sname) {
                        varians.insert(Some(index as u16));
                    }
                }
                if varians.len() == 0 {
                    return Ok(json!({"accounts": []}));
                } else {
                    if varians.len() == 1 {
                        let var = varians.iter().next().unwrap().clone();
                        filters_fns.push(Box::new(move |acc: &Account| var == acc.sname));
                    } else {
                        filters_fns.push(Box::new(move |acc: &Account| varians.contains(&acc.sname)));
                    }
                };
            }

            indexes.sort_by_key(|i| -(i.len() as isize));
            indexes_likes.sort_by_key(|i| -(i.len() as isize));


            // ФИЛЬТРАЦИЯ
            {
                if let Some(likes) = indexes_likes.pop() {
                    likes
                        .iter()
                        .rev()
                        .filter(|uid| indexes.iter().all(|i| i.contains(uid)))
                        .filter(|uid| indexes_likes.iter().all(|i| i.contains(*uid)))
                        .map(|k| (*k, self.accounts.get(k).unwrap()))
                        .filter(|(_, acc)| filters_fns.iter().all(|f| f(&acc)))
                        .filter(|(uid, _)| if indexes_options.is_empty() { true } else { indexes_options.iter().any(|i| i.contains(uid)) })
                        .take(filters.limit)
                        .collect()
                } else if let Some(min) = indexes.pop() {
                    min
                        .iter()
                        .rev()
                        .filter(|uid| indexes.iter().all(|i| i.contains(uid)))
                        .filter(|uid| indexes_likes.iter().all(|i| i.contains(uid)))
                        .map(|k| (k, self.accounts.get(k).unwrap()))
                        .filter(|(_, acc)| filters_fns.iter().all(|f| f(&acc)))
                        .filter(|(uid, _)| if indexes_options.is_empty() { true } else { indexes_options.iter().any(|i| i.contains(uid)) })
                        .take(filters.limit)
                        .collect()
                } else {
                    self.accounts
                        .iter()
                        .rev()
                        .filter(|(_, acc)| filters_fns.iter().all(|f| f(&acc)))
                        .filter(|(uid, _)| if indexes_options.is_empty() { true } else { indexes_options.iter().any(|i| i.contains(uid)) })
                        .take(filters.limit)
                        .collect()
                }
            }
        };

        // ФОРМИРОВАНИЕ РЕЗУЛЬТАТОВ

        let mut result = Vec::with_capacity(filters.limit);

        for (id, acc) in end_ids.into_iter().rev() {
            let mut elem = HashMap::new();
            elem.insert("id", json!(id));
            elem.insert("email", json!(acc.email));

            if filters.sex_eq.is_some() {
                elem.insert("sex", json!(acc.sex));
            };

            if filters.status_eq.is_some() {
                elem.insert("status", json!(acc.status));
            } else if filters.status_neq.is_some() {
                elem.insert("status", json!(acc.status));
            };

            if filters.birth_year.is_some() {
                elem.insert("birth", json!(acc.birth));
            } else if filters.birth_gt.is_some() {
                elem.insert("birth", json!(acc.birth));
            } else if filters.birth_lt.is_some() {
                elem.insert("birth", json!(acc.birth));
            };

            if filters.fname_eq.is_some() {
                if let Some(fname) = acc.fname {
                    if let Some((fname, _)) = self.fnames.get_index(fname as usize) {
                        elem.insert("fname", json!(fname));
                    }
                } else {
                    elem.insert("fname", serde_json::Value::Null);
                }
            } else if let Some(0) = filters.fname_null {
                if let Some(fname) = acc.fname {
                    if let Some((fname, _)) = self.fnames.get_index(fname as usize) {
                        elem.insert("fname", json!(fname));
                    }
                } else {
                    elem.insert("fname", serde_json::Value::Null);
                }
            } else if filters.fname_any.is_some() {
                if let Some(fname) = acc.fname {
                    if let Some((fname, _)) = self.fnames.get_index(fname as usize) {
                        elem.insert("fname", json!(fname));
                    }
                } else {
                    elem.insert("fname", serde_json::Value::Null);
                }
            };

            if filters.premium_now.is_some() {
                elem.insert("premium", json!(acc.premium));
            } else if let Some(0) = filters.premium_null {
                elem.insert("premium", json!(acc.premium));
            };

            if filters.sname_eq.is_some() {
                if let Some(sname) = acc.sname {
                    if let Some((sname, _)) = self.snames.get_index(sname as usize) {
                        elem.insert("sname", json!(sname));
                    }
                } else {
                    elem.insert("sname", serde_json::Value::Null);
                }
            } else if let Some(0) = filters.sname_null {
                if let Some(sname) = acc.sname {
                    if let Some((sname, _)) = self.snames.get_index(sname as usize) {
                        elem.insert("sname", json!(sname));
                    }
                } else {
                    elem.insert("sname", serde_json::Value::Null);
                }
            } else if filters.sname_starts.is_some() {
                if let Some(sname) = acc.sname {
                    if let Some((sname, _)) = self.snames.get_index(sname as usize) {
                        elem.insert("sname", json!(sname));
                    }
                } else {
                    elem.insert("sname", serde_json::Value::Null);
                }
            };

            if let Some(0) = filters.phone_null {
                elem.insert("phone", json!(acc.phone));
            } else if let Some(_) = filters.phone_code {
                elem.insert("phone", json!(acc.phone));
            };

            if filters.city_any.is_some() {
                if let Some(city) = acc.city {
                    if let Some((city, _)) = self.cities.get_index(city as usize) {
                        elem.insert("city", json!(city));
                    }
                } else {
                    elem.insert("city", serde_json::Value::Null);
                }
            } else if filters.city_eq.is_some() {
                if let Some(city) = acc.city {
                    if let Some((city, _)) = self.cities.get_index(city as usize) {
                        elem.insert("city", json!(city));
                    }
                } else {
                    elem.insert("city", serde_json::Value::Null);
                }
            } else if let Some(0) = filters.city_null {
                if let Some(city) = acc.city {
                    if let Some((city, _)) = self.cities.get_index(city as usize) {
                        elem.insert("city", json!(city));
                    }
                } else {
                    elem.insert("city", serde_json::Value::Null);
                }
            };

            if filters.country_eq.is_some() {
                if let Some(country) = acc.country {
                    if let Some((country, _)) = self.countries.get_index(country as usize) {
                        elem.insert("country", json!(country));
                    }
                } else {
                    elem.insert("country", serde_json::Value::Null);
                }
            } else if let Some(0) = filters.country_null {
                if let Some(country) = acc.country {
                    if let Some((country, _)) = self.countries.get_index(country as usize) {
                        elem.insert("country", json!(country));
                    }
                } else {
                    elem.insert("country", serde_json::Value::Null);
                }
            };

            result.push(elem);
        }
        Ok(json!({
    "accounts": result
}))
    }

    fn recommend(&self, uid: u32, suggest_filters: SuggestRecommend) -> Result<serde_json::Value, StatusCode> {
        let account = match self.accounts.get(&uid) {
            Some(account) => account,
            None => return Err(StatusCode::NOT_FOUND),
        };
        if account.interests.is_empty() {
            return Ok(json!({"accounts": []}));
        }

        let filter_func: Box<Fn(&Account) -> bool> = {
            if let Some(city) = suggest_filters.city {
                if let Some((number, _, _)) = self.cities.get_full(&city) {
                    let number = Some(number as u16);
                    let sex = if account.sex == Sex::F { Sex::M } else { Sex::F };
                    Box::new(move |acc: &Account| (acc.city == number && acc.sex == sex))
                } else {
                    return Ok(json!({"accounts": []}));
                }
            } else if let Some(country) = suggest_filters.country {
                if let Some((number, _, _)) = self.countries.get_full(&country) {
                    let number = Some(number as u16);
                    let sex = if account.sex == Sex::F { Sex::M } else { Sex::F };
                    Box::new(move |acc: &Account| (acc.country == number && acc.sex == sex))
                } else {
                    return Ok(json!({"accounts": []}));
                }
            } else {
                let sex = if account.sex == Sex::F { Sex::M } else { Sex::F };
                Box::new(move |acc: &Account| acc.sex == sex)
            }
        };

        let mut result = Vec::with_capacity(suggest_filters.limit);

        for (uid, account) in account.interests
            .iter()
            .filter_map(|ind| self.interests.get_index(*ind as usize))
            .flat_map(|(_, v)| v)
            .unique()
            .map(|u_id| (u_id, self.accounts.get(u_id).unwrap()))
            .filter(|(_, acc)| filter_func(acc))
            .sorted_by(|(u_id2, acc2), (u_id1, acc1)|
                self.premium_now.contains(u_id1).cmp(&self.premium_now.contains(u_id2))
                    .then((match acc1.status {
                        Status::Free => 2,
                        Status::AllHard => 1,
                        Status::Muted => 0
                    }).cmp(&(match acc2.status {
                        Status::Free => 2,
                        Status::AllHard => 1,
                        Status::Muted => 0
                    })))
                    .then((acc1.interests
                        .iter()
                        .map(|s| if account.interests.contains(s) { 1 } else { 0 }).sum::<u8>())
                        .cmp(&(acc2.interests
                            .iter()
                            .map(|s| if account.interests.contains(s) { 1 } else { 0 }).sum::<u8>())))
                    .then(
                        ((account.birth as i32) - (acc2.birth as i32)).abs()
                            .cmp(&((account.birth as i32) - (acc1.birth as i32)).abs()))
            ).take(suggest_filters.limit) {
            let mut elem = HashMap::new();
            elem.insert("id", json!(uid));
            let acc = self.accounts.get(uid).unwrap();
            elem.insert("email", json!(acc.email));
            elem.insert("status", json!(acc.status));
            elem.insert("birth", json!(acc.birth));
            if let Some(val) = acc.fname {
                if let Some((fname, _)) = self.fnames.get_index(val as usize) {
                    elem.insert("fname", json!(fname));
                }
            }
            if let Some(val) = acc.sname {
                if let Some((sname, _)) = self.snames.get_index(val as usize) {
                    elem.insert("sname", json!(sname));
                }
            }
            if let Some(val) = &acc.premium {
                elem.insert("premium", json!(val));
            }
            result.push(elem);
        }

        Ok(json!({"accounts": result}))
    }

    fn suggest(&self, uid: u32, suggest_filters: SuggestRecommend) -> Result<serde_json::Value, StatusCode> {
        if !self.accounts.contains_key(&uid) {
            return Err(StatusCode::NOT_FOUND);
        }
        let mut filter = None;
        if let Some(val) = suggest_filters.country {
            if let Some(index) = self.countries.get(&val) {
                filter = Some(index);
            } else {
                return Ok(json!({"accounts": []}));
            }
        } else if let Some(val) = suggest_filters.city {
            if let Some(index) = self.cities.get(&val) {
                filter = Some(index);
            } else {
                return Ok(json!({"accounts": []}));
            }
        }
        let mut identities_map: hashbrown::HashMap<&u32, hashbrown::HashMap<&u32, Vec<&u32>>> = hashbrown::HashMap::new();
        let mut acc_likes = hashbrown::HashMap::new();
        if let Some(likes_from_account) = self.liked.get(&uid) {
            for liked_user_uid in likes_from_account {
                if let Some(likes_from_another) = self.likes.get(&liked_user_uid) {
                    for (another_liker_uid, ts) in likes_from_another {
                        if another_liker_uid == &uid {
                            acc_likes.entry(liked_user_uid).or_insert(vec![]).push(ts);
                        } else {
                            if let Some(index) = filter {
                                if !index.contains(another_liker_uid) {
                                    continue;
                                }
                            }

                            identities_map
                                .entry(another_liker_uid)
                                .or_insert(hashbrown::HashMap::new())
                                .entry(liked_user_uid).or_insert(vec![]).push(ts);
                        }
                    }
                }
            }

            let mut result = Vec::with_capacity(suggest_filters.limit);
            for founded_uid in identities_map
                .into_iter()
                .map(|(uid, likes)|
                    (likes.iter()
                         .map(|(liked, a)|
                             sym(a, acc_likes.get(liked).unwrap()))
                         .sum::<f32>(), uid))
                .sorted_by(|(a, a_id), (b, b_id)|
                    b.partial_cmp(a).unwrap_or(Equal).then(b_id.cmp(a_id)))
                .filter_map(|(_, u_id)| self
                    .liked.get(u_id)
                    .and_then(|liked| {
                        Some(liked.iter().filter(|id| !likes_from_account.contains(id))
                            .sorted_by(|a, b| b.cmp(a)))
                    }))
                .flatten()
                .unique()
                .take(suggest_filters.limit)
                {
                    let mut elem = HashMap::new();
                    elem.insert("id", json!(founded_uid));
                    let acc = self.accounts.get(founded_uid).unwrap();
                    elem.insert("email", json!(acc.email));
                    elem.insert("status", json!(acc.status));
                    if let Some(val) = acc.fname {
                        if let Some((fname, _)) = self.fnames.get_index(val as usize) {
                            elem.insert("fname", json!(fname));
                        }
                    }
                    if let Some(val) = acc.sname {
                        if let Some((sname, _)) = self.snames.get_index(val as usize) {
                            elem.insert("sname", json!(sname));
                        }
                    }
                    result.push(elem);
                }
            Ok(json!({"accounts": result}))
        } else {
            return Ok(json!({"accounts": []}));
        }
    }

    fn group(&self, group_filters: Group) -> Result<serde_json::Value, StatusCode> {
        let mut keys: SmallSet<[GroupKeys; 5]> = SmallSet::new();
        let variants: HashMap<&str, GroupKeys> = [
            ("sex", GroupKeys::Sex),
            ("status", GroupKeys::Status),
            ("interests", GroupKeys::Interests),
            ("country", GroupKeys::Country),
            ("city", GroupKeys::City),
        ].iter().cloned().collect();

        for key in group_filters.keys.split(',') {
            match variants.get(key) {
                Some(group_key) => { keys.insert(group_key.clone()); }
                None => { return Err(StatusCode::BAD_REQUEST); }
            }
        }
        if keys.len() == 0 {
            return Err(StatusCode::BAD_REQUEST);
        }

        if group_filters.likes.is_some() ||
            group_filters.fname.is_some() ||
            group_filters.birth.is_some() ||
            group_filters.sname.is_some()
            {
                let mut filters_fns: Vec<Box<Fn(&u32, &Account) -> bool>> = vec![];
                let mut indexes_likes: Option<BTreeSet<&u32>> = None;
                let mut main_index: Option<&BTreeSet<u32>> = None;

                if let Some(filter) = &group_filters.likes {
                    if let Some(likes) = self.likes.get(filter) {
                        indexes_likes = Some(likes.iter().map(|(like, _)| like).collect());
                    } else {
                        return Ok(json!({"groups": []}));
                    }
                }

                if let Some(filter) = &group_filters.sname {
                    if let Some((index, _, snames)) = self.snames.get_full(filter) {
                        if indexes_likes.is_some() || main_index.is_some() {
                            let index = Some(index as u16);
                            filters_fns.push(Box::new(move |_, acc: &Account| acc.sname == index));
                        } else {
                            main_index = Some(snames);
                        }
                    } else {
                        return Ok(json!({"groups": []}));
                    }
                }

                if let Some(val) = &group_filters.city {
                    if let Some((number, _, index)) = self.cities.get_full(val) {
                        if indexes_likes.is_some() || main_index.is_some() {
                            let number = Some(number as u16);
                            filters_fns.push(Box::new(move |_, acc: &Account| acc.city == number));
                        } else {
                            main_index = Some(index);
                        }
                    } else {
                        return Ok(json!({"groups": []}));
                    }
                }

                if let Some(filter) = &group_filters.fname {
                    if let Some((index, _, fnames)) = self.fnames.get_full(filter) {
                        if indexes_likes.is_some() || main_index.is_some() {
                            let index = Some(index as u16);
                            filters_fns.push(Box::new(move |_, acc: &Account| acc.fname == index));
                        } else {
                            main_index = Some(fnames);
                        }
                    } else {
                        return Ok(json!({"groups": []}));
                    }
                }

                if let Some(val) = &group_filters.interests {
                    if let Some((number, _, index)) = self.interests.get_full(val) {
                        if indexes_likes.is_some() || main_index.is_some() {
                            let number = number as u16;
                            filters_fns.push(Box::new(move |_, acc: &Account| acc.interests.contains(&number)));
                        } else {
                            main_index = Some(index);
                        }
                    } else {
                        return Ok(json!({"groups": []}));
                    }
                }

                if let Some(val) = &group_filters.country {
                    if let Some((number, _, index)) = self.countries.get_full(val) {
                        if indexes_likes.is_some() || main_index.is_some() {
                            let number = Some(number as u16);
                            filters_fns.push(Box::new(move |_, acc: &Account| acc.country == number));
                        } else {
                            main_index = Some(index);
                        }
                    } else {
                        return Ok(json!({"groups": []}));
                    }
                }

                if let Some(filter) = &group_filters.birth {
                    if let Some(birth_index) = self.birth_index.get(filter) {
                        if indexes_likes.is_some() || main_index.is_some() {
                            filters_fns.push(Box::new(move |uid: &u32, _| birth_index.contains(uid)));
                        } else {
                            main_index = Some(birth_index);
                        }
                    } else {
                        return Ok(json!({"groups": []}));
                    }
                }

                if let Some(filter) = &group_filters.joined {
                    if let Some(joined_index) = self.joined_index.get(filter) {
                        if indexes_likes.is_some() || main_index.is_some() {
                            filters_fns.push(Box::new(move |uid: &u32, _| joined_index.contains(uid)));
                        } else {
                            main_index = Some(joined_index);
                        }
                    } else {
                        return Ok(json!({"groups": []}));
                    }
                }
                if let Some(val) = &group_filters.status {
                    filters_fns.push(Box::new(move |_, acc: &Account| &acc.status == val));
                }

                if let Some(val) = &group_filters.sex {
                    filters_fns.push(Box::new(move |_, acc: &Account| &acc.sex == val));
                }

                let has_interests = keys.contains(&GroupKeys::Interests);

                let mut result = hashbrown::HashMap::new();

                let city_key_func: fn(&Account) -> Option<usize> = {
                    if keys.contains(&GroupKeys::City) {
                        |acc: &Account| match acc.city {
                            Some(num) => Some(num as usize),
                            _ => None
                        }
                    } else {
                        |_| None
                    }
                };

                let country_key_func: fn(&Account) -> Option<usize> = {
                    if keys.contains(&GroupKeys::Country) {
                        |acc: &Account| match acc.country {
                            Some(num) => Some(num as usize),
                            _ => None
                        }
                    } else {
                        |_| None
                    }
                };

                let sex_key_func: fn(&Account) -> Option<&Sex> = {
                    if keys.contains(&GroupKeys::Sex) {
                        |acc: &Account| Some(&acc.sex)
                    } else {
                        |_| None
                    }
                };

                let status_key_func: fn(&Account) -> Option<&Status> = {
                    if keys.contains(&GroupKeys::Status) {
                        |acc: &Account| Some(&acc.status)
                    } else {
                        |_| None
                    }
                };

                if let Some(index) = indexes_likes {
                    for (_, acc) in index.iter()
                        .map(|k| (k, self.accounts.get(k).unwrap()))
                        .filter(|(uid, acc)| filters_fns.iter().all(|f| f(uid, &acc))) {
                        if has_interests {
                            for interest in &acc.interests {
                                *result.entry((
                                    city_key_func(acc),
                                    country_key_func(acc),
                                    Some(interest),
                                    sex_key_func(acc),
                                    status_key_func(acc),
                                )).or_insert(0) += 1;
                            }
                        } else {
                            *result.entry((
                                city_key_func(acc),
                                country_key_func(acc),
                                None,
                                sex_key_func(acc),
                                status_key_func(acc),
                            )).or_insert(0) += 1;
                        }
                    }
                } else if let Some(index) = main_index {
                    for (_, acc) in index.iter()
                        .map(|k| (k, self.accounts.get(k).unwrap()))
                        .filter(|(uid, acc)| filters_fns.iter().all(|f| f(uid, &acc))) {
                        if has_interests {
                            for interest in &acc.interests {
                                *result.entry((
                                    city_key_func(acc),
                                    country_key_func(acc),
                                    Some(interest),
                                    sex_key_func(acc),
                                    status_key_func(acc),
                                )).or_insert(0) += 1;
                            }
                        } else {
                            *result.entry((
                                city_key_func(acc),
                                country_key_func(acc),
                                None,
                                sex_key_func(acc),
                                status_key_func(acc),
                            )).or_insert(0) += 1;
                        }
                    }
                }

                let mut result_groups = Vec::with_capacity(group_filters.limit);

                for (entity, count) in result
                    .iter()
//                .filter(|(entity, _)|{
//                    keys.contains(&GroupKeys::Sex) ||
//                    keys.contains(&GroupKeys::Status) ||
//                    (keys.contains(&GroupKeys::City) && entity.0.is_some()) ||
//                    (keys.contains(&GroupKeys::Country) && entity.1.is_some()) ||
//                    (keys.contains(&GroupKeys::Interests) && entity.2.is_some())
//                })
                    .sorted_by(|(entity_a, count_a), (entity_b, count_b)| match group_filters.order {
                        Order::Asc =>
                            count_a.cmp(count_b)
                                .then((if keys.contains(&GroupKeys::City) && entity_a.0.is_some() {
                                    Some(self.cities.get_index(entity_a.0.unwrap() as usize).unwrap().0)
                                } else { None })
                                    .cmp(&(if keys.contains(&GroupKeys::City) && entity_b.0.is_some() {
                                        Some(self.cities.get_index(entity_b.0.unwrap() as usize).unwrap().0)
                                    } else { None })))
                                .then((if keys.contains(&GroupKeys::Country) && entity_a.1.is_some() {
                                    Some(self.countries.get_index(entity_a.1.unwrap() as usize).unwrap().0)
                                } else { None })
                                    .cmp(&(if keys.contains(&GroupKeys::Country) && entity_b.1.is_some() {
                                        Some(self.countries.get_index(entity_b.1.unwrap() as usize).unwrap().0)
                                    } else { None })))
                                .then((if keys.contains(&GroupKeys::Interests) && entity_a.2.is_some() {
                                    Some(self.interests.get_index(*entity_a.2.unwrap() as usize).unwrap().0)
                                } else { None })
                                    .cmp(&(if keys.contains(&GroupKeys::Interests) && entity_b.2.is_some() {
                                        Some(self.interests.get_index(*entity_b.2.unwrap() as usize).unwrap().0)
                                    } else { None })))
                                .then(if keys.contains(&GroupKeys::Sex) { entity_a.3.cmp(&entity_b.3) } else { Equal })
                                .then(if keys.contains(&GroupKeys::Status) { entity_a.4.cmp(&entity_b.4) } else { Equal })
                        ,
                        Order::Desc =>
                            count_a.cmp(count_b).reverse()
                                .then((if keys.contains(&GroupKeys::City) && entity_a.0.is_some() {
                                    Some(self.cities.get_index(entity_a.0.unwrap() as usize).unwrap().0)
                                } else { None })
                                    .cmp(&(if keys.contains(&GroupKeys::City) && entity_b.0.is_some() {
                                        Some(self.cities.get_index(entity_b.0.unwrap() as usize).unwrap().0)
                                    } else { None })).reverse())
                                .then((if keys.contains(&GroupKeys::Country) && entity_a.1.is_some() {
                                    Some(self.countries.get_index(entity_a.1.unwrap() as usize).unwrap().0)
                                } else { None })
                                    .cmp(&(if keys.contains(&GroupKeys::Country) && entity_b.1.is_some() {
                                        Some(self.countries.get_index(entity_b.1.unwrap() as usize).unwrap().0)
                                    } else { None })).reverse())
                                .then((if keys.contains(&GroupKeys::Interests) && entity_a.2.is_some() {
                                    Some(self.interests.get_index(*entity_a.2.unwrap() as usize).unwrap().0)
                                } else { None })
                                    .cmp(&(if keys.contains(&GroupKeys::Interests) && entity_b.2.is_some() {
                                        Some(self.interests.get_index(*entity_b.2.unwrap() as usize).unwrap().0)
                                    } else { None })).reverse())
                                .then(if keys.contains(&GroupKeys::Sex) { entity_a.3.cmp(&entity_b.3).reverse() } else { Equal })
                                .then(if keys.contains(&GroupKeys::Status) { entity_a.4.cmp(&entity_b.4).reverse() } else { Equal })
                    })
                    .take(group_filters.limit) {
                    let mut group = HashMap::new();
                    group.insert("count", json!(count));
                    if keys.contains(&GroupKeys::City) {
                        if let Some(val) = entity.0 {
                            group.insert("city", json!(self.cities.get_index(val as usize).unwrap().0));
                        }
                    }
                    if keys.contains(&GroupKeys::Country) {
                        if let Some(val) = entity.1 {
                            group.insert("country", json!(self.countries.get_index(val as usize).unwrap().0));
                        }
                    }
                    if keys.contains(&GroupKeys::Interests) {
                        if let Some(val) = entity.2 {
                            group.insert("interests", json!(self.interests.get_index(*val as usize).unwrap().0));
                        }
                    }
                    if keys.contains(&GroupKeys::Sex) {
                        group.insert("sex", json!(entity.3));
                    }
                    if keys.contains(&GroupKeys::Status) {
                        group.insert("status", json!(entity.4));
                    }

                    result_groups.push(group);
                }

                Ok(json!({"groups": result_groups}))
            } else {
            if group_filters.birth.is_some() {
                return Ok(json!({"groups": []}));
            }

            let mut group_keys = vec![];
            let mut filters_fns: Vec<Box<Fn(&GroupKey) -> bool>> = vec![];

            if keys.contains(&GroupKeys::City)
                || group_filters.city.is_some()
                {
                    group_keys.push(GroupKeys::City);
                }
            if keys.contains(&GroupKeys::Country)
                || group_filters.country.is_some()
                {
                    group_keys.push(GroupKeys::Country);
                }
            if keys.contains(&GroupKeys::Interests)
                || group_filters.interests.is_some()
                {
                    group_keys.push(GroupKeys::Interests);
                }
            group_keys.push(GroupKeys::Sex);
            group_keys.push(GroupKeys::Status);
            if group_filters.joined.is_some() {
                group_keys.push(GroupKeys::Joined);
            }

            if let Some(val) = &group_filters.city {
                if let Some((number, _, _)) = self.cities.get_full(val) {
                    let number = Some(number as u16);
                    filters_fns.push(Box::new(move |gk: &GroupKey| gk.0 == number));
                } else {
                    return Ok(json!({"groups": []}));
                }
            }

            if let Some(val) = &group_filters.country {
                if let Some((number, _, _)) = self.countries.get_full(val) {
                    let number = Some(number as u16);
                    filters_fns.push(Box::new(move |gk: &GroupKey| gk.1 == number));
                } else {
                    return Ok(json!({"groups": []}));
                }
            }

            if let Some(val) = &group_filters.interests {
                if let Some((number, _, _)) = self.interests.get_full(val) {
                    let number = Some(number as u16);
                    filters_fns.push(Box::new(move |gk: &GroupKey| gk.2 == number));
                } else {
                    return Ok(json!({"groups": []}));
                }
            }

            if let Some(val) = &group_filters.sex {
                filters_fns.push(Box::new(move |gk: &GroupKey| &gk.3 == val));
            }

            if let Some(val) = &group_filters.status {
                filters_fns.push(Box::new(move |gk: &GroupKey| &gk.4 == val));
            }

            if let Some(val) = &group_filters.joined {
                filters_fns.push(Box::new(move |gk: &GroupKey| gk.5 == Some(*val)));
            }

            let city_key_func: fn(Option<u16>) -> Option<u16> = {
                if keys.contains(&GroupKeys::City) {
                    |e: Option<u16>| e
                } else {
                    |_| None
                }
            };

            let country_key_func: fn(Option<u16>) -> Option<u16> = {
                if keys.contains(&GroupKeys::Country) {
                    |e: Option<u16>| e
                } else {
                    |_| None
                }
            };
            let interest_key_func: fn(Option<u16>) -> Option<u16> = {
                if keys.contains(&GroupKeys::Interests) {
                    |e: Option<u16>| e
                } else {
                    |_| None
                }
            };
            let sex_key_func: fn(&Sex) -> Option<&Sex> = {
                if keys.contains(&GroupKeys::Sex) {
                    |e: &Sex| Some(e)
                } else {
                    |_| None
                }
            };
            let status_key_func: fn(&Status) -> Option<&Status> = {
                if keys.contains(&GroupKeys::Status) {
                    |e: &Status| Some(e)
                } else {
                    |_| None
                }
            };

            let mut result_groups = Vec::with_capacity(group_filters.limit);

            if let Some(group) = self.groups.get(&group_keys) {
                for (entity, count) in group
                    .iter()
                    .filter(|(_, count)| **count > 0)
                    .filter(|(key, _)| filters_fns.iter().all(|f| f(&key)))
                    .sorted_by(|(entity_a, _), (entity_b, _)|
                        (
                            city_key_func(entity_a.0),
                            country_key_func(entity_a.1),
                            interest_key_func(entity_a.2),
                            sex_key_func(&entity_a.3),
                            status_key_func(&entity_a.4),
                        ).cmp(&(
                            city_key_func(entity_b.0),
                            country_key_func(entity_b.1),
                            interest_key_func(entity_b.2),
                            sex_key_func(&entity_b.3),
                            status_key_func(&entity_b.4),
                        ))
                    )
                    .group_by(|&(ent, _)| {
                        (
                            city_key_func(ent.0),
                            country_key_func(ent.1),
                            interest_key_func(ent.2),
                            sex_key_func(&ent.3),
                            status_key_func(&ent.4),
                        )
                    })
                    .into_iter()
                    .map(|(key, group)| (key, group.map(|(_, v)| v).sum::<u32>()))
                    .sorted_by(|(entity_a, count_a), (entity_b, count_b)| match group_filters.order {
                        Order::Asc =>
                            count_a.cmp(count_b)
                                .then((if keys.contains(&GroupKeys::City) && entity_a.0.is_some() {
                                    Some(self.cities.get_index(entity_a.0.unwrap() as usize).unwrap().0)
                                } else { None })
                                    .cmp(&(if keys.contains(&GroupKeys::City) && entity_b.0.is_some() {
                                        Some(self.cities.get_index(entity_b.0.unwrap() as usize).unwrap().0)
                                    } else { None })))
                                .then((if keys.contains(&GroupKeys::Country) && entity_a.1.is_some() {
                                    Some(self.countries.get_index(entity_a.1.unwrap() as usize).unwrap().0)
                                } else { None })
                                    .cmp(&(if keys.contains(&GroupKeys::Country) && entity_b.1.is_some() {
                                        Some(self.countries.get_index(entity_b.1.unwrap() as usize).unwrap().0)
                                    } else { None })))
                                .then((if keys.contains(&GroupKeys::Interests) && entity_a.2.is_some() {
                                    Some(self.interests.get_index(entity_a.2.unwrap() as usize).unwrap().0)
                                } else { None })
                                    .cmp(&(if keys.contains(&GroupKeys::Interests) && entity_b.2.is_some() {
                                        Some(self.interests.get_index(entity_b.2.unwrap() as usize).unwrap().0)
                                    } else { None })))
                                .then(if keys.contains(&GroupKeys::Sex) { entity_a.3.cmp(&entity_b.3) } else { Equal })
                                .then(if keys.contains(&GroupKeys::Status) { entity_a.4.cmp(&entity_b.4) } else { Equal })
                        ,
                        Order::Desc =>
                            count_a.cmp(count_b).reverse()
                                .then((if keys.contains(&GroupKeys::City) && entity_a.0.is_some() {
                                    Some(self.cities.get_index(entity_a.0.unwrap() as usize).unwrap().0)
                                } else { None })
                                    .cmp(&(if keys.contains(&GroupKeys::City) && entity_b.0.is_some() {
                                        Some(self.cities.get_index(entity_b.0.unwrap() as usize).unwrap().0)
                                    } else { None })).reverse())
                                .then((if keys.contains(&GroupKeys::Country) && entity_a.1.is_some() {
                                    Some(self.countries.get_index(entity_a.1.unwrap() as usize).unwrap().0)
                                } else { None })
                                    .cmp(&(if keys.contains(&GroupKeys::Country) && entity_b.1.is_some() {
                                        Some(self.countries.get_index(entity_b.1.unwrap() as usize).unwrap().0)
                                    } else { None })).reverse())
                                .then((if keys.contains(&GroupKeys::Interests) && entity_a.2.is_some() {
                                    Some(self.interests.get_index(entity_a.2.unwrap() as usize).unwrap().0)
                                } else { None })
                                    .cmp(&(if keys.contains(&GroupKeys::Interests) && entity_b.2.is_some() {
                                        Some(self.interests.get_index(entity_b.2.unwrap() as usize).unwrap().0)
                                    } else { None })).reverse())
                                .then(if keys.contains(&GroupKeys::Sex) { entity_a.3.cmp(&entity_b.3).reverse() } else { Equal })
                                .then(if keys.contains(&GroupKeys::Status) { entity_a.4.cmp(&entity_b.4).reverse() } else { Equal })
                    })
                    .take(group_filters.limit) {
                    let mut group = HashMap::new();
                    group.insert("count", json!(count));
                    if keys.contains(&GroupKeys::City) {
                        if let Some(val) = entity.0 {
                            group.insert("city", json!(self.cities.get_index(val as usize).unwrap().0));
                        }
                    }
                    if keys.contains(&GroupKeys::Country) {
                        if let Some(val) = entity.1 {
                            group.insert("country", json!(self.countries.get_index(val as usize).unwrap().0));
                        }
                    }
                    if keys.contains(&GroupKeys::Interests) {
                        if let Some(val) = entity.2 {
                            group.insert("interests", json!(self.interests.get_index(val as usize).unwrap().0));
                        }
                    }
                    if keys.contains(&GroupKeys::Sex) {
                        group.insert("sex", json!(entity.3));
                    }
                    if keys.contains(&GroupKeys::Status) {
                        group.insert("status", json!(entity.4));
                    }

                    result_groups.push(group);
                }
            } else {
                return Ok(json!({"groups": []}));
            }

            Ok(json!({"groups": result_groups}))
        }
    }
}

#[inline]
fn sym(numbers_a: &Vec<&u32>, numbers_b: &Vec<&u32>) -> f32 {
    let a = numbers_a.iter().map(|b| **b).sum::<u32>() as f32 / numbers_a.len() as f32;
    let b = numbers_b.iter().map(|b| **b).sum::<u32>() as f32 / numbers_b.len() as f32;
    if a == b { 1_f32 } else { 1_f32 / (a - b).abs() }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
enum Sex {
    #[serde(rename = "f")]
    F,
    #[serde(rename = "m")]
    M,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
enum Status {
    #[serde(rename = "всё сложно")]
    AllHard,
    #[serde(rename = "заняты")]
    Muted,
    #[serde(rename = "свободны")]
    Free,
}

#[repr(C)]
#[derive(Debug, Clone)]
struct Account {
    birth: u32,
    joined: u32,
    email_domain: u16,
    fname: Option<u16>,
    sname: Option<u16>,
    country: Option<u16>,
    city: Option<u16>,
    phone_code: Option<u16>,
    status: Status,
    sex: Sex,
    email: String,
    phone: Option<String>,
    premium: Option<Premium>,
    interests: Vec<u16>,
}

#[serde(deny_unknown_fields)]
#[derive(Debug, Validate, Serialize, Deserialize)]
struct AccountFull {
    #[validate(range(min = "1", max = "4294967295"))]
    id: u32,
    sex: Sex,
    status: Status,
    birth: u32,
    joined: u32,
    premium: Option<Premium>,
    #[validate(contains = "@")]
    #[validate(length(min = "1", max = "100"))]
    email: String,
    #[validate(length(min = "1", max = "50"))]
    fname: Option<String>,
    #[validate(length(min = "1", max = "50"))]
    sname: Option<String>,
    #[validate(length(min = "1", max = "16"))]
    phone: Option<String>,
    #[validate(length(min = "1", max = "50"))]
    country: Option<String>,
    #[validate(length(min = "1", max = "50"))]
    city: Option<String>,
    interests: Option<Vec<String>>,
    likes: Option<Vec<Likes>>,
}

#[serde(deny_unknown_fields)]
#[derive(Debug, Validate, Serialize, Deserialize)]
struct AccountOptional {
    #[validate(contains = "@")]
    #[validate(length(min = "1", max = "100"))]
    email: Option<String>,
    #[validate(length(min = "1", max = "50"))]
    fname: Option<String>,
    #[validate(length(min = "1", max = "50"))]
    sname: Option<String>,
    #[validate(length(min = "1", max = "16"))]
    phone: Option<String>,
    sex: Option<Sex>,
    birth: Option<u32>,
    #[validate(length(min = "1", max = "50"))]
    country: Option<String>,
    #[validate(length(min = "1", max = "50"))]
    city: Option<String>,
    joined: Option<u32>,
    status: Option<Status>,
    interests: Option<Vec<String>>,
    premium: Option<Premium>,
    likes: Option<Vec<Likes>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Premium {
    start: u32,
    finish: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Likes {
    id: u32,
    ts: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LikerLikee {
    liker: u32,
    likee: u32,
    ts: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LikesRequest {
    likes: Vec<LikerLikee>,
}

#[serde(deny_unknown_fields)]
#[derive(Debug, Validate, Serialize, Deserialize)]
struct Filters {
    #[validate(range(min = "1", max = "4294967295"))]
    limit: usize,
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
    phone_code: Option<String>,
    #[validate(range(min = "0", max = "1"))]
    phone_null: Option<u8>,
    country_eq: Option<String>,
    #[validate(range(min = "0", max = "1"))]
    country_null: Option<u8>,
    city_eq: Option<String>,
    city_any: Option<String>,
    #[validate(range(min = "0", max = "1"))]
    city_null: Option<u8>,
    birth_lt: Option<u32>,
    birth_gt: Option<u32>,
    birth_year: Option<u32>,
    interests_contains: Option<String>,
    interests_any: Option<String>,
    likes_contains: Option<String>,
    premium_now: Option<i64>,
    #[validate(range(min = "0", max = "1"))]
    premium_null: Option<u8>,
    query_id: Option<i64>,
}


#[serde(deny_unknown_fields)]
#[derive(Debug, Validate, Serialize, Deserialize)]
struct Group {
    query_id: Option<i64>,
    keys: String,
    #[validate(range(min = "1", max = "4294967295"))]
    limit: usize,
    order: Order,
    #[validate(contains = "@")]
    #[validate(length(min = "1", max = "50"))]
    fname: Option<String>,
    #[validate(length(min = "1", max = "50"))]
    sname: Option<String>,
    sex: Option<Sex>,
    birth: Option<u32>,
    #[validate(length(min = "1", max = "50"))]
    country: Option<String>,
    #[validate(length(min = "1", max = "50"))]
    city: Option<String>,
    joined: Option<u32>,
    status: Option<Status>,
    #[validate(length(min = "1", max = "100"))]
    interests: Option<String>,
    likes: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
enum Order {
    #[serde(rename = "1")]
    Asc,
    #[serde(rename = "-1")]
    Desc,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
enum GroupKeys {
    #[serde(rename = "sex")]
    Sex,
    #[serde(rename = "status")]
    Status,
    #[serde(rename = "interests")]
    Interests,
    #[serde(rename = "country")]
    Country,
    #[serde(rename = "city")]
    City,
    Joined,
}

#[serde(deny_unknown_fields)]
#[derive(Debug, Validate, Serialize, Deserialize)]
struct SuggestRecommend {
    query_id: Option<i64>,
    #[validate(range(min = "1", max = "4294967295"))]
    limit: usize,
    #[validate(length(min = "1", max = "50"))]
    country: Option<String>,
    #[validate(length(min = "1", max = "50"))]
    city: Option<String>,
}


fn filter(query: Query<Filters>, state: State<AppState>) -> HttpResponse {
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

fn group(query: Query<Group>, state: State<AppState>) -> HttpResponse {
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

fn suggest(query: Query<SuggestRecommend>, path: Path<(u32, )>, state: State<AppState>) -> HttpResponse {
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

fn recommend(query: Query<SuggestRecommend>, path: Path<(u32, )>, state: State<AppState>) -> HttpResponse {
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

fn likes(req: HttpRequest<AppState>, item: Json<LikesRequest>, state: State<AppState>) -> Result<AsyncResult<HttpResponse>> {
    let mut database = state.database.write();
    database.update_likes(item.0, req)
}

fn update(req: HttpRequest<AppState>, item: Json<AccountOptional>, path: Path<(u32, )>, state: State<AppState>) -> Result<AsyncResult<HttpResponse>> {
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

fn new(req: HttpRequest<AppState>, item: Json<AccountFull>, state: State<AppState>) -> Result<AsyncResult<HttpResponse>> {
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
        Arc::new(RwLock::new(DataBase::from_file_in_place(data_file)))
    } else {
        Arc::new(RwLock::new(DataBase::from_file(data_file)))
    };
    println!("Database ready!");

    server::new(move || {
        App::with_state(AppState {
            database: database.clone(),
        })
            .prefix("/accounts")
            .resource("/filter/", |r| {
                r.method(Method::GET).with_config(filter, |(cfg, _)| {
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
                r.method(Method::GET).with_config(group, |(cfg, _)| {
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
                r.method(Method::GET).with_config(recommend, |(cfg, _, _)| {
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
                r.method(Method::GET).with_config(suggest, |(cfg, _, _)| {
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
            .resource("/new/", |r| r.method(Method::POST).with(new))
            .resource("/likes/", |r| r.method(Method::POST).with(likes))
            .resource("/{id}/", |r| r.method(Method::POST).with(update))
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
    memory_info();
    let _ = sys.run();
    println!("Stopped http server");
    memory_info();
}

fn memory_info() {
    match sys_info::mem_info() {
        Ok(val) => println!("{:?}", val),
        Err(e) => println!("{:?}", e),
    };
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
