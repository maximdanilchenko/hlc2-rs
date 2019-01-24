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
extern crate fnv;
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

//use radix_trie::{Trie, TrieCommon};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::env;
use std::io::{BufReader, BufRead, Read};
use std::fs::File;
use std::sync::{Arc, Mutex};
//, RwLock};
use std::thread;
use std::time::{Instant, Duration};
use std::mem;

use actix_web::http::{header, Method, StatusCode};
use actix_web::{error, server, App, HttpResponse, Json, Path, Query, State};
use itertools::Itertools;
//use itertools::GroupBy;
use validator::Validate;
//use flat_map::FlatMap;

use chrono::{TimeZone, Utc, Local, Datelike};
use fnv::{FnvHashMap, FnvHashSet};
use indexmap::{IndexMap, IndexSet};
//, IndexSet};
use smallvec::SmallVec;
use smallset::SmallSet;

use parking_lot::RwLock;

struct AppState {
    database: Arc<RwLock<DataBase>>,
}

// city, country, interest, sex, status
type GroupKey = (Option<u16>, Option<u16>, Option<u16>, Sex, Status);

struct DataBase {
    accounts: BTreeMap<u32, Account>,
    interests: IndexMap<String, BTreeSet<u32>>,
    countries: IndexMap<String, BTreeSet<u32>>,
    cities: IndexMap<String, BTreeSet<u32>>,
    emails: hashbrown::HashSet<String>,
    phone_codes: IndexSet<String>,//, BTreeSet<u32>>,
    email_domains: IndexSet<String>, //, BTreeSet<u32>>,
    fnames: IndexMap<String, BTreeSet<u32>>,
    snames: IndexMap<String, BTreeSet<u32>>,

    birth_index: hashbrown::HashMap<u32, BTreeSet<u32>>,
    joined_index: hashbrown::HashMap<u32, BTreeSet<u32>>,
    snames_index: BTreeSet<String>,

    likes: hashbrown::HashMap<u32, BTreeSet<u32>>,

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
        groups.insert(vec![GroupKeys::Interests, GroupKeys::Sex, GroupKeys::Status], hashbrown::HashMap::new());
        groups.insert(vec![GroupKeys::City, GroupKeys::Country, GroupKeys::Sex, GroupKeys::Status], hashbrown::HashMap::new());
        groups.insert(vec![GroupKeys::City, GroupKeys::Interests, GroupKeys::Sex, GroupKeys::Status], hashbrown::HashMap::new());
        groups.insert(vec![GroupKeys::Country, GroupKeys::Interests, GroupKeys::Sex, GroupKeys::Status], hashbrown::HashMap::new());
        groups.insert(vec![GroupKeys::City, GroupKeys::Country, GroupKeys::Interests, GroupKeys::Sex, GroupKeys::Status], hashbrown::HashMap::new());

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
                match database.insert(account, false) {
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
                database.insert(account, false).unwrap();
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
//        database.accounts.sort_keys();

        database
    }

    fn print_len(&self) {
        println!(
            "Accs: {}, countries: {}, cities: {}, emails: {}, \
             interests: {}(cap: {}), codes: {}, doms: {}, fnames: {}, \
             snames: {}, likes: {}, liked: {}, snames_index: {}, birth_index: {}, joined_index: {}, groups_map: {}",
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
            self.groups.len(),
        );
    }

    fn subtract_from_group(&mut self, acc: &Account) {
        self.groups.entry(vec![GroupKeys::Sex, GroupKeys::Status])
            .and_modify(|index| {
                index
                    .entry(
                        (None, None, None, acc.sex.clone(), acc.status.clone())
                    ).and_modify(|v| *v -= 1);
            });
        self.groups.entry(vec![GroupKeys::City, GroupKeys::Sex, GroupKeys::Status])
            .and_modify(|index| {
                index
                    .entry(
                        (acc.city, None, None, acc.sex.clone(), acc.status.clone())
                    ).and_modify(|v| *v -= 1);
            });
        self.groups.entry(vec![GroupKeys::Country, GroupKeys::Sex, GroupKeys::Status])
            .and_modify(|index| {
                index
                    .entry(
                        (None, acc.country, None, acc.sex.clone(), acc.status.clone())
                    ).and_modify(|v| *v -= 1);
            });
        self.groups.entry(vec![GroupKeys::City, GroupKeys::Country, GroupKeys::Sex, GroupKeys::Status])
            .and_modify(|index| {
                index
                    .entry(
                        (acc.city, acc.country, None, acc.sex.clone(), acc.status.clone())
                    ).and_modify(|v| *v -= 1);
            });

        if acc.interests.is_empty() {
            self.groups.entry(vec![GroupKeys::Interests, GroupKeys::Sex, GroupKeys::Status])
                .and_modify(|index| {
                    index
                        .entry(
                            (None, None, None, acc.sex.clone(), acc.status.clone())
                        ).and_modify(|v| *v -= 1);
                });
            self.groups.entry(vec![GroupKeys::City, GroupKeys::Interests, GroupKeys::Sex, GroupKeys::Status])
                .and_modify(|index| {
                    index
                        .entry(
                            (acc.city, None, None, acc.sex.clone(), acc.status.clone())
                        ).and_modify(|v| *v -= 1);
                });
            self.groups.entry(vec![GroupKeys::Country, GroupKeys::Interests, GroupKeys::Sex, GroupKeys::Status])
                .and_modify(|index| {
                    index
                        .entry(
                            (None, acc.country, None, acc.sex.clone(), acc.status.clone())
                        ).and_modify(|v| *v -= 1);
                });
            self.groups.entry(vec![GroupKeys::City, GroupKeys::Country, GroupKeys::Interests, GroupKeys::Sex, GroupKeys::Status])
                .and_modify(|index| {
                    index
                        .entry(
                            (acc.city, acc.country, None, acc.sex.clone(), acc.status.clone())
                        ).and_modify(|v| *v -= 1);
                });
        } else {
            for interest in &acc.interests {
                self.groups.entry(vec![GroupKeys::Interests, GroupKeys::Sex, GroupKeys::Status])
                    .and_modify(|index| {
                        index
                            .entry(
                                (None, None, Some(interest.clone()), acc.sex.clone(), acc.status.clone())
                            ).and_modify(|v| *v -= 1);
                    });
                self.groups.entry(vec![GroupKeys::City, GroupKeys::Interests, GroupKeys::Sex, GroupKeys::Status])
                    .and_modify(|index| {
                        index
                            .entry(
                                (acc.city, None, Some(interest.clone()), acc.sex.clone(), acc.status.clone())
                            ).and_modify(|v| *v -= 1);
                    });
                self.groups.entry(vec![GroupKeys::Country, GroupKeys::Interests, GroupKeys::Sex, GroupKeys::Status])
                    .and_modify(|index| {
                        index
                            .entry(
                                (None, acc.country, Some(interest.clone()), acc.sex.clone(), acc.status.clone())
                            ).and_modify(|v| *v -= 1);
                    });
                self.groups.entry(vec![GroupKeys::City, GroupKeys::Country, GroupKeys::Interests, GroupKeys::Sex, GroupKeys::Status])
                    .and_modify(|index| {
                        index
                            .entry(
                                (acc.city, acc.country, Some(interest.clone()), acc.sex.clone(), acc.status.clone())
                            ).and_modify(|v| *v -= 1);
                    });
            }
        }
    }

    fn add_to_group(&mut self, acc: &Account) {
        self.groups.entry(vec![GroupKeys::Sex, GroupKeys::Status])
            .and_modify(|index|
                *index
                    .entry(
                        (None, None, None, acc.sex.clone(), acc.status.clone())
                    ).or_insert(0) += 1);
        self.groups.entry(vec![GroupKeys::City, GroupKeys::Sex, GroupKeys::Status])
            .and_modify(|index|
                *index
                    .entry(
                        (acc.city, None, None, acc.sex.clone(), acc.status.clone())
                    ).or_insert(0) += 1);
        self.groups.entry(vec![GroupKeys::Country, GroupKeys::Sex, GroupKeys::Status])
            .and_modify(|index|
                *index
                    .entry(
                        (None, acc.country, None, acc.sex.clone(), acc.status.clone())
                    ).or_insert(0) += 1);
        self.groups.entry(vec![GroupKeys::City, GroupKeys::Country, GroupKeys::Sex, GroupKeys::Status])
            .and_modify(|index|
                *index
                    .entry(
                        (acc.city, acc.country, None, acc.sex.clone(), acc.status.clone())
                    ).or_insert(0) += 1);

        if acc.interests.is_empty() {
            self.groups.entry(vec![GroupKeys::Interests, GroupKeys::Sex, GroupKeys::Status])
                .and_modify(|index|
                    *index
                        .entry(
                            (None, None, None, acc.sex.clone(), acc.status.clone())
                        ).or_insert(0) += 1);
            self.groups.entry(vec![GroupKeys::City, GroupKeys::Interests, GroupKeys::Sex, GroupKeys::Status])
                .and_modify(|index|
                    *index
                        .entry(
                            (acc.city, None, None, acc.sex.clone(), acc.status.clone())
                        ).or_insert(0) += 1);
            self.groups.entry(vec![GroupKeys::Country, GroupKeys::Interests, GroupKeys::Sex, GroupKeys::Status])
                .and_modify(|index|
                    *index
                        .entry(
                            (None, acc.country, None, acc.sex.clone(), acc.status.clone())
                        ).or_insert(0) += 1);
            self.groups.entry(vec![GroupKeys::City, GroupKeys::Country, GroupKeys::Interests, GroupKeys::Sex, GroupKeys::Status])
                .and_modify(|index|
                    *index
                        .entry(
                            (acc.city, acc.country, None, acc.sex.clone(), acc.status.clone())
                        ).or_insert(0) += 1);
        } else {
            for interest in &acc.interests {
                self.groups.entry(vec![GroupKeys::Interests, GroupKeys::Sex, GroupKeys::Status])
                    .and_modify(|index|
                        *index
                            .entry(
                                (None, None, Some(interest.clone()), acc.sex.clone(), acc.status.clone())
                            ).or_insert(0) += 1);
                self.groups.entry(vec![GroupKeys::City, GroupKeys::Interests, GroupKeys::Sex, GroupKeys::Status])
                    .and_modify(|index|
                        *index
                            .entry(
                                (acc.city, None, Some(interest.clone()), acc.sex.clone(), acc.status.clone())
                            ).or_insert(0) += 1);
                self.groups.entry(vec![GroupKeys::Country, GroupKeys::Interests, GroupKeys::Sex, GroupKeys::Status])
                    .and_modify(|index|
                        *index
                            .entry(
                                (None, acc.country, Some(interest.clone()), acc.sex.clone(), acc.status.clone())
                            ).or_insert(0) += 1);
                self.groups.entry(vec![GroupKeys::City, GroupKeys::Country, GroupKeys::Interests, GroupKeys::Sex, GroupKeys::Status])
                    .and_modify(|index|
                        *index
                            .entry(
                                (acc.city, acc.country, Some(interest.clone()), acc.sex.clone(), acc.status.clone())
                            ).or_insert(0) += 1);
            }
        }
    }

    fn insert_to_domains(&mut self, mail: String, uid: u32) -> u16 {
        let vec: Vec<&str> = mail.split('@').collect();
        if let Some(domain) = vec.get(1) {
            self.email_domains.insert_full(domain.to_string()).0 as u16
//                .entry(domain.to_string())
//                .or_insert(BTreeSet::new())
//                .insert(uid);
//            self.email_domains.get_full(&domain.to_string()).unwrap().0 as u16
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

    fn update_account(&mut self, uid: u32, account: AccountOptional) -> Result<u32, StatusCode> {
        let mut new_account = match self.accounts.get_mut(&uid) {
            Some(account) => account,
            None => return Err(StatusCode::NOT_FOUND),
        };
        let old_account = new_account.clone();
        let new_account = {
            if let Some(email) = account.email {
                if self.emails.contains(&email) {
                    if email != new_account.email {
                        return Err(StatusCode::BAD_REQUEST);
                    }
                } else {
                    let vec: Vec<&str> = email.split('@').collect();
                    new_account.email_domain = match vec.get(1) {
                        Some(domain) => {
//                            if let Some((_, v)) = self.email_domains.get_index_mut(new_account.email_domain as usize)
//                                {
//                                    v.remove(&uid);
//                                }
                            self.email_domains.insert_full(domain.to_string()).0 as u16
//                                .entry(domain.to_string())
//                                .or_insert(BTreeSet::new())
//                                .insert(uid);
//                            self.email_domains.get_full(&domain.to_string()).unwrap().0 as u16
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
//                            .entry(phone_code.to_string())
//                            .or_insert(BTreeSet::new())
//                            .insert(uid);
//                        self.phone_codes
//                            .get_full(&phone_code.to_string())
//                            .unwrap()
//                            .0 as u16
                    }),
                    None => None,
                };
                new_account.phone = Some(phone);
            };

            if let Some(birth) = account.birth {
                let year = Utc.timestamp(birth as i64, 0).year() as u32;
                self.birth_index
                    .entry(year)
                    .and_modify(|v| { v.remove(&uid); });
                self.birth_index
                    .entry(year)
                    .or_insert(BTreeSet::new())
                    .insert(uid);
                new_account.birth = birth;
            };

            if let Some(joined) = account.joined {
                let year = Utc.timestamp(joined  as i64, 0).year() as u32;
                self.joined_index
                    .entry(year)
                    .and_modify(|v| { v.remove(&uid); });
                self.joined_index
                    .entry(year)
                    .or_insert(BTreeSet::new())
                    .insert(uid);
                new_account.joined = joined;
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
                for like in likes {
                    self.likes.entry(like.id)
                        .or_insert(BTreeSet::new())
                        .insert(uid);
                self.liked.entry(uid)
                    .or_insert(Vec::new())
                    .push(like.id);
                }
//            new_account.likes.extend(likes);
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
            new_account.clone()
        };

        self.subtract_from_group(&old_account);
        self.add_to_group(&new_account);
        Ok(uid)
    }

    fn insert(&mut self, account: AccountFull, with_unique: bool) -> Result<u32, StatusCode> {
        if self.post_phase == 0 {
            self.post_phase = 1;
        }

        let uid = account.id;

        if with_unique {
            if let true = self.accounts.contains_key(&uid) {
                return Err(StatusCode::BAD_REQUEST);
            };

            if self.emails.contains(&account.email) {
                return Err(StatusCode::BAD_REQUEST);
            };
        }

        if let Some(likes) = account.likes {
            for like in likes {
                self.likes.entry(like.id)
                    .or_insert(BTreeSet::new())
                    .insert(uid);
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
//                        .entry(phone_code.to_string())
//                        .or_insert(BTreeSet::new())
//                        .insert(uid);
//                    self.phone_codes
//                        .get_full(&phone_code.to_string())
//                        .unwrap()
//                        .0 as u16
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

        let year = Utc.timestamp(account.birth as i64, 0).year() as u32;
        self.birth_index
            .entry(year)
            .or_insert(BTreeSet::new())
            .insert(uid);

        let year = Utc.timestamp(account.joined as i64, 0).year() as u32;
        self.joined_index
            .entry(year)
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
        self.add_to_group(&new_account);
        self.accounts.insert(
            uid,
            new_account,
        );

        Ok(uid)
    }

    fn update_likes(&mut self, likes: LikesRequest) -> Result<(), StatusCode> {
        for like in likes.likes.iter() {
            if !self.accounts.contains_key(&like.likee) || !self.accounts.contains_key(&like.liker) {
                return Err(StatusCode::BAD_REQUEST);
            }
        }

        for like in likes.likes.iter() {
            self.likes.entry(like.likee)
                .or_insert(BTreeSet::new())
                .insert(like.liker);
            self.liked.entry(like.liker)
                .or_insert(Vec::new())
                .push(like.likee);
        }

        Ok(())
    }

    fn filter(&self, filters: Filters) -> Result<serde_json::Value, ()> {
//        let end_ids: Vec<(&u32, &Account)> = Vec::with_capacity(filters.limit);
        let end_ids: BTreeMap<&u32, &Account> = {
            // ФИЛЬТРЫ ЧЕРЕЗ ИНДЕКСЫ

            let mut indexes = SmallVec::<[&BTreeSet<u32>; 4]>::new();
            let mut indexes_options = Vec::new();
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

            for (filter, index, n) in [
                (&filters.sname_eq, &self.snames, 0),
                (&filters.city_eq, &self.cities, 1),
                (&filters.fname_eq, &self.fnames, 2),
                (&filters.country_eq, &self.countries, 4),
            ].iter() {
                if let Some(val) = filter {
                    if indexes.is_empty() {
                        if let Some(res) = index.get(val) {
                            indexes.push(res);
                        } else {
                            return Ok(json!({"accounts": []}));
                        }
                    } else {
                        match n {
                            1 => {
                                if let Some((number, _, _)) = index.get_full(val) {
                                    let number = Some(number as u16);
                                    filters_fns.push(Box::new(move |acc: &Account| acc.city == number));
                                } else {
                                    return Ok(json!({"accounts": []}));
                                }
                            }
                            2 => {
                                if let Some((number, _, _)) = index.get_full(val) {
                                    let number = Some(number as u16);
                                    filters_fns.push(Box::new(move |acc: &Account| acc.fname == number));
                                } else {
                                    return Ok(json!({"accounts": []}));
                                }
                            }
                            4 => {
                                if let Some((number, _, _)) = index.get_full(val) {
                                    let number = Some(number as u16);
                                    filters_fns.push(Box::new(move |acc: &Account| acc.country == number));
                                } else {
                                    return Ok(json!({"accounts": []}));
                                }
                            }
                            _ => ()
                        }
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
                let mut res = vec![];
                for elem in val.split(',') {
                    let liked_id = match elem.parse::<u32>() {
                        Ok(r) => r,
                        Err(_) => return Err(())
                    };
                    if let Some(likes) = self.likes.get(&liked_id) {
                        res.push(likes);
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

            match &filters.country_null {
                Some(1) => {
                    filters_fns.push(Box::new(move |acc: &Account| acc.country.is_none()));
                }
                Some(0) => {
                    filters_fns.push(Box::new(move |acc: &Account| acc.country.is_some()));
                }
                _ => (),
            }

            match &filters.city_null {
                Some(1) => {
                    filters_fns.push(Box::new(move |acc: &Account| acc.city.is_none()));
                }
                Some(0) => {
                    filters_fns.push(Box::new(move |acc: &Account| acc.city.is_some()));
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

            // ФИЛЬТРАЦИЯ
            {
                if let Some(min) = indexes.pop() {
                    min
                        .iter()
                        .rev()
                        .filter(|uid| indexes.iter().all(|i| i.contains(uid)))
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
        Ok(json!({ "accounts": result }))
    }

    fn suggest(&self, uid: u32, suggest_filters: SuggestRecommend) -> Result<serde_json::Value, StatusCode> {
        let new_account = match self.accounts.get(&uid) {
            Some(account) => account,
            None => return Err(StatusCode::NOT_FOUND),
        };
        Ok(json!({"accounts": []}))
    }

    fn recommend(&self, uid: u32, suggest_filters: SuggestRecommend) -> Result<serde_json::Value, StatusCode> {
        let new_account = match self.accounts.get(&uid) {
            Some(account) => account,
            None => return Err(StatusCode::NOT_FOUND),
        };

        Ok(json!({"accounts": []}))
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

        let mut group_keys = vec![];
        let mut filters_fns: Vec<Box<Fn(&GroupKey) -> bool>> = vec![];

        if keys.contains(&GroupKeys::City) || group_filters.city.is_some() {
            group_keys.push(GroupKeys::City);
        }
        if keys.contains(&GroupKeys::Country) || group_filters.country.is_some() {
            group_keys.push(GroupKeys::Country);
        }
        if keys.contains(&GroupKeys::Interests) || group_filters.interests.is_some() {
            group_keys.push(GroupKeys::Interests);
        }
        group_keys.push(GroupKeys::Sex);
        group_keys.push(GroupKeys::Status);

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

        let mut result_groups = Vec::with_capacity(group_filters.limit);

        if let Some(group) = self.groups.get(&group_keys) {
            for (entity, count) in group
                .iter()
                .filter(|(key, _)| filters_fns.iter().all(|f| f(&key)))
                .sorted_by(|(entity_a, _), (entity_b, _)|
                    ((
                        if keys.contains(&GroupKeys::City) { entity_a.0 } else { None },
                        if keys.contains(&GroupKeys::Country) { entity_a.1 } else { None },
                        if keys.contains(&GroupKeys::Interests) { entity_a.2 } else { None },
                        if keys.contains(&GroupKeys::Sex) { Some(&entity_a.3) } else { None },
                        if keys.contains(&GroupKeys::Status) { Some(&entity_a.4) } else { None },
                    )).cmp(&(
                        if keys.contains(&GroupKeys::City) { entity_b.0 } else { None },
                        if keys.contains(&GroupKeys::Country) { entity_b.1 } else { None },
                        if keys.contains(&GroupKeys::Interests) { entity_b.2 } else { None },
                        if keys.contains(&GroupKeys::Sex) { Some(&entity_b.3) } else { None },
                        if keys.contains(&GroupKeys::Status) { Some(&entity_b.4) } else { None },
                    ))
                )
                .group_by(|&(ent, _)| {
                    (
                        if keys.contains(&GroupKeys::City) { ent.0 } else { None },
                        if keys.contains(&GroupKeys::Country) { ent.1 } else { None },
                        if keys.contains(&GroupKeys::Interests) { ent.2 } else { None },
                        if keys.contains(&GroupKeys::Sex) { Some(&ent.3) } else { None },
                        if keys.contains(&GroupKeys::Status) { Some(&ent.4) } else { None },
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
                            .then((if keys.contains(&GroupKeys::Sex) { Some(&entity_a.3) } else { None })
                                .cmp(&(if keys.contains(&GroupKeys::Sex) { Some(&entity_b.3) } else { None })))
                            .then((if keys.contains(&GroupKeys::Status) { Some(&entity_a.4) } else { None })
                                .cmp(&(if keys.contains(&GroupKeys::Status) { Some(&entity_b.4) } else { None })))
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
                            .then((if keys.contains(&GroupKeys::Sex) { Some(&entity_a.3) } else { None })
                                .cmp(&(if keys.contains(&GroupKeys::Sex) { Some(&entity_b.3) } else { None })).reverse())
                            .then((if keys.contains(&GroupKeys::Status) { Some(&entity_a.4) } else { None })
                                .cmp(&(if keys.contains(&GroupKeys::Status) { Some(&entity_b.4) } else { None })).reverse()),
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
            return Ok(json!({"groups": "nonee"}));
        }

        Ok(json!({"groups": result_groups}))
    }

//    fn group_(&self, group_filters: Group) -> Result<serde_json::Value, StatusCode> {
//        let mut keys: SmallSet<[GroupKeys; 5]> = SmallSet::new();
//        let variants: HashMap<&str, GroupKeys> = [
//            ("sex", GroupKeys::Sex),
//            ("status", GroupKeys::Status),
//            ("interests", GroupKeys::Interests),
//            ("country", GroupKeys::Country),
//            ("city", GroupKeys::City),
//        ].iter().cloned().collect();
//
//        for key in group_filters.keys.split(',') {
//            match variants.get(key) {
//                Some(group_key) => { keys.insert(group_key.clone()); }
//                None => { return Err(StatusCode::BAD_REQUEST); }
//            }
//        }
//
//        let mut indexes = Vec::new();
//        let mut filters_fns: Vec<Box<Fn(&Account) -> bool>> = vec![];
//
//        for (filter, index, n) in [
//            (&group_filters.sname, &self.snames, 0),
//            (&group_filters.city, &self.cities, 1),
//            (&group_filters.fname, &self.fnames, 2),
//            (&group_filters.interests, &self.interests, 3),
//            (&group_filters.country, &self.countries, 4),
//        ].iter() {
//            if let Some(val) = filter {
//                if indexes.is_empty() {
//                    if let Some(res) = index.get(val) {
//                        indexes.push(res);
//                    } else {
//                        return Ok(json!({"groups": []}));
//                    }
//                } else {
//                    match n {
//                        1 => {
//                            if let Some((number, _, _)) = index.get_full(val) {
//                                filters_fns.push(Box::new(move |acc: &Account| acc.city == Some(number)));
//                            } else {
//                                return Ok(json!({"groups": []}));
//                            }
//                        }
//                        2 => {
//                            if let Some((number, _, _)) = index.get_full(val) {
//                                filters_fns.push(Box::new(move |acc: &Account| acc.fname == Some(number)));
//                            } else {
//                                return Ok(json!({"groups": []}));
//                            }
//                        }
//                        3 => {
//                            if let Some(res) = index.get(val) {
//                                indexes.push(res);
//                            } else {
//                                return Ok(json!({"groups": []}));
//                            }
//                        }
//                        4 => {
//                            if let Some((number, _, _)) = index.get_full(val) {
//                                filters_fns.push(Box::new(move |acc: &Account| acc.country == Some(number)));
//                            } else {
//                                return Ok(json!({"groups": []}));
//                            }
//                        }
//                        _ => ()
//                    }
//                }
//            }
//        }
//
//        for (filter, index) in [
//            (&group_filters.birth, &self.birth_index),
//            (&group_filters.joined, &self.joined_index),
//        ].iter() {
//            if let Some(val) = filter {
//                if let Some(res) = index.get(val) {
//                    indexes.push(res);
//                } else {
//                    return Ok(json!({"groups": []}));
//                }
//            }
//        }
//
//        if let Some(val) = &group_filters.likes {
//            if let Some(res) = self.likes.get(val) {
//                indexes.push(res);
//            } else {
//                return Ok(json!({"groups": []}));
//            }
//        }
//
//        return Ok(json!({"groups": []}));
//
//        if let Some(val) = &group_filters.status {
//            filters_fns.push(Box::new(move |acc: &Account| &acc.status == val));
//        }
//
//        if let Some(val) = &group_filters.sex {
//            filters_fns.push(Box::new(move |acc: &Account| &acc.sex == val));
//        }
//
//        indexes.sort_by_key(|i| -(i.len() as isize));
//
//        let mut result = hashbrown::HashMap::new();
//
//        let city_key_func: Box<fn(&Account) -> &Option<usize>> = {
//            if keys.contains(&GroupKeys::City) {
//                Box::new(move |acc: &Account| &(acc.city as Option<usize>))
//            } else {
//                Box::new(move |_| &None)
//            }
//        };
//
//        let country_key_func: Box<fn(&Account) -> &Option<usize>> = {
//            if keys.contains(&GroupKeys::Country) {
//                Box::new(move |acc: &Account| &(acc.country as Option<usize>))
//            } else {
//                Box::new(move |_| &None)
//            }
//        };
//
//        let sex_key_func: Box<fn(&Account) -> Option<&Sex>> = {
//            if keys.contains(&GroupKeys::Sex) {
//                Box::new(move |acc: &Account| Some(&acc.sex))
//            } else {
//                Box::new(move |_| None)
//            }
//        };
//
//        let status_key_func: Box<fn(&Account) -> Option<&Status>> = {
//            if keys.contains(&GroupKeys::Status) {
//                Box::new(move |acc: &Account| Some(&acc.status))
//            } else {
//                Box::new(move |_| None)
//            }
//        };
//
//        let has_interests = if keys.contains(&GroupKeys::Interests) { true } else { false };
//
//        if let Some(index) = indexes.pop() {
//            for (_, acc) in index
//                .iter()
//                .filter(|uid| indexes.iter().all(|i| i.contains(uid)))
//                .map(|k| (k, self.accounts.get(k).unwrap()))
//                .filter(|(_, acc)| filters_fns.iter().all(|f| f(&acc)))
//                {
//                    if has_interests {
//                        for interest in &acc.interests {
//                            *result.entry((
//                                city_key_func(acc),
//                                country_key_func(acc),
//                                Some(interest),
//                                sex_key_func(acc),
//                                status_key_func(acc),
//                            )).or_insert(0) += 1;
//                        }
//                    } else {
//                        *result.entry((
//                            city_key_func(acc),
//                            country_key_func(acc),
//                            None,
//                            sex_key_func(acc),
//                            status_key_func(acc),
//                        )).or_insert(0) += 1;
//                    }
//                };
//        } else {
//            return Ok(json!({"groups": []}));
//            for (_, acc) in self.accounts
//                .iter()
//                .filter(|(_, acc)| filters_fns.iter().all(|f| f(&acc)))
//                {
//                    if keys.contains(&GroupKeys::Interests) {
//                        for interest in &acc.interests {
//                            let key = (
//                                if keys.contains(&GroupKeys::City) { &acc.city } else { &None },
//                                if keys.contains(&GroupKeys::Country) { &acc.country } else { &None },
//                                Some(interest),
//                                if keys.contains(&GroupKeys::Sex) { Some(&acc.sex) } else { None },
//                                if keys.contains(&GroupKeys::Status) { Some(&acc.status) } else { None },
//                            );
//                            *result.entry(key).or_insert(0) += 1;
//                        }
//                    } else {
//                        let key = (
//                            if keys.contains(&GroupKeys::City) { &acc.city } else { &None },
//                            if keys.contains(&GroupKeys::Country) { &acc.country } else { &None },
//                            None,
//                            if keys.contains(&GroupKeys::Sex) { Some(&acc.sex) } else { None },
//                            if keys.contains(&GroupKeys::Status) { Some(&acc.status) } else { None },
//                        );
//                        *result.entry(key).or_insert(0) += 1;
//                    }
//                };
//        }
//        let mut result_groups = Vec::with_capacity(group_filters.limit);
//
//        for (entity, count) in result
//            .iter()
//            .sorted_by(|(entity_a, count_a), (entity_b, count_b)| match group_filters.order {
//                Order::Asc =>
//                    count_a.cmp(count_b)
//                        .then((if keys.contains(&GroupKeys::City) && entity_a.0.is_some() {
//                            Some(self.cities.get_index(entity_a.0.unwrap()).unwrap().0)
//                        } else { None })
//                            .cmp(&(if keys.contains(&GroupKeys::City) && entity_b.0.is_some() {
//                                Some(self.cities.get_index(entity_b.0.unwrap()).unwrap().0)
//                            } else { None })))
//                        .then((if keys.contains(&GroupKeys::Country) && entity_a.1.is_some() {
//                            Some(self.countries.get_index(entity_a.1.unwrap()).unwrap().0)
//                        } else { None })
//                            .cmp(&(if keys.contains(&GroupKeys::Country) && entity_b.1.is_some() {
//                                Some(self.countries.get_index(entity_b.1.unwrap()).unwrap().0)
//                            } else { None })))
//                        .then((if keys.contains(&GroupKeys::Interests) && entity_a.2.is_some() {
//                            Some(self.interests.get_index(*entity_a.2.unwrap()).unwrap().0)
//                        } else { None })
//                            .cmp(&(if keys.contains(&GroupKeys::Interests) && entity_b.2.is_some() {
//                                Some(self.interests.get_index(*entity_b.2.unwrap()).unwrap().0)
//                            } else { None })))
//                        .then((if keys.contains(&GroupKeys::Sex) { entity_a.3 } else { None })
//                            .cmp(&(if keys.contains(&GroupKeys::Sex) { entity_b.3 } else { None })))
//                        .then((if keys.contains(&GroupKeys::Status) { entity_a.4 } else { None })
//                            .cmp(&(if keys.contains(&GroupKeys::Status) { entity_b.4 } else { None })))
//                ,
//                Order::Desc =>
//                    count_a.cmp(count_b).reverse()
//                        .then((if keys.contains(&GroupKeys::City) && entity_a.0.is_some() {
//                            Some(self.cities.get_index(entity_a.0.unwrap()).unwrap().0)
//                        } else { None })
//                            .cmp(&(if keys.contains(&GroupKeys::City) && entity_b.0.is_some() {
//                                Some(self.cities.get_index(entity_b.0.unwrap()).unwrap().0)
//                            } else { None })).reverse())
//                        .then((if keys.contains(&GroupKeys::Country) && entity_a.1.is_some() {
//                            Some(self.countries.get_index(entity_a.1.unwrap()).unwrap().0)
//                        } else { None })
//                            .cmp(&(if keys.contains(&GroupKeys::Country) && entity_b.1.is_some() {
//                                Some(self.countries.get_index(entity_b.1.unwrap()).unwrap().0)
//                            } else { None })).reverse())
//                        .then((if keys.contains(&GroupKeys::Interests) && entity_a.2.is_some() {
//                            Some(self.interests.get_index(*entity_a.2.unwrap()).unwrap().0)
//                        } else { None })
//                            .cmp(&(if keys.contains(&GroupKeys::Interests) && entity_b.2.is_some() {
//                                Some(self.interests.get_index(*entity_b.2.unwrap()).unwrap().0)
//                            } else { None })).reverse())
//                        .then((if keys.contains(&GroupKeys::Sex) { entity_a.3 } else { None })
//                            .cmp(&(if keys.contains(&GroupKeys::Sex) { entity_b.3 } else { None })).reverse())
//                        .then((if keys.contains(&GroupKeys::Status) { entity_a.4 } else { None })
//                            .cmp(&(if keys.contains(&GroupKeys::Status) { entity_b.4 } else { None })).reverse()),
//            })
//            .take(group_filters.limit)
//            {
//                let mut group = HashMap::new();
//                group.insert("count", json!(count));
//                if keys.contains(&GroupKeys::City) {
//                    if let Some(val) = entity.0 {
//                        group.insert("city", json!(self.cities.get_index(*val).unwrap().0));
//                    } else {
//                        group.insert("city", serde_json::Value::Null);
//                    }
//                }
//                if keys.contains(&GroupKeys::Country) {
//                    if let Some(val) = entity.1 {
//                        group.insert("country", json!(self.countries.get_index(*val).unwrap().0));
//                    } else {
//                        group.insert("country", serde_json::Value::Null);
//                    }
//                }
//                if keys.contains(&GroupKeys::Interests) {
//                    if let Some(val) = entity.2 {
//                        group.insert("interests", json!(self.interests.get_index(*val).unwrap().0));
//                    } else {
//                        group.insert("interests", serde_json::Value::Null);
//                    }
//                }
//                if keys.contains(&GroupKeys::Sex) {
//                    group.insert("sex", json!(entity.3));
//                }
//                if keys.contains(&GroupKeys::Status) {
//                    group.insert("status", json!(entity.4));
//                }
//
//                result_groups.push(group);
//            }
//
//        Ok(json!({"groups": result_groups}))
//    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
enum Sex {
    #[serde(rename = "m")]
    M,
    #[serde(rename = "f")]
    F,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
enum Status {
    #[serde(rename = "заняты")]
    Muted,
    #[serde(rename = "свободны")]
    Free,
    #[serde(rename = "всё сложно")]
    AllHard,
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
    //    likes: Vec<Likes>,
}

#[serde(deny_unknown_fields)]
#[derive(Debug, Validate, Serialize, Deserialize)]
struct AccountFull {
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
    ts: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LikerLikee {
    liker: u32,
    likee: u32,
    ts: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LikesRequest {
    likes: Vec<LikerLikee>,
}

#[serde(deny_unknown_fields)]
#[derive(Debug, Validate, Serialize, Deserialize)]
struct Filters {
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
    limit: usize,
    order: Order,
    #[validate(contains = "@")]
//    #[validate(length(min = "1", max = "100"))]
//    email: Option<String>,
    #[validate(length(min = "1", max = "50"))]
    fname: Option<String>,
    #[validate(length(min = "1", max = "50"))]
    sname: Option<String>,
    //    #[validate(length(min = "1", max = "16"))]
//    phone: Option<String>,
    sex: Option<Sex>,
    birth: Option<i32>,
    #[validate(length(min = "1", max = "50"))]
    country: Option<String>,
    #[validate(length(min = "1", max = "50"))]
    city: Option<String>,
    joined: Option<i32>,
    status: Option<Status>,
    #[validate(length(min = "1", max = "100"))]
    interests: Option<String>,
    //    premium: Option<u8>,
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
}

#[serde(deny_unknown_fields)]
#[derive(Debug, Validate, Serialize, Deserialize)]
struct SuggestRecommend {
    query_id: Option<i64>,
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

fn likes(item: Json<LikesRequest>, state: State<AppState>) -> HttpResponse {
    let mut database = state.database.write();
    match database.update_likes(item.0) {
        Ok(_) => HttpResponse::Accepted()
            .content_type("application/json")
            .body("{}"),
        Err(status) => HttpResponse::build(status).content_type("application/json")
            .header(header::CONNECTION, "keep-alive")
            .header(header::SERVER, "highload")
            .finish(),
    }
}

fn update(item: Json<AccountOptional>, path: Path<(u32, )>, state: State<AppState>) -> HttpResponse {
    match item.0.validate() {
        Ok(_) => {
            let mut database = state.database.write();
            match database.update_account(path.0, item.0) {
                Ok(_) => HttpResponse::Accepted()
                    .content_type("application/json")
                    .body("{}"),
                Err(status) => HttpResponse::build(status).content_type("application/json")
                    .header(header::CONNECTION, "keep-alive")
                    .header(header::SERVER, "highload")
                    .finish(),
            }
        }
        Err(_) => HttpResponse::BadRequest().content_type("application/json")
            .header(header::CONNECTION, "keep-alive")
            .header(header::SERVER, "highload")
            .finish(),
    }
}

fn new(item: Json<AccountFull>, state: State<AppState>) -> HttpResponse {
    match item.0.validate() {
        Ok(_) => {
            let mut database = state.database.write();
            match database.insert(item.0, true) {
                Ok(_) => HttpResponse::Created()
                    .content_type("application/json")
                    .body("{}"),
                Err(status) => HttpResponse::build(status).content_type("application/json")
                    .header(header::CONNECTION, "keep-alive")
                    .header(header::SERVER, "highload")
                    .finish(),
            }
        }
        Err(_) => HttpResponse::BadRequest().content_type("application/json")
            .header(header::CONNECTION, "keep-alive")
            .header(header::SERVER, "highload")
            .finish(),
    }
}

//fn mmain() {
////    let mut coll: hashbrown::HashMap<u32, BTreeSet<u32>> = hashbrown::HashMap::new(); // 148M
////    let mut coll: FnvHashMap<u32, BTreeSet<u32>> = FnvHashMap::default(); // 162M
////    let mut coll: IndexMap<u32, BTreeSet<u32>> = IndexMap::new(); // 148M
////    let mut coll: hashbrown::HashMap<u32, hashbrown::HashSet<u32>> = hashbrown::HashMap::new(); // 221M
////    let mut coll: hashbrown::HashMap<u32, Vec<u32>> = hashbrown::HashMap::new(); // 128M
//    let mut coll: IndexMap<u32, Vec<u32>> = IndexMap::new(); // 127M
////    let mut coll: hashbrown::HashMap<u32, Vec<u32>> = hashbrown::HashMap::new(); // 128M
////    let mut coll: IndexMap<u32, Vec<(u32, i64)>> = IndexMap::new(); // 270M
//
//    for i in 0..1300000 {
//        coll.insert(i, [1,2,3,4,5,6,7,8,9,10].iter().cloned().collect());
//    }
//    println!("Done");
//    thread::sleep(Duration::from_secs(30));
//    println!("Ended");
//}

fn main() {
    let prod = env::var("PROD").is_ok();
    let addr = if prod { "0.0.0.0:80" } else { "0.0.0.0:8010" };
    let data_file = if prod {
        "/tmp/data/data.zip"
    } else {
        "data 2.zip"
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


// interests - 86
// accs - 404
