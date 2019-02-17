use validator::Validate;


#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub enum Sex {
    #[serde(rename = "f")]
    F,
    #[serde(rename = "m")]
    M,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub enum Status {
    #[serde(rename = "всё сложно")]
    AllHard,
    #[serde(rename = "заняты")]
    Muted,
    #[serde(rename = "свободны")]
    Free,
}

#[repr(C)]
#[derive(Debug, Clone)]
pub struct Account {
    pub birth: u32,
    pub joined: u32,
    pub email_domain: u16,
    pub fname: Option<u16>,
    pub sname: Option<u16>,
    pub country: Option<u16>,
    pub city: Option<u16>,
    pub phone_code: Option<u16>,
    pub status: Status,
    pub sex: Sex,
    pub email: String,
    pub phone: Option<String>,
    pub premium: Option<Premium>,
    pub interests: Vec<u16>,
}

#[serde(deny_unknown_fields)]
#[derive(Debug, Validate, Serialize, Deserialize)]
pub struct AccountFull {
    #[validate(range(min = "1", max = "4294967295"))]
    pub id: u32,
    pub sex: Sex,
    pub status: Status,
    pub birth: u32,
    pub joined: u32,
    pub premium: Option<Premium>,
    #[validate(contains = "@")]
    #[validate(length(min = "1", max = "100"))]
    pub email: String,
    #[validate(length(min = "1", max = "50"))]
    pub fname: Option<String>,
    #[validate(length(min = "1", max = "50"))]
    pub sname: Option<String>,
    #[validate(length(min = "1", max = "16"))]
    pub phone: Option<String>,
    #[validate(length(min = "1", max = "50"))]
    pub country: Option<String>,
    #[validate(length(min = "1", max = "50"))]
    pub city: Option<String>,
    pub interests: Option<Vec<String>>,
    pub likes: Option<Vec<Likes>>,
}

#[serde(deny_unknown_fields)]
#[derive(Debug, Validate, Serialize, Deserialize)]
pub struct AccountOptional {
    #[validate(contains = "@")]
    #[validate(length(min = "1", max = "100"))]
    pub email: Option<String>,
    #[validate(length(min = "1", max = "50"))]
    pub fname: Option<String>,
    #[validate(length(min = "1", max = "50"))]
    pub sname: Option<String>,
    #[validate(length(min = "1", max = "16"))]
    pub phone: Option<String>,
    pub sex: Option<Sex>,
    pub birth: Option<u32>,
    #[validate(length(min = "1", max = "50"))]
    pub country: Option<String>,
    #[validate(length(min = "1", max = "50"))]
    pub city: Option<String>,
    pub joined: Option<u32>,
    pub status: Option<Status>,
    pub interests: Option<Vec<String>>,
    pub premium: Option<Premium>,
    pub likes: Option<Vec<Likes>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Premium {
    pub start: u32,
    pub finish: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Likes {
    pub id: u32,
    pub ts: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LikerLikee {
    pub liker: u32,
    pub likee: u32,
    pub ts: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LikesRequest {
    pub likes: Vec<LikerLikee>,
}

#[serde(deny_unknown_fields)]
#[derive(Debug, Validate, Serialize, Deserialize)]
pub struct Filters {
    #[validate(range(min = "1", max = "4294967295"))]
    pub limit: usize,
    pub sex_eq: Option<Sex>,
    pub email_domain: Option<String>,
    pub email_lt: Option<String>,
    pub email_gt: Option<String>,
    pub status_eq: Option<Status>,
    pub status_neq: Option<Status>,
    pub fname_eq: Option<String>,
    pub fname_any: Option<String>,
    #[validate(range(min = "0", max = "1"))]
    pub fname_null: Option<u8>,
    pub sname_eq: Option<String>,
    pub sname_starts: Option<String>,
    #[validate(range(min = "0", max = "1"))]
    pub sname_null: Option<u8>,
    pub phone_code: Option<String>,
    #[validate(range(min = "0", max = "1"))]
    pub phone_null: Option<u8>,
    pub country_eq: Option<String>,
    #[validate(range(min = "0", max = "1"))]
    pub country_null: Option<u8>,
    pub city_eq: Option<String>,
    pub city_any: Option<String>,
    #[validate(range(min = "0", max = "1"))]
    pub city_null: Option<u8>,
    pub birth_lt: Option<u32>,
    pub birth_gt: Option<u32>,
    pub birth_year: Option<u32>,
    pub interests_contains: Option<String>,
    pub interests_any: Option<String>,
    pub likes_contains: Option<String>,
    pub premium_now: Option<i64>,
    #[validate(range(min = "0", max = "1"))]
    pub premium_null: Option<u8>,
    pub query_id: Option<i64>,
}


#[serde(deny_unknown_fields)]
#[derive(Debug, Validate, Serialize, Deserialize)]
pub struct Group {
    pub query_id: Option<i64>,
    pub keys: String,
    #[validate(range(min = "1", max = "4294967295"))]
    pub limit: usize,
    pub order: Order,
    #[validate(contains = "@")]
    #[validate(length(min = "1", max = "50"))]
    pub fname: Option<String>,
    #[validate(length(min = "1", max = "50"))]
    pub sname: Option<String>,
    pub sex: Option<Sex>,
    pub birth: Option<u32>,
    #[validate(length(min = "1", max = "50"))]
    pub country: Option<String>,
    #[validate(length(min = "1", max = "50"))]
    pub city: Option<String>,
    pub joined: Option<u32>,
    pub status: Option<Status>,
    #[validate(length(min = "1", max = "100"))]
    pub interests: Option<String>,
    pub likes: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Order {
    #[serde(rename = "1")]
    Asc,
    #[serde(rename = "-1")]
    Desc,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum GroupKeys {
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
pub struct SuggestRecommend {
    pub query_id: Option<i64>,
    #[validate(range(min = "1", max = "4294967295"))]
    pub limit: usize,
    #[validate(length(min = "1", max = "50"))]
    pub country: Option<String>,
    #[validate(length(min = "1", max = "50"))]
    pub city: Option<String>,
}

