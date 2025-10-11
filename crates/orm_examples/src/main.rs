use orm::Model;

pub struct Item<T> {
    pub id: T,
    pub name: String,
}

#[derive(Model)]
#[orm(fn get_name(id: i32) -> (String,) { "SELECT name from users WHERE name = $1" })]
#[orm(fn get_by_name(name: &str) { "WHERE name = $1" })]
#[orm(fn list_adults() { "WHERE age >= 18" })]
#[orm(fn list_ages() -> (i32,) { "SELECT age from users" })]
#[orm(fn list_items() -> Item::<i32> { "SELECT id, name from users" })]
pub struct User {
    pub id: i32,
    pub age: i32,
    pub name: String,
}

fn main() {}
