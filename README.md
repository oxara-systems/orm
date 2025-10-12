# orm (real name TBD)

Simple, opinionated ORM around sqlx. Inspired by [ormlite](https://github.com/kurtbuilds/ormlite).

- Only supports PostgreSQL at the moment
  - Feel free to submit a PR that adds support for other sqlx backends
- This is being released so I can reuse it in other projects
  - Breaking changes may occur
  - Semantic versioning may not be properly followed
  - This won't be published to crates.io for now

# Example

```sql
CREATE TABLE users (
    id UUID PRIMARY KEY,
    age INTEGER NOT NULL,
    name TEXT NOT NULL
);
```

```rust
use orm::Model;
use sqlx::prelude::Type;
use uuid::Uuid;

#[derive(Type)]
#[sqlx(transparent)]
pub struct UserId(pub Uuid);

// define a model on the `users` table, along with several generated functions
// - the type of generated function is based on it's prefix
//   - get_* returns a Option<T>
//   - list_* returns a Vec<T>
// - specifying the return type allows you to use a custom query
//   - the return type still gets wrapped in `Option` or `Vec` depending on the prefix
//   - sqlx specific things like type overrides need to be handled manually:
//     - SEE https://docs.rs/sqlx/latest/sqlx/macro.query.html#type-overrides-output-columns
//     - {column_names_typed} can be used in custom queries, it will be replaced with a comma separated list of typed column names
#[derive(Model)]
#[orm(fn get_by_name(name: &str) { "WHERE name = $1" })]
#[orm(fn list_adults() { "WHERE age >= 18" })]
#[orm(fn list_adult_names() -> (String,) { r#"SELECT name as "name: _" WHERE age >= 18"# })]
pub struct User {
    pub id: UserId,
    pub age: i32,
    pub name: String,
}

#[tokio::main]
async fn main() {
    let db = orm::Db::connect("postgres://localhost/database").await?;

    // start a writable transaction
    // returning an error will rollback the transaction
    db.write(async |tx| {
        let id = UserId(Uuid::new_v4());

        // create a user
        let user = User {
            id,
            age: 20,
            name: "Test",
        }.insert(tx).await?;

        // get a user by primary key
        if let Some(mut user) = User::get(tx, &id).await? {
            // update the user
            user.age += 1;
            let user = user.update(tx).await?;

            // delete the user
            user.delete(tx).await?;
        }

        Ok(())
    })
    .await?;

    // start a read only transaction
    // - orm_macro generated functions that take a WritableTransaction can't be called here
    // - PostgreSQL should throw an error if you attempt to write in here
    db.read(async |tx| {
        if let Some(user) = User::get_by_name(tx, "Test").await? {
            println!("{} exists", user.name);
        }

        for adult in User::list_adults(tx).await? {
            println!("{} is {}", adult.name, adult.age);
        }

        Ok(())
    })
    .await?;
}
```

# Compound primary keys

```sql
CREATE TYPE OrganizationUserRole AS ENUM ('Admin', 'Member', 'Owner');

CREATE TABLE organization_users (
    organization_id UUID REFERENCES organizations(id) ON DELETE CASCADE,
    user_id UUID REFERENCES users(id) ON DELETE CASCADE,
    role OrganizationUserRole NOT NULL,
    PRIMARY KEY (organization_id, user_id)
);
```

```rust
use orm::Model;
use sqlx::prelude::Type;
use uuid::Uuid;

#[derive(Type)]
pub enum OrganizationUserRole {
    Admin,
    Member,
    Owner,
}

#[derive(Type)]
#[sqlx(transparent)]
pub struct OrganizationId(pub Uuid);

#[derive(Type)]
#[sqlx(transparent)]
pub struct UserId(pub Uuid);

#[derive(Model)]
#[orm(primary_keys(organization_id, user_id))]
#[orm(fn is_owner(organization_id: OrganizationId, user_id: UserId) { "where organization_id = $1 and user_id = $2 and role = 'Owner'" })]
pub struct OrganizationUser {
    pub organization_id: OrganizationId,
    pub user_id: UserId,
    pub role: OrganizationUserRole,
}
```
