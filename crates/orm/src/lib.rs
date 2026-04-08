use std::{marker::PhantomData, time::Duration};

pub use orm_macros::*;
use rand::RngExt;
pub use sqlx;

use sqlx::{PgConnection, PgTransaction, Pool, Postgres, Result};

pub struct ReadTransaction<'a>(PgTransaction<'a>);
pub struct WriteTransaction<'a>(PgTransaction<'a>);

pub trait SqlxError {
    fn sqlx_error(&self) -> Option<&sqlx::Error>;
}

pub trait RoTransaction {
    fn ro_connection(&mut self) -> &mut PgConnection;
}

pub trait RwTransaction {
    fn rw_connection(&mut self) -> &mut PgConnection;
}

impl<'a> RoTransaction for ReadTransaction<'a> {
    fn ro_connection(&mut self) -> &mut PgConnection {
        &mut self.0
    }
}

impl<'a> RoTransaction for WriteTransaction<'a> {
    fn ro_connection(&mut self) -> &mut PgConnection {
        &mut self.0
    }
}

impl<'a> RwTransaction for WriteTransaction<'a> {
    fn rw_connection(&mut self) -> &mut PgConnection {
        &mut self.0
    }
}

pub struct Db<E: From<sqlx::Error> + Into<E> + SqlxError> {
    e: PhantomData<E>,
    parameters: Vec<(String, String)>,
    pub store: Pool<Postgres>,
}

impl<E: From<sqlx::Error> + Into<E> + SqlxError> Clone for Db<E> {
    fn clone(&self) -> Self {
        Self {
            e: PhantomData,
            parameters: Vec::new(),
            store: self.store.clone(),
        }
    }
}

impl SqlxError for sqlx::Error {
    fn sqlx_error(&self) -> Option<&sqlx::Error> {
        Some(self)
    }
}

impl<E: From<sqlx::Error> + Into<E> + SqlxError> Db<E> {
    fn is_retryable_error(err: &impl SqlxError) -> bool {
        if let Some(code) = err
            .sqlx_error()
            .and_then(|x| x.as_database_error())
            .and_then(|x| x.code())
        {
            if code == "40001" {
                return true;
            } else {
                println!("{code} occurred");
            }
        }
        false
    }

    fn new(store: Pool<Postgres>) -> Self {
        Self {
            e: PhantomData,
            parameters: Vec::new(),
            store,
        }
    }

    pub async fn connect(url: &str) -> Result<Self> {
        Ok(Self::new(Pool::connect(url).await?))
    }

    pub fn connect_lazy(url: &str) -> Result<Self> {
        Ok(Self::new(Pool::connect_lazy(url)?))
    }

    pub async fn _read<T, F>(&self, deferrable: bool, cb: F) -> Result<T, E>
    where
        F: AsyncFnOnce(&mut ReadTransaction) -> Result<T, E> + Clone,
    {
        let mut retries = 0;
        loop {
            let mut tx = self
                .store
                .begin_with(if deferrable {
                    "BEGIN TRANSACTION READ ONLY DEFERRABLE"
                } else {
                    "BEGIN TRANSACTION READ ONLY"
                })
                .await?;
            for (key, value) in &self.parameters {
                sqlx::query("SELECT set_config($1, $2, true)")
                    .bind(key)
                    .bind(value)
                    .execute(&mut *tx)
                    .await?;
            }
            let mut tx = ReadTransaction(tx);
            match cb.clone()(&mut tx).await {
                Ok(r) => {
                    if let Err(err) = tx.0.commit().await {
                        if retries < 10 && Self::is_retryable_error(&err) {
                            retries += 1;
                            sleep(retries).await;
                            continue;
                        }
                        Err(err)?;
                    }
                    return Ok(r);
                }
                Err(err) => {
                    tx.0.rollback().await?;
                    if retries < 10 && Self::is_retryable_error(&err) {
                        retries += 1;
                        sleep(retries).await;
                        continue;
                    }
                    return Err(err);
                }
            }
        }
    }

    pub async fn read<T, F>(&self, cb: F) -> Result<T, E>
    where
        F: AsyncFnOnce(&mut ReadTransaction) -> Result<T, E> + Clone,
    {
        self._read(false, cb).await
    }

    pub async fn read_deferrable<T, F>(&self, cb: F) -> Result<T, E>
    where
        F: AsyncFnOnce(&mut ReadTransaction) -> Result<T, E> + Clone,
    {
        self._read(true, cb).await
    }

    pub fn set(&mut self, key: impl ToString, value: impl ToString) -> &mut Self {
        self.parameters.push((key.to_string(), value.to_string()));
        self
    }

    pub async fn write<T, F>(&self, cb: F) -> Result<T, E>
    where
        F: AsyncFnOnce(&mut WriteTransaction) -> Result<T, E> + Clone,
    {
        let mut retries = 0;
        loop {
            let mut tx = self
                .store
                .begin_with("BEGIN TRANSACTION READ WRITE")
                .await?;
            for (key, value) in &self.parameters {
                sqlx::query("SELECT set_config($1, $2, true)")
                    .bind(key)
                    .bind(value)
                    .execute(&mut *tx)
                    .await?;
            }
            let mut tx = WriteTransaction(tx);
            match cb.clone()(&mut tx).await {
                Ok(r) => {
                    if let Err(err) = tx.0.commit().await {
                        if retries < 10 && Self::is_retryable_error(&err) {
                            retries += 1;
                            sleep(retries).await;
                            continue;
                        }
                        Err(err)?;
                    }
                    return Ok(r);
                }
                Err(err) => {
                    tx.0.rollback().await?;
                    if retries < 10 && Self::is_retryable_error(&err) {
                        retries += 1;
                        sleep(retries).await;
                        continue;
                    }
                    return Err(err);
                }
            }
        }
    }
}

async fn sleep(retries: u64) {
    let jitter: u64 = rand::rng().random_range(..50);
    let duration = Duration::from_millis(10 * retries + jitter);
    println!("Sleeping {duration:?}");
    tokio::time::sleep(duration).await;
}
