pub use orm_macros::*;
pub use sqlx;

use sqlx::{PgConnection, PgTransaction, Pool, Postgres, Result};

pub struct ReadTransaction<'a>(PgTransaction<'a>);
pub struct WriteTransaction<'a>(PgTransaction<'a>);

pub trait ReadableTransaction {
    fn connection(&mut self) -> &mut PgConnection;
}

pub trait WritableTransaction {
    fn connection(&mut self) -> &mut PgConnection;
}

impl<'a> ReadableTransaction for ReadTransaction<'a> {
    fn connection(&mut self) -> &mut PgConnection {
        &mut self.0
    }
}

impl<'a> ReadableTransaction for WriteTransaction<'a> {
    fn connection(&mut self) -> &mut PgConnection {
        &mut self.0
    }
}

impl<'a> WritableTransaction for WriteTransaction<'a> {
    fn connection(&mut self) -> &mut PgConnection {
        &mut self.0
    }
}

#[derive(Clone)]
pub struct Db {
    pub store: Pool<Postgres>,
}

impl Db {
    pub async fn connect(url: &str) -> Result<Self> {
        let store = Pool::connect(url).await?;
        Ok(Self { store })
    }

    pub async fn read<T, E>(
        &self,
        cb: impl AsyncFnOnce(&mut ReadTransaction) -> Result<T, E>,
    ) -> Result<T, E>
    where
        E: From<sqlx::Error>,
    {
        let tx = self.store.begin_with("begin transaction read only").await?;
        let mut tx = ReadTransaction(tx);
        let r = cb(&mut tx).await?;
        tx.0.commit().await?;
        Ok(r)
    }

    pub async fn write<T, E>(
        &self,
        cb: impl AsyncFnOnce(&mut WriteTransaction) -> Result<T, E>,
    ) -> Result<T, E>
    where
        E: From<sqlx::Error>,
    {
        let tx = self
            .store
            .begin_with("begin transaction read write")
            .await?;
        let mut tx = WriteTransaction(tx);
        let r = cb(&mut tx).await?;
        tx.0.commit().await?;
        Ok(r)
    }
}
