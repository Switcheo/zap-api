use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::query_builder::*;
use diesel::query_dsl::methods::LoadQuery;
use diesel::sql_types::BigInt;
use serde::{Serialize};
use std::cmp::{max, min};

pub trait Paginate: Sized {
    fn paginate(self, page: Option<i64>) -> Paginated<Self>;
}

impl<T> Paginate for T {
    fn paginate(self, page: Option<i64>) -> Paginated<Self> {
      let r = Paginated {
        query: self,
        per_page: DEFAULT_PER_PAGE,
        page: 1,
      };
      match page {
        Some(p) => Paginated { page: max(p, 1), ..r },
        None => r
      }
    }
}

const DEFAULT_PER_PAGE: i64 = 10;
const MAXIMUM_PER_PAGE: i64 = 50;

#[derive(Debug, Clone, Copy, QueryId)]
pub struct Paginated<T> {
    query: T,
    page: i64,
    per_page: i64,
}

#[derive(Serialize)]
pub struct PaginatedResult<T> {
  records: Vec<T>,
  total_pages: i64
}

impl<T> Paginated<T> {
    pub fn per_page(self, per_page: Option<i64>) -> Self {
        match per_page {
          Some(p) => Paginated { per_page: max(min(MAXIMUM_PER_PAGE, p), 1), ..self },
          None => self
        }
    }

    pub fn load_and_count_pages<U>(self, conn: &PgConnection) -> QueryResult<PaginatedResult<U>>
    where
        Self: LoadQuery<PgConnection, (U, i64)>,
    {
        let per_page = self.per_page;
        let results = self.load::<(U, i64)>(conn)?;
        let total = results.get(0).map(|x| x.1).unwrap_or(0);
        let records = results.into_iter().map(|x| x.0).collect();
        let total_pages = (total as f64 / per_page as f64).ceil() as i64;
        Ok(PaginatedResult{ records: records, total_pages: total_pages })
    }
}

impl<T: Query> Query for Paginated<T> {
    type SqlType = (T::SqlType, BigInt);
}

impl<T> RunQueryDsl<PgConnection> for Paginated<T> {}

impl<T> QueryFragment<Pg> for Paginated<T>
where
    T: QueryFragment<Pg>,
{
    fn walk_ast(&self, mut out: AstPass<Pg>) -> QueryResult<()> {
        out.push_sql("SELECT *, COUNT(*) OVER () FROM (");
        self.query.walk_ast(out.reborrow())?;
        out.push_sql(") t LIMIT ");
        out.push_bind_param::<BigInt, _>(&self.per_page)?;
        out.push_sql(" OFFSET ");
        let offset = (self.page - 1) * self.per_page;
        out.push_bind_param::<BigInt, _>(&offset)?;
        Ok(())
    }
}
