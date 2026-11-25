//! Queries of the search commands: filters joined by AND, with ranges, descending order and
//! map fields, which the store's own [`Query`](crate::store::fs::Query) leaves out because its
//! overlay must evaluate them locally. These run outside transactions.

use firestore::gcloud_sdk::google::firestore::v1::{
    Cursor, StructuredQuery,
    structured_query::{
        self, CompositeFilter, FieldFilter, FieldReference, Filter, Order, composite_filter,
        field_filter::Operator, filter,
    },
    value::ValueType,
};

use crate::store::fs::{Db, Document, Value};

pub(super) fn string(s: impl Into<String>) -> Value {
    Value {
        value_type: Some(ValueType::StringValue(s.into())),
    }
}

pub(super) fn int(i: i64) -> Value {
    Value {
        value_type: Some(ValueType::IntegerValue(i)),
    }
}

pub(super) fn boolean(b: bool) -> Value {
    Value {
        value_type: Some(ValueType::BooleanValue(b)),
    }
}

/// A field path segment as Firestore wants it: quoted with backticks unless it is a plain
/// identifier.
pub(super) fn segment(name: &str) -> String {
    let plain = name
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    if plain {
        name.to_string()
    } else {
        format!("`{}`", name.replace('\\', "\\\\").replace('`', "\\`"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Direction {
    Ascending,
    Descending,
}

#[derive(Debug, Clone, Default)]
pub(super) struct Search {
    filters: Vec<(String, Operator, Value)>,
    order: Vec<(String, Direction)>,
    limit: Option<usize>,
    /// The values of the order fields to start after.
    start_after: Option<Vec<Value>>,
}

impl Search {
    #[must_use]
    pub(super) fn filter(mut self, field: impl Into<String>, op: Operator, value: Value) -> Self {
        self.filters.push((field.into(), op, value));
        self
    }

    #[must_use]
    pub(super) fn eq(self, field: impl Into<String>, value: Value) -> Self {
        self.filter(field, Operator::Equal, value)
    }

    /// Filter by `eq` if `value` is given.
    #[must_use]
    pub(super) fn eq_opt(self, field: impl Into<String>, value: Option<Value>) -> Self {
        match value {
            Some(value) => self.eq(field, value),
            None => self,
        }
    }

    #[must_use]
    pub(super) fn order_by(mut self, field: impl Into<String>, direction: Direction) -> Self {
        self.order.push((field.into(), direction));
        self
    }

    #[must_use]
    pub(super) fn limit(mut self, limit: Option<usize>) -> Self {
        self.limit = limit;
        self
    }

    /// The page of `size` documents after `after`, a document of the previous page. The order
    /// must end with `__name__`, and its other fields must be top-level.
    pub(super) async fn page(
        &self,
        db: &Db,
        collection: &str,
        after: Option<&Document>,
        size: usize,
    ) -> Result<Vec<Document>, lakekeeper::service::CatalogBackendError> {
        let mut page = self.clone().limit(Some(size));
        page.start_after = after.map(|doc| {
            self.order
                .iter()
                .map(|(field, _)| {
                    if field == "__name__" {
                        Value {
                            value_type: Some(ValueType::ReferenceValue(doc.name.clone())),
                        }
                    } else {
                        doc.fields.get(field).cloned().unwrap_or(Value {
                            value_type: Some(ValueType::NullValue(0)),
                        })
                    }
                })
                .collect()
        });
        page.run(db, collection).await
    }

    fn structured(&self) -> StructuredQuery {
        let field_filter = |(field, op, value): &(String, Operator, Value)| Filter {
            filter_type: Some(filter::FilterType::FieldFilter(FieldFilter {
                field: Some(FieldReference {
                    field_path: field.clone(),
                }),
                op: (*op).into(),
                value: Some(value.clone()),
            })),
        };
        let r#where = match self.filters.as_slice() {
            [] => None,
            [only] => Some(field_filter(only)),
            filters => Some(Filter {
                filter_type: Some(filter::FilterType::CompositeFilter(CompositeFilter {
                    op: composite_filter::Operator::And.into(),
                    filters: filters.iter().map(field_filter).collect(),
                })),
            }),
        };
        StructuredQuery {
            r#where,
            order_by: self
                .order
                .iter()
                .map(|(field, direction)| Order {
                    field: Some(FieldReference {
                        field_path: field.clone(),
                    }),
                    direction: match direction {
                        Direction::Ascending => structured_query::Direction::Ascending,
                        Direction::Descending => structured_query::Direction::Descending,
                    }
                    .into(),
                })
                .collect(),
            start_at: self.start_after.clone().map(|values| Cursor {
                values,
                before: false,
            }),
            limit: self.limit.map(|l| i32::try_from(l).unwrap_or(i32::MAX)),
            ..Default::default()
        }
    }

    pub(super) async fn run(
        &self,
        db: &Db,
        collection: &str,
    ) -> Result<Vec<Document>, lakekeeper::service::CatalogBackendError> {
        db.run_query(collection, self.structured()).await
    }
}
