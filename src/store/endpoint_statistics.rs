//! Endpoint statistics. The server records none (no statistics sink is configured, see
//! `serve.rs`), so every window is empty. The page tokens step through windows as upstream's
//! do, in this store's own format: `{end, microseconds}|{interval, seconds}`.

use lakekeeper::{
    api::{
        ErrorModel,
        management::v1::project::{EndpointStatisticsResponse, TimeWindowSelector},
    },
    service::Result,
};

fn token(end: chrono::DateTime<chrono::Utc>, interval: chrono::Duration) -> String {
    format!("{}|{}", end.timestamp_micros(), interval.num_seconds())
}

fn parse_token(token: &str) -> Result<(chrono::DateTime<chrono::Utc>, chrono::Duration)> {
    let invalid = || {
        ErrorModel::bad_request(
            format!("Invalid page token `{token}`"),
            "InvalidPaginationToken",
            None,
        )
    };
    let (end, interval) = token.split_once('|').ok_or_else(invalid)?;
    let end = end
        .parse()
        .ok()
        .and_then(chrono::DateTime::from_timestamp_micros)
        .ok_or_else(invalid)?;
    let interval = interval
        .parse()
        .ok()
        .and_then(chrono::Duration::try_seconds)
        .ok_or_else(invalid)?;
    Ok((end, interval))
}

pub(super) fn get(range: TimeWindowSelector) -> Result<EndpointStatisticsResponse> {
    let (end, interval) = match range {
        TimeWindowSelector::Window { end, interval } => (end, interval),
        TimeWindowSelector::PageToken { token } => parse_token(&token)?,
    };
    Ok(EndpointStatisticsResponse {
        timestamps: Vec::new(),
        called_endpoints: Vec::new(),
        previous_page_token: token(end - interval, interval),
        next_page_token: token(end + interval, interval),
    })
}
