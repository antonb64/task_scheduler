use std::str::FromStr;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use cron::Schedule;

use crate::CronSpec;

pub fn parse_cron(spec: &CronSpec) -> Result<(Schedule, Tz)> {
    let schedule = Schedule::from_str(&spec.expression).context("invalid cron expression")?;
    let timezone = Tz::from_str(&spec.timezone).context("invalid IANA timezone")?;
    Ok((schedule, timezone))
}

pub fn next_occurrences(
    spec: &CronSpec,
    after: DateTime<Utc>,
    count: usize,
) -> Result<Vec<DateTime<Utc>>> {
    let (schedule, timezone) = parse_cron(spec)?;
    let localized = after.with_timezone(&timezone);
    Ok(schedule
        .after(&localized)
        .take(count)
        .map(|value| value.with_timezone(&Utc))
        .collect())
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    #[test]
    fn computes_occurrences_in_requested_timezone() {
        let spec = CronSpec {
            expression: "0 0 9 * * *".into(),
            timezone: "Europe/Vienna".into(),
        };
        let after = Utc.with_ymd_and_hms(2026, 7, 20, 0, 0, 0).unwrap();
        let next = next_occurrences(&spec, after, 1).expect("cron");
        assert_eq!(next[0].hour(), 7);
    }

    use chrono::Timelike;
}
