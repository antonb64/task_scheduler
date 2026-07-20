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

    #[test]
    fn skips_a_nonexistent_local_time_during_spring_dst() {
        let spec = CronSpec {
            expression: "0 30 2 * * *".into(),
            timezone: "Europe/Vienna".into(),
        };
        let after = Utc.with_ymd_and_hms(2026, 3, 28, 23, 0, 0).unwrap();
        let next = next_occurrences(&spec, after, 1).expect("cron");
        assert_eq!(
            next[0],
            Utc.with_ymd_and_hms(2026, 3, 30, 0, 30, 0).unwrap()
        );
    }

    #[test]
    fn emits_both_ambiguous_local_times_during_fall_dst() {
        let spec = CronSpec {
            expression: "0 30 2 * * *".into(),
            timezone: "Europe/Vienna".into(),
        };
        let after = Utc.with_ymd_and_hms(2026, 10, 24, 22, 0, 0).unwrap();
        let next = next_occurrences(&spec, after, 2).expect("cron");
        assert_eq!(
            next,
            [
                Utc.with_ymd_and_hms(2026, 10, 25, 0, 30, 0).unwrap(),
                Utc.with_ymd_and_hms(2026, 10, 25, 1, 30, 0).unwrap(),
            ]
        );
    }

    use chrono::Timelike;
}
