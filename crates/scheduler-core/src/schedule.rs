use std::str::FromStr;

use anyhow::{Context, Result};
use chrono::{DateTime, Duration, LocalResult, Offset, TimeZone, Utc};
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
    if count == 0 {
        return Ok(Vec::new());
    }

    // `cron::ScheduleIterator` walks local wall-clock values. During a fall-back it emits
    // each ambiguous value's earlier and later instant next to one another. That is correct
    // for a sparse schedule containing one ambiguous wall time, but is not UTC-monotonic for
    // a dense schedule: 02:45 CEST, 02:45 CET, 02:50 CEST, ... . Grouping the complete
    // ambiguous interval and sorting that group by UTC restores chronological ordering.
    //
    // If `after` is near a fall-back, start before every possible instant on its local date.
    // This is necessary when `after` lies between the two folds: an occurrence in the second
    // fold can have a wall-clock value earlier than `after` while still being a later UTC
    // instant. Chrono offsets are strictly less than 24 hours, so interpreting local midnight
    // as UTC and subtracting one day is an absolute lower bound for the local date. Avoid that
    // rewind on ordinary days so dense every-few-seconds schedules remain cheap to query.
    let offset_before = (after - Duration::days(1))
        .with_timezone(&timezone)
        .offset()
        .fix()
        .local_minus_utc();
    let offset_after = (after + Duration::days(1))
        .with_timezone(&timezone)
        .offset()
        .fix()
        .local_minus_utc();
    let scan_from = if offset_before > offset_after {
        let local_midnight = after
            .with_timezone(&timezone)
            .date_naive()
            .and_hms_opt(0, 0, 0)
            .expect("midnight is a valid naive time");
        let scan_from_utc =
            DateTime::<Utc>::from_naive_utc_and_offset(local_midnight, Utc) - Duration::days(1);
        scan_from_utc.with_timezone(&timezone)
    } else {
        after.with_timezone(&timezone)
    };

    let mut occurrences = Vec::with_capacity(count);
    let mut ambiguous = Vec::new();

    for value in schedule.after(&scan_from) {
        let occurrence = value.with_timezone(&Utc);
        if matches!(
            timezone.from_local_datetime(&value.naive_local()),
            LocalResult::Ambiguous(_, _)
        ) {
            ambiguous.push(occurrence);
            continue;
        }

        append_ambiguous_after(&mut occurrences, &mut ambiguous, after, count);
        if occurrences.len() == count {
            break;
        }

        if occurrence > after && occurrences.last() != Some(&occurrence) {
            occurrences.push(occurrence);
            if occurrences.len() == count {
                break;
            }
        }
    }

    // A year-bounded expression can end while its final matching wall time is ambiguous.
    append_ambiguous_after(&mut occurrences, &mut ambiguous, after, count);
    Ok(occurrences)
}

fn append_ambiguous_after(
    occurrences: &mut Vec<DateTime<Utc>>,
    ambiguous: &mut Vec<DateTime<Utc>>,
    after: DateTime<Utc>,
    count: usize,
) {
    ambiguous.sort_unstable();
    ambiguous.dedup();
    for occurrence in ambiguous.drain(..) {
        if occurrence > after && occurrences.last() != Some(&occurrence) {
            occurrences.push(occurrence);
            if occurrences.len() == count {
                return;
            }
        }
    }
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

    #[test]
    fn dense_fall_dst_occurrences_are_ordered_by_absolute_time() {
        let spec = CronSpec {
            expression: "0 */5 * * * *".into(),
            timezone: "Europe/Vienna".into(),
        };
        let after = Utc.with_ymd_and_hms(2026, 10, 25, 0, 43, 0).unwrap();
        let next = next_occurrences(&spec, after, 6).expect("cron");
        assert_eq!(
            next,
            [
                Utc.with_ymd_and_hms(2026, 10, 25, 0, 45, 0).unwrap(),
                Utc.with_ymd_and_hms(2026, 10, 25, 0, 50, 0).unwrap(),
                Utc.with_ymd_and_hms(2026, 10, 25, 0, 55, 0).unwrap(),
                Utc.with_ymd_and_hms(2026, 10, 25, 1, 0, 0).unwrap(),
                Utc.with_ymd_and_hms(2026, 10, 25, 1, 5, 0).unwrap(),
                Utc.with_ymd_and_hms(2026, 10, 25, 1, 10, 0).unwrap(),
            ]
        );
    }

    #[test]
    fn after_between_fall_dst_folds_keeps_later_ambiguous_occurrence() {
        let spec = CronSpec {
            expression: "0 30 2 * * *".into(),
            timezone: "Europe/Vienna".into(),
        };
        let after = Utc.with_ymd_and_hms(2026, 10, 25, 0, 45, 0).unwrap();
        let next = next_occurrences(&spec, after, 2).expect("cron");
        assert_eq!(
            next,
            [
                Utc.with_ymd_and_hms(2026, 10, 25, 1, 30, 0).unwrap(),
                Utc.with_ymd_and_hms(2026, 10, 26, 1, 30, 0).unwrap(),
            ]
        );
    }

    use chrono::Timelike;
}
