use chrono::{DateTime, Duration, Offset, TimeZone, Utc};
use scheduler_core::{CronSpec, schedule};

fn utc(year: i32, month: u32, day: u32, hour: u32, minute: u32, second: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(year, month, day, hour, minute, second)
        .single()
        .expect("valid UTC test instant")
}

/// Independent oracle: walk absolute UTC seconds and ask only whether their local calendar
/// fields satisfy the expression. It does not use cron's occurrence iterator and therefore
/// naturally sees both folds of an ambiguous local wall clock in absolute-time order.
fn brute_force_utc(
    spec: &CronSpec,
    after: DateTime<Utc>,
    count: usize,
    through: DateTime<Utc>,
) -> Vec<DateTime<Utc>> {
    if count == 0 {
        return Vec::new();
    }
    let (expression, timezone) = schedule::parse_cron(spec).expect("valid oracle cron");
    let mut candidate = after + Duration::seconds(1);
    let mut matches = Vec::with_capacity(count);
    while candidate <= through && matches.len() < count {
        if expression.includes(candidate.with_timezone(&timezone)) {
            matches.push(candidate);
        }
        candidate += Duration::seconds(1);
    }
    matches
}

fn assert_first_absolute_instants(
    expression: &str,
    timezone: &str,
    after: DateTime<Utc>,
    count: usize,
    through: DateTime<Utc>,
) {
    let spec = CronSpec {
        expression: expression.into(),
        timezone: timezone.into(),
    };
    let expected = brute_force_utc(&spec, after, count, through);
    assert_eq!(
        expected.len(),
        count,
        "oracle horizon was too short for {expression} in {timezone} after {after}"
    );
    let actual = schedule::next_occurrences(&spec, after, count).expect("next occurrences");
    assert_eq!(
        actual, expected,
        "iterator did not return the first matching absolute instants for {expression} in {timezone} after {after}"
    );
}

#[test]
fn vienna_fallback_before_between_and_after_folds_match_utc_oracle() {
    for after in [
        utc(2026, 10, 24, 23, 55, 0),
        utc(2026, 10, 25, 0, 45, 0),
        utc(2026, 10, 25, 2, 5, 0),
    ] {
        assert_first_absolute_instants(
            "0 */5 * * * *",
            "Europe/Vienna",
            after,
            24,
            after + Duration::hours(5),
        );
    }
}

#[test]
fn new_york_fallback_before_between_and_after_folds_match_utc_oracle() {
    for after in [
        utc(2026, 11, 1, 4, 55, 0),
        utc(2026, 11, 1, 5, 45, 0),
        utc(2026, 11, 1, 7, 5, 0),
    ] {
        assert_first_absolute_instants(
            "0 */5 * * * *",
            "America/New_York",
            after,
            24,
            after + Duration::hours(5),
        );
    }
}

#[test]
fn lord_howe_half_hour_fallback_matches_utc_oracle() {
    for after in [
        utc(2026, 4, 4, 14, 40, 0),
        utc(2026, 4, 4, 14, 55, 0),
        utc(2026, 4, 4, 15, 35, 0),
    ] {
        assert_first_absolute_instants(
            "0 */5 * * * *",
            "Australia/Lord_Howe",
            after,
            18,
            after + Duration::hours(4),
        );
    }
}

#[test]
fn casey_three_hour_fallback_matches_utc_oracle_when_present_in_tzdata() {
    let (_, timezone) = schedule::parse_cron(&CronSpec {
        expression: "0 */15 * * * *".into(),
        timezone: "Antarctica/Casey".into(),
    })
    .expect("Casey timezone");
    let before_offset = utc(2020, 3, 7, 15, 59, 59)
        .with_timezone(&timezone)
        .offset()
        .fix()
        .local_minus_utc();
    let after_offset = utc(2020, 3, 7, 16, 0, 0)
        .with_timezone(&timezone)
        .offset()
        .fix()
        .local_minus_utc();
    assert_eq!(
        before_offset - after_offset,
        3 * 60 * 60,
        "bundled tzdata no longer contains Casey's 2020 three-hour fallback"
    );

    for after in [
        utc(2020, 3, 7, 14, 30, 0),
        utc(2020, 3, 7, 15, 30, 0),
        utc(2020, 3, 7, 19, 15, 0),
    ] {
        assert_first_absolute_instants(
            "0 */15 * * * *",
            "Antarctica/Casey",
            after,
            20,
            after + Duration::hours(9),
        );
    }
}

#[test]
fn historical_vienna_midnight_crossing_matches_utc_oracle() {
    // On 1980-09-27/28 Vienna moved from UTC+02 to UTC+01 at local midnight,
    // crossing the date boundary backwards and repeating 23:00..23:59 on September 27.
    for after in [
        utc(1980, 9, 27, 20, 50, 0),
        utc(1980, 9, 27, 21, 45, 0),
        utc(1980, 9, 27, 23, 10, 0),
    ] {
        assert_first_absolute_instants(
            "0 */10 * * * *",
            "Europe/Vienna",
            after,
            16,
            after + Duration::hours(5),
        );
    }
}

#[test]
fn spring_gaps_dense_and_sparse_schedules_match_utc_oracle() {
    assert_first_absolute_instants(
        "0 30 2 * * *",
        "Europe/Vienna",
        utc(2026, 3, 28, 22, 0, 0),
        2,
        utc(2026, 3, 31, 3, 0, 0),
    );
    assert_first_absolute_instants(
        "0 30 2 * * *",
        "America/New_York",
        utc(2026, 3, 7, 5, 0, 0),
        2,
        utc(2026, 3, 10, 12, 0, 0),
    );
    assert_first_absolute_instants(
        "* * * * * *",
        "Europe/Vienna",
        utc(2026, 7, 20, 12, 0, 0),
        1_000,
        utc(2026, 7, 20, 12, 20, 0),
    );
    assert_first_absolute_instants(
        "0 17 */6 * * *",
        "America/New_York",
        utc(2026, 7, 20, 12, 0, 0),
        4,
        utc(2026, 7, 21, 18, 0, 0),
    );
}

#[test]
fn bounded_year_and_zero_or_one_count_match_utc_oracle() {
    assert_first_absolute_instants(
        "0 0 0 31 12 * 2026",
        "UTC",
        utc(2026, 12, 30, 23, 59, 55),
        1,
        utc(2027, 1, 1, 0, 0, 0),
    );

    let bounded = CronSpec {
        expression: "0 0 0 31 12 * 2026".into(),
        timezone: "UTC".into(),
    };
    assert!(
        schedule::next_occurrences(&bounded, utc(2026, 12, 31, 0, 0, 0), 1)
            .expect("bounded expression")
            .is_empty()
    );
    assert!(
        schedule::next_occurrences(&bounded, utc(2026, 1, 1, 0, 0, 0), 0)
            .expect("zero count")
            .is_empty()
    );
}
