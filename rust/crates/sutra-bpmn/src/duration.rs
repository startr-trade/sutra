//! ISO-8601 duration parsing for BPMN timers.
//!
//! Supports the exact-length forms a timer can schedule deterministically:
//! `PnW` and `PnDTnHnMnS` (each component optional, seconds may carry a fraction).
//! Calendar components (`Y`ears / `M`onths before the `T`) are REJECTED — their length
//! depends on the calendar position, which a duration-only timer contract cannot honor
//! (the contract requires only durations; date/cycle timers are load-time-rejected by the
//! loader with `SUTRA.DISPATCH.TIMER.UNSUPPORTED`).

/// Parse an ISO-8601 duration into a [`std::time::Duration`] (nanosecond resolution on
/// the fractional-seconds component). Errors carry a human-readable reason; the caller
/// wraps them in `SUTRA.DISPATCH.TIMER.DURATION_INVALID`.
pub fn parse_iso8601_duration(input: &str) -> Result<std::time::Duration, String> {
    let s = input.trim();
    let Some(rest) = s.strip_prefix('P').or_else(|| s.strip_prefix('p')) else {
        return Err(format!(
            "'{input}' is not an ISO-8601 duration (must start with 'P')"
        ));
    };
    if rest.is_empty() {
        return Err(format!("'{input}' declares no duration components"));
    }

    let mut total_secs: f64 = 0.0;
    let mut in_time_part = false;
    let mut saw_component = false;
    let mut number = String::new();

    for c in rest.chars() {
        match c {
            'T' | 't' => {
                if in_time_part {
                    return Err(format!("'{input}' has more than one 'T' separator"));
                }
                if !number.is_empty() {
                    return Err(format!("'{input}' has a number with no unit before 'T'"));
                }
                in_time_part = true;
            }
            '0'..='9' | '.' | ',' => number.push(if c == ',' { '.' } else { c }),
            unit => {
                let value: f64 = number
                    .parse()
                    .map_err(|_| format!("'{input}' has an unparseable number '{number}'"))?;
                number.clear();
                saw_component = true;
                let seconds_per_unit = match (unit.to_ascii_uppercase(), in_time_part) {
                    ('W', false) => 7.0 * 86_400.0,
                    ('D', false) => 86_400.0,
                    ('H', true) => 3_600.0,
                    ('M', true) => 60.0,
                    ('S', true) => 1.0,
                    ('Y', false) | ('M', false) => {
                        return Err(format!(
                            "'{input}' uses the calendar component '{unit}'; years/months are \
                             not exact lengths — use weeks/days/hours/minutes/seconds"
                        ));
                    }
                    _ => {
                        return Err(format!(
                            "'{input}' has an unexpected component '{unit}'{}",
                            if in_time_part {
                                " in the time part"
                            } else {
                                " (did you forget the 'T' before a time component?)"
                            }
                        ));
                    }
                };
                total_secs += value * seconds_per_unit;
            }
        }
    }
    if !number.is_empty() {
        return Err(format!(
            "'{input}' ends with a number ('{number}') that has no unit"
        ));
    }
    if !saw_component {
        return Err(format!("'{input}' declares no duration components"));
    }
    if !total_secs.is_finite() || total_secs < 0.0 {
        return Err(format!("'{input}' is not a non-negative finite duration"));
    }
    Ok(std::time::Duration::from_secs_f64(total_secs))
}

#[cfg(test)]
mod tests {
    use super::parse_iso8601_duration;
    use std::time::Duration;

    #[test]
    fn parses_the_exact_length_forms() {
        assert_eq!(
            parse_iso8601_duration("PT5S").unwrap(),
            Duration::from_secs(5)
        );
        assert_eq!(
            parse_iso8601_duration("PT0.25S").unwrap(),
            Duration::from_millis(250)
        );
        assert_eq!(
            parse_iso8601_duration("PT1M30S").unwrap(),
            Duration::from_secs(90)
        );
        assert_eq!(
            parse_iso8601_duration("PT2H").unwrap(),
            Duration::from_secs(7_200)
        );
        assert_eq!(
            parse_iso8601_duration("P1DT1H").unwrap(),
            Duration::from_secs(90_000)
        );
        assert_eq!(
            parse_iso8601_duration("P2W").unwrap(),
            Duration::from_secs(14 * 86_400)
        );
        // Comma decimal separator (ISO-8601 allows both).
        assert_eq!(
            parse_iso8601_duration("PT0,5S").unwrap(),
            Duration::from_millis(500)
        );
    }

    #[test]
    fn rejects_calendar_and_malformed_forms() {
        for bad in [
            "",
            "P",
            "PT",
            "5S",
            "P1Y",
            "P1M",
            "P1YT1S",
            "PT5",
            "PT5X",
            "P1S",
            "PTT5S",
            "R5/PT1S",
            "2026-01-01T00:00:00Z",
        ] {
            assert!(
                parse_iso8601_duration(bad).is_err(),
                "'{bad}' must be rejected"
            );
        }
    }

    #[test]
    fn minutes_are_time_scoped_and_months_rejected() {
        assert_eq!(
            parse_iso8601_duration("PT3M").unwrap(),
            Duration::from_secs(180)
        );
        let err = parse_iso8601_duration("P3M").unwrap_err();
        assert!(err.contains("calendar"), "got: {err}");
    }
}
