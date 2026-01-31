use once_cell::sync::Lazy;
use regex::Regex;

/// PII Scrubber for sanitizing SQL queries and log messages.
///
/// ### WARNING
/// This utility uses regex-based patterns which is a **best-effort** approach.
/// It is intended for defense-in-depth and does not guarantee complete sanitization
/// of all PII, especially in complex SQL dialects or concatenated strings.
///
/// For high-compliance environments, consider disabling literal logging entirely
/// or using a full SQL parser for dialet-accurate sanitization.
static EMAIL_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}").unwrap());

static SSN_REGEX: Lazy<Regex> = Lazy::new(|| {
    // Basic US SSN pattern: XXX-XX-XXXX
    Regex::new(r"\b\d{3}-\d{2}-\d{4}\b").unwrap()
});

static CREDIT_CARD_REGEX: Lazy<Regex> = Lazy::new(|| {
    // Basic 13-16 digit pattern, often grouped by hyphens or spaces
    Regex::new(r"\b(?:\d[ -]*?){13,16}\b").unwrap()
});

static PHONE_REGEX: Lazy<Regex> = Lazy::new(|| {
    // Matches common phone formats like (XXX) XXX-XXXX or XXX-XXX-XXXX
    // Removed leading/trailing \b to handle (XXX) better, using grouping instead.
    Regex::new(r"(?:\+?1[-. ]?)?\(?\d{3}\)?[-. ]?\d{3}[-. ]?\d{4}").unwrap()
});

pub fn scrub(input: &str) -> String {
    let mut scrubbed = input.to_string();

    // Scrub Emails
    scrubbed = EMAIL_REGEX.replace_all(&scrubbed, "[EMAIL]").to_string();

    // Scrub SSNs
    scrubbed = SSN_REGEX.replace_all(&scrubbed, "[SSN]").to_string();

    // Scrub Credit Cards
    // Note: This matches 13-16 digits which might catch IDs too,
    // but better safe for an audit log.
    scrubbed = CREDIT_CARD_REGEX
        .replace_all(&scrubbed, "[CREDIT_CARD]")
        .to_string();

    // Scrub Phone Numbers
    scrubbed = PHONE_REGEX.replace_all(&scrubbed, "[PHONE]").to_string();

    scrubbed
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scrub_email() {
        let input = "SELECT * FROM users WHERE email = 'test@example.com'";
        assert_eq!(scrub(input), "SELECT * FROM users WHERE email = '[EMAIL]'");
    }

    #[test]
    fn test_scrub_ssn() {
        let input = "INSERT INTO sensitive (ssn) VALUES ('123-45-6789')";
        assert_eq!(scrub(input), "INSERT INTO sensitive (ssn) VALUES ('[SSN]')");
    }

    #[test]
    fn test_scrub_credit_card() {
        let input = "The card number is 1234-5678-9012-3456.";
        assert_eq!(scrub(input), "The card number is [CREDIT_CARD].");
    }

    #[test]
    fn test_scrub_phone() {
        let input = "Call me at 123-456-7890 or (555) 123-4567";
        assert_eq!(scrub(input), "Call me at [PHONE] or [PHONE]");
    }

    #[test]
    fn test_scrub_mixed() {
        let input = "User test@test.com with SSN 000-00-0000 checked out.";
        assert_eq!(scrub(input), "User [EMAIL] with SSN [SSN] checked out.");
    }
}
