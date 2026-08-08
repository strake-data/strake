use super::error::SqlGenError;
use sqlparser::ast::Ident;

/// Validates that an identifier (table or column name) is safe for SQL use.
pub fn validate_identifier(name: &str) -> Result<(), SqlGenError> {
    if name.is_empty() {
        return Err(SqlGenError::InvalidIdentifier("empty".to_string()));
    }
    if name.len() > 256 {
        return Err(SqlGenError::InvalidIdentifier(format!(
            "too long: {}",
            name.len()
        )));
    }
    if name.contains('"')
        || name.contains('\x00')
        || name.contains(';')
        || name.contains('`')
        || name.contains('\\')
    {
        return Err(SqlGenError::InvalidIdentifier(format!(
            "forbidden characters in: {}",
            name
        )));
    }
    Ok(())
}

/// Sanitize and quote a SQL identifier.
pub fn safe_ident(name: &str) -> Result<Ident, SqlGenError> {
    validate_identifier(name)?;
    Ok(Ident::with_quote('"', name))
}

/// Sanitize a SQL identifier but return it unquoted.
pub fn safe_ident_unquoted(name: &str) -> Result<Ident, SqlGenError> {
    validate_identifier(name)?;
    Ok(Ident::new(name))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_identifier() {
        assert!(validate_identifier("users").is_ok());
        assert!(validate_identifier("user_id").is_ok());

        assert!(validate_identifier("").is_err());
        assert!(validate_identifier("foo\"bar").is_err());
        assert!(validate_identifier("x; DROP TABLE users").is_err());
        assert!(validate_identifier("null\0byte").is_err());
    }

    #[test]
    fn test_safe_ident() {
        let ident = safe_ident("users").unwrap();
        assert_eq!(ident.value, "users");
        assert_eq!(ident.quote_style, Some('"'));
    }
}
