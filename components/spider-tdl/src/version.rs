//! Semantic version of the `spider-tdl` ABI shared across the TDL package / task executor C-FFI
//! boundary.
//!
//! [`Version`] is `#[repr(C)]` so it can be returned directly from a TDL package's
//! `__spider_tdl_package_get_version` FFI entry point. The package manager reads the value via
//! `dlsym` at load time and refuses to install packages whose declared version is incompatible
//! with the executor's [`Version::SPIDER_TDL`].

/// `#[repr(C)]` semantic-version triple shared across the TDL package / task executor FFI boundary.
///
/// The struct is intentionally `Copy` so it can be returned by value from an `extern "C"` function.
/// Compatibility is decided by [`Self::is_compatible_with`].
#[repr(C)]
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct Version {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

impl Version {
    /// Compile-time `spider-tdl` version, derived from the crate's Cargo manifest via
    /// `CARGO_PKG_VERSION_*` environment variables.
    pub const SPIDER_TDL: Self = Self {
        major: const_parse_u32(env!("CARGO_PKG_VERSION_MAJOR")),
        minor: const_parse_u32(env!("CARGO_PKG_VERSION_MINOR")),
        patch: const_parse_u32(env!("CARGO_PKG_VERSION_PATCH")),
    };

    /// Constructs a [`Version`] from raw components.
    ///
    /// # Returns
    ///
    /// A [`Version`] with the given components.
    #[must_use]
    pub const fn new(major: u32, minor: u32, patch: u32) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }

    /// Decides whether `self` (the executor) can load a package built against `other`.
    ///
    /// The rule follows the standard semver convention:
    ///
    /// * For pre-1.0 versions (`major == 0`), each minor bump is treated as breaking, so both
    ///   `major` and `minor` must match.
    /// * For post-1.0 versions, only `major` must match. Minor and patch differences are considered
    ///   backward compatible.
    ///
    /// # Returns
    ///
    /// Whether `other` is compatible with `self` under the rule above.
    #[must_use]
    pub const fn is_compatible_with(&self, other: &Self) -> bool {
        if self.major == 0 {
            self.major == other.major && self.minor == other.minor
        } else {
            self.major == other.major
        }
    }
}

/// Parses a decimal `u32` from a `&str` in a `const` context.
///
/// Used to evaluate `CARGO_PKG_VERSION_*` (which `env!` exposes as `&'static str`) at compile time.
/// The parser is intentionally minimal: ASCII digits only, no sign, no whitespace.
///
/// # Returns
///
/// The parsed `u32` value.
///
/// # Panics
///
/// Panics in const evaluation if:
///
/// * `s` is an empty string.
/// * `s` contains a non-ASCII-digit byte.
/// * The parsed value would overflow `u32`.
const fn const_parse_u32(s: &str) -> u32 {
    let bytes = s.as_bytes();
    assert!(!bytes.is_empty(), "`const_parse_u32`: empty input");
    let mut value: u32 = 0;
    let mut i = 0;
    while i < bytes.len() {
        let byte = bytes[i];
        assert!(
            !(byte < b'0' || byte > b'9'),
            "`const_parse_u32`: non-digit byte"
        );
        let digit = (byte - b'0') as u32;
        let Some(scaled) = value.checked_mul(10) else {
            panic!("`const_parse_u32`: overflow");
        };
        let Some(next) = scaled.checked_add(digit) else {
            panic!("`const_parse_u32`: overflow");
        };
        value = next;
        i += 1;
    }
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn const_parse_u32_basic() {
        assert_eq!(const_parse_u32("0"), 0);
        assert_eq!(const_parse_u32("1"), 1);
        assert_eq!(const_parse_u32("42"), 42);
        assert_eq!(const_parse_u32("4294967295"), u32::MAX);
    }

    #[test]
    fn spider_tdl_version_matches_cargo_pkg_version() {
        let cargo_major: u32 = env!("CARGO_PKG_VERSION_MAJOR")
            .parse()
            .expect("parse major");
        let cargo_minor: u32 = env!("CARGO_PKG_VERSION_MINOR")
            .parse()
            .expect("parse minor");
        let cargo_patch: u32 = env!("CARGO_PKG_VERSION_PATCH")
            .parse()
            .expect("parse patch");
        assert_eq!(Version::SPIDER_TDL.major, cargo_major);
        assert_eq!(Version::SPIDER_TDL.minor, cargo_minor);
        assert_eq!(Version::SPIDER_TDL.patch, cargo_patch);
    }

    #[test]
    fn pre_one_zero_compatibility() {
        let executor = Version::new(0, 1, 0);
        assert!(executor.is_compatible_with(&Version::new(0, 1, 0)));
        assert!(executor.is_compatible_with(&Version::new(0, 1, 99)));
        assert!(!executor.is_compatible_with(&Version::new(0, 2, 0)));
        assert!(!executor.is_compatible_with(&Version::new(1, 1, 0)));
    }

    #[test]
    fn post_one_zero_compatibility() {
        let executor = Version::new(1, 2, 3);
        assert!(executor.is_compatible_with(&Version::new(1, 0, 0)));
        assert!(executor.is_compatible_with(&Version::new(1, 99, 99)));
        assert!(!executor.is_compatible_with(&Version::new(2, 2, 3)));
        assert!(!executor.is_compatible_with(&Version::new(0, 2, 3)));
    }

    #[test]
    fn copy_and_eq() {
        let a = Version::new(1, 2, 3);
        let b = a;
        assert_eq!(a, b);
        assert_ne!(a, Version::new(1, 2, 4));
    }
}
