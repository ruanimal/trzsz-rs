/*
MIT License

Copyright (c) 2022-2026 The Trzsz Authors.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

pub const TRZSZ_VERSION: &str = "1.3.0";

/// A parsed trzsz version string like "1.2.3" → (1, 2, 3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrzszVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

impl TrzszVersion {
    pub fn parse(s: &str) -> Option<Self> {
        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() < 2 || parts.len() > 3 {
            return None;
        }
        let major = parts[0].parse::<u32>().ok()?;
        let minor = parts[1].parse::<u32>().ok()?;
        if parts.len() == 3 {
            let patch = parts[2].parse::<u32>().ok()?;
            Some(TrzszVersion {
                major,
                minor,
                patch,
            })
        } else {
            // Go code requires exactly 3 parts (e.g. "1.0" is invalid)
            None
        }
    }

    /// Compare two versions. Returns:
    /// - negative if self < other
    /// - 0 if self == other
    /// - positive if self > other
    pub fn compare(&self, other: &TrzszVersion) -> i32 {
        let a = (self.major as i64) << 32 | (self.minor as i64) << 16 | (self.patch as i64);
        let b = (other.major as i64) << 32 | (other.minor as i64) << 16 | (other.patch as i64);
        if a < b {
            -1
        } else if a > b {
            1
        } else {
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_version() {
        assert_eq!(
            TrzszVersion::parse("1.2.3"),
            Some(TrzszVersion {
                major: 1,
                minor: 2,
                patch: 3
            })
        );
        assert_eq!(
            TrzszVersion::parse("1.0.0"),
            Some(TrzszVersion {
                major: 1,
                minor: 0,
                patch: 0
            })
        );
        assert_eq!(
            TrzszVersion::parse("0.0.0"),
            Some(TrzszVersion {
                major: 0,
                minor: 0,
                patch: 0
            })
        );
        assert_eq!(TrzszVersion::parse("1"), None);
        assert_eq!(TrzszVersion::parse("1."), None);
        assert_eq!(TrzszVersion::parse("1.0"), None); // Go requires 3 parts
        assert_eq!(TrzszVersion::parse("1.0."), None);
        assert_eq!(TrzszVersion::parse("1.0.a"), None);
    }

    #[test]
    fn test_compare_version() {
        assert!(
            TrzszVersion {
                major: 2,
                minor: 1,
                patch: 1
            }
            .compare(&TrzszVersion {
                major: 1,
                minor: 1,
                patch: 2
            }) > 0
        );
        assert_eq!(
            TrzszVersion {
                major: 3,
                minor: 2,
                patch: 1
            }
            .compare(&TrzszVersion {
                major: 3,
                minor: 2,
                patch: 1
            }),
            0
        );
        assert!(
            TrzszVersion {
                major: 1,
                minor: 1,
                patch: 1
            }
            .compare(&TrzszVersion {
                major: 1,
                minor: 2,
                patch: 0
            }) < 0
        );
    }
}
