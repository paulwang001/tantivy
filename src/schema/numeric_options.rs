use std::ops::BitOr;

use serde::{Deserialize, Serialize};

use crate::schema::flags::{FastFlag, IndexedFlag, SchemaFlagList, StoredFlag};

#[deprecated(since = "0.17.0", note = "Use NumericOptions instead.")]
/// Deprecated use [`NumericOptions`] instead.
pub type IntOptions = NumericOptions;

/// Define how an `u64`, `i64`, or `f64` field should be handled by tantivy.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(from = "NumericOptionsDeser")]
pub struct NumericOptions {
    indexed: bool,
    // This boolean has no effect if the field is not marked as indexed too.
    fieldnorms: bool, // This attribute only has an effect if indexed is true.
    fast: bool,
    stored: bool,
}

/// For backward compatibility we add an intermediary to interpret the
/// lack of fieldnorms attribute as "true" if and only if indexed.
///
/// (Downstream, for the moment, this attribute is not used anyway if not indexed...)
/// Note that: newly serialized `NumericOptions` will include the new attribute.
#[derive(Deserialize)]
struct NumericOptionsDeser {
    indexed: bool,
    #[serde(default)]
    fieldnorms: Option<bool>, // This attribute only has an effect if indexed is true.
    #[serde(default)]
    fast: bool,
    stored: bool,
}

impl From<NumericOptionsDeser> for NumericOptions {
    fn from(deser: NumericOptionsDeser) -> Self {
        NumericOptions {
            indexed: deser.indexed,
            fieldnorms: deser.fieldnorms.unwrap_or(deser.indexed),
            fast: deser.fast,
            stored: deser.stored,
        }
    }
}

impl NumericOptions {
    /// Returns true iff the value is stored in the doc store.
    pub fn is_stored(&self) -> bool {
        self.stored
    }

    /// Returns true iff the value is indexed and therefore searchable.
    pub fn is_indexed(&self) -> bool {
        self.indexed
    }

    /// Returns true iff the field has fieldnorm.
    pub fn fieldnorms(&self) -> bool {
        self.fieldnorms && self.indexed
    }

    /// Returns true iff the value is a fast field.
    pub fn is_fast(&self) -> bool {
        self.fast
    }

    /// Set the field as stored.
    ///
    /// Only the fields that are set as *stored* are
    /// persisted into the Tantivy's store.
    #[must_use]
    pub fn set_stored(mut self) -> NumericOptions {
        self.stored = true;
        self
    }

    /// Set the field as indexed.
    ///
    /// Setting an integer as indexed will generate
    /// a posting list for each value taken by the integer.
    ///
    /// This is required for the field to be searchable.
    #[must_use]
    pub fn set_indexed(mut self) -> NumericOptions {
        self.indexed = true;
        self
    }

    /// Set the field with fieldnorm.
    ///
    /// Setting an integer as fieldnorm will generate
    /// the fieldnorm data for it.
    #[must_use]
    pub fn set_fieldnorm(mut self) -> NumericOptions {
        self.fieldnorms = true;
        self
    }

    /// Set the field as a single-valued fast field.
    ///
    /// Fast fields are designed for random access.
    /// Access time are similar to a random lookup in an array.
    /// If more than one value is associated with a fast field, only the last one is
    /// kept.
    #[must_use]
    pub fn set_fast(mut self) -> NumericOptions {
        self.fast = true;
        self
    }
}

impl From<()> for NumericOptions {
    fn from(_: ()) -> NumericOptions {
        NumericOptions::default()
    }
}

impl From<FastFlag> for NumericOptions {
    fn from(_: FastFlag) -> Self {
        NumericOptions {
            indexed: false,
            fieldnorms: false,
            stored: false,
            fast: true,
        }
    }
}

impl From<StoredFlag> for NumericOptions {
    fn from(_: StoredFlag) -> Self {
        NumericOptions {
            indexed: false,
            fieldnorms: false,
            stored: true,
            fast: false,
        }
    }
}

impl From<IndexedFlag> for NumericOptions {
    fn from(_: IndexedFlag) -> Self {
        NumericOptions {
            indexed: true,
            fieldnorms: true,
            stored: false,
            fast: false,
        }
    }
}

impl<T: Into<NumericOptions>> BitOr<T> for NumericOptions {
    type Output = NumericOptions;

    fn bitor(self, other: T) -> NumericOptions {
        let other = other.into();
        NumericOptions {
            indexed: self.indexed | other.indexed,
            fieldnorms: self.fieldnorms | other.fieldnorms,
            stored: self.stored | other.stored,
            fast: self.fast | other.fast,
        }
    }
}

impl<Head, Tail> From<SchemaFlagList<Head, Tail>> for NumericOptions
where
    Head: Clone,
    Tail: Clone,
    Self: BitOr<Output = Self> + From<Head> + From<Tail>,
{
    fn from(head_tail: SchemaFlagList<Head, Tail>) -> Self {
        Self::from(head_tail.head) | Self::from(head_tail.tail)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_int_options_deser_if_fieldnorm_missing_indexed_true() {
        let json = r#"{
            "indexed": true,
            "stored": false
        }"#;
        let int_options: NumericOptions = serde_json::from_str(json).unwrap();
        assert_eq!(
            &int_options,
            &NumericOptions {
                indexed: true,
                fieldnorms: true,
                fast: false,
                stored: false
            }
        );
    }

    #[test]
    fn test_int_options_deser_if_fieldnorm_missing_indexed_false() {
        let json = r#"{
            "indexed": false,
            "stored": false
        }"#;
        let int_options: NumericOptions = serde_json::from_str(json).unwrap();
        assert_eq!(
            &int_options,
            &NumericOptions {
                indexed: false,
                fieldnorms: false,
                fast: false,
                stored: false
            }
        );
    }

    #[test]
    fn test_int_options_deser_if_fieldnorm_false_indexed_true() {
        let json = r#"{
            "indexed": true,
            "fieldnorms": false,
            "stored": false
        }"#;
        let int_options: NumericOptions = serde_json::from_str(json).unwrap();
        assert_eq!(
            &int_options,
            &NumericOptions {
                indexed: true,
                fieldnorms: false,
                fast: false,
                stored: false
            }
        );
    }

    #[test]
    fn test_int_options_deser_if_fieldnorm_true_indexed_false() {
        // this one is kind of useless, at least at the moment
        let json = r#"{
            "indexed": false,
            "fieldnorms": true,
            "stored": false
        }"#;
        let int_options: NumericOptions = serde_json::from_str(json).unwrap();
        assert_eq!(
            &int_options,
            &NumericOptions {
                indexed: false,
                fieldnorms: true,
                fast: false,
                stored: false
            }
        );
    }
}
