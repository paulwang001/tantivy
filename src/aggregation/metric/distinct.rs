use std::collections::BTreeSet;
use std::fmt::Debug;

use fastfield_codecs::Column;
use serde::{Deserialize, Serialize};

use crate::schema::Type;
use crate::DocId;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// A single-value metric aggregation that computes the average of numeric values that are
/// extracted from the aggregated documents.
/// See [super::SingleMetricResult] for return value.
///
/// # JSON Format
/// ```json
/// {
///     "distinct": {
///         "field": "score",
///     }
/// }
/// ```
pub struct DistinctAggregation {
    /// The field name to compute the stats on.
    pub field: String,
}
impl DistinctAggregation {
    /// Create new DistinctAggregation from a field.
    pub fn from_field_name(field_name: String) -> Self {
        DistinctAggregation { field: field_name }
    }
    /// Return the field name.
    pub fn field_name(&self) -> &str {
        &self.field
    }
}

#[derive(Clone, PartialEq)]
pub(crate) struct SegmentDistinctCollector {
    pub data: IntermediateDistinct,
    field_type: Type,
}

impl Debug for SegmentDistinctCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DistinctCollector")
            .field("data", &self.data)
            .finish()
    }
}

impl SegmentDistinctCollector {
    pub fn from_req(field_type: Type) -> Self {
        Self {
            field_type,
            data: Default::default(),
        }
    }
    pub(crate) fn collect_block(&mut self, doc: &[DocId], field: &dyn Column<u64>) {
        let mut iter = doc.chunks_exact(4);

        for docs in iter.by_ref() {
            let val1 = field.get_val(docs[0]);
            let val2 = field.get_val(docs[1]);
            let val3 = field.get_val(docs[2]);
            let val4 = field.get_val(docs[3]);
            self.data.collect(val1);
            self.data.collect(val2);
            self.data.collect(val3);
            self.data.collect(val4);
        }
        for &doc in iter.remainder() {
            let val = field.get_val(doc);
            self.data.collect(val);
        }
    }
}

/// Contains mergeable version of average data.
#[derive(Default, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IntermediateDistinct {
    pub(crate) terms: BTreeSet<u64>,
    pub(crate) term_count: u64,
}

impl IntermediateDistinct {
    pub(crate) fn from_collector(collector: SegmentDistinctCollector) -> Self {
        collector.data
    }

    /// Merge average data into this instance.
    pub fn merge_fruits(&mut self, other: IntermediateDistinct) {
        self.terms.extend(other.terms);
        self.term_count += other.term_count;
    }
    /// compute final result
    pub fn finalize(&self) -> Option<f64> {
        if self.term_count == 0 {
            None
        } else {
            Some(self.term_count as f64)
        }
    }
    #[inline]
    fn collect(&mut self, val: u64) {
        if self.terms.insert(val) {
            self.term_count += 1;
        }
    }
}
