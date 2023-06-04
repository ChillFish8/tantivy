use crate::collector::top_collector::{TopCollector, TopSegmentCollector};
use crate::collector::{Collector, SegmentCollector};
use crate::{DocAddress, DocId, Score, SegmentReader};
use crate::schema::DocumentAccess;

pub(crate) struct CustomScoreTopCollector<D, TCustomScorer, TScore = Score> {
    custom_scorer: TCustomScorer,
    collector: TopCollector<D, TScore>,
}

impl<D, TCustomScorer, TScore> CustomScoreTopCollector<D, TCustomScorer, TScore>
where
    TScore: Clone + PartialOrd
{
    pub(crate) fn new(
        custom_scorer: TCustomScorer,
        collector: TopCollector<D, TScore>,
    ) -> Self {
        Self {
            custom_scorer,
            collector,
        }
    }
}

/// A custom segment scorer makes it possible to define any kind of score
/// for a given document belonging to a specific segment.
///
/// It is the segment local version of the [`CustomScorer`].
pub trait CustomSegmentScorer<TScore>: 'static {
    /// Computes the score of a specific `doc`.
    fn score(&mut self, doc: DocId) -> TScore;
}

/// `CustomScorer` makes it possible to define any kind of score.
///
/// The `CustomerScorer` itself does not make much of the computation itself.
/// Instead, it helps constructing `Self::Child` instances that will compute
/// the score at a segment scale.
pub trait CustomScorer<D, TScore>: Sync {
    /// Type of the associated [`CustomSegmentScorer`].
    type Child: CustomSegmentScorer<TScore>;
    /// Builds a child scorer for a specific segment. The child scorer is associated with
    /// a specific segment.
    fn segment_scorer(&self, segment_reader: &SegmentReader<D>) -> crate::Result<Self::Child>;
}

impl<D, TCustomScorer, TScore> Collector<D> for CustomScoreTopCollector<D, TCustomScorer, TScore>
where
    D: DocumentAccess,
    TCustomScorer: CustomScorer<D, TScore> + Send + Sync,
    TScore: 'static + PartialOrd + Clone + Send + Sync,
{
    type Fruit = Vec<(TScore, DocAddress)>;

    type Child = CustomScoreTopSegmentCollector<TCustomScorer::Child, TScore>;

    fn for_segment(
        &self,
        segment_local_id: u32,
        segment_reader: &SegmentReader<D>,
    ) -> crate::Result<Self::Child> {
        let segment_collector = self.collector.for_segment(segment_local_id, segment_reader);
        let segment_scorer = self.custom_scorer.segment_scorer(segment_reader)?;
        Ok(CustomScoreTopSegmentCollector {
            segment_collector,
            segment_scorer,
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, segment_fruits: Vec<Self::Fruit>) -> crate::Result<Self::Fruit> {
        self.collector.merge_fruits(segment_fruits)
    }
}

pub struct CustomScoreTopSegmentCollector<T, TScore>
where
    TScore: 'static + PartialOrd + Clone + Send + Sync + Sized,
    T: CustomSegmentScorer<TScore>,
{
    segment_collector: TopSegmentCollector<TScore>,
    segment_scorer: T,
}

impl<T, TScore> SegmentCollector for CustomScoreTopSegmentCollector<T, TScore>
where
    TScore: 'static + PartialOrd + Clone + Send + Sync,
    T: 'static + CustomSegmentScorer<TScore>,
{
    type Fruit = Vec<(TScore, DocAddress)>;

    fn collect(&mut self, doc: DocId, _score: Score) {
        let score = self.segment_scorer.score(doc);
        self.segment_collector.collect(doc, score);
    }

    fn harvest(self) -> Vec<(TScore, DocAddress)> {
        self.segment_collector.harvest()
    }
}

impl<D, F, TScore, T> CustomScorer<D, TScore> for F
where
    D: DocumentAccess,
    F: 'static + Send + Sync + Fn(&SegmentReader<D>) -> T,
    T: CustomSegmentScorer<TScore>,
{
    type Child = T;

    fn segment_scorer(&self, segment_reader: &SegmentReader<D>) -> crate::Result<Self::Child> {
        Ok((self)(segment_reader))
    }
}

impl<F, TScore> CustomSegmentScorer<TScore> for F
where F: 'static + FnMut(DocId) -> TScore
{
    fn score(&mut self, doc: DocId) -> TScore {
        (self)(doc)
    }
}
