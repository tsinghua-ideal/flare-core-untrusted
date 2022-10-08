use std::fmt::Display;
use std::net::Ipv4Addr;
use std::sync::Arc;

use crate::dependency::ShuffleDependencyTrait;
use crate::env;
use crate::rdd::{RddBase, STAGE_LOCK};
use crate::scheduler::{Task, TaskBase};
use crate::serializable_traits::AnyData;
use crate::shuffle::*;
use serde_derive::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct ShuffleMapTask {
    pub task_id: usize,
    pub run_id: usize,
    pub stage_id: usize,
    pinned: bool,
    #[serde(with = "serde_traitobject")]
    pub dep: Arc<dyn ShuffleDependencyTrait>,
    pub partition: usize,
    pub locs: Vec<Ipv4Addr>,
}

impl ShuffleMapTask {
    pub fn new(
        task_id: usize,
        run_id: usize,
        stage_id: usize,
        dep: Arc<dyn ShuffleDependencyTrait>,
        partition: usize,
        locs: Vec<Ipv4Addr>,
    ) -> Self {
        ShuffleMapTask {
            task_id,
            run_id,
            stage_id,
            pinned: dep.get_rdd_base().is_pinned(),
            dep,
            partition,
            locs,
        }
    }
}

impl Display for ShuffleMapTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ShuffleMapTask({:?}, {:?})",
            self.stage_id, self.partition
        )
    }
}

impl TaskBase for ShuffleMapTask {
    fn get_run_id(&self) -> usize {
        self.run_id
    }

    fn get_stage_id(&self) -> usize {
        self.stage_id
    }

    fn get_task_id(&self) -> usize {
        self.task_id
    }

    fn is_pinned(&self) -> bool {
        self.pinned
    }

    fn preferred_locations(&self) -> Vec<Ipv4Addr> {
        self.locs.clone()
    }

    fn generation(&self) -> Option<i64> {
        Some(env::Env::get().map_output_tracker.get_generation())
    }
}

impl Task for ShuffleMapTask {
    fn run(&self, _id: usize) -> SerBox<dyn AnyData> {
        let dep_info = self.dep.get_dep_info();
        let rdd_base = self.dep.get_rdd_base();
        let rdd_id_pair = (
            dep_info.child_rdd_id,
            dep_info.parent_rdd_id,
            dep_info.identifier,
        );
        let num_splits = rdd_base.number_of_splits();
        STAGE_LOCK.insert_stage(rdd_id_pair, self.task_id);
        STAGE_LOCK.set_num_splits(rdd_id_pair, num_splits);
        let res = SerBox::new(
            self.dep
                .do_shuffle_task(self.stage_id, rdd_base, self.partition),
        ) as SerBox<dyn AnyData>;
        STAGE_LOCK.remove_stage(rdd_id_pair, self.task_id);
        //sync in order to clear the sort cache
        futures::executor::block_on(ShuffleFetcher::fetch_sync(
            (self.stage_id, 0, num_splits),
            self.partition,
            Vec::new(),
        ))
        .unwrap();
        //clear the sort cache
        env::SORT_CACHE.retain(|k, _| k.0 .0 != self.stage_id);
        res
    }
}
