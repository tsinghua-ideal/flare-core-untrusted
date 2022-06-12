use std::fmt::Display;
use std::marker::PhantomData;
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Instant;

use crate::dependency::DepInfo;
use crate::env;
use crate::rdd::{ItemE, OpId, Rdd, STAGE_LOCK};
use crate::scheduler::{Task, TaskBase, TaskContext};
use crate::serializable_traits::{AnyData, Data};
use crate::shuffle::ShuffleFetcher;
use crate::SerBox;
use serde_derive::{Deserialize, Serialize};
use serde_traitobject::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub(crate) struct ResultTask<T: Data, U: Data, F>
where
    F: Fn(
            (
                TaskContext,
                (Box<dyn Iterator<Item = T>>, Box<dyn Iterator<Item = ItemE>>),
            ),
        ) -> U
        + 'static
        + Send
        + Sync
        + Serialize
        + Deserialize
        + Clone,
{
    pub task_id: usize,
    pub run_id: usize,
    pub stage_id: usize,
    pinned: bool,
    #[serde(with = "serde_traitobject")]
    pub rdd: Arc<dyn Rdd<Item = T>>,
    pub action_id: Option<OpId>,
    pub func: Arc<F>,
    pub partition: usize,
    pub locs: Vec<Ipv4Addr>,
    pub output_id: usize,
}

impl<T: Data, U: Data, F> Display for ResultTask<T, U, F>
where
    F: Fn(
            (
                TaskContext,
                (Box<dyn Iterator<Item = T>>, Box<dyn Iterator<Item = ItemE>>),
            ),
        ) -> U
        + 'static
        + Send
        + Sync
        + Serialize
        + Deserialize
        + Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ResultTask({}, {})", self.stage_id, self.partition)
    }
}

impl<T: Data, U: Data, F> ResultTask<T, U, F>
where
    F: Fn(
            (
                TaskContext,
                (Box<dyn Iterator<Item = T>>, Box<dyn Iterator<Item = ItemE>>),
            ),
        ) -> U
        + 'static
        + Send
        + Sync
        + Serialize
        + Deserialize
        + Clone,
{
    pub fn clone(&self) -> Self {
        ResultTask {
            task_id: self.task_id,
            run_id: self.run_id,
            stage_id: self.stage_id,
            pinned: self.rdd.is_pinned(),
            rdd: self.rdd.clone(),
            action_id: self.action_id.clone(),
            func: self.func.clone(),
            partition: self.partition,
            locs: self.locs.clone(),
            output_id: self.output_id,
        }
    }
}

impl<T: Data, U: Data, F> ResultTask<T, U, F>
where
    F: Fn(
            (
                TaskContext,
                (Box<dyn Iterator<Item = T>>, Box<dyn Iterator<Item = ItemE>>),
            ),
        ) -> U
        + 'static
        + Send
        + Sync
        + Serialize
        + Deserialize
        + Clone,
{
    pub fn new(
        task_id: usize,
        run_id: usize,
        stage_id: usize,
        rdd: Arc<dyn Rdd<Item = T>>,
        action_id: Option<OpId>,
        func: Arc<F>,
        partition: usize,
        locs: Vec<Ipv4Addr>,
        output_id: usize,
    ) -> Self {
        ResultTask {
            task_id,
            run_id,
            stage_id,
            pinned: rdd.is_pinned(),
            rdd,
            action_id,
            func,
            partition,
            locs,
            output_id,
        }
    }
}

impl<T: Data, U: Data, F> TaskBase for ResultTask<T, U, F>
where
    F: Fn(
            (
                TaskContext,
                (Box<dyn Iterator<Item = T>>, Box<dyn Iterator<Item = ItemE>>),
            ),
        ) -> U
        + 'static
        + Send
        + Sync
        + Serialize
        + Deserialize
        + Clone,
{
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

impl<T: Data, U: Data, F> Task for ResultTask<T, U, F>
where
    F: Fn(
            (
                TaskContext,
                (Box<dyn Iterator<Item = T>>, Box<dyn Iterator<Item = ItemE>>),
            ),
        ) -> U
        + 'static
        + Send
        + Sync
        + Serialize
        + Deserialize
        + Clone,
{
    fn run(&self, id: usize) -> SerBox<dyn AnyData> {
        log::debug!("resulttask runs");
        let rdd_id = self.rdd.get_rdd_id();
        let num_splits = self.rdd.number_of_splits();
        STAGE_LOCK.insert_stage((rdd_id, rdd_id, 0), self.task_id);
        STAGE_LOCK.set_num_splits((rdd_id, rdd_id, 0), num_splits);
        let split = self.rdd.splits()[self.partition].clone();
        let context = TaskContext::new(self.stage_id, self.partition, id);

        let now = Instant::now();

        let res = SerBox::new((self.func)((
            context,
            match self.rdd.get_secure() {
                true => (Box::new(Vec::new().into_iter()), {
                    STAGE_LOCK.get_stage_lock((rdd_id, rdd_id, 0));
                    let dep_info = DepInfo::padding_new(0);
                    let res = match self.rdd.secure_iterator(
                        self.stage_id,
                        split,
                        dep_info,
                        self.action_id.clone(),
                    ) {
                        Ok(r) => r,
                        Err(_) => Box::new(Vec::new().into_iter()),
                    };
                    STAGE_LOCK.free_stage_lock();
                    //sync in order to clear the sort cache
                    futures::executor::block_on(ShuffleFetcher::fetch_sync((
                        self.stage_id,
                        0,
                        num_splits,
                    )))
                    .unwrap();
                    //clear the sort cache
                    env::SORT_CACHE.retain(|k, _| k.0 .0 != self.stage_id);

                    res
                }),
                false => (
                    match self.rdd.iterator(split.clone()) {
                        Ok(r) => r,
                        Err(_) => Box::new(Vec::new().into_iter()),
                    },
                    Box::new(Vec::new().into_iter()),
                ),
            },
        ))) as SerBox<dyn AnyData>;
        let dur = now.elapsed().as_nanos() as f64 * 1e-9;
        println!("result_task {:?}s", dur);
        STAGE_LOCK.remove_stage((rdd_id, rdd_id, 0), self.task_id);
        res
    }
}
