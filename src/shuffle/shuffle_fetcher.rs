use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::{atomic, atomic::AtomicBool, Arc};

use crate::dependency::DepInfo;
use crate::env;
use crate::map_output_tracker::GetServerUriReq;
use crate::rdd::{
    get_encrypted_data, wrapper_get_cnt_per_partition, wrapper_secure_execute,
    wrapper_set_cnt_per_partition, AccArg, ItemE,
};
use crate::serializable_traits::Data;
use crate::shuffle::shuffle_manager::MAX_LEN;
use crate::shuffle::*;
use futures::future;
use hyper::body::Bytes;
use hyper::{client::Client, Uri};
use tokio::sync::Mutex;

/// Parallel shuffle fetcher.
pub(crate) struct ShuffleFetcher;

impl ShuffleFetcher {
    pub async fn fetch<K: Data, V: Data>(
        shuffle_id: usize,
        reduce_id: usize,
    ) -> Result<impl Iterator<Item = (K, V)>> {
        log::debug!("inside fetch function");
        let mut inputs_by_uri = HashMap::new();
        let server_uris = env::Env::get()
            .map_output_tracker
            .get_server_uris(&GetServerUriReq::PrevStage(shuffle_id))
            .await
            .map_err(|err| ShuffleError::FailFetchingShuffleUris {
                source: Box::new(err),
            })?;
        log::debug!(
            "server uris for shuffle id #{}: {:?}",
            shuffle_id,
            server_uris
        );
        for (index, server_uri) in server_uris.into_iter().enumerate() {
            inputs_by_uri
                .entry(server_uri)
                .or_insert_with(Vec::new)
                .push(index);
        }
        let mut server_queue = Vec::new();
        let mut total_results = 0;
        for (key, value) in inputs_by_uri {
            total_results += value.len();
            server_queue.push((key, value));
        }
        log::debug!(
            "servers for shuffle id #{:?} & reduce id #{}: {:?}",
            shuffle_id,
            reduce_id,
            server_queue
        );
        let num_tasks = server_queue.len();
        let server_queue = Arc::new(Mutex::new(server_queue));
        let failure = Arc::new(AtomicBool::new(false));
        let mut tasks = Vec::with_capacity(num_tasks);
        for _ in 0..num_tasks {
            let server_queue = server_queue.clone();
            let failure = failure.clone();
            // spawn a future for each expected result set
            let task = async move {
                let client = Client::builder().http2_only(true).build_http::<Body>();
                let mut lock = server_queue.lock().await;
                if let Some((server_uri, input_ids)) = lock.pop() {
                    let server_uri = format!("{}/shuffle/{}", server_uri, shuffle_id);
                    let mut chunk_uri_str = String::with_capacity(server_uri.len() + 12);
                    chunk_uri_str.push_str(&server_uri);
                    let mut shuffle_chunks = Vec::with_capacity(input_ids.len());
                    for input_id in input_ids.clone() {
                        if failure.load(atomic::Ordering::Acquire) {
                            // Abort early since the work failed in an other future
                            return Err(ShuffleError::Other);
                        }
                        log::debug!("inside parallel fetch {}", input_id);
                        let mut section_id: usize = 0;
                        let mut final_bytes = Vec::<u8>::new();
                        loop {
                            let chunk_uri = ShuffleFetcher::make_chunk_uri(
                                &server_uri,
                                &mut chunk_uri_str,
                                input_id,
                                reduce_id,
                                section_id,
                            )?;
                            let data_bytes = {
                                let res = client.get(chunk_uri).await?;
                                hyper::body::to_bytes(res.into_body()).await
                            };
                            if let Ok(bytes) = data_bytes {
                                let len = bytes.len();
                                final_bytes.extend_from_slice(&bytes[..]);
                                if len < MAX_LEN {
                                    break;
                                }
                            } else {
                                failure.store(true, atomic::Ordering::Release);
                                return Err(ShuffleError::FailedFetchOp);
                            }
                            section_id += 1;
                        }
                        let deser_data = bincode::deserialize::<Vec<(K, V)>>(&final_bytes)?;
                        shuffle_chunks.push(deser_data);
                    }
                    Ok::<Box<dyn Iterator<Item = (K, V)> + Send>, _>(Box::new(
                        shuffle_chunks.into_iter().flatten(),
                    ))
                } else {
                    Ok::<Box<dyn Iterator<Item = (K, V)> + Send>, _>(Box::new(std::iter::empty()))
                }
            };
            tasks.push(tokio::spawn(task));
        }
        log::debug!("total_results fetch results: {}", total_results);
        let task_results = future::join_all(tasks.into_iter()).await;
        let results = task_results.into_iter().fold(
            Ok(Vec::<(K, V)>::with_capacity(total_results)),
            |curr, res| {
                if let Ok(mut curr) = curr {
                    if let Ok(Ok(res)) = res {
                        curr.extend(res);
                        Ok(curr)
                    } else {
                        Err(ShuffleError::FailedFetchOp)
                    }
                } else {
                    Err(ShuffleError::FailedFetchOp)
                }
            },
        )?;
        Ok(results.into_iter())
    }

    pub async fn fetch_sync(part_group: (usize, usize, usize)) -> Result<()> {
        log::debug!(
            "begin fetch_sync, part_group (stage_id = {:?}, part_id_offset = {:?}, cur_num_splits = {:?})",
            part_group.0,
            part_group.1,
            part_group.2,
        );
        env::Env::get()
            .map_output_tracker
            .check_ready(part_group)
            .await
            .map_err(|_| ShuffleError::FailCheckingReady)?;
        log::debug!(
            "finish fetch_sync, part_group (stage_id = {:?}, part_id_offset = {:?}, cur_num_splits = {:?})",
            part_group.0,
            part_group.1,
            part_group.2,
        );
        Ok(())
    }

    pub async fn fetch_cnt(
        //(stage_id, part_id_offset, cur num split)
        part_group: (usize, usize, usize),
        mut cnt: usize,
    ) -> Result<usize> {
        cnt = env::Env::get()
            .map_output_tracker
            .get_max_cnt(part_group, cnt)
            .await
            .map_err(|_| ShuffleError::FailGettingMaxCnt)?;
        Ok(cnt)
    }

    pub async fn secure_fetch(
        req: GetServerUriReq,
        reduce_id: usize,
        fetch_from: usize,
    ) -> Result<impl Iterator<Item = Vec<ItemE>>> {
        log::debug!("inside fetch function");
        let mut server_uris = env::Env::get()
            .map_output_tracker
            .get_server_uris(&req)
            .await
            .map_err(|err| ShuffleError::FailFetchingShuffleUris {
                source: Box::new(err),
            })?;
        if let GetServerUriReq::CurStage((_, s, n)) = req {
            server_uris = server_uris[s..s + n].to_vec();
        }
        //get the uri of current worker
        let uri = env::Env::get().shuffle_manager.get_server_uri();
        assert_eq!(uri, server_uris[reduce_id]);
        log::debug!(
            "server uris for req #{:?}, reduce_id #{}: {:?}",
            req,
            reduce_id,
            server_uris
        );
        let total_results = server_uris.len();
        let server_queue = if fetch_from == usize::MAX {
            //fetch from all workers
            let mut inputs_by_uri = HashMap::new();
            for (index, server_uri) in server_uris.into_iter().enumerate() {
                inputs_by_uri
                    .entry(server_uri)
                    .or_insert_with(Vec::new)
                    .push(index);
            }
            let mut server_queue = Vec::new();
            let mut total_results_ = 0;
            for (key, value) in inputs_by_uri {
                total_results_ += value.len();
                server_queue.push((key, value));
            }
            assert_eq!(total_results_, total_results);

            server_queue
        } else {
            vec![(server_uris[fetch_from].clone(), vec![fetch_from])]
        };

        let num_tasks = server_queue.len();
        let server_queue = Arc::new(Mutex::new(server_queue));
        let failure = Arc::new(AtomicBool::new(false));
        let mut tasks = Vec::with_capacity(num_tasks);
        for _ in 0..num_tasks {
            let server_queue = server_queue.clone();
            let failure = failure.clone();
            // spawn a future for each expected result set
            let req = req.clone();
            let task = async move {
                let client = Client::builder().http2_only(true).build_http::<Body>();
                let mut lock = server_queue.lock().await;
                if let Some((server_uri, input_ids)) = lock.pop() {
                    let server_uri = match req {
                        GetServerUriReq::CurStage(part_group) => {
                            format!(
                                "{}/sort/{}/{}/{}",
                                server_uri, part_group.0, part_group.1, part_group.2
                            )
                        }
                        GetServerUriReq::PrevStage(shuffle_id) => {
                            format!("{}/shuffle/{}", server_uri, shuffle_id)
                        }
                    };
                    let mut chunk_uri_str = String::with_capacity(server_uri.len() + 12);
                    chunk_uri_str.push_str(&server_uri);
                    let mut shuffle_chunks = Vec::with_capacity(input_ids.len());
                    for input_id in input_ids.clone() {
                        if failure.load(atomic::Ordering::Acquire) {
                            // Abort early since the work failed in an other future
                            return Err(ShuffleError::Other);
                        }
                        log::debug!(
                            "inside parallel fetch, stage {:?}/{}/{}",
                            req,
                            input_id,
                            reduce_id
                        );
                        let mut section_id: usize = 0;
                        let mut final_bytes = Vec::<u8>::new();
                        loop {
                            let chunk_uri = ShuffleFetcher::make_chunk_uri(
                                &server_uri,
                                &mut chunk_uri_str,
                                input_id,
                                reduce_id,
                                section_id,
                            )?;
                            let data_bytes = {
                                let res = client.get(chunk_uri).await?;
                                hyper::body::to_bytes(res.into_body()).await
                            };
                            if let Ok(bytes) = data_bytes {
                                let len = bytes.len();
                                final_bytes.extend_from_slice(&bytes[..]);
                                if len < MAX_LEN {
                                    break;
                                }
                            } else {
                                failure.store(true, atomic::Ordering::Release);
                                return Err(ShuffleError::FailedFetchOp);
                            }
                            section_id += 1;
                        }
                        let deser_data = bincode::deserialize::<Vec<ItemE>>(&final_bytes)?;
                        shuffle_chunks.push(deser_data);
                    }
                    Ok::<Box<dyn Iterator<Item = Vec<ItemE>> + Send>, _>(Box::new(
                        shuffle_chunks.into_iter(),
                    ))
                } else {
                    Ok::<Box<dyn Iterator<Item = Vec<ItemE>> + Send>, _>(Box::new(
                        std::iter::empty(),
                    ))
                }
            };
            tasks.push(tokio::spawn(task));
        }
        log::debug!("total_results fetch results: {}", total_results);
        let task_results = future::join_all(tasks.into_iter()).await;
        let results = task_results.into_iter().fold(
            Ok(Vec::<Vec<ItemE>>::with_capacity(total_results)),
            |curr, res| {
                if let Ok(mut curr) = curr {
                    if let Ok(Ok(res)) = res {
                        curr.extend(res);
                        Ok(curr)
                    } else {
                        Err(ShuffleError::FailedFetchOp)
                    }
                } else {
                    Err(ShuffleError::FailedFetchOp)
                }
            },
        )?;
        Ok(results.into_iter())
    }

    pub fn fetch_sort(
        data: Vec<Vec<ItemE>>,
        stage_id: usize,
        acc_arg: &mut AccArg,
        is_shuffle_list: Vec<u8>,
    ) -> Vec<ItemE> {
        assert_eq!(is_shuffle_list.len(), 3);
        let cur_rdd_ids = vec![*acc_arg.rdd_ids.last().unwrap()];
        let cur_op_ids = vec![*acc_arg.op_ids.last().unwrap()];
        let reduce_id = *acc_arg.part_ids.last().unwrap();
        let cur_part_ids = vec![reduce_id];
        let part_id_offset = if acc_arg.part_ids[0] == usize::MAX {
            acc_arg.part_ids[1] - acc_arg.part_ids.last().unwrap()
        } else {
            acc_arg.part_ids[0] - acc_arg.part_ids.last().unwrap()
        };
        let num_splits = *acc_arg.split_nums.last().unwrap();
        let part_group = (stage_id, part_id_offset, num_splits);

        let cnt_per_partition = {
            let mut cnt_per_partition = wrapper_get_cnt_per_partition(cur_op_ids[0], reduce_id);
            //meet the requirement
            let limit = 2 * (num_splits - 1) * (num_splits - 1);
            if cnt_per_partition < limit {
                cnt_per_partition = limit;
            }
            //ensure that cnt % 2 == 0 and cnt % num_splits == 0
            let d = match num_splits % 2 {
                0 => num_splits,
                1 => 2 * num_splits,
                _ => unreachable!(),
            };
            let r = cnt_per_partition % d;
            cnt_per_partition += (d - r) % d;
            cnt_per_partition = futures::executor::block_on(ShuffleFetcher::fetch_cnt(
                (stage_id, part_id_offset, num_splits),
                cnt_per_partition,
            ))
            .unwrap();
            assert!(
                cnt_per_partition % 2 == 0 && cnt_per_partition % num_splits == 0 //&& cnt_per_partition >= 2 * (num_splits - 1) * (num_splits - 1)
            );
            wrapper_set_cnt_per_partition(cur_op_ids[0], reduce_id, cnt_per_partition);
            cnt_per_partition
        };
        log::debug!("finish get max cnt: {:?}", cnt_per_partition);

        //step 3: sort + step 4: shuffle (untranspose)
        let data = {
            acc_arg.get_enclave_lock();
            let dep_info = DepInfo::padding_new(is_shuffle_list[0]);
            let result_ptr = wrapper_secure_execute(
                &cur_rdd_ids,
                &cur_op_ids,
                &cur_part_ids,
                Default::default(),
                dep_info,
                &data,
                &acc_arg.captured_vars,
            );
            let mut buckets =
                get_encrypted_data::<Vec<ItemE>>(cur_op_ids[0], dep_info, result_ptr as *mut u8);
            acc_arg.free_enclave_lock();
            buckets.resize(num_splits, Vec::new());
            for (i, bucket) in buckets.into_iter().enumerate() {
                let ser_bytes = bincode::serialize(&bucket).unwrap();
                log::debug!(
                    "during step 4. bucket #{} in stage id #{}, partition #{}: {:?}",
                    i,
                    stage_id,
                    reduce_id,
                    bucket
                );
                env::SORT_CACHE.insert((part_group, reduce_id, i), ser_bytes);
            }
            futures::executor::block_on(ShuffleFetcher::fetch_sync(part_group)).unwrap();
            let fut = ShuffleFetcher::secure_fetch(
                GetServerUriReq::CurStage(part_group),
                reduce_id,
                usize::MAX,
            );
            futures::executor::block_on(fut)
                .unwrap()
                .filter(|x| !x.is_empty())
                .collect::<Vec<_>>()
        };
        log::debug!(
            "step 4 finished. partition = {:?}, data = {:?}",
            reduce_id,
            data
        );

        //step 5: sort + step 6: shuffle (shift)
        let data = {
            acc_arg.get_enclave_lock();
            let dep_info = DepInfo::padding_new(is_shuffle_list[1]);
            let result_ptr = wrapper_secure_execute(
                &cur_rdd_ids,
                &cur_op_ids,
                &cur_part_ids,
                Default::default(),
                dep_info,
                &data,
                &acc_arg.captured_vars,
            );
            let mut buckets =
                get_encrypted_data::<Vec<ItemE>>(cur_op_ids[0], dep_info, result_ptr as *mut u8);
            acc_arg.free_enclave_lock();
            if buckets.is_empty() {
                buckets.resize(2, Vec::new());
            } else {
                assert_eq!(buckets.len(), 2);
            }
            let bucket = buckets.pop().unwrap();
            let ser_bytes = bincode::serialize(&bucket).unwrap();
            log::debug!(
                "during step 6. stage id #{}, input partition #{}, output partition #{}, {:?}",
                stage_id,
                reduce_id,
                (reduce_id + 1) % num_splits,
                bucket,
            );
            env::SORT_CACHE.insert(
                (part_group, reduce_id, (reduce_id + 1) % num_splits),
                ser_bytes,
            );
            futures::executor::block_on(ShuffleFetcher::fetch_sync(part_group)).unwrap();
            let fut = ShuffleFetcher::secure_fetch(
                GetServerUriReq::CurStage(part_group),
                reduce_id,
                (reduce_id + num_splits - 1) % num_splits,
            );
            buckets.append(
                &mut futures::executor::block_on(fut)
                    .unwrap()
                    .collect::<Vec<_>>(),
            );
            buckets
                .into_iter()
                .filter(|x| !x.is_empty())
                .collect::<Vec<_>>()
        };
        log::debug!("step 6 finished. partition #{}, data {:?}", reduce_id, data);
        //step 7: sort + step 8: shuffle (unshift)
        let res = {
            acc_arg.get_enclave_lock();
            let dep_info = DepInfo::padding_new(is_shuffle_list[2]);
            let result_ptr = wrapper_secure_execute(
                &cur_rdd_ids,
                &cur_op_ids,
                &cur_part_ids,
                Default::default(),
                dep_info,
                &data,
                &acc_arg.captured_vars,
            );
            let mut buckets =
                get_encrypted_data::<Vec<ItemE>>(cur_op_ids[0], dep_info, result_ptr as *mut u8);
            acc_arg.free_enclave_lock();
            if buckets.is_empty() {
                buckets.resize(2, Vec::new());
            } else {
                assert_eq!(buckets.len(), 2);
            }
            if reduce_id != 0 {
                buckets.swap(0, 1);
            }
            let bucket = buckets.pop().unwrap();
            let ser_bytes = bincode::serialize(&bucket).unwrap();
            log::debug!(
                "during step 8. stage id #{}, input partition #{}, output partition #{}, {:?}",
                stage_id,
                reduce_id,
                (reduce_id + num_splits - 1) % num_splits,
                bucket,
            );
            env::SORT_CACHE.insert(
                (
                    part_group,
                    reduce_id,
                    (reduce_id + num_splits - 1) % num_splits,
                ),
                ser_bytes,
            );

            futures::executor::block_on(ShuffleFetcher::fetch_sync(part_group)).unwrap();
            let fut = ShuffleFetcher::secure_fetch(
                GetServerUriReq::CurStage(part_group),
                reduce_id,
                (reduce_id + 1) % num_splits,
            );
            buckets
                .pop()
                .unwrap()
                .into_iter()
                .chain(
                    futures::executor::block_on(fut)
                        .unwrap()
                        .into_iter()
                        .flatten(),
                )
                .filter(|x| !x.is_empty())
                .collect::<Vec<_>>()
        };
        log::debug!("step 8 finished. partition #{}, {:?}", reduce_id, res);
        //assert!(res.is_sorted_by_key(|i| i.0.clone()));
        res
    }

    fn make_chunk_uri(
        base: &str,
        chunk: &mut String,
        input_id: usize,
        reduce_id: usize,
        section_id: usize,
    ) -> Result<Uri> {
        let input_id = input_id.to_string();
        let reduce_id = reduce_id.to_string();
        let path_tail = [
            "/".to_string(),
            input_id,
            "/".to_string(),
            reduce_id,
            "/".to_string(),
            section_id.to_string(),
        ]
        .concat();
        if chunk.len() == base.len() {
            chunk.push_str(&path_tail);
        } else {
            chunk.replace_range(base.len().., &path_tail);
        }
        Ok(Uri::try_from(chunk.as_str())?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(core_threads = 4)]
    async fn fetch_ok() -> StdResult<(), Box<dyn std::error::Error + 'static>> {
        {
            let addr = format!(
                "http://127.0.0.1:{}",
                env::Env::get().shuffle_manager.server_port
            );
            let servers = &env::Env::get().map_output_tracker.server_uris;
            servers.insert(11000, vec![Some(addr)]);

            let data = vec![(0i32, "example data".to_string())];
            let serialized_data = bincode::serialize(&data).unwrap();
            env::SHUFFLE_CACHE.insert((11000, 0, 11001), serialized_data);
        }

        let result: Vec<(i32, String)> = ShuffleFetcher::fetch(11000, 11001)
            .await?
            .into_iter()
            .collect();
        assert_eq!(result[0].0, 0);
        assert_eq!(result[0].1, "example data");

        Ok(())
    }

    #[tokio::test(core_threads = 4)]
    async fn fetch_failure() -> StdResult<(), Box<dyn std::error::Error + 'static>> {
        {
            let addr = format!(
                "http://127.0.0.1:{}",
                env::Env::get().shuffle_manager.server_port
            );
            let servers = &env::Env::get().map_output_tracker.server_uris;
            servers.insert(10000, vec![Some(addr)]);

            let data = "corrupted data";
            let serialized_data = bincode::serialize(&data).unwrap();
            env::SHUFFLE_CACHE.insert((10000, 0, 10001), serialized_data);
        }

        let err = ShuffleFetcher::fetch::<i32, String>(10000, 10001).await;
        assert!(err.is_err());

        Ok(())
    }

    #[test]
    fn build_shuffle_id_uri() -> StdResult<(), Box<dyn std::error::Error + 'static>> {
        let base = "http://127.0.0.1/shuffle";
        let mut chunk = base.to_owned();

        let uri0 = ShuffleFetcher::make_chunk_uri(base, &mut chunk, 0, 1, 0)?;
        let expected = format!("{}/0/1", base);
        assert_eq!(expected.as_str(), uri0);

        let uri1 = ShuffleFetcher::make_chunk_uri(base, &mut chunk, 123, 123, 0)?;
        let expected = format!("{}/123/123", base);
        assert_eq!(expected.as_str(), uri1);

        Ok(())
    }
}
