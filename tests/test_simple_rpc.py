import logging
import os
import threading
from time import sleep

import pandas as pd
import pytest
import redis
from pandas.util.testing import assert_frame_equal
from parquery.write import df_to_parquet

import parqueryd.config
from parqueryd.controller import ControllerNode
from parqueryd.rpc import RPC
from parqueryd.util import get_my_ip
from parqueryd.worker import WorkerNode, DownloaderNode

TEST_REDIS = 'redis://redis:6379/0'
NR_SHARDS = 5


@pytest.fixture(scope='module')
def taxi_df():
    taxi_df = pd.read_csv(
        '/srv/datasets/yellow_tripdata_2016-01.csv',
        parse_dates=['tpep_pickup_datetime', 'tpep_dropoff_datetime']
    )
    yield taxi_df


class DummyDownloader(DownloaderNode):

    def download_file(self, ticket, filename):
        self.file_downloader_progress(ticket, filename, 'DONE')


@pytest.fixture(scope='module')
def rpc():
    redis_server = redis.from_url(TEST_REDIS)
    redis_server.flushdb()
    controller = ControllerNode(redis_url=TEST_REDIS, loglevel=logging.DEBUG)
    controller_thread = threading.Thread(target=controller.go)
    controller_thread.daemon = True
    controller_thread.start()
    # Sleep 5 seconds, just to make sure all connections are properly established
    sleep(5)

    worker = WorkerNode(redis_url=TEST_REDIS, loglevel=logging.DEBUG,
                        restart_check=False)
    worker_thread = threading.Thread(target=worker.go)
    worker_thread.daemon = True
    worker_thread.start()
    # Sleep 5 seconds, just to make sure all connections are properly established
    sleep(5)

    downloader = DummyDownloader(redis_url=TEST_REDIS, loglevel=logging.DEBUG)
    downloader_thread = threading.Thread(target=downloader.go)
    downloader_thread.daemon = True
    downloader_thread.start()
    sleep(5)

    rpc = parqueryd.rpc.RPC(timeout=100, redis_url=TEST_REDIS, loglevel=logging.DEBUG)
    yield rpc

    # shutdown the controller and worker
    controller.running = False
    worker.running = False
    downloader.running = False


@pytest.fixture(scope='module')
def shards(taxi_df):
    shard_filenames = []
    single_parquet = os.path.join(parqueryd.config.DEFAULT_DATA_DIR, 'yellow_tripdata_2016-01.parquet')
    df_to_parquet(taxi_df, single_parquet)
    shard_filenames.append(single_parquet)

    NR_SHARDS = 10
    step = len(taxi_df) // NR_SHARDS
    remainder = len(taxi_df) - step * NR_SHARDS
    count = 0
    for idx in range(0, len(taxi_df), step):
        if count == NR_SHARDS - 1 and remainder >= 0:
            step += remainder
        elif count == NR_SHARDS:
            break

        shard_file = os.path.join(parqueryd.config.DEFAULT_DATA_DIR, 'yellow_tripdata_2016-01-%s.parquet' % count)
        df_to_parquet(taxi_df[idx:idx + step], shard_file)
        shard_filenames.append(shard_file)
        count += 1

    yield shard_filenames


def test_rpc_info(rpc):
    result = rpc.info()

    address = result['address']
    my_ip = get_my_ip()
    assert my_ip == address.split(':')[1].replace('//', '')

    workers = result['workers']
    assert len(workers) == 2
    worker_node = list(v for v in workers.values() if v['workertype'] == 'calc')[0]

    node = result['node']
    assert node == worker_node['node']

    downloader_node = list(v for v in workers.values() if v['workertype'] == 'download')[0]
    assert node == downloader_node['node']

    assert result['others'] == {}


def test_download(rpc):
    ticket_nr = rpc.download(filenames=['test_download.parquet'], bucket='parquet', wait=False)
    redis_server = redis.from_url(TEST_REDIS)
    download_entries = redis_server.hgetall(parqueryd.config.REDIS_TICKET_KEY_PREFIX + ticket_nr)
    assert len(download_entries) == 1
    for key, value in download_entries.items():
        # filename
        assert '_'.join(key.split('_')[1:]) == "s3://parquet/test_download.parquet"
        # progress slot
        assert value.split('_')[-1] == "-1"


def test_compare_with_pandas_total_amount_sum(taxi_df, rpc, shards):
    compare_with_pandas(taxi_df, rpc, shards, 'payment_type', 'total_amount', 'sum')


def test_compare_with_pandas_passenger_count_sum(taxi_df, rpc, shards):
    compare_with_pandas(taxi_df, rpc, shards, 'payment_type', 'passenger_count', 'sum')


@pytest.mark.skip('Skipping now as parquery does not implement means well yet')
def test_compare_with_pandas_total_amount_mean(taxi_df, rpc, shards):
    compare_with_pandas(taxi_df, rpc, shards, 'payment_type', 'total_amount', 'mean')


@pytest.mark.skip('Skipping now as parquery does not implement counts well yet')
def test_compare_with_pandas_payment_type_count(taxi_df, rpc, shards):
    compare_with_pandas(taxi_df, rpc, shards, 'payment_type', 'passenger_count', 'count')


def compare_with_pandas(taxi_df, rpc, shards, group_col, agg_col, method):
    full = os.path.basename(shards[0])
    full_result = rpc.groupby([full], [group_col], [[agg_col, method, agg_col]], [])
    full_result = full_result.sort_values(by=group_col)
    full_result = full_result.reset_index(drop=True)

    gp = taxi_df.groupby(group_col, sort=True, as_index=False)[agg_col]
    if method == 'sum':
        pandas_result = gp.sum()
    elif method == 'mean':
        pandas_result = gp.mean()
    elif method == 'count':
        pandas_result = gp.count()
    else:
        assert False, "Unknown method: {}".format(method)

    pandas_result = pandas_result.sort_values(by=group_col)
    pandas_result = pandas_result.reset_index(drop=True)

    assert_frame_equal(full_result, pandas_result, check_less_precise=True)


def test_compare_full_with_shard(rpc, shards):
    shard_filenames = [os.path.basename(x) for x in shards]

    full, parts = shard_filenames[:1], shard_filenames[1:]
    full_result = rpc.groupby(full, ['payment_type'], [['passenger_count', 'sum', 'passenger_count']], [])
    parts_result = rpc.groupby(parts, ['payment_type'], [['passenger_count', 'sum', 'passenger_count']], [])

    assert isinstance(full_result, pd.DataFrame)
    assert isinstance(parts_result, pd.DataFrame)

    # This returns a single DataFrame with the results from each part pasted in it separately
    # so we need to use a further groupby to produce the same result as with the full parquet
    parts_result = parts_result.groupby('payment_type', sort=True).sum()
    full_result = full_result.set_index('payment_type').sort_index()

    assert_frame_equal(full_result, parts_result)


if __name__ == '__main__':
    pytest.main([__file__, '-s'])
