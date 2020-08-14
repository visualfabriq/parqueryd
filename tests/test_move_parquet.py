import logging
import os
import shutil
import socket
import threading
import time
from time import sleep
from uuid import uuid4

import numpy as np
import pandas as pd
import pytest
import redis
from parquery.write import df_to_parquet

import parqueryd.config
from parqueryd.worker import MoveparquetNode

TEST_REDIS = 'redis://redis:6379/0'


@pytest.fixture
def redis_server():
    """
    Return a Redis Client connected to local (test) redis.
    Remove all keys from REDIS before and after use.
    """
    redis_server = redis.from_url(TEST_REDIS)
    redis_server.flushdb()
    yield redis_server
    redis_server.flushdb()


@pytest.fixture
def clear_dirs():
    if os.path.isdir(parqueryd.config.INCOMING):
        shutil.rmtree(parqueryd.config.INCOMING)
    if os.path.isdir(parqueryd.config.DEFAULT_DATA_DIR):
        shutil.rmtree(parqueryd.config.DEFAULT_DATA_DIR)
    os.makedirs(parqueryd.config.DEFAULT_DATA_DIR)
    os.makedirs(parqueryd.config.INCOMING)

@pytest.fixture
def mover():
    mover = MoveparquetNode(redis_url=TEST_REDIS, loglevel=logging.DEBUG)
    mover_thread = threading.Thread(target=mover.go)
    mover_thread.daemon = True
    mover_thread.start()
    # Sleep 5 seconds, just to make sure all connections are properly established
    sleep(5)

    yield mover

    # shutdown the thread
    mover.running = False


@pytest.mark.usefixtures('clear_dirs')
@pytest.mark.usefixtures('mover')
def test_moveparquet(redis_server, tmpdir):
    # Make a parquet from a pandas DataFrame
    data_df = pd.DataFrame(
        data=np.random.rand(100, 10),
        columns=['col_{}'.format(i + 1) for i in range(10)])
    local_parquet = str(tmpdir.join('test_mover.parquet'))
    df_to_parquet(data_df, local_parquet)

    assert os.path.exists(local_parquet)

    # copy the parquet directory to bqueyd.INCOMING
    ticket = str(uuid4())
    ticket_dir = os.path.join(parqueryd.config.INCOMING, ticket + '_test_mover.parquet')
    shutil.copy(local_parquet, ticket_dir)

    # Construct the redis entry that before downloading
    progress_slot = '%s_%s' % (time.time() - 60, -1)
    node_filename_slot = '%s_%s' % (socket.gethostname(), 's3://parquet/test_mover.parquet')

    redis_server.hset(parqueryd.config.REDIS_TICKET_KEY_PREFIX + ticket, node_filename_slot, progress_slot)

    # wait for some time
    sleep(5)

    # At this stage, we don't expect the parquet directory to be moved to parqueryd.DEFAULT_DATA_DIR, because the progress
    # slot has not been updated yet
    files_in_default_data_dir = os.listdir(parqueryd.config.DEFAULT_DATA_DIR)
    files_in_default_data_dir.sort()
    assert files_in_default_data_dir == ['incoming']
    # ticket_dir still exists
    assert os.path.exists(ticket_dir)

    # Now update progress slot
    new_progress_slot = '%s_%s' % (time.time(), 'DONE')
    redis_server.hset(parqueryd.config.REDIS_TICKET_KEY_PREFIX + ticket, node_filename_slot, new_progress_slot)

    # Sleep again
    sleep(5)
    files_in_default_data_dir = os.listdir(parqueryd.config.DEFAULT_DATA_DIR)
    files_in_default_data_dir.sort()
    assert files_in_default_data_dir == ['incoming', 'test_mover.parquet', 'test_mover.parquet.metadata']
    # ticket_dir should have been deleted.
    assert not os.path.exists(ticket_dir)


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])
