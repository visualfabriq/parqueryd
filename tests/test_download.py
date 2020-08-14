import logging
import os
import shutil
import socket
import threading
import time
from contextlib import contextmanager
from time import sleep
from uuid import uuid4
import glob

import boto3
import numpy as np
import pandas as pd
import pytest
import redis
from parquery.write import df_to_parquet

import parqueryd
import parqueryd.config
from parqueryd.worker import DownloaderNode

TEST_REDIS = 'redis://redis:6379/0'
FAKE_ACCESS_KEY = 'fake access key'
FAKE_SECRET_KEY = 'fake secret key'


class LocalS3Downloader(DownloaderNode):
    """Extend ``parqueryd.DownloaderNode`` to return an S3 Resource connected to local (test) S3."""

    def __init__(self, *args, **kwargs):
        super(LocalS3Downloader, self).__init__(*args, **kwargs)

        session = boto3.session.Session(
            aws_access_key_id=FAKE_ACCESS_KEY,
            aws_secret_access_key=FAKE_SECRET_KEY,
            region_name='eu-west-1')

        self._conn = session.resource('s3', endpoint_url='http://localstack:4572')

    def _get_s3_conn(self):
        return FAKE_ACCESS_KEY, FAKE_SECRET_KEY, self._conn

    def _get_transport_params(self):
        return {
            'resource_kwargs':
                {'endpoint_url': 'http://localstack:4572'}
        }


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
def clear_incoming():
    if os.path.isdir(parqueryd.config.INCOMING):
        shutil.rmtree(parqueryd.config.INCOMING)
    os.makedirs(parqueryd.config.INCOMING)


@pytest.fixture
def downloader():
    downloader = LocalS3Downloader(redis_url=TEST_REDIS, loglevel=logging.DEBUG)
    downloader_thread = threading.Thread(target=downloader.go)
    downloader_thread.daemon = True
    downloader_thread.start()
    # Sleep 5 seconds, just to make sure all connections are properly established
    sleep(5)

    yield downloader

    # shutdown the downloader
    downloader.running = False


@contextmanager
def clean_bucket(s3_conn, bucket_name):
    bucket = s3_conn.create_bucket(Bucket=bucket_name)
    for item in bucket.objects.all():
        item.delete()

    yield bucket

    for item in bucket.objects.all():
        item.delete()

    bucket.delete()


@pytest.mark.usefixtures('clear_incoming')
def test_downloader(redis_server, downloader, tmpdir):
    # Make a parquet from a pandas DataFrame
    data_df = pd.DataFrame(
        data=np.random.rand(100, 10),
        columns=['col_{}'.format(i + 1) for i in range(10)])
    local_parquet = str(tmpdir.join('test.parquet'))
    df_to_parquet(data_df, local_parquet)

    assert os.path.exists(local_parquet)

    s3_conn = downloader._get_s3_conn()[-1]

    with clean_bucket(s3_conn, 'parquet') as bucket:
        bucket.put_object(Key='test.parquet', Body=open(local_parquet, 'rb'))

        uploads = [key.key for key in bucket.objects.all()]
        assert uploads == ['test.parquet']

        # Construct the redis entry that the downloader is looking for
        progress_slot = '%s_%s' % (time.time() - 60, -1)
        node_filename_slot = '%s_%s' % (socket.gethostname(), 's3://parquet/test.parquet')
        ticket = str(uuid4())

        incoming_dir = parqueryd.config.INCOMING
        assert not glob.glob(os.path.join(incoming_dir, '*'))

        redis_server.hset(parqueryd.config.REDIS_TICKET_KEY_PREFIX + ticket, node_filename_slot, progress_slot)

        # wait for the downloader to catch up
        sleep(10)

        # Check that incoming dir now has the test.parquet file.
        assert glob.glob(os.path.join(incoming_dir, '*')) == [os.path.join(incoming_dir, ticket + '_test.parquet')]

        # Check that the progress slot has been updated
        updated_slot = redis_server.hget(parqueryd.config.REDIS_TICKET_KEY_PREFIX + ticket, node_filename_slot)
        assert updated_slot.split('_')[-1] == 'DONE'


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])
