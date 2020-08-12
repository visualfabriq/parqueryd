# parqueryd

A companion library to [parquery](https://github.com/visualfabriq/parquery/) to make distributed parquery calls possible. Think of it as a far more rudimentary alternative to [Hadoop](http://hadoop.apache.org/) or [Dask](https://dask.pydata.org/en/latest/)

## The Idea

Web applications or client analysis tools do not perform the heavy lifting of calculations over large sets of data themselves, the data is stored on a collection of other servers which respond to queries over the network. Data files that are used in computations are stored in [parquet](http://parquet.blosc.org/en/latest/) files.
For _really_ large datasets, the parquet files can also be split up into 'shards' over several servers, and a query can then be performed over several servers and the results combined to the calling function by the parqueryd library.

## Getting started

Make sure you have Python virtualenv installed first.
As a start we need some interesting data, that is reasonably large in size. Download some Taxi data from the [NYC Taxi & Limousine Commission](http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml)

    virtualenv parqueryd_getting_started
    cd parqueryd_getting_started
    . bin/activate
    wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-01.csv
    pip install parqueryd 

We are only downloading the data for one month, a more interesting test is of course download the data for an entire year. But this is a good start. The data for one month is already 10 million records.

Run ipython, and let's convert the CSV file to a parquet file.

    from parquery.write import df_to_parquet
    import pandas as pd
    data = pd.read_csv('yellow_tripdata_2016-01.csv',  parse_dates=['tpep_pickup_datetime', 'tpep_dropoff_datetime'])
    df_to_parquet(data, 'tripdata_2016-01.parquet')

Now we have a parquet file on disk that can be queried using [parquery](https://github.com/visualfabriq/parquery/). But we also want to show how to use the distributed functionality  of parqueryd, so we split the file that we have just created into some smaller chunks.

    NR_SHARDS = 10
    step = len(data) // NR_SHARDS
    remainder = len(data) - step * NR_SHARDS
    count = 0
    for idx in range(0, len(data), step):
        if count == NR_SHARDS - 1 and remainder >= 0:
            step += remainder
        elif count == NR_SHARDS:
            break
        print('Creating file tripdata_2016-01-%s.parquet'%count)
        df_to_parquet(data[idx:idx+step], 'tripdata_2016-01-%s.parquet'%count)
        count += 1
        
## Running parqueryd

Now to test using parqueryd. If the parqueryd was successfully installed using pip, and your virtualenvironment is activated, you should now have a script named ```parqueryd``` on your path. You can start up a controller. Before starting parqueryd, also make sure that you have a locally running [Redis](https://redis.io/) server.

    parqueryd controller &

If you already have a controller running, you can now also run ```parqueryd``` without any arguments and it will try and connect to your controller and then drop you into an [IPython](https://ipython.org/) shell to communicate with your parqueryd cluster.

    parqueryd

From the ipython prompt you have access to a variable named 'rpc'. (if you had at least one running controller). From the rpc variable you can execute commands to the parqueryd cluster. For example:

    >>> rpc.info()

Will show status information on your current cluster, with only one controller node running there is not so much info yet. First exist your ipython session to the shell.
Lets also start two worker nodes:

    parqueryd worker --data_dir=`pwd` &
    parqueryd worker --data_dir=`pwd` &

At this point you should have a controller and two workers running in the background. When you execute ```parqueryd``` again and do:

    >>> rpc.info()

There should be more information on the running controller plus the two worker nodes. By default worker nodes check for parquet files in the ```/srv/parquet/``` directory. In the previous section we ran some worker nodes with a command line argument --data_dir to use the parquet files in the current directory.

So what kind of other commands can we send to the nodes? Here are some things to try:

    >>> rpc.ping()
    >>> rpc.sleep(10)
    >>> rpc.loglevel('debug')
    >>> rpc.sleep(2)
    >>> rpc.loglevel('info')
    >>> rpc.killworkers()

Notice the last command sent, this kills all the workers connected to all running controllers in the network. The controllers still keep on running. In typical setup the nodes will have been started up and kept running by a tool like [Supervisor](http://supervisord.org/) By using the 'killworkers' command it effectively re-boots all your workers.

The 'sleep' call is just for testing to see if any workers are responding. The call is not performed on the caller or the connecting node, but perfomeed by a worker chosen at random.

It is possible to stop all workers and all controllers in the parqueryd network by issuing the command:

    >>> rpc.killall()


## Configuration

There is minimally **one** thing to configure to use parqueryd on a network, assuming that all other defaults are chosen. **The address of the Redis server used**.
This is set in the file named ```/etc/parqueryd.cfg```
You could create this file and change the line to read, for example:

    redis_url = redis://127.0.0.1:6379/0

And change the IP address to the address of your running Redis instance. This needs to be done on every machine that you plan on running a parqueryd node.

As a convenience there is also en example configuration file for running a parquery installation using Supervisor in [misc/supervisor.conf](misc/supervisor.conf)

## Doing calculations

The whole point of having a parqueryd cluster running is doing some calculations. So once you have a controller with some worker nodes running and connected, you can drop into the parqueryd ipython shell, and for example do:

    >>> rpc.groupby(['tripdata_2016-01.parquet'], ['payment_type'], ['fare_amount'], [])

But we can also use the sharded data to do the same calculation:

    >>> import os
    >>> parquets_files = [x for x in os.listdir('.') if x.endswith('.parquets')]
    >>> rpc.groupby(parquets_files, ['payment_type'], [['fare_amount', 'sum', 'fare_amount']], [], aggregate=True)

To see how long a rpc call took, you can check:

    >>> rpc.last_call_duration

The sharded version actually takes longer to run than the version using the parquet file only. But if we start up more workers, the call is speeded up. For relatively small files like in this example, the speedup is small, but for larger datasets the overhead is worthwhile. Start up a few more workers, and run the query above.

## Executing arbitrary code

It is possible to your parqueryd workers to import and execute arbitrary Python code. **This is a potentially huge big security risk if you do not run your nodes on trusted servers behind a good firewall** Make sure you know what you are doing before just starting up and running parqueryd nodes. With that being said, if you have a cluster running, try something like:

    >>> rpc.execute_code(function='os.listdir', args=['.'], wait=True)

This should pick a random worker from those connected to the controller and run the Python listdir command in the args specified. The use of this is to run code to execute other functions from the built-in parquery/parquet aggregation logic. This enables one to perform other business specific operations over the netwok using parqueryd nodes.

## Distributing parquet files

If your system is properly configured to use [boto](https://github.com/boto/boto3) for communication with Amazon Web Services, you can use parqueryd to automically distribute collections of files to all nodes in the parqueryd cluster.
Create some parquet files in the default parqueryd directory ```/srv/parquet/``` (or move the ones we created in the getting started section of this readme)
To manage the download process, some other special types of workers need to be started.

    parqueryd downloader &
    parqueryd moveparquet &

Make sure you have an Amazon S3 bucket that you have write access to from your boto installation. Then you can specify to use that bucket for downloads.
Then from a parquery interactive shell you can distribute the files with:

    >>> rpc.distribute(['tripdata_2016-01-1.parquets'], '<some-bucket-name>')

The first parameter is a list of filesnames to distribute. To download files that already exist in a bucket (they might have veen created there by some other process):

    >>> rpc.download(filenames=['tripdata_2016-01-1.parquets', 'tripdata_2016-01-2.parquets'], bucket='<some-bucket-name>')

If you specify multiple files to download, or have several servers running a parquery node, the download will be co-ordinated across all servers and files. Only when all files are downloaded on all nodes are they switched into use by the calculation nodes. When distributing really large datafiles one would not some nodes to be out of sync serving new or old data out of step with other nodes.

### Cancelling Downloads

To list what downloads are currently in progress, from the parquery shell do:

    >>> rpc.downloads()

This will return a list of download tickets in progress, and the progress per ticket, eg:

    [('parqueryd_download_ticket_e0f42ed5ef93e084', '0/1')]

The first entry is the ticket number, followed by the number of nodes on which the download is taking place plus the completed nodes.
To cancel a download:

    >>>  rpc.delete_download(parqueryd_download_ticket_e0f42ed5ef93e084')

