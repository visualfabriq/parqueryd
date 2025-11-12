from __future__ import annotations

import binascii
import contextlib
import datetime
import gc
import glob
import importlib
import json
import logging
import os
import random
import shutil
import signal
import socket
import time
import traceback
from ssl import SSLError
from typing import Any

import boto3
import psutil
import redis
import smart_open
import zmq
from azure.storage.blob import BlobClient
from parquery.aggregate import aggregate_pq
from parquery.transport import serialize_pa_table_bytes

import parqueryd.config
from parqueryd.exceptions import WORKER_MAX_MEMORY_KB, FileTooBigError, RPCError
from parqueryd.messages import (
    BusyMessage,
    DoneMessage,
    ErrorMessage,
    Message,
    StopMessage,
    TicketDoneMessage,
    WorkerRegisterMessage,
    msg_factory,
)
from parqueryd.tool import ens_bytes, ens_unicode, rm_file_or_dir

DATA_FILE_EXTENSION = ".parquet"
# timeout in ms : how long to wait for network poll, this also affects frequency of seeing new controllers and datafiles
POLLING_TIMEOUT = 5000
# how often in seconds to send a WorkerRegisterMessage
WRM_DELAY = 20
DOWNLOAD_DELAY = 5  # how often in seconds to check for downloads


class WorkerBase:
    """Base class for all worker node types."""

    workertype: str = "base"

    def __init__(
        self,
        data_dir: str = parqueryd.config.DEFAULT_DATA_DIR,
        redis_url: str = "redis://127.0.0.1:6379/0",
        loglevel: int = logging.DEBUG,
        restart_check: bool = True,
        azure_conn_string: str | None = None,
    ) -> None:
        """Initialize worker base.

        Args:
            data_dir: Directory containing data files
            redis_url: Redis connection URL
            loglevel: Logging level
            restart_check: Whether to check memory usage and restart if needed
            azure_conn_string: Azure storage connection string
        """
        if not os.path.exists(data_dir) or not os.path.isdir(data_dir):
            raise Exception(f"Datadir {data_dir} is not a valid directory")
        self.worker_id: str = binascii.hexlify(os.urandom(8)).decode()
        self.node_name: str = socket.gethostname()
        self.data_dir: str = data_dir
        self.data_files: set[str] = set()
        self.restart_check: bool = restart_check
        context = zmq.Context()
        self.socket: zmq.Socket = context.socket(zmq.ROUTER)
        self.socket.setsockopt(zmq.LINGER, 500)
        self.socket.identity = bytes((self.worker_id).encode("utf-8"))
        self.poller: zmq.Poller = zmq.Poller()
        self.poller.register(self.socket, zmq.POLLIN | zmq.POLLOUT)
        self.redis_server: redis.Redis = redis.from_url(redis_url)
        self.controllers: dict[
            bytes, dict[str, Any]
        ] = {}  # Keep a dict of timestamps when you last spoke to controllers
        self.check_controllers()
        self.last_wrm: float = 0
        self.start_time: float = time.time()
        self.logger: logging.Logger = parqueryd.logger.getChild(f"worker {ens_unicode(self.worker_id)}")
        self.logger.setLevel(loglevel)
        self.msg_count: int = 0
        signal.signal(signal.SIGTERM, self.term_signal())
        self.running: bool = False
        self.azure_conn_string: str | None = azure_conn_string

    def term_signal(self) -> Any:
        """Create signal handler for SIGTERM.

        Returns:
            Signal handler function
        """

        def _signal_handler(signum: int, frame: Any) -> None:
            self.logger.info("Received TERM signal, stopping.")
            self.running = False

        return _signal_handler

    def send(self, addr: bytes | str, msg: Message) -> None:
        """Send message to a controller.

        Args:
            addr: Controller address
            msg: Message to send
        """
        addr = ens_bytes(addr)
        try:
            if "data" in msg:
                data = msg["data"]
                del msg["data"]
                self.logger.debug(f"Sending data message back to {ens_unicode(addr)}")
                self.socket.send_multipart([addr, msg.to_json(), data])
            else:
                self.logger.debug(f"Sending non-data message back to {ens_unicode(addr)}")
                self.socket.send_multipart([addr, msg.to_json()])
        except zmq.ZMQError as ze:
            self.logger.warning(f"Problem with {ens_unicode(addr)}: {ze}")

    def check_controllers(self) -> None:
        """Check the Redis set of controllers to see if any new ones have appeared."""
        listed_controllers = list(self.redis_server.smembers(parqueryd.config.REDIS_SET_KEY))
        current_controllers: list[bytes] = []
        new_controllers: list[bytes] = []
        for k in list(self.controllers.keys()):
            if k not in listed_controllers:
                del self.controllers[k]
                self.socket.disconnect(k)
            else:
                current_controllers.append(k)

        new_controllers = [c for c in listed_controllers if c not in current_controllers]
        for controller_address in new_controllers:
            self.socket.connect(controller_address)
            self.controllers[controller_address] = {
                "last_seen": 0,
                "last_sent": 0,
                "address": controller_address.decode(),
            }

    def check_datafiles(self) -> bool:
        """Check for new data files in the data directory.

        Returns:
            True if new files were found, False otherwise
        """
        has_new_files = False
        replacement_data_files: set[str] = set()
        for data_file in [filename for filename in os.listdir(self.data_dir) if filename.endswith(DATA_FILE_EXTENSION)]:
            if data_file not in self.data_files:
                has_new_files = True
            replacement_data_files.add(data_file)
        self.data_files = replacement_data_files
        return has_new_files

    def prepare_wrm(self) -> WorkerRegisterMessage:
        """Prepare worker registration message.

        Returns:
            WorkerRegisterMessage instance
        """
        wrm = WorkerRegisterMessage()
        wrm["worker_id"] = self.worker_id
        wrm["node"] = self.node_name
        wrm["data_files"] = list(self.data_files)
        wrm["data_dir"] = self.data_dir
        wrm["controllers"] = list(self.controllers.values())
        wrm["uptime"] = int(time.time() - self.start_time)
        wrm["msg_count"] = self.msg_count
        wrm["pid"] = os.getpid()
        wrm["workertype"] = self.workertype
        return wrm

    def heartbeat(self) -> None:
        """Send heartbeat to controllers."""
        time.sleep(0.001)  # to prevent too tight loop
        since_last_wrm = time.time() - self.last_wrm
        if since_last_wrm > WRM_DELAY:
            self.check_controllers()
            has_new_files = self.check_datafiles()
            self.last_wrm = time.time()
            wrm = self.prepare_wrm()
            for controller, data in self.controllers.items():
                if has_new_files or (time.time() - data["last_seen"] > WRM_DELAY):
                    self.send(controller, wrm)
                    data["last_sent"] = time.time()
                    self.logger.debug("heartbeat to %s", data["address"])

    def handle_in(self) -> None:
        """Handle incoming messages from controllers."""
        try:
            tmp = self.socket.recv_multipart()
        except zmq.Again:
            return
        if len(tmp) != 2:
            self.logger.critical("Received a msg with len != 2, something seriously wrong. ")
            return
        sender, msg_buf = tmp
        self.logger.info("Received message from sender %s", ens_unicode(sender))
        msg = msg_factory(msg_buf)

        data = self.controllers.get(sender)
        if not data:
            self.logger.critical(f"Received a msg from {ens_unicode(sender)} - this is an unknown sender")
            return
        data["last_seen"] = time.time()

        # TODO Notify Controllers that we are busy, no more messages to be sent
        # The above busy notification is not perfect as other messages might be on their way already
        # but for long-running queries it will at least ensure other controllers
        # don't try and overuse this node by filling up a queue
        busy_msg = BusyMessage()
        self.send_to_all(busy_msg)

        try:
            tmp = self.handle(msg)
        except Exception:
            tmp = ErrorMessage(msg)
            tmp["payload"] = traceback.format_exc()
            self.logger.exception("Unable to handle message [%s]", msg)
        if tmp is not None:
            self.send(sender, tmp)

        self.send_to_all(DoneMessage())  # Send a DoneMessage to all controllers, this flags you as 'Done'. Duh

    def go(self) -> None:
        """Start the worker main loop."""
        self.logger.info("Starting")
        self.running = True
        while self.running:
            self.heartbeat()
            for _sock, event in self.poller.poll(timeout=POLLING_TIMEOUT):
                if event & zmq.POLLIN:
                    self.handle_in()

        # Also send a message to all your controllers, that you are stopping
        self.send_to_all(StopMessage())
        for k in self.controllers:
            self.socket.disconnect(k)

        self.logger.info("Stopping")

    def send_to_all(self, msg: Message) -> None:
        """Send message to all controllers.

        Args:
            msg: Message to send
        """
        for controller in self.controllers:
            self.send(controller, msg)

    def handle(self, msg: Message) -> Message | None:
        """Handle a message from a controller.

        Args:
            msg: Message to handle

        Returns:
            Response message or None
        """
        if msg.isa("kill"):
            self.running = False
            return None
        elif msg.isa("info"):
            msg = self.prepare_wrm()
        elif msg.isa("loglevel"):
            args, kwargs = msg.get_args_kwargs()
            if args:
                loglevel = {"info": logging.INFO, "debug": logging.DEBUG}.get(args[0], logging.INFO)
                self.logger.setLevel(loglevel)
                self.logger.info(f"Set loglevel to {loglevel}")
            return None
        elif msg.isa("readfile"):
            args, kwargs = msg.get_args_kwargs()
            msg["data"] = open(args[0]).read()  # noqa: SIM115
        elif msg.isa("sleep"):
            args, kwargs = msg.get_args_kwargs()
            time.sleep(float(args[0]))
            snore = "z" * random.randint(1, 20)
            self.logger.debug(snore)
            msg["result"] = snore
        else:
            msg = self.handle_work(msg)
            self.msg_count += 1
            gc.collect()
            self._check_mem(msg)

        return msg

    def _check_mem(self, msg: Message) -> None:
        """Check memory usage and restart if needed.

        Args:
            msg: Current message being processed
        """
        # RSS is in bytes, convert to Kilobytes
        rss_kb = psutil.Process().memory_full_info().rss / (2**10)
        self.logger.debug("RSS is: %s KB", rss_kb)
        if self.restart_check and rss_kb > WORKER_MAX_MEMORY_KB:
            args = msg.get_args_kwargs()[0]
            self.logger.critical("args are: %s", args)
            self.logger.critical(f"Memory usage (KB) {rss_kb} > {WORKER_MAX_MEMORY_KB}, restarting")
            self.running = False
            raise FileTooBigError()

    def handle_work(self, msg: Message) -> Message:
        """Handle work messages. Must be implemented by subclasses.

        Args:
            msg: Work message to handle

        Returns:
            Response message
        """
        raise NotImplementedError


class WorkerNode(WorkerBase):
    """Worker node that performs calculations on parquet files."""

    workertype = "calc"

    def execute_code(self, msg: Message) -> Message:
        """Execute arbitrary Python code.

        Args:
            msg: Message containing function to execute

        Returns:
            Message with execution result
        """
        args, kwargs = msg.get_args_kwargs()

        if isinstance(kwargs["function"], bytes):
            func_fully_qualified_name = kwargs["function"].decode().split(".")
        else:
            func_fully_qualified_name = kwargs["function"].split(".")
        module_name = ".".join(func_fully_qualified_name[:-1])
        func_name = func_fully_qualified_name[-1]
        func_args = kwargs.get("args", [])
        func_kwargs = kwargs.get("kwargs", {})

        self.logger.debug(f"Importing module: {module_name}")
        mod = importlib.import_module(module_name)
        function = getattr(mod, func_name)
        self.logger.debug(f"Executing function: {func_name}")
        self.logger.debug(f"args: {func_args} kwargs: {func_kwargs}")
        result = function(*func_args, **func_kwargs)

        msg["result"] = result
        return msg

    def handle_work(self, msg: Message) -> Message:
        """Handle work messages for calculations.

        Args:
            msg: Work message to handle

        Returns:
            Response message with calculation results
        """
        if msg.isa("execute_code"):
            return self.execute_code(msg)

        args, kwargs = msg.get_args_kwargs()
        self.logger.info(f"doing calc {args}")
        filename = args[0]
        groupby_col_list = args[1]
        aggregation_list = args[2]
        where_terms_list = args[3]
        aggregate = kwargs.get("aggregate", True)

        # create rootdir
        full_file_name = os.path.join(self.data_dir, filename)
        try:
            pa_table = aggregate_pq(
                full_file_name,
                groupby_col_list,
                aggregation_list,
                data_filter=where_terms_list,
                aggregate=aggregate,
                as_df=False,
            )

            # create message
            if pa_table.num_rows == 0:
                msg["data"] = b""
            else:
                msg["data"] = serialize_pa_table_bytes(pa_table)
        except Exception as e:
            # TODO: parse to proper exception type
            raise RPCError(e)

        return msg


class DownloaderNode(WorkerBase):
    """Worker node that downloads files from S3 or Azure."""

    workertype = "download"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize downloader node."""
        super().__init__(*args, **kwargs)
        self.last_download_check: float = 0

    def heartbeat(self) -> None:
        """Send heartbeat and check for downloads."""
        super().heartbeat()
        if time.time() - self.last_download_check > DOWNLOAD_DELAY:
            self.check_downloads()

    def check_downloads(self) -> None:
        """Check for files to download from Redis queue."""
        # Note, the files being downloaded are stored per key on filename,
        # Yet files are grouped as being inside a ticket for downloading at the same time
        # this done so that a group of files can be synchronized in downloading
        # when called from rpc.download(filenames=[...]

        self.last_download_check = time.time()
        tickets = self.redis_server.keys(parqueryd.config.REDIS_TICKET_KEY_PREFIX + "*")

        for ticket_w_prefix in tickets:
            ticket_details = self.redis_server.hgetall(ticket_w_prefix)
            ticket = ticket_w_prefix[len(parqueryd.config.REDIS_TICKET_KEY_PREFIX) :]

            ticket_details_items = [(k, v) for k, v in ticket_details.items()]
            random.shuffle(ticket_details_items)

            for node_filename_slot, progress_slot in ticket_details_items:
                if isinstance(node_filename_slot, bytes):
                    tmp = node_filename_slot.decode().split("_")
                else:
                    tmp = node_filename_slot.split("_")
                if len(tmp) < 2:
                    self.logger.critical("Bogus node_filename_slot %s", node_filename_slot)
                    continue
                node = tmp[0]
                filename = "_".join(tmp[1:])

                if isinstance(progress_slot, bytes):
                    tmp = progress_slot.decode().split("_")
                else:
                    tmp = progress_slot.split("_")
                if len(tmp) != 2:
                    self.logger.critical("Bogus progress_slot %s", progress_slot)
                    continue
                _timestamp, progress = float(tmp[0]), tmp[1]

                if node != self.node_name:
                    continue

                # If every progress slot for this ticket is DONE, we can consider the whole ticket done
                if progress == "DONE":
                    continue

                try:
                    # acquire a lock for this node_filename
                    lock_key = "".join(
                        [
                            ens_unicode(x)
                            for x in (parqueryd.config.REDIS_DOWNLOAD_LOCK_PREFIX, self.node_name, ticket, filename)
                        ]
                    )
                    lock = self.redis_server.lock(lock_key, timeout=parqueryd.config.REDIS_DOWNLOAD_LOCK_DURATION)
                    if lock.acquire(False):
                        self.download_file(ticket, filename)
                        break  # break out of the loop so we don't end up staying in a large loop for giant tickets
                except Exception:  # noqa: E722
                    self.logger.exception("Problem downloading %s %s", ticket, filename)
                    # Clean up the whole ticket if an error occured
                    self.remove_ticket(ticket)
                    break
                finally:
                    with contextlib.suppress(redis.lock.LockError):
                        lock.release()

    def file_downloader_progress(self, ticket: bytes | str, filename: bytes | str, progress: int | str) -> None:
        """Update download progress in Redis.

        Args:
            ticket: Download ticket ID
            filename: File being downloaded
            progress: Progress indicator (size or 'DONE')
        """
        node_filename_slot = f"{ens_unicode(self.node_name)}_{ens_unicode(filename)}"
        # Check to see if the progress slot exists at all, if it does not exist this ticket has been cancelled
        # by some kind of intervention, stop the download and clean up.
        tmp = self.redis_server.hget(parqueryd.config.REDIS_TICKET_KEY_PREFIX + ens_unicode(ticket), node_filename_slot)
        if not tmp:
            # Clean up the whole ticket contents from disk
            ticket_path = os.path.join(parqueryd.config.INCOMING, ens_unicode(ticket))
            self.logger.debug(f"Now removing entire ticket {ens_unicode(ticket_path)}")
            for filename in glob.glob(ticket_path + "*"):
                rm_file_or_dir(os.path.join(parqueryd.config.INCOMING, ens_unicode(filename)))
            raise Exception(
                f"Ticket {ens_unicode(ticket)} progress slot {ens_unicode(node_filename_slot)} not found, aborting download"
            )
        # A progress slot contains a timestamp_filesize
        progress_slot = f"{time.time()}_{ens_unicode(progress)}"
        self.redis_server.hset(
            parqueryd.config.REDIS_TICKET_KEY_PREFIX + ens_unicode(ticket),
            ens_unicode(node_filename_slot),
            progress_slot,
        )

    def _get_transport_params(self) -> dict[str, Any]:
        """Get transport parameters for smart_open.

        Returns:
            Dictionary of transport parameters
        """
        return {}

    def download_file(self, ticket: bytes | str, fileurl: bytes | str) -> None:
        """Download a file from S3 or Azure.

        Args:
            ticket: Download ticket ID
            fileurl: URL of file to download
        """
        ticket = ens_unicode(ticket)
        fileurl = ens_unicode(fileurl)

        if self.azure_conn_string:
            self._download_file_azure(ticket, fileurl)
        else:
            self._download_file_aws(ticket, fileurl)

    def _download_file_aws(self, ticket: str, fileurl: str) -> None:
        """Download file from AWS S3.

        Args:
            ticket: Download ticket ID
            fileurl: S3 URL of file to download
        """
        if isinstance(fileurl, bytes):
            tmp = fileurl.replace(b"s3://", b"").decode().split("/")
        else:
            tmp = fileurl.replace("s3://", "").split("/")
        bucket = tmp[0]
        filename = "/".join(tmp[1:])

        incoming_file = self._get_temp_name(ticket, filename)

        if os.path.exists(incoming_file):
            self.logger.info(f"{incoming_file} exists, skipping download")
            self.file_downloader_progress(ticket, fileurl, "DONE")
        else:
            self.logger.info(f"Downloading ticket [{ticket}], bucket [{bucket}], filename [{filename}]")

            access_key, secret_key, s3_conn = self._get_s3_conn()
            object_summary = s3_conn.Object(bucket, filename)
            size = object_summary.content_length

            key = f"s3://{access_key}:{secret_key}@{bucket}/{filename}"

            with open(incoming_file, "wb") as fd:
                # See: https://github.com/RaRe-Technologies/smart_open/commit/a751b7575bfc5cc277ae176cecc46dbb109e47a4
                # Sometime we get timeout errors on the SSL connections
                for x in range(3):
                    try:
                        transport_params = self._get_transport_params()
                        with smart_open.open(key, "rb", transport_params=transport_params) as fin:
                            buf = True
                            progress = 0
                            while buf:
                                buf = fin.read(pow(2, 20) * 16)  # Use a bigger buffer
                                fd.write(buf)
                                progress += len(buf)
                                self.file_downloader_progress(ticket, fileurl, size)
                        break
                    except SSLError as e:
                        if x == 2:
                            raise e
                        else:
                            pass

        self.logger.debug("Download done %s: %s", ticket, fileurl)
        self.file_downloader_progress(ticket, fileurl, "DONE")

    def _get_prod_name(self, file_name: str) -> str:
        """Get production file path.

        Args:
            file_name: Name of file

        Returns:
            Full path to production file
        """
        return os.path.join(parqueryd.config.DEFAULT_DATA_DIR, ens_unicode(file_name))

    def _get_temp_name(self, ticket: str, file_name: str) -> str:
        """Get temporary file path for downloading.

        Args:
            ticket: Download ticket ID
            file_name: Name of file

        Returns:
            Full path to temporary file
        """
        return os.path.join(parqueryd.config.INCOMING, f"{ens_unicode(ticket)}_{ens_unicode(file_name)}")

    def _get_s3_conn(self) -> tuple[str, str, Any]:
        """Create a boto3 S3 connection.

        Returns:
            Tuple of (access_key, secret_key, s3_resource)
        """
        session = boto3.Session()
        credentials = session.get_credentials()
        if not credentials:
            raise ValueError("Missing S3 credentials")
        credentials = credentials.get_frozen_credentials()
        access_key = credentials.access_key
        secret_key = credentials.secret_key
        s3_conn = boto3.resource("s3")
        return access_key, secret_key, s3_conn

    def _download_file_azure(self, ticket: str, fileurl: str) -> None:
        """Download file from Azure Blob Storage.

        Args:
            ticket: Download ticket ID
            fileurl: Azure URL of file to download
        """
        if isinstance(fileurl, bytes):
            tmp = fileurl.decode().replace("azure://", "").split("/")
        else:
            tmp = fileurl.replace("azure://", "").split("/")
        container_name = tmp[0]
        blob_name = tmp[1]
        incoming_file = self._get_temp_name(ticket, blob_name)

        if os.path.exists(incoming_file):
            self.logger.info(f"{incoming_file} exists, skipping download")
            self.file_downloader_progress(ticket, fileurl, "DONE")
        else:
            self.logger.info(
                f"Downloading ticket [{ticket}], container name [{container_name}], blob name [{blob_name}]"
            )

            # Download blob
            with open(incoming_file, "wb") as fh:
                blob_client = BlobClient.from_connection_string(
                    conn_str=self.azure_conn_string, container_name=container_name, blob_name=blob_name
                )
                download_stream = blob_client.download_blob()
                fh.write(download_stream.readall())

            self.logger.debug(f"Download done {ticket} azure://{container_name}/{blob_name}")
            self.file_downloader_progress(ticket, fileurl, "DONE")

    def remove_ticket(self, ticket: bytes | str) -> None:
        """Remove ticket from Redis and notify controllers.

        Args:
            ticket: Ticket ID to remove
        """
        # Remove all Redis entries for this node and ticket
        # it can't be done per file as we don't have the bucket name from which a file was downloaded
        ticket = ens_unicode(ticket)
        self.logger.debug(f"Removing ticket {ticket} from redis")
        for node_filename_slot in self.redis_server.hgetall(parqueryd.config.REDIS_TICKET_KEY_PREFIX + ticket):
            if ens_unicode(node_filename_slot).startswith(ens_unicode(self.node_name)):
                self.logger.debug(f"Removing ticket_{ticket} {ens_unicode(node_filename_slot)}")
                self.redis_server.hdel(
                    parqueryd.config.REDIS_TICKET_KEY_PREFIX + ticket, ens_unicode(node_filename_slot)
                )
        tdm = TicketDoneMessage({"ticket": ticket})
        self.send_to_all(tdm)


class MoveparquetNode(DownloaderNode):
    """Worker node that moves downloaded files into production."""

    workertype = "moveparquet"

    def moveparquet(self, ticket: bytes | str) -> None:
        """Move downloaded files to production directory.

        Args:
            ticket: Ticket ID of files to move
        """
        ticket = ens_unicode(ticket)

        # A notification from the controller that all files are downloaded on all nodes,
        # the files in this ticket can be moved into place
        ticket_path = os.path.join(parqueryd.config.INCOMING, ticket + "_*")
        file_name_list = glob.glob(ticket_path)
        if file_name_list:
            for filename in file_name_list:
                filename_without_ticket = filename[filename.index("_") + 1 :]
                prod_path = self._get_prod_name(filename_without_ticket)
                if os.path.exists(prod_path):
                    rm_file_or_dir(prod_path)
                incoming_path = self._get_temp_name(ticket, filename_without_ticket)

                # Add a metadata file to the downloaded item
                metadata_filepath = self._get_prod_name(filename_without_ticket + ".metadata")
                metadata = {
                    "ticket": ticket,
                    "timestamp": time.time(),
                    "localtime": time.ctime(),
                    "utc": str(datetime.datetime.utcnow()),
                }
                open(metadata_filepath, "w").write(json.dumps(metadata, indent=2))  # noqa: SIM115

                self.logger.debug(f"Moving {incoming_path} to {prod_path}")
                shutil.move(incoming_path, prod_path)
        else:
            self.logger.debug(f"Doing a moveparquet for files {ticket_path} which do not exist")

    def check_downloads(self) -> None:
        """Check all tickets and move files to production when all downloads are complete."""
        # Check all the entries for a specific ticket over all the nodes
        # only if ALL the nodes are _DONE downloading, move the parquet files in this ticket into place.

        self.last_download_check = time.time()
        tickets = self.redis_server.keys(parqueryd.config.REDIS_TICKET_KEY_PREFIX + "*")

        for ticket_w_prefix in tickets:
            ticket_details = self.redis_server.hgetall(ticket_w_prefix)
            ticket = ticket_w_prefix[len(parqueryd.config.REDIS_TICKET_KEY_PREFIX) :]

            in_progress_count = 0
            ticket_details_items = ticket_details.items()

            ticket_on_this_node = False

            for node_filename_slot, progress_slot in ticket_details_items:
                if isinstance(node_filename_slot, bytes):
                    tmp = node_filename_slot.decode().split("_")
                else:
                    tmp = node_filename_slot.split("_")
                if len(tmp) < 2:
                    self.logger.critical("Bogus node_filename_slot %s", node_filename_slot)
                    continue
                node = tmp[0]
                "_".join(tmp[1:])

                if isinstance(progress_slot, bytes):
                    tmp = progress_slot.decode().split("_")
                else:
                    tmp = progress_slot.split("_")
                if len(tmp) != 2:
                    self.logger.critical("Bogus progress_slot %s", progress_slot)
                    continue
                _timestamp, progress = float(tmp[0]), tmp[1]

                # If every progress slot for this ticket is DONE, we can consider the whole ticket done
                if progress != "DONE":
                    in_progress_count += 1

                if node == self.node_name:
                    ticket_on_this_node = True

            if in_progress_count == 0 and ticket_on_this_node:
                try:
                    self.moveparquet(ticket)
                except Exception:  # noqa: E722
                    self.logger.exception(f"Error occurred in moveparquet {ticket}")
                finally:
                    self.remove_ticket(ticket)
