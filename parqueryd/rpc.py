from __future__ import annotations

import binascii
import logging
import os
import random
import time
from typing import Any

import redis
import zmq
from pyarrow import ArrowInvalid

import parqueryd.config
from parqueryd.exceptions import (
    ResourceTemporarilyUnavailableError,
    RetriesExceededError,
    RPCError,
    StateError,
    UnableToConnect,
)
from parqueryd.messages import ErrorMessage, RPCMessage, msg_factory
from parqueryd.tool import ens_bytes


class RPC:
    """RPC client for communicating with parqueryd controller nodes."""

    def __init__(
        self,
        address: str | bytes | None = None,
        timeout: int = 120,
        redis_url: str = "redis://127.0.0.1:6379/0",
        loglevel: int = logging.INFO,
        retries: int = 2,
    ) -> None:
        """Initialize RPC client.

        Args:
            address: Controller address to connect to (or None to auto-discover)
            timeout: Network timeout in seconds
            redis_url: Redis connection URL
            loglevel: Logging level
            retries: Number of retry attempts for failed requests
        """
        self.logger: logging.Logger = parqueryd.logger.getChild("rpc")
        self.logger.setLevel(loglevel)
        self.context: zmq.Context = zmq.Context()
        self.redis_url: str = redis_url
        redis_server: redis.Redis = redis.from_url(redis_url)
        self.retries: int = retries
        self.timeout: int = timeout
        self.identity: str = binascii.hexlify(os.urandom(8)).decode()
        self.controller: zmq.Socket
        self.address: str | bytes
        self.last_call_duration: float

        if not address:
            # Bind to a random controller
            controllers = list(redis_server.smembers(parqueryd.config.REDIS_SET_KEY))
            if len(controllers) < 1:
                raise Exception(f"No Controllers found in Redis set: {parqueryd.config.REDIS_SET_KEY}")
            random.shuffle(controllers)
        else:
            controllers = [address]
        self.controllers: list[str | bytes] = controllers
        self.connect_socket()

    def connect_socket(self) -> None:
        """Establish socket connection to a controller."""
        reply = None
        for c in self.controllers:
            self.logger.debug("Establishing socket connection to %s", c)
            tmp_sock = self.context.socket(zmq.REQ)
            tmp_sock.setsockopt(zmq.RCVTIMEO, 2000)
            tmp_sock.setsockopt(zmq.LINGER, 0)
            tmp_sock.identity = ens_bytes(self.identity)
            tmp_sock.connect(c)
            # first ping the controller to see if it responds at all
            msg = RPCMessage({"payload": "ping"})
            tmp_sock.send_json(msg)
            try:
                reply = msg_factory(tmp_sock.recv_json())
                self.address = c
                break
            except Exception:
                continue
        if reply:
            # Now set the timeout to the actual requested
            self.logger.debug("Connection OK, setting network timeout to %s milliseconds", self.timeout * 1000)
            self.controller = tmp_sock
            self.controller.setsockopt(zmq.RCVTIMEO, self.timeout * 1000)
        else:
            raise Exception("No controller connection")

    def __getattr__(self, name: str) -> Any:
        """Dynamic method dispatcher for RPC calls.

        Args:
            name: Name of the RPC method to call

        Returns:
            A callable that performs the RPC call
        """

        def _parse_exception(e: Exception) -> Exception:
            """Parse exceptions and convert to appropriate error types."""
            if isinstance(e, zmq.Again):
                return ResourceTemporarilyUnavailableError()

            if isinstance(e, zmq.ZMQError):
                if "Operation cannot be accomplished in current state" in e["strerror"]:
                    return StateError()
                if "UnableToConnect" in e["strerror"]:
                    return UnableToConnect()

                return RPCError(e["strerror"])

            return e

        def _rpc(*args: Any, **kwargs: Any) -> Any:
            """Execute the RPC call."""
            self.logger.debug(f"Call {name} on {self.address}")
            start_time = time.time()
            params: dict[str, Any] = {}
            if args:
                params["args"] = args
            if kwargs:
                params["kwargs"] = kwargs

            # We do not want string args to be converted into unicode by the JSON machinery
            msg = RPCMessage({"payload": name})
            msg["params"] = params
            rep = None
            last_except: Exception | None = None
            for x in range(self.retries):
                try:
                    self.controller.send_json(msg)
                    rep = self.controller.recv()
                    break
                except Exception as e:
                    last_except = e
                    self.controller.close()
                    self.logger.critical(e)
                    if x == self.retries:
                        raise e
                    else:
                        self.logger.debug(f"Error, retrying {x + 1}")
                        self.connect_socket()
                        pass
            if name == "groupby" and rep in ["", b""]:
                # this is the placeholder for an empty result from a groupby and needs to be explicitly caught
                return None

            elif not rep and last_except:
                parsed_exception = _parse_exception(last_except)
                if parsed_exception:
                    self.logger.critical(f"No response from DQE, retries {self.retries} exceeded")
                    raise parsed_exception
                else:
                    raise RetriesExceededError(self.retries)

            elif not rep:
                raise RetriesExceededError(self.retries)

            resp_msg = msg_factory(rep)
            if isinstance(resp_msg, ErrorMessage):
                raise RPCError(resp_msg.get("payload"))

            if name == "groupby":
                groupby_col_list = args[1]
                agg_list = args[2]
                aggregate = kwargs.get("aggregate", False)
                try:
                    result = self.uncompress_groupby_to_pq(rep, groupby_col_list, agg_list, aggregate=aggregate)
                except ArrowInvalid:
                    if isinstance(resp_msg, RPCMessage) and resp_msg["result"] == "":
                        # Specific edge-case, don't know where this is coming from. Maybe just an empty collection?
                        return None

                    self.logger.exception(f"Could not use RPC method: {name}/{resp_msg}")
                    raise ValueError(result)
            else:
                result = resp_msg.get("result", {})

            stop_time = time.time()
            self.last_call_duration = stop_time - start_time
            return result

        return _rpc

    def uncompress_groupby_to_pq(
        self, result: Any, groupby_col_list: list[str], agg_list: list[Any], aggregate: bool = False
    ) -> Any:
        """Uncompress result returned by the groupby and convert it to a Pandas DataFrame.

        Args:
            result: Compressed groupby result
            groupby_col_list: List of groupby columns
            agg_list: List of aggregation functions
            aggregate: Whether to aggregate results

        Returns:
            Uncompressed result or None
        """
        if not result:
            return None
        raise NotImplementedError("Not supported on python 3")

    def get_download_data(self) -> dict[bytes, dict[bytes, bytes]]:
        """Get download ticket data from Redis.

        Returns:
            Dictionary mapping ticket IDs to their download progress data
        """
        redis_server: redis.Redis = redis.from_url(self.redis_url)
        tickets = set(redis_server.keys(parqueryd.config.REDIS_TICKET_KEY_PREFIX + "*"))
        data: dict[bytes, dict[bytes, bytes]] = {}
        for ticket in tickets:
            tmp = redis_server.hgetall(ticket)
            data[ticket] = tmp
        return data

    def downloads(self) -> list[tuple[bytes, str]]:
        """Get list of downloads with their progress.

        Returns:
            List of tuples containing ticket ID and progress string
        """
        data = self.get_download_data()
        buf: list[tuple[bytes, str]] = []
        for k, v in data.items():
            done_count = 0
            for _kk, vv in v.items():
                if vv.endswith(b"_DONE"):
                    done_count += 1
            buf.append((k, f"{done_count}/{len(v)}"))
        return buf

    def delete_download(self, ticket: str | bytes) -> int:
        """Delete a download ticket from Redis.

        Args:
            ticket: Ticket ID to delete

        Returns:
            Number of fields deleted
        """
        redis_server: redis.Redis = redis.from_url(self.redis_url)
        tmp = redis_server.hgetall(ticket)
        count = 0
        for k, _v in tmp.items():
            count += redis_server.hdel(ticket, k)
        return count
