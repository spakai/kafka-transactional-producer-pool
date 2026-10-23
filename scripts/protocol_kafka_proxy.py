#!/usr/bin/env python3
"""Plaintext Kafka TCP proxy that drops correlated EndTxn responses.

This proxy is only for the local disposable chaos cluster. It forwards each
Kafka frame unchanged and records the API key and correlation ID needed to
prove that an EndTxn request reached a broker and its response was dropped.
"""

import asyncio
import json
import struct
import time
from collections import defaultdict


BROKERS = {
    19192: ("kafka-1", 39093),
    29192: ("kafka-2", 39093),
    39192: ("kafka-3", 39093),
}
ADMIN_PORT = 18080
END_TXN_API_KEY = 26

state = {"armed": False, "triggered": False, "events": []}
pending_end_txn = defaultdict(set)
event_lock = asyncio.Lock()


async def record(event, **fields):
    async with event_lock:
        state["events"].append({"time_unix_ms": int(time.time() * 1000),
                                "event": event, **fields})


async def read_frame(reader):
    size_bytes = await reader.readexactly(4)
    size = struct.unpack(">i", size_bytes)[0]
    if size < 4 or size > 100 * 1024 * 1024:
        raise ValueError(f"invalid Kafka frame size: {size}")
    return size_bytes + await reader.readexactly(size)


def request_header(frame):
    """Return (api_key, api_version, correlation_id) from a Kafka request."""
    if len(frame) < 12:
        raise ValueError("Kafka request shorter than fixed request header")
    return struct.unpack_from(">hhi", frame, 4)


def response_correlation_id(frame):
    if len(frame) < 8:
        raise ValueError("Kafka response shorter than correlation ID header")
    return struct.unpack_from(">i", frame, 4)[0]


def read_uvarint(data, offset):
    value = 0
    shift = 0
    while True:
        byte = data[offset]
        offset += 1
        value |= (byte & 0x7F) << shift
        if byte < 0x80:
            return value, offset
        shift += 7
        if shift > 28:
            raise ValueError("invalid Kafka unsigned varint")


def read_compact_nullable_string(data, offset):
    encoded_length, offset = read_uvarint(data, offset)
    if encoded_length == 0:
        return None, offset
    length = encoded_length - 1
    value = data[offset:offset + length].decode("utf-8")
    return value, offset + length


def skip_tagged_fields(data, offset):
    count, offset = read_uvarint(data, offset)
    for _ in range(count):
        _, offset = read_uvarint(data, offset)  # tag id
        size, offset = read_uvarint(data, offset)
        offset += size
    return offset


def endtxn_details(frame):
    api_key, api_version, correlation_id = request_header(frame)
    if api_key != END_TXN_API_KEY:
        raise ValueError(f"expected EndTxn API key, got {api_key}")
    offset = 12
    if api_version >= 3:
        # RequestHeader v2 uses the legacy two-byte nullable string for
        # client_id, followed by a tagged-fields buffer.
        client_id_length = struct.unpack_from(">h", frame, offset)[0]
        offset += 2 + max(0, client_id_length)
        offset = skip_tagged_fields(frame, offset)
        transactional_id, offset = read_compact_nullable_string(frame, offset)
    else:
        client_id_length = struct.unpack_from(">h", frame, offset)[0]
        offset += 2 + max(0, client_id_length)
        transactional_id_length = struct.unpack_from(">h", frame, offset)[0]
        offset += 2
        if transactional_id_length < 0:
            transactional_id = None
        else:
            transactional_id = frame[offset:offset + transactional_id_length].decode("utf-8")
            offset += transactional_id_length
    producer_id, producer_epoch = struct.unpack_from(">qh", frame, offset)
    offset += 10
    committed = frame[offset] != 0
    return {"api_key": api_key, "api_version": api_version,
            "correlation_id": correlation_id, "transactional_id": transactional_id,
            "producer_id": producer_id, "producer_epoch": producer_epoch,
            "committed": committed}


async def proxy_connection(client_reader, client_writer, broker_host, broker_port):
    peer = client_writer.get_extra_info("peername")
    try:
        broker_reader, broker_writer = await asyncio.open_connection(broker_host, broker_port)
    except Exception as error:
        await record("broker_connect_failed", broker=f"{broker_host}:{broker_port}",
                     error=str(error))
        client_writer.close()
        await client_writer.wait_closed()
        return

    connection_id = f"{id(client_writer):x}"

    async def client_to_broker():
        while True:
            frame = await read_frame(client_reader)
            api_key, api_version, correlation_id = request_header(frame)
            details = endtxn_details(frame) if api_key == END_TXN_API_KEY else None
            if details is not None and details["committed"] and state["armed"]:
                state["triggered"] = True
                pending_end_txn[connection_id].add(correlation_id)
                await record("endtxn_request_forwarded", broker=f"{broker_host}:{broker_port}",
                             **details, connection_id=connection_id, peer=str(peer))
            broker_writer.write(frame)
            await broker_writer.drain()

    async def broker_to_client():
        while True:
            frame = await read_frame(broker_reader)
            correlation_id = response_correlation_id(frame)
            if correlation_id in pending_end_txn[connection_id]:
                pending_end_txn[connection_id].remove(correlation_id)
                await record("endtxn_response_dropped", broker=f"{broker_host}:{broker_port}",
                             correlation_id=correlation_id, connection_id=connection_id,
                             peer=str(peer))
                # Keep the client connection open so its request timeout, rather
                # than an immediate disconnect/retry, determines the outcome.
                continue
            client_writer.write(frame)
            await client_writer.drain()

    tasks = [asyncio.create_task(client_to_broker()),
             asyncio.create_task(broker_to_client())]
    try:
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        for task in done:
            exception = task.exception()
            if exception and not isinstance(exception, (asyncio.IncompleteReadError,
                                                          ConnectionResetError,
                                                          BrokenPipeError)):
                await record("connection_error", error=str(exception), peer=str(peer))
    finally:
        pending_end_txn.pop(connection_id, None)
        client_writer.close()
        broker_writer.close()
        await asyncio.gather(client_writer.wait_closed(), broker_writer.wait_closed(),
                             return_exceptions=True)


async def handle_protocol_client(reader, writer, broker):
    await proxy_connection(reader, writer, *broker)


def proof_summary():
    requests = [e for e in state["events"] if e["event"] == "endtxn_request_forwarded"]
    dropped = [e for e in state["events"] if e["event"] == "endtxn_response_dropped"]
    matched = {(e["connection_id"], e["correlation_id"]) for e in requests} & {
        (e["connection_id"], e["correlation_id"]) for e in dropped}
    confirmed = bool(matched)
    return {
        "proof": "ENDTXN_RESPONSE_DROPPED" if confirmed else "NO_PROTOCOL_PROOF",
        "request_forwarded": len(requests),
        "response_dropped": len(dropped),
        "matched_correlation_ids": sorted({correlation_id
                                            for _, correlation_id in matched}),
        "events": list(state["events"]),
    }


async def admin_handler(reader, writer):
    try:
        request_line = (await reader.readline()).decode("ascii", "replace").strip()
        parts = request_line.split()
        while await reader.readline() not in (b"\r\n", b"\n", b""):
            pass
        method, path = (parts[0], parts[1]) if len(parts) >= 2 else ("", "")
        status = "200 OK"
        if method == "GET" and path == "/health":
            body = {"healthy": True}
        elif method == "POST" and path == "/arm":
            state["armed"] = True
            state["triggered"] = False
            state["events"] = []
            body = {"armed": True, "target": "EndTxn responses during the armed window"}
        elif method == "POST" and path == "/heal":
            state["armed"] = False
            body = proof_summary()
        elif method == "GET" and path == "/status":
            body = {"armed": state["armed"], **proof_summary()}
        else:
            status = "404 Not Found"
            body = {"error": "not found"}
        encoded = json.dumps(body, separators=(",", ":")).encode()
        writer.write((f"HTTP/1.1 {status}\r\nContent-Type: application/json\r\n"
                      f"Content-Length: {len(encoded)}\r\nConnection: close\r\n\r\n").encode()
                     + encoded)
        await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()


async def main():
    for listen_port, broker in BROKERS.items():
        await asyncio.start_server(
            lambda reader, writer, target=broker: handle_protocol_client(reader, writer, target),
            "0.0.0.0", listen_port)
    await asyncio.start_server(admin_handler, "0.0.0.0", ADMIN_PORT)
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
