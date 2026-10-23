import asyncio
import importlib.util
import pathlib
import struct
import unittest


MODULE_PATH = pathlib.Path(__file__).with_name("protocol_kafka_proxy.py")
SPEC = importlib.util.spec_from_file_location("protocol_kafka_proxy", MODULE_PATH)
proxy = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(proxy)


def kafka_request(api_key, api_version, correlation_id, committed=True):
    frame = struct.pack(">hhi", api_key, api_version, correlation_id)
    if api_key == 26 and api_version >= 3:
        client_id = b"test"
        transactional_id = b"chaos-test"
        frame += struct.pack(">h", len(client_id)) + client_id + b"\x00"
        frame += bytes([len(transactional_id) + 1]) + transactional_id
        frame += struct.pack(">qhB", 101, 2, int(committed)) + b"\x00"
    return struct.pack(">i", len(frame)) + frame


def kafka_response(correlation_id):
    return struct.pack(">ii", 4, correlation_id)


class ProtocolHeaderTests(unittest.TestCase):
    def test_reads_endtxn_api_key_version_and_correlation_id(self):
        self.assertEqual((26, 3, 415), proxy.request_header(kafka_request(26, 3, 415)))

    def test_reads_correlation_id_from_response_header(self):
        self.assertEqual(415, proxy.response_correlation_id(kafka_response(415)))

    def test_extracts_endtxn_identity_for_broker_state_lookup(self):
        details = proxy.endtxn_details(kafka_request(26, 3, 415))
        self.assertEqual("chaos-test", details["transactional_id"])
        self.assertEqual(101, details["producer_id"])
        self.assertEqual(2, details["producer_epoch"])
        self.assertTrue(details["committed"])

    def test_identifies_abort_endtxn_as_not_a_commit_response_target(self):
        details = proxy.endtxn_details(kafka_request(26, 3, 415, committed=False))
        self.assertFalse(details["committed"])

    def test_proof_requires_matching_request_and_dropped_response(self):
        proxy.state["events"] = [
            {"event": "endtxn_request_forwarded", "correlation_id": 415,
             "connection_id": "a"},
            {"event": "endtxn_response_dropped", "correlation_id": 415,
             "connection_id": "a"},
        ]
        self.assertEqual("ENDTXN_RESPONSE_DROPPED", proxy.proof_summary()["proof"])
        proxy.state["events"].append(
            {"event": "endtxn_response_dropped", "correlation_id": 416,
             "connection_id": "b"})
        self.assertEqual([415], proxy.proof_summary()["matched_correlation_ids"])

    def test_same_correlation_id_on_different_connections_is_not_a_match(self):
        proxy.state["events"] = [
            {"event": "endtxn_request_forwarded", "correlation_id": 415,
             "connection_id": "request-connection"},
            {"event": "endtxn_response_dropped", "correlation_id": 415,
             "connection_id": "other-connection"},
        ]
        self.assertEqual("NO_PROTOCOL_PROOF", proxy.proof_summary()["proof"])


class ProxyIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_forwards_endtxn_then_drops_its_correlated_response(self):
        proxy.state.update(armed=True, triggered=False, events=[])

        async def fake_broker(reader, writer):
            request = await proxy.read_frame(reader)
            api_key, api_version, correlation_id = proxy.request_header(request)
            self.assertEqual((26, 3, 415), (api_key, api_version, correlation_id))
            writer.write(kafka_response(correlation_id))
            await writer.drain()
            try:
                await reader.read()
            finally:
                writer.close()
                await writer.wait_closed()

        broker = await asyncio.start_server(fake_broker, "127.0.0.1", 0)
        broker_port = broker.sockets[0].getsockname()[1]
        frontend = await asyncio.start_server(
            lambda r, w: proxy.proxy_connection(r, w, "127.0.0.1", broker_port),
            "127.0.0.1", 0)
        frontend_port = frontend.sockets[0].getsockname()[1]
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", frontend_port)
            writer.write(kafka_request(26, 3, 415))
            await writer.drain()
            with self.assertRaises(asyncio.TimeoutError):
                await asyncio.wait_for(reader.read(1), timeout=0.05)
            writer.close()
            await writer.wait_closed()
            await asyncio.sleep(0)
            proof = proxy.proof_summary()
            self.assertEqual("ENDTXN_RESPONSE_DROPPED", proof["proof"])
            self.assertEqual([415], proof["matched_correlation_ids"])
        finally:
            frontend.close()
            broker.close()
            await frontend.wait_closed()
            await broker.wait_closed()


if __name__ == "__main__":
    unittest.main()
