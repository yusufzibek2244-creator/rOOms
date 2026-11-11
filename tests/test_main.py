import json
import unittest

import main


class FakeWebSocket:
    def __init__(self):
        self.messages = []
        self.accepted = False
        self.closed = False

    async def accept(self):
        self.accepted = True

    async def send_text(self, message):
        if self.closed:
            raise RuntimeError("WebSocket is closed")
        self.messages.append(json.loads(message))

    async def close(self, code=1000, reason=None):
        self.closed = True


class ConnectionManagerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.previous_manager = main.manager
        main.manager = main.ConnectionManager()
        self.manager = main.manager

    def tearDown(self):
        main.manager = self.previous_manager

    async def test_invalid_identity_is_rejected(self):
        websocket = FakeWebSocket()

        connected = await self.manager.connect(websocket, "../room", "guest")

        self.assertFalse(connected)
        self.assertTrue(websocket.closed)
        self.assertEqual(self.manager.rooms, {})

    async def test_public_rooms_only_include_ready_public_rooms(self):
        websocket = FakeWebSocket()
        await self.manager.connect(websocket, "room_123", "host")

        self.assertEqual(self.manager.get_public_rooms(), [])

        await main.handle_websocket_message(
            websocket,
            "room_123",
            "host",
            {"type": "room_update", "video_id": "dQw4w9WgXcQ", "is_public": True},
        )

        self.assertEqual(len(self.manager.get_public_rooms()), 1)
        self.assertEqual(self.manager.get_public_rooms()[0]["video_id"], "dQw4w9WgXcQ")

    async def test_only_host_can_update_or_broadcast_video_state(self):
        host = FakeWebSocket()
        guest = FakeWebSocket()
        await self.manager.connect(host, "room_123", "host")
        await self.manager.connect(guest, "room_123", "guest")
        guest.messages.clear()

        await main.handle_websocket_message(
            guest,
            "room_123",
            "guest",
            {"type": "room_update", "video_id": "abcdefghijk", "is_public": False},
        )
        await main.handle_websocket_message(
            guest,
            "room_123",
            "guest",
            {"type": "sync", "state": 1, "time": 30, "sent_at": 1000},
        )
        await main.handle_websocket_message(
            host,
            "room_123",
            "host",
            {"type": "sync", "state": 1, "time": 30, "sent_at": 1000},
        )

        room = self.manager.rooms["room_123"]
        self.assertEqual(room["video_id"], "Bekleniyor")
        self.assertTrue(room["is_public"])
        self.assertEqual(
            [message["type"] for message in guest.messages],
            ["sync"],
        )
        self.assertEqual(guest.messages[0]["sender"], "host")

    async def test_chat_sender_comes_from_connection_identity(self):
        host = FakeWebSocket()
        guest = FakeWebSocket()
        await self.manager.connect(host, "room_456", "host")
        await self.manager.connect(guest, "room_456", "guest")
        host.messages.clear()

        await main.handle_websocket_message(
            guest,
            "room_456",
            "guest",
            {"type": "chat", "sender": "host", "text": "hello"},
        )

        chat = next(message for message in host.messages if message["type"] == "chat")
        self.assertEqual(chat["sender"], "guest")
        self.assertEqual(chat["text"], "hello")


if __name__ == "__main__":
    unittest.main()
