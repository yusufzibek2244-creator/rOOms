import json
import logging
import math
import os
import re
import time
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse


BASE_DIR = Path(__file__).resolve().parent
ROOM_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
VIDEO_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]{11}$")
USERNAME_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,24}$")
MAX_MESSAGE_SIZE = 8_192
MAX_CHAT_LENGTH = 2_000
logger = logging.getLogger(__name__)

app = FastAPI(title="rOOms", docs_url=None, redoc_url=None)
allowed_origins = [
    origin.strip()
    for origin in os.environ.get("ROOMS_ALLOWED_ORIGINS", "*").split(",")
    if origin.strip()
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins or ["*"],
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["Content-Type"],
)


def is_valid_room_id(room_id: str) -> bool:
    return bool(ROOM_ID_PATTERN.fullmatch(room_id))


def is_valid_video_id(video_id: Any) -> bool:
    return isinstance(video_id, str) and bool(VIDEO_ID_PATTERN.fullmatch(video_id))


def is_valid_username(username: str) -> bool:
    return bool(USERNAME_PATTERN.fullmatch(username))


def json_message(message_type: str, **payload: Any) -> str:
    return json.dumps({"type": message_type, **payload}, ensure_ascii=False, allow_nan=False)


class ConnectionManager:
    def __init__(self) -> None:
        self.rooms: dict[str, dict[str, Any]] = {}

    async def connect(self, websocket: WebSocket, room_id: str, username: str) -> bool:
        if not is_valid_room_id(room_id) or not is_valid_username(username):
            await websocket.close(code=1008, reason="Geçersiz oda veya kullanıcı adı")
            return False

        await websocket.accept()
        room = self.rooms.get(room_id)
        if room is None:
            room = {
                "clients": {},
                "is_public": True,
                "video_id": "Bekleniyor",
                "host": username,
                "hierarchy": [],
            }
            self.rooms[room_id] = room

        if username not in room["hierarchy"]:
            room["hierarchy"].append(username)
        room["clients"][websocket] = username

        old_host = room["host"]
        connected_users = set(room["clients"].values())
        room["host"] = next(
            (user for user in room["hierarchy"] if user in connected_users),
            username,
        )

        await self.broadcast_user_list(room_id)
        await self.broadcast_to_room(
            json_message("chat", sender="Sistem", text=f"{username} odaya katıldı.", color="text-emerald-400"),
            room_id,
        )
        if old_host != room["host"] and room["host"] == username:
            await self.broadcast_to_room(
                json_message(
                    "chat",
                    sender="Sistem",
                    text=f"👑 Kurucu lider {username} odaya geri döndü ve yöneticiliği otomatik devraldı!",
                    color="text-rooms-accent",
                ),
                room_id,
            )
            await self.broadcast_to_room(
                json_message("force_sync_request", target=old_host),
                room_id,
            )
        return True

    async def disconnect(self, websocket: WebSocket, room_id: str) -> None:
        room = self.rooms.get(room_id)
        if room is None:
            return

        username = room["clients"].pop(websocket, None)
        if username is None:
            return
        if not room["clients"]:
            self.rooms.pop(room_id, None)
            return

        old_host = room["host"]
        connected_users = set(room["clients"].values())
        room["host"] = next(
            (user for user in room["hierarchy"] if user in connected_users),
            next(iter(connected_users)),
        )

        if old_host != room["host"]:
            await self.broadcast_to_room(
                json_message(
                    "chat",
                    sender="Sistem",
                    text=f"Yönetici ayrıldı. Yeni lider: {room['host']}",
                    color="text-amber-500",
                ),
                room_id,
            )
        await self.broadcast_user_list(room_id)
        await self.broadcast_to_room(
            json_message("chat", sender="Sistem", text=f"{username} odadan ayrıldı.", color="text-gray-500"),
            room_id,
        )

    async def broadcast_to_room(
        self,
        message: str,
        room_id: str,
        exclude: WebSocket | None = None,
    ) -> None:
        room = self.rooms.get(room_id)
        if room is None:
            return

        dead_connections = []
        for connection in list(room["clients"]):
            if connection is exclude:
                continue
            try:
                await connection.send_text(message)
            except (WebSocketDisconnect, RuntimeError):
                dead_connections.append(connection)

        for connection in dead_connections:
            await self.disconnect(connection, room_id)

    async def broadcast_user_list(self, room_id: str) -> None:
        room = self.rooms.get(room_id)
        if room is None:
            return
        users = list(dict.fromkeys(room["clients"].values()))
        await self.broadcast_to_room(
            json_message("user_list", users=users, host=room["host"]),
            room_id,
        )

    def get_public_rooms(self) -> list[dict[str, Any]]:
        return [
            {
                "room_id": room_id,
                "users_count": len(set(room["clients"].values())),
                "video_id": room["video_id"],
                "host": room["host"],
            }
            for room_id, room in self.rooms.items()
            if room["is_public"] and is_valid_video_id(room["video_id"])
        ]


manager = ConnectionManager()


async def handle_websocket_message(
    websocket: WebSocket,
    room_id: str,
    username: str,
    message: dict[str, Any],
) -> None:
    room = manager.rooms.get(room_id)
    if room is None or websocket not in room["clients"]:
        return

    message_type = message.get("type")
    if message_type == "ping":
        client_time = message.get("client_time")
        if isinstance(client_time, (int, float)) and math.isfinite(client_time):
            await websocket.send_text(
                json_message("pong", client_time=client_time, server_time=time.time() * 1000)
            )
        return

    if message_type == "chat":
        text = message.get("text")
        if isinstance(text, str) and text.strip():
            await manager.broadcast_to_room(
                json_message("chat", sender=username, text=text.strip()[:MAX_CHAT_LENGTH]),
                room_id,
            )
        return

    if message_type == "request_sync":
        await manager.broadcast_to_room(
            json_message("request_sync", sender=username),
            room_id,
            exclude=websocket,
        )
        return

    if message_type in {"sync", "sync_check"}:
        if room["host"] != username:
            return
        state = message.get("state")
        current_time = message.get("time")
        sent_at = message.get("sent_at")
        if (
            state not in (1, 2)
            or isinstance(state, bool)
            or not isinstance(current_time, (int, float))
            or isinstance(current_time, bool)
            or not math.isfinite(current_time)
            or current_time < 0
            or not isinstance(sent_at, (int, float))
            or isinstance(sent_at, bool)
            or not math.isfinite(sent_at)
        ):
            return
        await manager.broadcast_to_room(
            json_message(
                message_type,
                sender=username,
                state=state,
                time=current_time,
                sent_at=sent_at,
            ),
            room_id,
            exclude=websocket,
        )
        return

    if message_type == "room_update":
        if room["host"] != username:
            return
        video_id = message.get("video_id")
        is_public = message.get("is_public")
        if is_valid_video_id(video_id):
            room["video_id"] = video_id
        if isinstance(is_public, bool):
            room["is_public"] = is_public
        return

    if message_type == "kick":
        if room["host"] != username:
            return
        target = message.get("target")
        if not isinstance(target, str) or target == username:
            return
        targets = [
            connection
            for connection, connected_username in list(room["clients"].items())
            if connected_username == target
        ]
        for target_socket in targets:
            try:
                await target_socket.send_text(json_message("kicked"))
            except (WebSocketDisconnect, RuntimeError):
                pass
            try:
                await target_socket.close(code=1008, reason="Odadan çıkarıldınız")
            except (WebSocketDisconnect, RuntimeError):
                await manager.disconnect(target_socket, room_id)
        return

    if message_type == "transfer_host":
        if room["host"] != username:
            return
        target = message.get("target")
        connected_users = set(room["clients"].values())
        if not isinstance(target, str) or target == username or target not in connected_users:
            return
        room["hierarchy"] = [target] + [
            user for user in room["hierarchy"] if user != target
        ]
        room["host"] = target
        await manager.broadcast_user_list(room_id)
        await manager.broadcast_to_room(
            json_message(
                "chat",
                sender="Sistem",
                text=f"👑 {username}, yöneticiliği {target} kişisine devretti.",
                color="text-rooms-accent",
            ),
            room_id,
        )


@app.websocket("/ws/{room_id}/{username}")
async def websocket_endpoint(websocket: WebSocket, room_id: str, username: str) -> None:
    if not await manager.connect(websocket, room_id, username):
        return

    try:
        while True:
            raw_message = await websocket.receive_text()
            if len(raw_message.encode("utf-8")) > MAX_MESSAGE_SIZE:
                await websocket.close(code=1009, reason="Mesaj çok büyük")
                break
            try:
                message = json.loads(raw_message)
            except json.JSONDecodeError:
                logger.warning("Geçersiz JSON mesajı alındı (oda=%s, kullanıcı=%s)", room_id, username)
                continue
            if not isinstance(message, dict):
                continue
            await handle_websocket_message(websocket, room_id, username, message)
    except WebSocketDisconnect:
        pass
    finally:
        await manager.disconnect(websocket, room_id)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/rooms")
async def api_get_rooms() -> dict[str, list[dict[str, Any]]]:
    return {"active_rooms": manager.get_public_rooms()}


@app.get("/api/room/{room_id}")
async def api_get_room(room_id: str) -> dict[str, Any]:
    if not is_valid_room_id(room_id):
        raise HTTPException(status_code=400, detail="Geçersiz oda kimliği")
    room = manager.rooms.get(room_id)
    if room is None:
        raise HTTPException(status_code=404, detail="Oda bulunamadı")
    return {
        "room_id": room_id,
        "video_id": room["video_id"],
        "is_public": room["is_public"],
        "host": room["host"],
        "users_count": len(set(room["clients"].values())),
    }


@app.get("/")
async def serve_home() -> FileResponse:
    return FileResponse(BASE_DIR / "index.html", headers={"Cache-Control": "no-cache"})


@app.get("/manifest.json")
async def serve_manifest() -> FileResponse:
    return FileResponse(BASE_DIR / "manifest.json", headers={"Cache-Control": "no-cache"})


@app.get("/sw.js")
async def serve_sw() -> FileResponse:
    return FileResponse(
        BASE_DIR / "sw.js",
        media_type="application/javascript",
        headers={"Cache-Control": "no-cache"},
    )


if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        proxy_headers=True,
        forwarded_allow_ips="*",
        ws_ping_interval=20,
        ws_ping_timeout=20,
    )
