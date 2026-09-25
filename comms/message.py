"""Message format shared by every node: sender, receiver, type, timestamp, payload."""
import json
import time
import uuid
from dataclasses import dataclass, field, asdict


@dataclass
class Message:
    sender: str
    receiver: str
    type: str
    payload: dict = field(default_factory=dict)
    ts: float = field(default_factory=time.time)
    id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])

    def encode(self) -> bytes:
        return (json.dumps(asdict(self)) + "\n").encode()

    @staticmethod
    def decode(line: bytes) -> "Message":
        return Message(**json.loads(line))
