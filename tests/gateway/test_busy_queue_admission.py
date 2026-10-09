"""Queue receipts must agree with storage, including rejection during ack debounce."""

import time
from types import SimpleNamespace

import pytest

from gateway.config import GatewayConfig, Platform, PlatformConfig
from gateway.platforms.base import BasePlatformAdapter, SendResult
from gateway.platforms.event import MessageEvent, MessageType
from gateway.run import GatewayRunner, _AGENT_PENDING_SENTINEL
from gateway.session import SessionSource, build_session_key


class QueueAdapter(BasePlatformAdapter):
    def __init__(self):
        super().__init__(PlatformConfig(enabled=True), Platform.TELEGRAM)
        self.sent = []

    @property
    def name(self):
        return "telegram"

    async def connect(self, *, is_reconnect=False):
        return True

    async def disconnect(self):
        pass

    async def send(self, chat_id, content, reply_to=None, metadata=None):
        self.sent.append(content)
        return SendResult(success=True)

    async def get_chat_info(self, chat_id):
        return {"id": chat_id, "type": "private"}


@pytest.fixture
def busy_queue():
    runner = object.__new__(GatewayRunner)
    runner.config = GatewayConfig()
    adapter = QueueAdapter()
    runner.adapters = {Platform.TELEGRAM: adapter}
    runner._busy_input_mode = "queue"
    runner._busy_text_mode = "queue"
    runner._draining = False
    runner.session_store = None
    runner._is_user_authorized = lambda source: True
    source = SessionSource(platform=Platform.TELEGRAM, chat_id="123", chat_type="dm", user_id="user1")
    key = build_session_key(source)
    runner._running_agents[key] = SimpleNamespace(_active_children=[])
    return runner, adapter, source, key


def event_for(source, text="follow-up", **kwargs):
    return MessageEvent(text=text, message_type=kwargs.pop("message_type", MessageType.TEXT),
                        source=source, message_id=text, **kwargs)


@pytest.mark.parametrize("case", ["accepted", "merged", "full", "unavailable", "internal_full"])
def test_queue_receipt_matches_admission(busy_queue, case):
    runner, adapter, source, key = busy_queue
    incoming = event_for(source, internal=case == "internal_full")
    if case in {"merged", "full", "internal_full"}:
        head = event_for(source, "head", message_type=MessageType.PHOTO if case == "merged" else MessageType.TEXT)
        adapter._pending_messages[key] = head
        runner._session_state(key).conversation.queued_events = [
            event_for(source, str(i)) for i in range(runner._BUSY_QUEUE_MAX_PENDING - 1)
        ]
    if case == "unavailable":
        adapter._pending_messages = None
    before = list(runner._overflow_queue(key) or ())

    result = runner._queue_or_replace_pending_event(key, incoming)

    assert result == ("full" if case == "internal_full" else case)
    assert incoming._gateway_accepted is (case in {"accepted", "merged"})
    if case == "accepted":
        assert adapter._pending_messages[key] is incoming
    elif case == "merged":
        assert incoming.text in adapter._pending_messages[key].text
    else:
        assert list(runner._overflow_queue(key) or ()) == before
        if case != "unavailable":
            assert adapter._pending_messages[key] is head


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["busy", "queue", "steer_fallback", "drain", "priority"])
@pytest.mark.parametrize("rejection", ["full", "unavailable"])
async def test_rejected_queue_input_gets_no_success_ack(busy_queue, path, rejection):
    runner, adapter, source, key = busy_queue
    if rejection == "full":
        adapter._pending_messages[key] = event_for(source, "head")
        runner._session_state(key).conversation.queued_events = [
            event_for(source, str(i)) for i in range(runner._BUSY_QUEUE_MAX_PENDING - 1)
        ]
    else:
        adapter._pending_messages = None
    runner._session_state(key).turn.busy_ack_ts = time.time()
    before = list(runner._overflow_queue(key) or ())
    incoming = event_for(source)
    incoming.text = {"queue": "/queue follow-up", "steer_fallback": "/steer follow-up"}.get(
        path, incoming.text,
    )
    runner._restart_requested = path == "drain"
    if path == "steer_fallback":
        runner._running_agents[key] = _AGENT_PENDING_SENTINEL
    handlers = {
        "busy": lambda: runner._handle_active_session_busy_message(incoming, key),
        "queue": lambda: runner._busy_queue_command(incoming, key, source),
        "steer_fallback": lambda: runner._busy_steer_command(incoming, key, source),
        "drain": lambda: runner._send_busy_drain_notice(incoming, key, "queue"),
        "priority": lambda: runner._hm_handle_running_session_message(incoming, source, key),
    }
    reply = await handlers[path]()
    if path == "busy":
        assert reply is True
    if not isinstance(reply, str):
        reply = adapter.sent[-1]

    assert "not accepted" in reply.lower()
    assert "queued" not in reply.lower()
    assert incoming._gateway_accepted is False
    assert list(runner._overflow_queue(key) or ()) == before
