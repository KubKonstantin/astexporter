import asyncio
import logging
import os
import re
import shutil
import signal
import time
from asyncio.subprocess import PIPE
from contextlib import suppress
from typing import Dict, Set

from aiohttp import ClientSession, ClientTimeout, UnixConnector, web
from panoramisk import Manager
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)
import prometheus_client


try:
    import uvloop
except ImportError:
    uvloop = None

if uvloop is not None:
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

AMI_HOST = os.getenv("AMI_HOST", "127.0.0.1")
AMI_PORT = int(os.getenv("AMI_PORT", "5038"))
AMI_USER = os.getenv("AMI_USER", "exporter")
AMI_SECRET = os.getenv("AMI_SECRET", "strongpassword")

EXPORTER_HOST = os.getenv("EXPORTER_HOST", "0.0.0.0")
EXPORTER_PORT = int(os.getenv("EXPORTER_PORT", "9631"))

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "15"))
ASTERISK_BIN = os.getenv("ASTERISK_BIN", "asterisk")
ENABLE_CLI = os.getenv("ENABLE_CLI", "false").lower() == "true"
ENABLE_AMI_COMMAND = os.getenv("ENABLE_AMI_COMMAND", "true").lower() == "true"
FORCE_AMI_COMMAND = os.getenv("FORCE_AMI_COMMAND", "true").lower() == "true"
CLI_DOCKER_SOCKET = os.getenv("CLI_DOCKER_SOCKET", "/var/run/docker.sock")
CLI_DOCKER_CONTAINER = os.getenv("CLI_DOCKER_CONTAINER", "voip-asterisk")
COMMAND_TIMEOUT = float(os.getenv("COMMAND_TIMEOUT", "8"))
CLI_REQUIRED = os.getenv("CLI_REQUIRED", "false").lower() == "true"

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("astexporter")

if hasattr(prometheus_client, "disable_created_metrics"):
    prometheus_client.disable_created_metrics()

registry = CollectorRegistry()

asterisk_up = Gauge("asterisk_up", "Asterisk availability (AMI or CLI)", registry=registry)
asterisk_ami_up = Gauge("asterisk_ami_up", "AMI connection availability", registry=registry)
asterisk_cli_up = Gauge("asterisk_cli_up", "Asterisk CLI availability", registry=registry)
asterisk_uptime_seconds = Gauge(
    "asterisk_uptime_seconds", "Asterisk uptime seconds", registry=registry
)
asterisk_active_channels = Gauge(
    "asterisk_active_channels", "Active channels", registry=registry
)
asterisk_active_calls = Gauge("asterisk_active_calls", "Active calls", registry=registry)

pjsip_endpoint_status = Gauge(
    "asterisk_pjsip_endpoint_status", "Endpoint status", ["endpoint"], registry=registry
)
pjsip_endpoint_rtt = Gauge(
    "asterisk_pjsip_endpoint_rtt_ms", "Endpoint RTT ms", ["endpoint"], registry=registry
)
pjsip_registration_status = Gauge(
    "asterisk_pjsip_registration_status",
    "Registration status",
    ["registration"],
    registry=registry,
)

queue_calls = Gauge("asterisk_queue_calls", "Calls waiting in queue", ["queue"], registry=registry)
queue_agents = Gauge("asterisk_queue_agents", "Agents total", ["queue"], registry=registry)
queue_holdtime = Gauge(
    "asterisk_queue_holdtime_avg_seconds", "Average hold time", ["queue"], registry=registry
)
queue_completed = Counter(
    "asterisk_queue_completed_total", "Completed queue calls", ["queue"], registry=registry
)
queue_abandoned = Counter(
    "asterisk_queue_abandoned_total", "Abandoned queue calls", ["queue"], registry=registry
)

taskprocessor_queue_depth = Gauge(
    "asterisk_taskprocessor_queue_depth", "Taskprocessor queue depth", ["name"], registry=registry
)
taskprocessor_processed = Gauge(
    "asterisk_taskprocessor_processed_total", "Taskprocessor processed", ["name"], registry=registry
)
taskprocessor_high_water = Gauge(
    "asterisk_taskprocessor_high_water", "Taskprocessor high water", ["name"], registry=registry
)

calls_answered_total = Counter("asterisk_calls_answered_total", "Answered calls", registry=registry)
calls_failed_total = Counter("asterisk_calls_failed_total", "Failed calls", registry=registry)
call_duration = Histogram(
    "asterisk_call_duration_seconds",
    "Call duration",
    buckets=(5, 10, 30, 60, 120, 300, 600, 1800, 3600),
    registry=registry,
)

active_channels: Dict[str, float] = {}
active_bridges: Set[str] = set()
queue_stats_cache: Dict[str, int] = {}
queue_totals_cache: Dict[str, Dict[str, float]] = {}
cli_error_logged = False
for g in (asterisk_uptime_seconds, asterisk_active_channels, asterisk_active_calls):
    g.set(float("nan"))
asterisk_ami_up.set(0)
asterisk_cli_up.set(0)

TASKPROCESSOR_REGEX = re.compile(
    r"^(?P<name>\S+)\s+(?P<processed>\d+)\s+(?P<inqueue>\d+)\s+(?P<maxdepth>\d+)"
)
UPTIME_REGEX = re.compile(r"System uptime:\s*(?P<seconds>\d+)")
CHANNELS_REGEX = re.compile(r"(?P<channels>\d+) active channels")
CALLS_REGEX = re.compile(r"(?P<calls>\d+) active calls")
ENDPOINT_REGEX = re.compile(r"Endpoint:\s+(?P<endpoint>[^\s(]+)")
ENDPOINT_STATUS_INLINE_REGEX = re.compile(
    r"\bAvail(?:able)?[:\s]+(?P<status>[A-Za-z]+)|\b(?P<state>Unavailable|Unknown|Reachable|NonQual|In use|Not in use|Avail)\b",
    re.IGNORECASE,
)
CONTACT_REGEX = re.compile(
    r"Contact:\s+.*?(?:\bAvail(?:able)?[:\s]+(?P<status>[A-Za-z]+)|\b(?P<state>Unavailable|Unknown|Reachable|NonQual)\b)",
    re.IGNORECASE,
)
RTT_REGEX = re.compile(r"\bRTT[:\s]+(?P<rtt>[\d\.]+)", re.IGNORECASE)
CONTACT_STATUS_RTT_TAIL_REGEX = re.compile(
    r"Contact:\s+.*\s(?P<status>Avail|Unavail|Unavailable|Reachable|Unknown|NonQual)\s+(?P<rtt>[\d\.]+)\s*$",
    re.IGNORECASE,
)
REGISTRATION_OK_REGEX = re.compile(r"^(?P<name>\S+)\s+Registered\b")
REGISTRATION_ANY_REGEX = re.compile(
    r"(?P<name>\S+)\s+(?P<state>Registered|Rejected|Unregistered|Request Sent|No Authentication|Failed|Timeout)",
    re.IGNORECASE,
)
OUTBOUND_REG_REGEX = re.compile(r"Outbound Registration:\s*(?P<name>\S+)", re.IGNORECASE)
REG_STATUS_REGEX = re.compile(r"\b(?:Status|State)\s*:\s*(?P<state>.+)$", re.IGNORECASE)
QUEUE_REGEX = re.compile(
    r"^(?P<queue>\S+)\s+has\s+(?P<calls>\d+)\s+calls.*?(?P<agents>\d+)\s+members?",
    re.IGNORECASE,
)
QUEUE_CALLS_FALLBACK_REGEX = re.compile(r"^(?P<queue>\S+)\s+has\s+(?P<calls>\d+)\s+calls", re.IGNORECASE)
MEMBER_REGEX = re.compile(r"Members:\s+(?P<count>\d+)", re.IGNORECASE)
HOLDTIME_REGEX = re.compile(r"(?:(?P<seconds1>\d+)s\s+holdtime|holdtime\s+(?P<seconds2>\d+))", re.IGNORECASE)
QUEUE_MEMBER_LINE_REGEX = re.compile(r"^\s+\S+.*\((?:PJSIP|SIP|Local|IAX2)/", re.IGNORECASE)
QUEUE_COMPLETED_REGEX = re.compile(r"\bC:(?P<completed>\d+)\b")
QUEUE_ABANDONED_REGEX = re.compile(r"\bA:(?P<abandoned>\d+)\b")




def update_asterisk_up() -> None:
    asterisk_up.set(1 if (asterisk_ami_up._value.get() > 0 or asterisk_cli_up._value.get() > 0) else 0)


def _pjsip_status_to_value(status: str) -> int:
    st = status.lower()
    if st in {"avail", "available", "ok", "reachable", "lagged", "in use", "not in use"}:
        return 1
    return 0


def _extract_status(match) -> str:
    return (match.groupdict().get("status") or match.groupdict().get("state") or "").strip()


def _normalize_command_output(text: str) -> str:
    # Some transports (docker exec / AMI command) may contain CR chars.
    return text.replace("\r", "")


def parse_uptime_seconds(output: str) -> int | None:
    match = UPTIME_REGEX.search(output)
    if match:
        return int(match.group("seconds"))

    text = output.lower()
    units = {"week": 604800, "day": 86400, "hour": 3600, "minute": 60, "second": 1}
    total = 0
    found = False
    for unit, mult in units.items():
        m = re.search(rf"(\d+)\s+{unit}s?", text)
        if m:
            total += int(m.group(1)) * mult
            found = True
    return total if found else None


update_asterisk_up()


async def _run_subprocess_cmd(cmd: list[str], command_name: str) -> str:
    proc = await asyncio.create_subprocess_exec(*cmd, stdout=PIPE, stderr=PIPE)
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=COMMAND_TIMEOUT)
    except asyncio.TimeoutError:
        with suppress(ProcessLookupError):
            proc.kill()
        raise RuntimeError(f"timeout running: {command_name}")

    if proc.returncode != 0:
        err = stderr.decode(errors="replace").strip()
        raise RuntimeError(f"{command_name} failed: {err}")
    return stdout.decode(errors="replace")


async def run_asterisk_cmd_via_docker_socket(command: str) -> str:
    global cli_available, cli_error_logged

    connector = UnixConnector(path=CLI_DOCKER_SOCKET)
    timeout = ClientTimeout(total=COMMAND_TIMEOUT)
    async with ClientSession(connector=connector, timeout=timeout) as session:
        create_payload = {
            "AttachStdout": True,
            "AttachStderr": True,
            "Tty": False,
            "Cmd": [ASTERISK_BIN, "-rx", command],
        }
        async with session.post(
            f"http://docker/containers/{CLI_DOCKER_CONTAINER}/exec",
            json=create_payload,
        ) as resp:
            create_body = await resp.json(content_type=None)
            if resp.status >= 400:
                raise RuntimeError(f"docker exec create failed ({resp.status}): {create_body}")
            exec_id = create_body.get("Id")
            if not exec_id:
                raise RuntimeError(f"docker exec create returned no Id: {create_body}")

        async with session.post(
            f"http://docker/exec/{exec_id}/start",
            json={"Detach": False, "Tty": False},
        ) as resp:
            raw_output = await resp.read()
            if resp.status >= 400:
                raise RuntimeError(
                    f"docker exec start failed ({resp.status}): {raw_output.decode(errors='replace')}"
                )

        async with session.get(f"http://docker/exec/{exec_id}/json") as resp:
            inspect_body = await resp.json(content_type=None)
            if resp.status >= 400:
                raise RuntimeError(f"docker exec inspect failed ({resp.status}): {inspect_body}")

    exit_code = inspect_body.get("ExitCode")
    decoded = _decode_docker_exec_output(raw_output)
    if exit_code not in (0, None):
        raise RuntimeError(f"{command} failed in container '{CLI_DOCKER_CONTAINER}': {decoded.strip()}")
    return decoded


def _decode_docker_exec_output(raw_output: bytes) -> str:
    # Docker API may return multiplexed stream frames when TTY is disabled:
    # 1 byte stream id, 3 bytes padding, 4 bytes big-endian payload length.
    if len(raw_output) < 8:
        return raw_output.decode(errors="replace")

    out_chunks: list[bytes] = []
    i = 0
    parsed_any = False
    total = len(raw_output)
    while i + 8 <= total:
        stream_type = raw_output[i]
        if stream_type not in (0, 1, 2):
            break
        frame_len = int.from_bytes(raw_output[i + 4 : i + 8], byteorder="big")
        frame_start = i + 8
        frame_end = frame_start + frame_len
        if frame_end > total:
            break
        out_chunks.append(raw_output[frame_start:frame_end])
        parsed_any = True
        i = frame_end

    if parsed_any and i == total:
        return b"".join(out_chunks).decode(errors="replace")
    return raw_output.decode(errors="replace")


async def run_asterisk_cmd(command: str) -> str:
    global cli_error_logged

    if not ENABLE_CLI:
        raise RuntimeError("CLI collection is disabled (ENABLE_CLI=false)")

    try:
        output = await _run_local_or_docker_cli(command)
        asterisk_cli_up.set(1)
        update_asterisk_up()
        return output
    except Exception as exc:
        asterisk_cli_up.set(0)
        update_asterisk_up()
        if not cli_error_logged:
            logger.error(
                "CLI transport failed for '%s'. local_bin=%s docker_socket=%s container=%s error=%s",
                command,
                ASTERISK_BIN,
                CLI_DOCKER_SOCKET,
                CLI_DOCKER_CONTAINER,
                exc,
            )
            cli_error_logged = True
        raise RuntimeError(str(exc))


async def _run_local_or_docker_cli(command: str) -> str:
    if shutil.which(ASTERISK_BIN) is not None:
        return await _run_subprocess_cmd([ASTERISK_BIN, "-rx", command], command)
    return await run_asterisk_cmd_via_docker_socket(command)


def _extract_ami_command_output(response) -> str:
    def _clean_output_line(value) -> str:
        text = str(value)
        # Some AMI transports return each line prefixed with "Output: ".
        lines = []
        for raw_line in text.splitlines():
            if raw_line.startswith("Output:"):
                lines.append(raw_line.split(":", 1)[1].lstrip())
            else:
                lines.append(raw_line)
        return "\n".join(lines)

    if response is None:
        return ""
    if isinstance(response, dict):
        chunks = []
        if "Output" in response and response["Output"] is not None:
            chunks.append(_clean_output_line(response["Output"]))
        if "output" in response and response["output"] is not None:
            chunks.append(_clean_output_line(response["output"]))
        if "data" in response and response["data"] is not None:
            chunks.append(_clean_output_line(response["data"]))
        return "\n".join(chunks).strip()
    if isinstance(response, (list, tuple)):
        chunks = []
        for item in response:
            if isinstance(item, dict):
                out = item.get("Output")
                if out is not None:
                    chunks.append(_clean_output_line(out))
                low_out = item.get("output")
                if low_out is not None:
                    chunks.append(_clean_output_line(low_out))
            elif item is not None:
                chunks.append(_clean_output_line(item))
        return "\n".join(chunks).strip()
    return _clean_output_line(response).strip()


def _extract_output_lines_from_text(text: str) -> str:
    lines = []
    for raw in text.splitlines():
        line = raw.strip("\r")
        if line.startswith("Output:"):
            lines.append(line.split(":", 1)[1].lstrip())
    return "\n".join(lines).strip()


def _iter_ami_events(response):
    if response is None:
        return
    if isinstance(response, str):
        # Fallback parser for plain-text AMI dumps split by blank lines.
        block: Dict[str, str] = {}
        for raw_line in response.splitlines():
            line = raw_line.strip("\r")
            if not line.strip():
                if block:
                    yield block
                    block = {}
                continue
            if ":" not in line:
                continue
            key, value = line.split(":", 1)
            block[key.strip()] = value.strip()
        if block:
            yield block
        return
    if isinstance(response, dict):
        if isinstance(response.get("events"), list):
            for item in response["events"]:
                if isinstance(item, dict):
                    yield item
            return
        yield response
        return
    if isinstance(response, (list, tuple)):
        for item in response:
            if isinstance(item, dict):
                yield item
            elif isinstance(item, str):
                for parsed in _iter_ami_events(item):
                    yield parsed
    else:
        # Last-resort parsing for custom AMI response objects.
        text = str(response)
        for parsed in _iter_ami_events(text):
            yield parsed


async def run_asterisk_command(command: str) -> str:
    if ENABLE_CLI and not FORCE_AMI_COMMAND:
        return await run_asterisk_cmd(command)

    if not ENABLE_AMI_COMMAND:
        raise RuntimeError("Both CLI and AMI command modes are disabled")

    response = await manager.send_action({"Action": "Command", "Command": command})
    output = _extract_ami_command_output(response)
    if not output:
        # Fallback for AMI clients returning full text transcript
        # instead of structured Output fields.
        output = _extract_output_lines_from_text(str(response))
    if not output:
        # Last chance: parse as AMI key-value event blocks and collect Output keys.
        parsed_chunks = []
        for event in _iter_ami_events(response):
            if "Output" in event:
                parsed_chunks.append(_extract_output_lines_from_text(f"Output: {event.get('Output', '')}"))
            elif "output" in event:
                parsed_chunks.append(_extract_output_lines_from_text(f"Output: {event.get('output', '')}"))
        output = "\n".join(parsed_chunks).strip()
    if not output:
        raise RuntimeError(f"empty AMI Command output for: {command}")
    return output


async def collect_pjsip_via_ami_actions() -> tuple[bool, int, int]:
    response = await manager.send_action({"Action": "PJSIPShowEndpoints"})
    seen = 0
    rtt_seen = 0
    for event in _iter_ami_events(response):
        if str(event.get("Event", "")).lower() != "endpointlist":
            continue
        endpoint = event.get("ObjectName") or event.get("EndpointName")
        if not endpoint:
            continue
        seen += 1
        status = event.get("DeviceState") or event.get("Status") or event.get("Active")
        if status:
            pjsip_endpoint_status.labels(endpoint=endpoint).set(_pjsip_status_to_value(str(status)))
        rtt_raw = event.get("RoundtripUsec") or event.get("Roundtrip")
        if rtt_raw:
            with suppress(ValueError):
                pjsip_endpoint_rtt.labels(endpoint=endpoint).set(float(rtt_raw) / 1000.0)
                rtt_seen += 1

    reg_resp = await manager.send_action({"Action": "PJSIPShowRegistrationsOutbound"})
    reg_seen = 0
    for event in _iter_ami_events(reg_resp):
        ev = str(event.get("Event", "")).lower()
        if ev not in {"outboundregistrationdetail", "outboundregistrationdetailcomplete"}:
            if "registration" not in ev:
                continue
        reg = event.get("ObjectName") or event.get("Registration") or event.get("Endpoint")
        status = str(event.get("Status") or event.get("State") or "")
        if reg:
            pjsip_registration_status.labels(registration=reg).set(1 if "registered" in status.lower() else 0)
            reg_seen += 1
    return seen > 0, rtt_seen, reg_seen


async def collect_taskprocessors() -> None:
    while True:
        try:
            output = await run_asterisk_command("core show taskprocessors")
            seen: Set[str] = set()
            for line in output.splitlines():
                stripped = line.strip()
                if not stripped or stripped.lower().startswith("processor "):
                    continue
                match = TASKPROCESSOR_REGEX.search(stripped)
                if not match:
                    continue
                name = match.group("name")
                seen.add(name)
                taskprocessor_queue_depth.labels(name=name).set(int(match.group("inqueue")))
                taskprocessor_processed.labels(name=name).set(int(match.group("processed")))
                taskprocessor_high_water.labels(name=name).set(int(match.group("maxdepth")))
        except Exception as exc:
            if (ENABLE_CLI and not FORCE_AMI_COMMAND) or CLI_REQUIRED:
                logger.exception("taskprocessor collector error")
            else:
                logger.debug("taskprocessor collector skipped: %s", exc)
        await asyncio.sleep(POLL_INTERVAL)


async def collect_core() -> None:
    while True:
        try:
            uptime_output = await run_asterisk_command("core show uptime seconds")
            uptime_seconds = parse_uptime_seconds(uptime_output)
            if uptime_seconds is not None:
                asterisk_uptime_seconds.set(uptime_seconds)
            else:
                logger.warning("unable to parse uptime from output: %s", uptime_output.strip())

            channels_output = await run_asterisk_command("core show channels count")
            channels_match = CHANNELS_REGEX.search(channels_output)
            calls_match = CALLS_REGEX.search(channels_output)

            if channels_match:
                asterisk_active_channels.set(int(channels_match.group("channels")))
            if calls_match:
                asterisk_active_calls.set(int(calls_match.group("calls")))

            asterisk_cli_up.set(1 if (ENABLE_CLI and not FORCE_AMI_COMMAND) else 0)
            update_asterisk_up()
        except Exception as exc:
            if (ENABLE_CLI and not FORCE_AMI_COMMAND) or CLI_REQUIRED:
                logger.exception("core collector error")
            else:
                logger.debug("core collector skipped: %s", exc)
            if ENABLE_CLI and not FORCE_AMI_COMMAND:
                asterisk_cli_up.set(0)
        update_asterisk_up()
        await asyncio.sleep(POLL_INTERVAL)


async def collect_pjsip() -> None:
    while True:
        try:
            # Use "Action: Command" output path for endpoints/registrations parsing
            # so behavior matches CLI table parsing semantics.
            output = await run_asterisk_command("pjsip show endpoints")
            output = _normalize_command_output(output)
            current_endpoint = None
            seen_endpoints: Set[str] = set()
            for line in output.splitlines():
                endpoint_match = ENDPOINT_REGEX.search(line)
                if endpoint_match:
                    current_endpoint = endpoint_match.group("endpoint")
                    if current_endpoint.startswith("<"):
                        current_endpoint = None
                        continue
                    inline_match = ENDPOINT_STATUS_INLINE_REGEX.search(line)
                    if inline_match:
                        seen_endpoints.add(current_endpoint)
                        status = _extract_status(inline_match)
                        pjsip_endpoint_status.labels(endpoint=current_endpoint).set(
                            _pjsip_status_to_value(status)
                        )
                        rtt_match = RTT_REGEX.search(line)
                        inline_rtt = rtt_match.group("rtt") if rtt_match else None
                        if inline_rtt:
                            with suppress(ValueError):
                                pjsip_endpoint_rtt.labels(endpoint=current_endpoint).set(
                                    float(inline_rtt)
                                )
                    continue
                contact_match = CONTACT_REGEX.search(line)
                if contact_match and current_endpoint:
                    seen_endpoints.add(current_endpoint)
                    status = _extract_status(contact_match)
                    pjsip_endpoint_status.labels(endpoint=current_endpoint).set(
                        _pjsip_status_to_value(status)
                    )
                    rtt_match = RTT_REGEX.search(line)
                    rtt = rtt_match.group("rtt") if rtt_match else None
                    if rtt is None:
                        tail_match = CONTACT_STATUS_RTT_TAIL_REGEX.search(line)
                        if tail_match:
                            status = tail_match.group("status")
                            pjsip_endpoint_status.labels(endpoint=current_endpoint).set(
                                _pjsip_status_to_value(status)
                            )
                            rtt = tail_match.group("rtt")
                    if rtt:
                        with suppress(ValueError):
                            pjsip_endpoint_rtt.labels(endpoint=current_endpoint).set(float(rtt))

            reg_output = await run_asterisk_command("pjsip show registrations")
            reg_output = _normalize_command_output(reg_output)
            current_registration = None
            for line in reg_output.splitlines():
                out_match = OUTBOUND_REG_REGEX.search(line.strip())
                if out_match:
                    current_registration = out_match.group("name")
                    continue
                state_match = REG_STATUS_REGEX.search(line.strip())
                if current_registration and state_match:
                    state = state_match.group("state").lower()
                    pjsip_registration_status.labels(registration=current_registration).set(
                        1 if "registered" in state else 0
                    )
                    continue
                match = REGISTRATION_ANY_REGEX.search(line.strip())
                if match:
                    state = match.group("state").lower()
                    pjsip_registration_status.labels(registration=match.group("name")).set(
                        1 if state == "registered" else 0
                    )
        except Exception as exc:
            if (ENABLE_CLI and not FORCE_AMI_COMMAND) or CLI_REQUIRED:
                logger.exception("pjsip collector error")
            else:
                logger.debug("pjsip collector skipped: %s", exc)
        await asyncio.sleep(POLL_INTERVAL)


manager = Manager(host=AMI_HOST, port=AMI_PORT, username=AMI_USER, secret=AMI_SECRET)


@manager.register_event("Newchannel")
async def on_new_channel(_manager, event):
    uniqueid = event.get("Uniqueid")
    if uniqueid:
        active_channels[uniqueid] = time.time()
        asterisk_active_channels.set(len(active_channels))


@manager.register_event("Hangup")
async def on_hangup(_manager, event):
    uniqueid = event.get("Uniqueid")
    if uniqueid and uniqueid in active_channels:
        duration = time.time() - active_channels.pop(uniqueid)
        call_duration.observe(duration)
    asterisk_active_channels.set(len(active_channels))


@manager.register_event("DialEnd")
async def on_dial_end(_manager, event):
    if event.get("DialStatus") == "ANSWER":
        calls_answered_total.inc()
    else:
        calls_failed_total.inc()


@manager.register_event("BridgeEnter")
async def on_bridge_enter(_manager, event):
    bridge_id = event.get("BridgeUniqueid")
    if bridge_id:
        active_bridges.add(bridge_id)
        asterisk_active_calls.set(len(active_bridges))


@manager.register_event("BridgeLeave")
async def on_bridge_leave(_manager, event):
    bridge_id = event.get("BridgeUniqueid")
    if bridge_id and bridge_id in active_bridges:
        active_bridges.discard(bridge_id)
    asterisk_active_calls.set(len(active_bridges))


@manager.register_event("ContactStatus")
async def on_contact_status(_manager, event):
    endpoint = event.get("EndpointName") or event.get("AOR")
    status = event.get("ContactStatus") or event.get("Status") or ""
    rtt = event.get("RoundtripUsec") or event.get("Roundtrip") or ""
    if endpoint:
        pjsip_endpoint_status.labels(endpoint=endpoint).set(_pjsip_status_to_value(status))
        if rtt:
            with suppress(ValueError):
                # RoundtripUsec from AMI is in microseconds.
                pjsip_endpoint_rtt.labels(endpoint=endpoint).set(float(rtt) / 1000.0)


@manager.register_event("Registry")
async def on_registry(_manager, event):
    registration = event.get("Domain") or event.get("Username") or event.get("ChannelType")
    state = event.get("Status") or ""
    if registration:
        pjsip_registration_status.labels(registration=registration).set(
            1 if "registered" in state.lower() else 0
        )


@manager.register_event("QueueCallerJoin")
async def on_queue_join(_manager, event):
    queue = event.get("Queue")
    if not queue:
        return
    queue_stats_cache[queue] = queue_stats_cache.get(queue, 0) + 1
    queue_calls.labels(queue=queue).set(queue_stats_cache[queue])


@manager.register_event("QueueCallerLeave")
async def on_queue_leave(_manager, event):
    queue = event.get("Queue")
    if not queue:
        return
    queue_stats_cache[queue] = max(0, queue_stats_cache.get(queue, 0) - 1)
    queue_calls.labels(queue=queue).set(queue_stats_cache[queue])
    queue_completed.labels(queue=queue).inc()


@manager.register_event("QueueCallerAbandon")
async def on_queue_abandon(_manager, event):
    queue = event.get("Queue")
    if queue:
        queue_abandoned.labels(queue=queue).inc()


async def collect_queues() -> None:
    while True:
        try:
            queue_resp = await manager.send_action({"Action": "QueueStatus"})
            got_queue_events = False
            member_counts: Dict[str, int] = {}
            snapshot_totals: Dict[str, Dict[str, float]] = {}
            for event in _iter_ami_events(queue_resp):
                ev = str(event.get("Event", "")).lower()
                if ev == "queuemember":
                    q = event.get("Queue")
                    if q:
                        got_queue_events = True
                        member_counts[q] = member_counts.get(q, 0) + 1
                elif ev == "queueparams":
                    q = event.get("Queue")
                    if q:
                        got_queue_events = True
                        with suppress(ValueError):
                            queue_calls.labels(queue=q).set(float(event.get("Calls", 0)))
                        with suppress(ValueError):
                            queue_holdtime.labels(queue=q).set(float(event.get("Holdtime", 0)))
                        with suppress(ValueError):
                            snapshot_totals.setdefault(q, {})["completed"] = float(
                                event.get("Completed", 0)
                            )
                        with suppress(ValueError):
                            snapshot_totals.setdefault(q, {})["abandoned"] = float(
                                event.get("Abandoned", 0)
                            )
            if not got_queue_events:
                logger.debug("QueueStatus AMI returned no queue events; falling back to command parser")
            for q, count in member_counts.items():
                queue_agents.labels(queue=q).set(count)
            for q, totals in snapshot_totals.items():
                prev = queue_totals_cache.get(q, {"completed": 0.0, "abandoned": 0.0})
                completed = totals.get("completed", prev["completed"])
                abandoned = totals.get("abandoned", prev["abandoned"])
                if completed >= prev["completed"]:
                    delta = completed - prev["completed"]
                    if delta:
                        queue_completed.labels(queue=q).inc(delta)
                if abandoned >= prev["abandoned"]:
                    delta = abandoned - prev["abandoned"]
                    if delta:
                        queue_abandoned.labels(queue=q).inc(delta)
                queue_totals_cache[q] = {"completed": completed, "abandoned": abandoned}

            if got_queue_events:
                await asyncio.sleep(POLL_INTERVAL)
                continue

            output = await run_asterisk_command("queue show")
            output = _normalize_command_output(output)
            current_queue = None
            current_members = 0
            for line in output.splitlines():
                queue_match = QUEUE_REGEX.search(line)
                if queue_match:
                    current_queue = queue_match.group("queue")
                    queue_calls.labels(queue=current_queue).set(int(queue_match.group("calls")))
                    queue_agents.labels(queue=current_queue).set(int(queue_match.group("agents")))
                    current_members = 0
                    holdtime_match = HOLDTIME_REGEX.search(line)
                    if holdtime_match:
                        hold = holdtime_match.group("seconds1") or holdtime_match.group("seconds2")
                        queue_holdtime.labels(queue=current_queue).set(int(hold))
                    continue
                queue_calls_fallback_match = QUEUE_CALLS_FALLBACK_REGEX.search(line)
                if queue_calls_fallback_match:
                    current_queue = queue_calls_fallback_match.group("queue")
                    queue_calls.labels(queue=current_queue).set(
                        int(queue_calls_fallback_match.group("calls"))
                    )
                    current_members = 0
                    continue

                member_match = MEMBER_REGEX.search(line)
                if member_match and current_queue:
                    queue_agents.labels(queue=current_queue).set(int(member_match.group("count")))

                holdtime_match = HOLDTIME_REGEX.search(line)
                if holdtime_match and current_queue:
                    hold = holdtime_match.group("seconds1") or holdtime_match.group("seconds2")
                    queue_holdtime.labels(queue=current_queue).set(int(hold))
                if current_queue and QUEUE_MEMBER_LINE_REGEX.search(line):
                    current_members += 1
                    queue_agents.labels(queue=current_queue).set(current_members)
        except Exception as exc:
            if (ENABLE_CLI and not FORCE_AMI_COMMAND) or CLI_REQUIRED:
                logger.exception("queue collector error")
            else:
                logger.debug("queue collector skipped: %s", exc)
        await asyncio.sleep(POLL_INTERVAL)


async def metrics_handler(_request):
    data = generate_latest(registry)
    return web.Response(body=data, headers={"Content-Type": CONTENT_TYPE_LATEST})


async def health_handler(_request):
    return web.Response(text="ok")


async def main() -> None:
    app = web.Application()
    app.router.add_get("/metrics", metrics_handler)
    app.router.add_get("/health", health_handler)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, EXPORTER_HOST, EXPORTER_PORT)
    await site.start()

    await manager.connect()
    asterisk_ami_up.set(1)
    update_asterisk_up()
    tasks = [
        asyncio.create_task(collect_core()),
        asyncio.create_task(collect_taskprocessors()),
        asyncio.create_task(collect_pjsip()),
        asyncio.create_task(collect_queues()),
    ]
    if FORCE_AMI_COMMAND:
        logger.info("Command collectors are forced to AMI Action: Command (FORCE_AMI_COMMAND=true)")
    elif ENABLE_CLI:
        logger.info("Command collectors use CLI transport when enabled.")
    if not ENABLE_CLI:
        logger.info("CLI disabled. Collecting command-based metrics via AMI Action: Command.")
    if ENABLE_CLI and shutil.which(ASTERISK_BIN) is None:
        logger.warning("Starting without CLI collectors: ASTERISK_BIN=%s not found", ASTERISK_BIN)
    logger.info("Exporter started on %s:%s", EXPORTER_HOST, EXPORTER_PORT)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop_event.set)

    await stop_event.wait()

    for task in tasks:
        task.cancel()
    for task in tasks:
        with suppress(asyncio.CancelledError):
            await task

    await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
