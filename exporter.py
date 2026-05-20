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
CLI_DOCKER_SOCKET = os.getenv("CLI_DOCKER_SOCKET", "/var/run/docker.sock")
CLI_DOCKER_CONTAINER = os.getenv("CLI_DOCKER_CONTAINER", "voip-asterisk")
COMMAND_TIMEOUT = float(os.getenv("COMMAND_TIMEOUT", "8"))
CLI_REQUIRED = os.getenv("CLI_REQUIRED", "false").lower() == "true"

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("astexporter")

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
ENDPOINT_REGEX = re.compile(r"Endpoint:\s+(?P<endpoint>\S+)")
ENDPOINT_STATUS_INLINE_REGEX = re.compile(
    r"Endpoint:\s+\S+.*\bAvail:\s*(?P<status>[A-Za-z]+)(?:\s+.*RTT:\s*(?P<rtt>[\d\.]+))?"
)
CONTACT_REGEX = re.compile(
    r"Contact:\s+.*\bAvail:\s*(?P<status>[A-Za-z]+)(?:\s+.*RTT:\s*(?P<rtt>[\d\.]+))?"
)
REGISTRATION_OK_REGEX = re.compile(r"^(?P<name>\S+)\s+Registered\b")
QUEUE_REGEX = re.compile(r"(?P<queue>\S+)\s+has\s+(?P<calls>\d+)\s+calls")
MEMBER_REGEX = re.compile(r"Members:\s+(?P<count>\d+)")
HOLDTIME_REGEX = re.compile(r"holdtime\s+(?P<holdtime>\d+)")




def update_asterisk_up() -> None:
    asterisk_up.set(1 if (asterisk_ami_up._value.get() > 0 or asterisk_cli_up._value.get() > 0) else 0)


def _pjsip_status_to_value(status: str) -> int:
    st = status.lower()
    if st in {"avail", "available", "ok", "reachable", "lagged"}:
        return 1
    return 0


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
    if response is None:
        return ""
    if isinstance(response, dict):
        chunks = []
        if "Output" in response and response["Output"] is not None:
            chunks.append(str(response["Output"]))
        if "output" in response and response["output"] is not None:
            chunks.append(str(response["output"]))
        if "data" in response and response["data"] is not None:
            chunks.append(str(response["data"]))
        return "\n".join(chunks).strip()
    if isinstance(response, (list, tuple)):
        chunks = []
        for item in response:
            if isinstance(item, dict):
                out = item.get("Output")
                if out is not None:
                    chunks.append(str(out))
                low_out = item.get("output")
                if low_out is not None:
                    chunks.append(str(low_out))
            elif item is not None:
                chunks.append(str(item))
        return "\n".join(chunks).strip()
    return str(response).strip()


async def run_asterisk_command(command: str) -> str:
    if ENABLE_CLI:
        return await run_asterisk_cmd(command)

    if not ENABLE_AMI_COMMAND:
        raise RuntimeError("Both CLI and AMI command modes are disabled")

    response = await manager.send_action({"Action": "Command", "Command": command})
    output = _extract_ami_command_output(response)
    if not output:
        raise RuntimeError(f"empty AMI Command output for: {command}")
    return output


async def collect_taskprocessors() -> None:
    while True:
        try:
            output = await run_asterisk_command("core show taskprocessors")
            seen: Set[str] = set()
            for line in output.splitlines():
                match = TASKPROCESSOR_REGEX.search(line)
                if not match:
                    continue
                name = match.group("name")
                seen.add(name)
                taskprocessor_queue_depth.labels(name=name).set(int(match.group("inqueue")))
                taskprocessor_processed.labels(name=name).set(int(match.group("processed")))
                taskprocessor_high_water.labels(name=name).set(int(match.group("maxdepth")))
        except Exception as exc:
            if ENABLE_CLI or CLI_REQUIRED:
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

            asterisk_cli_up.set(1 if ENABLE_CLI else 0)
            update_asterisk_up()
        except Exception as exc:
            if ENABLE_CLI or CLI_REQUIRED:
                logger.exception("core collector error")
            else:
                logger.debug("core collector skipped: %s", exc)
            if ENABLE_CLI:
                asterisk_cli_up.set(0)
        update_asterisk_up()
        await asyncio.sleep(POLL_INTERVAL)


async def collect_pjsip() -> None:
    while True:
        try:
            output = await run_asterisk_command("pjsip show endpoints")
            current_endpoint = None
            seen_endpoints: Set[str] = set()
            for line in output.splitlines():
                endpoint_match = ENDPOINT_REGEX.search(line)
                if endpoint_match:
                    current_endpoint = endpoint_match.group("endpoint")
                    inline_match = ENDPOINT_STATUS_INLINE_REGEX.search(line)
                    if inline_match:
                        seen_endpoints.add(current_endpoint)
                        status = inline_match.group("status")
                        pjsip_endpoint_status.labels(endpoint=current_endpoint).set(
                            _pjsip_status_to_value(status)
                        )
                        inline_rtt = inline_match.group("rtt")
                        if inline_rtt:
                            with suppress(ValueError):
                                pjsip_endpoint_rtt.labels(endpoint=current_endpoint).set(
                                    float(inline_rtt)
                                )
                    continue
                contact_match = CONTACT_REGEX.search(line)
                if contact_match and current_endpoint:
                    seen_endpoints.add(current_endpoint)
                    status = contact_match.group("status")
                    pjsip_endpoint_status.labels(endpoint=current_endpoint).set(
                        _pjsip_status_to_value(status)
                    )
                    rtt = contact_match.group("rtt")
                    if rtt:
                        with suppress(ValueError):
                            pjsip_endpoint_rtt.labels(endpoint=current_endpoint).set(float(rtt))

            reg_output = await run_asterisk_command("pjsip show registrations")
            for line in reg_output.splitlines():
                match = REGISTRATION_OK_REGEX.search(line.strip())
                if match:
                    pjsip_registration_status.labels(registration=match.group("name")).set(1)
        except Exception as exc:
            if ENABLE_CLI or CLI_REQUIRED:
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
            output = await run_asterisk_command("queue show")
            current_queue = None
            for line in output.splitlines():
                queue_match = QUEUE_REGEX.search(line)
                if queue_match:
                    current_queue = queue_match.group("queue")
                    queue_calls.labels(queue=current_queue).set(int(queue_match.group("calls")))
                    continue

                member_match = MEMBER_REGEX.search(line)
                if member_match and current_queue:
                    queue_agents.labels(queue=current_queue).set(int(member_match.group("count")))

                holdtime_match = HOLDTIME_REGEX.search(line)
                if holdtime_match and current_queue:
                    queue_holdtime.labels(queue=current_queue).set(int(holdtime_match.group("holdtime")))
        except Exception as exc:
            if ENABLE_CLI or CLI_REQUIRED:
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
