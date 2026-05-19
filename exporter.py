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

from aiohttp import web
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
cli_available = True
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
CONTACT_REGEX = re.compile(r"Contact:\s+.*Avail:\s+(?P<status>\w+).*RTT:\s+(?P<rtt>[\d\.]+)")
REGISTRATION_OK_REGEX = re.compile(r"^(?P<name>\S+)\s+Registered\b")
QUEUE_REGEX = re.compile(r"(?P<queue>\S+)\s+has\s+(?P<calls>\d+)\s+calls")
MEMBER_REGEX = re.compile(r"Members:\s+(?P<count>\d+)")
HOLDTIME_REGEX = re.compile(r"holdtime\s+(?P<holdtime>\d+)")




def update_asterisk_up() -> None:
    asterisk_up.set(1 if (asterisk_ami_up._value.get() > 0 or asterisk_cli_up._value.get() > 0) else 0)


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


async def run_asterisk_cmd(command: str) -> str:
    global cli_available, cli_error_logged

    if not cli_available:
        raise RuntimeError("asterisk CLI unavailable")

    if not ENABLE_CLI:
        raise RuntimeError("CLI collection is disabled (ENABLE_CLI=false)")

    if shutil.which(ASTERISK_BIN) is None:
        cli_available = False
        asterisk_cli_up.set(0)
        update_asterisk_up()
        if not cli_error_logged:
            logger.error(
                "Asterisk CLI binary '%s' not found in PATH. "
                "For containerized setup prefer AMI metrics; only enable CLI when binary is available in astexporter container.",
                ASTERISK_BIN,
            )
            cli_error_logged = True
        raise RuntimeError(f"{ASTERISK_BIN} not found")
    cmd = [ASTERISK_BIN, "-rx", command]

    proc = await asyncio.create_subprocess_exec(*cmd, stdout=PIPE, stderr=PIPE)
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=COMMAND_TIMEOUT)
    except asyncio.TimeoutError:
        with suppress(ProcessLookupError):
            proc.kill()
        raise RuntimeError(f"timeout running: {command}")

    if proc.returncode != 0:
        err = stderr.decode(errors="replace").strip()
        if "not found" in err.lower():
            cli_available = False
            asterisk_cli_up.set(0)
            update_asterisk_up()
            if not cli_error_logged:
                logger.error(
                    "Asterisk CLI command failed because executable/container was not found: %s",
                    err,
                )
                cli_error_logged = True
        raise RuntimeError(f"{command} failed: {err}")
    return stdout.decode(errors="replace")


async def collect_taskprocessors() -> None:
    while True:
        try:
            output = await run_asterisk_cmd("core show taskprocessors")
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
            if cli_available or CLI_REQUIRED:
                logger.exception("taskprocessor collector error")
            else:
                logger.debug("taskprocessor collector skipped: %s", exc)
        await asyncio.sleep(POLL_INTERVAL)


async def collect_core() -> None:
    while True:
        try:
            uptime_output = await run_asterisk_cmd("core show uptime seconds")
            uptime_seconds = parse_uptime_seconds(uptime_output)
            if uptime_seconds is not None:
                asterisk_uptime_seconds.set(uptime_seconds)
            else:
                logger.warning("unable to parse uptime from output: %s", uptime_output.strip())

            channels_output = await run_asterisk_cmd("core show channels count")
            channels_match = CHANNELS_REGEX.search(channels_output)
            calls_match = CALLS_REGEX.search(channels_output)

            if channels_match:
                asterisk_active_channels.set(int(channels_match.group("channels")))
            if calls_match:
                asterisk_active_calls.set(int(calls_match.group("calls")))

            asterisk_cli_up.set(1)
            update_asterisk_up()
        except Exception as exc:
            if cli_available or CLI_REQUIRED:
                logger.exception("core collector error")
            else:
                logger.debug("core collector skipped: %s", exc)
            asterisk_cli_up.set(0)
        update_asterisk_up()
        await asyncio.sleep(POLL_INTERVAL)


async def collect_pjsip() -> None:
    while True:
        try:
            output = await run_asterisk_cmd("pjsip show endpoints")
            current_endpoint = None
            seen_endpoints: Set[str] = set()
            for line in output.splitlines():
                endpoint_match = ENDPOINT_REGEX.search(line)
                if endpoint_match:
                    current_endpoint = endpoint_match.group("endpoint")
                    continue
                contact_match = CONTACT_REGEX.search(line)
                if contact_match and current_endpoint:
                    seen_endpoints.add(current_endpoint)
                    status = contact_match.group("status")
                    rtt = float(contact_match.group("rtt"))
                    pjsip_endpoint_status.labels(endpoint=current_endpoint).set(
                        1 if status.lower() == "avail" else 0
                    )
                    pjsip_endpoint_rtt.labels(endpoint=current_endpoint).set(rtt)

            reg_output = await run_asterisk_cmd("pjsip show registrations")
            for line in reg_output.splitlines():
                match = REGISTRATION_OK_REGEX.search(line.strip())
                if match:
                    pjsip_registration_status.labels(registration=match.group("name")).set(1)
        except Exception as exc:
            if cli_available or CLI_REQUIRED:
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
            output = await run_asterisk_cmd("queue show")
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
            if cli_available or CLI_REQUIRED:
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

    tasks = []
    if ENABLE_CLI:
        tasks.extend(
            [
                asyncio.create_task(collect_core()),
                asyncio.create_task(collect_taskprocessors()),
                asyncio.create_task(collect_pjsip()),
                asyncio.create_task(collect_queues()),
            ]
        )
    else:
        logger.info("CLI collectors are disabled (ENABLE_CLI=false). Using AMI-first metrics mode.")

    await manager.connect()
    asterisk_ami_up.set(1)
    update_asterisk_up()
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
