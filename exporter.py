import asyncio
import logging
import os
import re
import shlex
import signal
import time
from asyncio.subprocess import PIPE
from contextlib import suppress
from typing import Dict, Set

import uvloop
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


asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

AMI_HOST = os.getenv("AMI_HOST", "127.0.0.1")
AMI_PORT = int(os.getenv("AMI_PORT", "5038"))
AMI_USER = os.getenv("AMI_USER", "exporter")
AMI_SECRET = os.getenv("AMI_SECRET", "strongpassword")

EXPORTER_HOST = os.getenv("EXPORTER_HOST", "0.0.0.0")
EXPORTER_PORT = int(os.getenv("EXPORTER_PORT", "9631"))

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "15"))
ASTERISK_BIN = os.getenv("ASTERISK_BIN", "asterisk")
COMMAND_TIMEOUT = float(os.getenv("COMMAND_TIMEOUT", "8"))

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("astexporter")

registry = CollectorRegistry()

asterisk_up = Gauge("asterisk_up", "Asterisk availability", registry=registry)
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
queue_stats_cache: Dict[str, int] = {}

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


async def run_asterisk_cmd(command: str) -> str:
    full_cmd = f"{shlex.quote(ASTERISK_BIN)} -rx {shlex.quote(command)}"
    proc = await asyncio.create_subprocess_shell(full_cmd, stdout=PIPE, stderr=PIPE)
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=COMMAND_TIMEOUT)
    except asyncio.TimeoutError:
        with suppress(ProcessLookupError):
            proc.kill()
        raise RuntimeError(f"timeout running: {command}")

    if proc.returncode != 0:
        raise RuntimeError(f"{command} failed: {stderr.decode().strip()}")
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
        except Exception:
            logger.exception("taskprocessor collector error")
        await asyncio.sleep(POLL_INTERVAL)


async def collect_core() -> None:
    while True:
        try:
            uptime_output = await run_asterisk_cmd("core show uptime seconds")
            uptime_match = UPTIME_REGEX.search(uptime_output)
            if uptime_match:
                asterisk_uptime_seconds.set(int(uptime_match.group("seconds")))
            else:
                logger.warning("unable to parse uptime from output: %s", uptime_output.strip())

            channels_output = await run_asterisk_cmd("core show channels count")
            channels_match = CHANNELS_REGEX.search(channels_output)
            calls_match = CALLS_REGEX.search(channels_output)

            if channels_match:
                asterisk_active_channels.set(int(channels_match.group("channels")))
            if calls_match:
                asterisk_active_calls.set(int(calls_match.group("calls")))

            asterisk_up.set(1)
        except Exception:
            logger.exception("core collector error")
            asterisk_up.set(0)
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
        except Exception:
            logger.exception("pjsip collector error")
        await asyncio.sleep(POLL_INTERVAL)


manager = Manager(host=AMI_HOST, port=AMI_PORT, username=AMI_USER, secret=AMI_SECRET)


@manager.register_event("Newchannel")
async def on_new_channel(_manager, event):
    uniqueid = event.get("Uniqueid")
    if uniqueid:
        active_channels[uniqueid] = time.time()


@manager.register_event("Hangup")
async def on_hangup(_manager, event):
    uniqueid = event.get("Uniqueid")
    if uniqueid and uniqueid in active_channels:
        duration = time.time() - active_channels.pop(uniqueid)
        call_duration.observe(duration)


@manager.register_event("DialEnd")
async def on_dial_end(_manager, event):
    if event.get("DialStatus") == "ANSWER":
        calls_answered_total.inc()
    else:
        calls_failed_total.inc()


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
        except Exception:
            logger.exception("queue collector error")
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

    tasks = [
        asyncio.create_task(collect_core()),
        asyncio.create_task(collect_taskprocessors()),
        asyncio.create_task(collect_pjsip()),
        asyncio.create_task(collect_queues()),
    ]

    await manager.connect()
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
