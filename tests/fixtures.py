import logging
from pathlib import Path
import asyncio
import pytest
import time

import aiopg
import docker
import psycopg2
import pytest_asyncio

from . import testdata
from .yatesim import YateGlobalSim
from ywsd.engine import YateRoutingEngine
from ywsd.settings import Settings


@pytest.fixture(scope="session")
def postgres_server():
    client = docker.from_env()

    # Find and stop old containers
    running_containers = client.containers.list()
    for container in running_containers:
        postgres_port = container.ports.get("5432/tcp")
        if postgres_port is not None and postgres_port[0]["HostPort"] == "50432":
            container.stop()

    container = client.containers.run(
        "postgres:15-bookworm",
        detach=True,
        auto_remove=True,
        ports={"5432/tcp": 50432},
        environment={
            "POSTGRES_USER": "ywsd",
            "POSTGRES_DB": "ywsd",
            "POSTGRES_PASSWORD": "123456",
        },
    )
    # wait until the database is ready
    while True:
        try:
            conn = psycopg2.connect(
                host="localhost",
                port=50432,
                user="ywsd",
                password="123456",
                database="ywsd",
            )
            break
        except psycopg2.OperationalError:
            time.sleep(0.5)
    conn.close()
    yield {
        "host": "localhost",
        "port": 50432,
        "database": "ywsd",
        "user": "ywsd",
        "password": "123456",
    }
    container.stop()


@pytest_asyncio.fixture(scope="session")
async def postgres_server_with_data(postgres_server):
    async with aiopg.sa.create_engine(**postgres_server) as engine:
        async with engine.acquire() as conn:
            await testdata.write_testdata(conn)

    yield postgres_server


@pytest.fixture()
def ywsd_test_config():
    return Path(__file__).parent / "testdata" / "test_config.yaml"


@pytest_asyncio.fixture()
async def ywsd_engine_web(postgres_server_with_data, ywsd_test_config):
    settings = Settings(ywsd_test_config)
    start_complete = asyncio.Event()
    engine = YateRoutingEngine(
        settings=settings, web_only=True, startup_complete_event=start_complete
    )

    main_task = asyncio.create_task(engine.main(42))
    await start_complete.wait()
    yield engine
    engine.trigger_shutdown()
    await main_task


@pytest_asyncio.fixture()
async def yate_simulator(tmp_path):
    sim = YateGlobalSim(tmp_path / "ysim_sock")
    await sim.run()
    yield sim
    await sim.stop()


@pytest_asyncio.fixture()
async def ywsd_engine_ysim(postgres_server_with_data, ywsd_test_config, yate_simulator):
    settings = Settings(ywsd_test_config)
    connection = {"sockpath": yate_simulator.path}
    start_complete = asyncio.Event()
    engine = YateRoutingEngine(
        settings=settings,
        web_only=False,
        startup_complete_event=start_complete,
        **connection
    )

    main_task = asyncio.create_task(engine._amain(engine.main))
    await start_complete.wait()
    yield engine, yate_simulator
    engine.trigger_shutdown()
    await main_task
