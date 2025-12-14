import asyncio

import pytest

from yate.protocol import MessageRequest

# Please note that this testfile is called zbusy cache because the ywsd_web test suite has a test case
# to test database initialization. However, this test case needs to run before all other tests as
# otherwise the testdata set cannot be ingested into the database.
# This is probably not the most elegant solution but works for now.


@pytest.mark.asyncio
@pytest.mark.usefixtures("ywsd_engine_ysim")
async def test_ywsd_busy_cache_empty(ywsd_engine_ysim):
    engine, yate_sim = ywsd_engine_ysim
    assert len(await engine.busy_cache.busy_status()) == 0


@pytest.mark.asyncio
@pytest.mark.usefixtures("ywsd_engine_ysim")
async def test_ywsd_busy_extension(ywsd_engine_ysim):
    engine, yate_sim = ywsd_engine_ysim

    msg = MessageRequest("call.cdr", {"operation": "initialize", "external": "2042"})
    await yate_sim.submit_message(msg)
    await asyncio.sleep(
        0.2
    )  # ywsd processes this asynchronously. Give the other async task a chance to complete here
    assert await engine.busy_cache.is_busy("2042")


@pytest.mark.asyncio
@pytest.mark.usefixtures("ywsd_engine_ysim")
async def test_ywsd_extension_was_busy(ywsd_engine_ysim):
    engine, yate_sim = ywsd_engine_ysim

    msg = MessageRequest("call.cdr", {"operation": "initialize", "external": "2042"})
    await yate_sim.submit_message(msg)
    await asyncio.sleep(
        0.2
    )  # ywsd processes this asynchronously. Give the other async task a chance to complete here
    msg = MessageRequest("call.cdr", {"operation": "finalize", "external": "2042"})
    await yate_sim.submit_message(msg)
    await asyncio.sleep(0.2)
    assert await engine.busy_cache.is_busy("2042") is False


@pytest.mark.asyncio
@pytest.mark.usefixtures("ywsd_engine_ysim")
async def test_ywsd_busy_extension_when_call_knocking(ywsd_engine_ysim):
    engine, yate_sim = ywsd_engine_ysim

    msg = MessageRequest("call.cdr", {"operation": "initialize", "external": "2042"})
    await yate_sim.submit_message(msg)
    await asyncio.sleep(
        0.2
    )  # ywsd processes this asynchronously. Give the other async task a chance to complete here
    # another call knocks
    msg = MessageRequest("call.cdr", {"operation": "initialize", "external": "2042"})
    await yate_sim.submit_message(msg)
    await asyncio.sleep(0.2)
    # first call is finished
    msg = MessageRequest("call.cdr", {"operation": "finalize", "external": "2042"})
    await yate_sim.submit_message(msg)
    await asyncio.sleep(0.2)
    # Extension should still be busy
    assert await engine.busy_cache.is_busy("2042")


@pytest.mark.asyncio
@pytest.mark.usefixtures("ywsd_engine_ysim")
async def test_ywsd_extension_free_when_all_calls_finalized(ywsd_engine_ysim):
    engine, yate_sim = ywsd_engine_ysim

    msg = MessageRequest("call.cdr", {"operation": "initialize", "external": "2042"})
    await yate_sim.submit_message(msg)
    await asyncio.sleep(
        0.2
    )  # ywsd processes this asynchronously. Give the other async task a chance to complete here
    # another call knocks
    msg = MessageRequest("call.cdr", {"operation": "initialize", "external": "2042"})
    await yate_sim.submit_message(msg)
    await asyncio.sleep(0.2)
    # first call is finished
    msg = MessageRequest("call.cdr", {"operation": "finalize", "external": "2042"})
    await yate_sim.submit_message(msg)
    await asyncio.sleep(0.2)
    # second call is finished
    msg = MessageRequest("call.cdr", {"operation": "finalize", "external": "2042"})
    await yate_sim.submit_message(msg)
    await asyncio.sleep(0.2)

    assert await engine.busy_cache.is_busy("2042") is False


@pytest.mark.asyncio
@pytest.mark.usefixtures("ywsd_engine_ysim")
async def test_ywsd_extension_free_when_other_busy(ywsd_engine_ysim):
    engine, yate_sim = ywsd_engine_ysim

    msg = MessageRequest("call.cdr", {"operation": "initialize", "external": "2024"})
    await yate_sim.submit_message(msg)
    await asyncio.sleep(
        0.2
    )  # ywsd processes this asynchronously. Give the other async task a chance to complete here
    assert await engine.busy_cache.is_busy("2042") is False
