import asyncio
from unittest import mock

import pytest

from yate.protocol import MessageRequest


@pytest.mark.asyncio
@pytest.mark.usefixtures("ywsd_engine_ysim")
async def test_ywsd_sim_base(ywsd_engine_ysim):
    engine, yate_sim = ywsd_engine_ysim
    assert True


@pytest.mark.asyncio
@pytest.mark.usefixtures("ywsd_engine_ysim")
async def test_ywsd_noroute_4748_to_4747(ywsd_engine_ysim):
    engine, yate_sim = ywsd_engine_ysim

    msg = MessageRequest("call.route", {"caller": "4748", "called": "4747"})
    result = await yate_sim.submit_message(msg)
    # ywsd doesn't answer the message
    assert not result.processed


@pytest.mark.asyncio
@pytest.mark.usefixtures("ywsd_engine_ysim")
async def test_ywsd_noroute_caller_param_population_2001_to_4747(ywsd_engine_ysim):
    engine, yate_sim = ywsd_engine_ysim

    msg = MessageRequest(
        "call.route", {"caller": "2001", "username": "2001", "called": "4747"}
    )
    result = await yate_sim.submit_message(msg)
    # ywsd doesn't answer the message
    assert not result.processed
    assert result.params["callername"] == "PoC Sascha"
    assert result.params["osip_X-Caller-Language"] == "de_DE"


@pytest.mark.asyncio
@pytest.mark.usefixtures("ywsd_engine_ysim")
async def test_ywsd_basic_4748_to_2004(ywsd_engine_ysim):
    engine, yate_sim = ywsd_engine_ysim

    msg = MessageRequest("call.route", {"caller": "4748", "called": "2004"})
    result = await yate_sim.submit_message(msg)
    assert result.processed
    assert result.return_value == "sip/sip:2004@dect"
    assert result.params["oconnection_id"] == "local"
    assert result.params["calledname"] == "PoC BeF"


@pytest.mark.asyncio
@pytest.mark.usefixtures("ywsd_engine_ysim")
async def test_ywsd_caller_authentication(ywsd_engine_ysim):
    engine, yate_sim = ywsd_engine_ysim

    msg = MessageRequest("call.route", {"caller": "2001", "called": "2004"})
    result = await yate_sim.submit_message(msg)
    assert result.processed
    assert result.return_value == ""
    assert result.params["error"] == "noauth"

    msg = MessageRequest(
        "call.route", {"caller": "2001", "called": "2004", "username": "2001"}
    )
    result = await yate_sim.submit_message(msg)
    assert result.processed
    assert result.return_value == "sip/sip:2004@dect"
    assert result.params["oconnection_id"] == "local"
    assert result.params["calledname"] == "PoC BeF"


@pytest.mark.asyncio
@pytest.mark.usefixtures("ywsd_engine_ysim")
async def test_ywsd_authenticated_caller_info_population(ywsd_engine_ysim):
    engine, yate_sim = ywsd_engine_ysim

    msg = MessageRequest(
        "call.route", {"caller": "2001", "called": "2004", "username": "2001"}
    )
    result = await yate_sim.submit_message(msg)
    assert result.processed
    assert result.params["callername"] == "PoC Sascha"
    assert result.params["osip_X-Caller-Language"] == "de_DE"


@pytest.mark.asyncio
@pytest.mark.usefixtures("ywsd_engine_ysim")
async def test_ywsd_ringback_4748_to_2002(ywsd_engine_ysim):
    with mock.patch("ywsd.routing_tree.RoutingTree.ringback_exists", lambda x: True):
        engine, yate_sim = ywsd_engine_ysim

        msg = MessageRequest("call.route", {"caller": "4748", "called": "2002"})
        result = await yate_sim.submit_message(msg)
        assert result.processed
        assert result.return_value == "fork"
        assert (
            result.params["callto.1"]
            == "wave/play//opt/sounds/39bb3bad01bf931b34f3983536c0f331e4b4e3e38fb78abfc75e5b09efd6507f.slin"
        )
        assert result.params["callto.2"] == "sip/sip:2002@dect"
        assert result.params["callto.1.fork.calltype"] == "persistent"
        assert result.params["callto.1.fork.autoring"] == "true"
        assert result.params["callto.1.fork.automessage"] == "call.progress"


@pytest.mark.asyncio
@pytest.mark.usefixtures("ywsd_engine_ysim")
async def test_ywsd_no_ringback_4748_to_2002_if_no_file(ywsd_engine_ysim):
    engine, yate_sim = ywsd_engine_ysim

    msg = MessageRequest("call.route", {"caller": "4748", "called": "2002"})
    result = await yate_sim.submit_message(msg)
    assert result.processed
    assert result.return_value == "sip/sip:2002@dect"


@pytest.mark.asyncio
@pytest.mark.usefixtures("ywsd_engine_ysim")
async def test_ywsd_full_routing_with_stage2(ywsd_engine_ysim):
    engine, yate_sim = ywsd_engine_ysim

    msg = MessageRequest("call.route", {"caller": "4748", "called": "2005"})
    result = await yate_sim.submit_message(msg)
    assert result.processed
    assert result.return_value == "lateroute/2005"
    assert result.params["eventphone_stage2"] == "1"

    # trigger stage 2 routing
    msg = MessageRequest("call.route", result.params)
    result = await yate_sim.submit_message(msg)
    assert result.processed
    assert result.return_value == "sip/sip:2005@1.2.3.4/foo"


@pytest.mark.asyncio
@pytest.mark.usefixtures("ywsd_engine_ysim")
async def test_ywsd_full_routing_with_stage2_no_callwaiting(ywsd_engine_ysim):
    engine, yate_sim = ywsd_engine_ysim

    # we inform the internal busy cache that 2042 is on a call
    msg = MessageRequest("call.cdr", {"operation": "initialize", "external": "2042"})
    result = await yate_sim.submit_message(msg)
    assert not result.processed
    # unfortunately the routing engine processes this asynchronously, we need to artificially wait here for completion
    await asyncio.sleep(0.5)

    msg = MessageRequest("call.route", {"caller": "4748", "called": "2042"})
    result = await yate_sim.submit_message(msg)
    assert result.processed
    assert result.return_value == "lateroute/2042"
    assert result.params["eventphone_stage2"] == "1"

    # trigger stage 2 routing
    msg = MessageRequest("call.route", result.params)
    result = await yate_sim.submit_message(msg)
    assert result.processed
    assert result.return_value == ""
    assert result.params["error"] == "busy"


@pytest.mark.asyncio
@pytest.mark.usefixtures("ywsd_engine_ysim")
async def test_ywsd_multi_stage_call_4748_to_2000(ywsd_engine_ysim):
    engine, yate_sim = ywsd_engine_ysim

    msg = MessageRequest("call.route", {"caller": "4748", "called": "2000"})
    result = await yate_sim.submit_message(msg)
    assert result.processed
    assert result.return_value == "fork"
    assert result.params["callto.1"].startswith("lateroute/stage1-")
    assert result.params["callto.2"] == "sip/sip:2002@dect"
    assert result.params["callto.3"] == "sip/sip:2004@dect"
    assert result.params["callto.4"] == "lateroute/2042"
    assert result.params["callto.4.eventphone_stage2"] == "1"

    # Fetch intermediate lateroute result
    forkleg_parameters = {
        key: value
        for (key, value) in result.params.items()
        if not key.startswith("callto.")
    }
    forkleg_parameters["called"] = result.params["callto.1"].lstrip("lateroute/")

    msg = MessageRequest("call.route", forkleg_parameters)
    result = await yate_sim.submit_message(msg)
    assert result.processed
    assert result.return_value == "fork"
    assert result.params["callto.1"] == "sip/sip:2001@dect"
    assert result.params["callto.2"] == "lateroute/2005"
    assert result.params["callto.2.eventphone_stage2"] == "1"

    # Now we want to to know, let's also evaluate stage2 for 2005
    forkleg_parameters = {
        key: value
        for (key, value) in result.params.items()
        if not key.startswith("callto.")
    }
    forkleg_parameters.update(
        {
            key[len("callto.2.") :]: value
            for (key, value) in result.params.items()
            if key.startswith("callto.2.")
        }
    )
    forkleg_parameters["called"] = result.params["callto.2"].lstrip("lateroute/")
    msg = MessageRequest("call.route", forkleg_parameters)
    result = await yate_sim.submit_message(msg)
    assert result.processed
    assert result.return_value == "sip/sip:2005@1.2.3.4/foo"


@pytest.mark.asyncio
@pytest.mark.usefixtures("ywsd_engine_ysim")
async def test_ywsd_delayed_call_fwd_4748_to_2099(ywsd_engine_ysim):
    engine, yate_sim = ywsd_engine_ysim

    msg = MessageRequest("call.route", {"caller": "4748", "called": "2099"})
    result = await yate_sim.submit_message(msg)
    assert result.processed
    assert result.return_value == "fork"
    assert result.params["x_originally_called"] == "2099"
    assert result.params["osip_X-Originally-Called"] == "2099"
    assert result.params["callto.1"] == "lateroute/2099"
    assert result.params["callto.2"] == "|drop=20"
    assert result.params["callto.3"] == "lateroute/2042"


@pytest.mark.asyncio
@pytest.mark.usefixtures("ywsd_engine_ysim")
async def test_ywsd_immediate_call_fwd_4748_to_2098(ywsd_engine_ysim):
    engine, yate_sim = ywsd_engine_ysim

    msg = MessageRequest("call.route", {"caller": "4748", "called": "2098"})
    result = await yate_sim.submit_message(msg)
    assert result.processed
    assert result.return_value == "lateroute/2005"
    assert result.params["eventphone_stage2"] == "1"
    assert result.params["x_originally_called"] == "2098"
    assert result.params["osip_X-Originally-Called"] == "2098"
