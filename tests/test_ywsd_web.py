from unittest import mock
import json
import logging
import sys

import aiohttp
import pytest

from ywsd import objects

logging.basicConfig(level=logging.DEBUG)


def validate_routing_target(observed, expected):
    assert observed["target"] == expected["target"]
    if "parameters" in expected:
        for key in expected["parameters"].keys():
            assert observed["parameters"][key] == expected["parameters"][key]


def validate_fork_targets(observed, expected):
    assert len(observed) == len(expected)
    for observed_item, expected_item in zip(observed, expected):
        validate_routing_target(observed_item, expected_item)


def validate_routing_tree(observed, expected):
    # we validate routing trees recursively
    for key in expected.keys():
        if key == "fork_ranks":
            continue
        assert observed[key] == expected[key]

    assert ("fork_ranks" in observed) == ("fork_ranks" in expected)
    if "fork_ranks" in observed:
        for observed_rank, expected_rank in zip(
            observed["fork_ranks"], expected["fork_ranks"]
        ):
            for key in expected_rank.keys():
                if key == "members":
                    continue
                assert observed_rank[key] == expected_rank[key]
        assert len(observed_rank["members"]) == len(expected_rank["members"])
        for observed_member, expected_member in zip(
            observed_rank["members"], expected_rank["members"]
        ):
            if "type" in expected_member:
                assert observed_member["type"] == expected_member["type"]
            if "active" in expected_member:
                assert observed_member["active"] == expected_member["active"]
            validate_routing_tree(
                observed_member["extension"], expected_member["extension"]
            )


def test_database_installation(postgres_server, ywsd_test_config):
    init_args = ["ywsd_init", "--config", str(ywsd_test_config)]
    with mock.patch.object(sys, "argv", init_args):
        objects.main()


@pytest.mark.asyncio
@pytest.mark.usefixtures("ywsd_engine_web")
async def test_webserver_noroute(ywsd_engine_web):
    async with aiohttp.ClientSession() as session:
        logging.debug("Test 2000 -> 4747")
        async with session.get(
            "http://localhost:9042/stage1?caller=2000&called=4747"
        ) as response:
            data = await response.json()
            assert data["routing_status"] == "ERROR"
            assert "noroute" in data["routing_status_details"]


@pytest.mark.asyncio
@pytest.mark.usefixtures("ywsd_engine_web")
async def test_webserver_simple_4748_to_2004(ywsd_engine_web):
    async with aiohttp.ClientSession() as session:
        logging.debug("Test 4748 -> 2002")
        async with session.get(
            "http://localhost:9042/stage1?caller=4748&called=2004"
        ) as response:
            data = await response.json()
            assert data["routing_status"] == "OK"
            main_routing_result = data["main_routing_result"]
            assert main_routing_result["type"] == "Type.SIMPLE"
            expected_routing_target = {
                "target": "sip/sip:2004@dect",
                "parameters": {
                    "oconnection_id": "local",
                    "calledname": "PoC BeF",
                },
            }

            validate_routing_target(
                main_routing_result["target"], expected_routing_target
            )
            expected_routing_tree = {
                "extension": "2004",
                "type": "Type.SIMPLE",
            }
            validate_routing_tree(data["routing_tree"], expected_routing_tree)


@pytest.mark.asyncio
@pytest.mark.usefixtures("ywsd_engine_web")
async def test_webserver_simple_with_ringback_4748_to_2002(ywsd_engine_web):
    with mock.patch("ywsd.routing_tree.RoutingTree.ringback_exists", lambda x: True):
        async with aiohttp.ClientSession() as session:
            logging.debug("Test 4748 -> 2002")
            async with session.get(
                "http://localhost:9042/stage1?caller=4748&called=2002"
            ) as response:
                data = await response.json()
                assert data["routing_status"] == "OK"
                main_routing_result = data["main_routing_result"]
                assert main_routing_result["type"] == "Type.FORK"
                expected_fork_targets = [
                    {
                        "target": "wave/play//opt/sounds/39bb3bad01bf931b34f3983536c0f331e4b4e3e38fb78abfc75e5b09efd6507f.slin",
                        "parameters": {
                            "fork.automessage": "call.progress",
                            "fork.autoring": "true",
                            "fork.calltype": "persistent",
                        },
                    },
                    {
                        "target": "sip/sip:2002@dect",
                        "parameters": {
                            "oconnection_id": "local",
                            "calledname": "PoC Bernie",
                        },
                    },
                ]
                validate_fork_targets(
                    main_routing_result["fork_targets"], expected_fork_targets
                )
                expected_routing_tree = {
                    "extension": "2002",
                    "type": "Type.SIMPLE",
                    "ringback": "39bb3bad01bf931b34f3983536c0f331e4b4e3e38fb78abfc75e5b09efd6507f",
                }
                validate_routing_tree(data["routing_tree"], expected_routing_tree)


@pytest.mark.asyncio
@pytest.mark.usefixtures("ywsd_engine_web")
async def test_webserver_4748_to_2001(ywsd_engine_web):
    async with aiohttp.ClientSession() as session:
        logging.debug("Test 4748 -> 2001")
        async with session.get(
            "http://localhost:9042/stage1?caller=4748&called=2001"
        ) as response:
            data = await response.json()
            assert data["routing_status"] == "OK"
            main_routing_result = data["main_routing_result"]
            assert main_routing_result["type"] == "Type.FORK"
            expected_fork_targets = [
                {
                    "target": "sip/sip:2001@dect",
                    "parameters": {"oconnection_id": "local"},
                },
                {"target": "lateroute/2005", "parameters": {"eventphone_stage2": "1"}},
            ]
            validate_fork_targets(
                main_routing_result["fork_targets"], expected_fork_targets
            )
            expected_routing_tree = {
                "extension": "2001",
                "type": "Type.MULTIRING",
                "fork_ranks": [
                    {
                        "delay": None,
                        "members": [
                            {"active": True, "extension": {"extension": "2005"}}
                        ],
                    }
                ],
            }
            validate_routing_tree(data["routing_tree"], expected_routing_tree)


@pytest.mark.asyncio
@pytest.mark.usefixtures("ywsd_engine_web")
async def test_webserver_groupcall_4748_to_2000(ywsd_engine_web):
    async with aiohttp.ClientSession() as session:
        logging.debug("Test 4748 -> 2001")
        async with session.get(
            "http://localhost:9042/stage1?caller=4748&called=2000"
        ) as response:
            data = await response.json()
            assert data["routing_status"] == "OK"
            main_routing_result = data["main_routing_result"]
            assert main_routing_result["type"] == "Type.FORK"
            fork_targets = main_routing_result["fork_targets"]
            assert fork_targets[0]["target"].startswith("lateroute/stage1-")
            assert fork_targets[1]["target"] == "sip/sip:2002@dect"
            assert fork_targets[2]["target"] == "sip/sip:2004@dect"
            assert fork_targets[3]["target"] == "lateroute/2042"
            expected_routing_tree = {
                "extension": "2000",
                "type": "Type.GROUP",
                "fork_ranks": [
                    {
                        "delay": None,
                        "members": [
                            {
                                "active": True,
                                "extension": {
                                    "extension": "2001",
                                    "type": "Type.MULTIRING",
                                    "fork_ranks": [
                                        {
                                            "delay": None,
                                            "members": [
                                                {
                                                    "active": True,
                                                    "extension": {"extension": "2005"},
                                                }
                                            ],
                                        }
                                    ],
                                },
                            },
                            {
                                "active": True,
                                "extension": {
                                    "extension": "2002",
                                    "type": "Type.SIMPLE",
                                },
                            },
                            {
                                "active": True,
                                "extension": {
                                    "extension": "2004",
                                    "type": "Type.SIMPLE",
                                },
                            },
                            {
                                "active": True,
                                "extension": {
                                    "extension": "2042",
                                    "type": "Type.SIMPLE",
                                },
                            },
                        ],
                    }
                ],
            }
            validate_routing_tree(data["routing_tree"], expected_routing_tree)


@pytest.mark.asyncio
@pytest.mark.usefixtures("ywsd_engine_web")
async def test_webserver_delayed_forward_4748_to_2099(ywsd_engine_web):
    async with aiohttp.ClientSession() as session:
        async with session.get(
            "http://localhost:9042/stage1?caller=4748&called=2099"
        ) as response:
            data = await response.json()
            assert data["routing_status"] == "OK"
            main_routing_result = data["main_routing_result"]
            assert main_routing_result["type"] == "Type.FORK"
            assert (
                main_routing_result["target"]["parameters"]["x_originally_called"]
                == "2099"
            )
            assert (
                main_routing_result["target"]["parameters"]["osip_X-Originally-Called"]
                == "2099"
            )
            expected_fork_targets = [
                {
                    "target": "lateroute/2099",
                    "parameters": {"eventphone_stage2": "1"},
                },
                {"target": "|drop=20"},
                {
                    "target": "lateroute/2042",
                    "parameters": {
                        "eventphone_stage2": "1",
                    },
                },
            ]
            validate_fork_targets(
                main_routing_result["fork_targets"], expected_fork_targets
            )
            expected_routing_tree = {
                "extension": "2099",
                "type": "Type.SIMPLE",
                "forwarding_mode": "ForwardingMode.ENABLED",
                "forwarding_delay": 20,
            }
            validate_routing_tree(data["routing_tree"], expected_routing_tree)
            assert data["routing_tree"]["forwarding_extension"]["extension"] == "2042"


@pytest.mark.asyncio
@pytest.mark.usefixtures("ywsd_engine_web")
async def test_webserver_immediate_forward_4748_to_2098(ywsd_engine_web):
    async with aiohttp.ClientSession() as session:
        async with session.get(
            "http://localhost:9042/stage1?caller=4748&called=2098"
        ) as response:
            data = await response.json()
            assert data["routing_status"] == "OK"
            main_routing_result = data["main_routing_result"]
            assert main_routing_result["type"] == "Type.SIMPLE"
            validate_routing_target(
                main_routing_result["target"],
                {
                    "target": "lateroute/2005",
                    "parameters": {
                        "x_originally_called": "2098",
                        "osip_X-Originally-Called": "2098",
                    },
                },
            )
            expected_routing_tree = {
                "extension": "2098",
                "type": "Type.SIMPLE",
                "forwarding_mode": "ForwardingMode.ENABLED",
                "forwarding_delay": 0,
            }
            validate_routing_tree(data["routing_tree"], expected_routing_tree)
            assert data["routing_tree"]["forwarding_extension"]["extension"] == "2005"


@pytest.mark.asyncio
@pytest.mark.usefixtures("ywsd_engine_web")
async def test_webserver_delayed_forward_to_empty_group_4000(ywsd_engine_web):
    async with aiohttp.ClientSession() as session:
        async with session.get(
            "http://localhost:9042/stage1?caller=4748&called=4001"
        ) as response:
            data = await response.json()
            print(repr(data))
            assert data["routing_status"] == "OK"
            main_routing_result = data["main_routing_result"]
            assert main_routing_result["type"] == "Type.FORK"
            expected_fork_targets = [
                {
                    "target": "lateroute/4001",
                    "parameters": {"eventphone_stage2": "1"},
                },
            ]
            validate_fork_targets(
                main_routing_result["fork_targets"], expected_fork_targets
            )
            expected_routing_tree = {
                "extension": "4001",
                "type": "Type.SIMPLE",
                "forwarding_mode": "ForwardingMode.ENABLED",
                "forwarding_delay": 10,
            }
            validate_routing_tree(data["routing_tree"], expected_routing_tree)
            assert data["routing_tree"]["forwarding_extension"]["extension"] == "4000"
