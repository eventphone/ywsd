from datetime import datetime

from ywsd.objects import *


yates_dict = {1: "dect", 2: "sip", 3: "app"}


async def write_testdata(conn):
    await conn.execute(
        Yate.table.insert().values(
            [
                {
                    "hostname": "dect",
                    "voip_listener": "local",
                    "guru3_identifier": "DECT",
                },
                {
                    "hostname": "sip",
                    "voip_listener": "local",
                    "guru3_identifier": "SIP",
                },
                {
                    "hostname": "app",
                    "voip_listener": "local",
                    "guru3_identifier": "APP",
                },
            ]
        )
    )

    yates = {}
    async for row in conn.execute(Yate.table.select()):
        yates[row.hostname] = row.id

    await conn.execute(
        Extension.table.insert().values(
            [
                {
                    "yate_id": None,
                    "extension": "2000",
                    "name": "PoC",
                    "type": "GROUP",
                    "forwarding_mode": "DISABLED",
                    "forwarding_delay": None,
                    "lang": "de_DE",
                    "ringback": None,
                },
                {
                    "yate_id": yates["dect"],
                    "extension": "2001",
                    "name": "PoC Sascha",
                    "type": "MULTIRING",
                    "forwarding_mode": "DISABLED",
                    "forwarding_delay": None,
                    "lang": "de_DE",
                    "ringback": None,
                },
                {
                    "yate_id": yates["dect"],
                    "extension": "2002",
                    "name": "PoC Bernie",
                    "type": "SIMPLE",
                    "forwarding_mode": "DISABLED",
                    "forwarding_delay": None,
                    "lang": "de_DE",
                    "ringback": "39bb3bad01bf931b34f3983536c0f331e4b4e3e38fb78abfc75e5b09efd6507f",
                },
                {
                    "yate_id": yates["dect"],
                    "extension": "2004",
                    "name": "PoC BeF",
                    "type": "SIMPLE",
                    "forwarding_mode": "DISABLED",
                    "forwarding_delay": None,
                    "lang": "de_DE",
                    "ringback": None,
                },
                {
                    "yate_id": yates["sip"],
                    "extension": "2005",
                    "name": "PoC Sascha (SIP)",
                    "type": "SIMPLE",
                    "forwarding_mode": "DISABLED",
                    "forwarding_delay": None,
                    "lang": "de_DE",
                    "ringback": None,
                },
                {
                    "yate_id": yates["sip"],
                    "extension": "2042",
                    "name": "PoC Garwin",
                    "type": "SIMPLE",
                    "forwarding_mode": "DISABLED",
                    "forwarding_delay": None,
                    "lang": "de_DE",
                    "ringback": None,
                },
                {
                    "yate_id": yates["sip"],
                    "extension": "2099",
                    "name": "PoC Helpdesk",
                    "type": "SIMPLE",
                    "forwarding_mode": "DISABLED",
                    "forwarding_delay": 20,
                    "lang": "de_DE",
                    "ringback": None,
                },
                {
                    "yate_id": yates["sip"],
                    "extension": "2098",
                    "name": "PoC Helpdesk II",
                    "type": "SIMPLE",
                    "forwarding_mode": "DISABLED",
                    "forwarding_delay": 0,
                    "lang": "de_DE",
                    "ringback": None,
                },
                {
                    "yate_id": None,
                    "extension": "4000",
                    "name": "Empty Group",
                    "type": "GROUP",
                    "forwarding_mode": "DISABLED",
                    "forwarding_delay": None,
                    "lang": "de_DE",
                    "ringback": None,
                },
                {
                    "yate_id": yates["sip"],
                    "extension": "4001",
                    "name": "I Forward to empty group",
                    "type": "SIMPLE",
                    "forwarding_mode": "DISABLED",
                    "forwarding_delay": 10,
                    "lang": "de_DE",
                    "ringback": None,
                },
            ]
        )
    )

    exts = {}
    async for row in conn.execute(Extension.table.select()):
        exts[row.extension] = row.id

    await conn.execute(
        Extension.table.update()
        .where(Extension.table.c.extension == "2099")
        .values({"forwarding_extension_id": exts["2042"], "forwarding_mode": "ENABLED"})
    )
    await conn.execute(
        Extension.table.update()
        .where(Extension.table.c.extension == "2098")
        .values({"forwarding_extension_id": exts["2005"], "forwarding_mode": "ENABLED"})
    )
    await conn.execute(
        Extension.table.update()
        .where(Extension.table.c.extension == "4001")
        .values({"forwarding_extension_id": exts["4000"], "forwarding_mode": "ENABLED"})
    )

    await conn.execute(
        ForkRank.table.insert().values(
            [
                {"extension_id": exts["2000"], "index": 0, "mode": "DEFAULT"},
                {"extension_id": exts["2001"], "index": 0, "mode": "DEFAULT"},
            ]
        )
    )

    cgr = {}
    async for row in conn.execute(ForkRank.table.select()):
        cgr[row.extension_id] = row.id

    await conn.execute(
        ForkRank.member_table.insert().values(
            [
                {
                    "forkrank_id": cgr[exts["2000"]],
                    "extension_id": exts["2001"],
                    "rankmember_type": "DEFAULT",
                    "active": True,
                },
                {
                    "forkrank_id": cgr[exts["2000"]],
                    "extension_id": exts["2002"],
                    "rankmember_type": "DEFAULT",
                    "active": True,
                },
                {
                    "forkrank_id": cgr[exts["2000"]],
                    "extension_id": exts["2004"],
                    "rankmember_type": "DEFAULT",
                    "active": True,
                },
                {
                    "forkrank_id": cgr[exts["2000"]],
                    "extension_id": exts["2042"],
                    "rankmember_type": "DEFAULT",
                    "active": True,
                },
                {
                    "forkrank_id": cgr[exts["2001"]],
                    "extension_id": exts["2005"],
                    "rankmember_type": "DEFAULT",
                    "active": True,
                },
            ]
        )
    )

    # Stage 2 testdata
    await conn.execute(
        User.table.insert().values(
            [
                {
                    "username": "2005",
                    "displayname": "PoC Sascha (SIP)",
                    "password": "secret",
                    "call_waiting": True,
                    "inuse": 0,
                },
                {
                    "username": "2042",
                    "displayname": "PoC Garwin",
                    "password": "secret",
                    "call_waiting": False,
                    "inuse": 1,
                },
            ]
        )
    )
    await conn.execute(
        Registration.table.insert().values(
            [
                {
                    "username": "2005",
                    "location": "sip/sip:2005@1.2.3.4/foo",
                    "oconnection_id": "internet",
                    "expires": datetime(2199, 12, 31, 10, 10),
                },
                {
                    "username": "2042",
                    "location": "sip/sip:2042@4.3.2.1/bar",
                    "oconnection_id": "internet",
                    "expires": datetime(2199, 12, 31, 10, 10),
                },
            ]
        )
    )
    await conn.execute(
        ActiveCall.table.insert().values(
            [
                {
                    "username": "2042",
                    "x_eventphone_id": "83ded8b334034789a2c0e1405a54af76",
                },
            ]
        )
    )
