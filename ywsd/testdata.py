import asyncio

from ywsd.settings import Settings
from ywsd.objects import *
from aiopg.sa import create_engine

from ywsd.routing_tree import RoutingTree


settings = Settings()


async def reinstall_testdata():
    async with create_engine(**settings.DB_CONFIG) as engine:
        async with engine.acquire() as conn:
            await regenerate_database_objects(conn)
            await write_testdata(conn)


async def get_extension(ext):
    async with create_engine(**settings.DB_CONFIG) as engine:
        async with engine.acquire() as conn:
            res = await Extension.load_extension(ext, conn)
            return res


async def exec_with_db(func):
    async with create_engine(**settings.DB_CONFIG) as engine:
        async with engine.acquire() as conn:
            return await func(conn)


async def async_test_route(src, target, local_yate):
    async with create_engine(**settings.DB_CONFIG) as engine:
        async with engine.acquire() as conn:
            yates = await Yate.load_yates_dict(conn)
            tree = RoutingTree(src, target, settings)
            await tree.discover_tree(conn)
            tree.calculate_routing(local_yate, yates)
            return tree


def test_route(src, target, local_yate):
    l = asyncio.get_event_loop()
    return l.run_until_complete(async_test_route(src, target, local_yate))


def test_route_display(src, target, local_yate):
    tree = test_route(src, target, local_yate)
    print(repr(tree.routing_result))
    print(repr(tree.new_routing_cache_content))


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

    print(repr(yates))

    await conn.execute(
        Extension.table.insert().values(
            [
                {
                    "yate_id": None,
                    "extension": "2000",
                    "name": "PoC",
                    "type": "GROUP",
                    "forwarding_mode": "DISABLED",
                    "lang": "de_DE",
                },
                {
                    "yate_id": yates["dect"],
                    "extension": "2001",
                    "name": "PoC Sascha",
                    "type": "MULTIRING",
                    "forwarding_mode": "DISABLED",
                    "lang": "de_DE",
                },
                {
                    "yate_id": yates["dect"],
                    "extension": "2002",
                    "name": "PoC Bernie",
                    "type": "SIMPLE",
                    "forwarding_mode": "DISABLED",
                    "lang": "de_DE",
                },
                {
                    "yate_id": yates["dect"],
                    "extension": "2004",
                    "name": "PoC BeF",
                    "type": "SIMPLE",
                    "forwarding_mode": "DISABLED",
                    "lang": "de_DE",
                },
                {
                    "yate_id": yates["sip"],
                    "extension": "2005",
                    "name": "PoC Sascha (SIP)",
                    "type": "SIMPLE",
                    "forwarding_mode": "DISABLED",
                    "lang": "de_DE",
                },
                {
                    "yate_id": yates["sip"],
                    "extension": "2042",
                    "name": "PoC Garwin",
                    "type": "SIMPLE",
                    "forwarding_mode": "DISABLED",
                    "lang": "de_DE",
                },
            ]
        )
    )

    exts = {}
    async for row in conn.execute(Extension.table.select()):
        exts[row.extension] = row.id
    print(repr(exts))

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
    print(repr(cgr))

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
