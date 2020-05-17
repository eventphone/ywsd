import asyncio
from collections import namedtuple
from enum import Enum

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import ENUM
from sqlalchemy.sql.ddl import CreateTable, CreateIndex
from sqlalchemy.sql.expression import bindparam

metadata = sa.MetaData()


def _plain_loader(fields, source, target, prefix=None):
    for field in fields:
        setattr(
            target, field, getattr(source, field if prefix is None else prefix + field)
        )


def _transform_loader(fields, source, target, prefix=None):
    for field, transform in fields:
        setattr(
            target,
            field,
            transform(getattr(source, field if prefix is None else prefix + field)),
        )


class DoesNotExist(Exception):
    pass


class User:
    table = sa.Table(
        "users",
        metadata,
        sa.Column(
            "username", sa.String(32), unique=True, nullable=False, primary_key=True
        ),
        sa.Column(
            "displayname",
            sa.String(64),
            server_default="EventphoneUser",
            nullable=False,
        ),
        sa.Column("password", sa.String(128), nullable=False),
        sa.Column("inuse", sa.Integer, nullable=False, server_default="0"),
        sa.Column("type", sa.String(20), server_default="user"),
        sa.Column(
            "dect_displaymode",
            ENUM("NUMBER", "NUMBER_AND_NAME", "NAME", name="dect_displaymode"),
        ),
        sa.Column("trunk", sa.Boolean, nullable=False, server_default="0"),
        sa.Column("call_waiting", sa.Boolean, nullable=False, server_default="1"),
    )

    FIELDS_PLAIN = (
        "username",
        "displayname",
        "password",
        "inuse",
        "type",
        "trunk",
        "call_waiting",
    )
    FIELDS_TRANSFORM = (
        (
            "dect_displaymode",
            lambda x: User.DectDisplaymode[x] if x is not None else None,
        ),
    )

    class DectDisplaymode(Enum):
        NUMBER = 0
        NUMBER_AND_NAME = 1
        NAME = 3

    def __init__(self, db_row, prefix=None):
        _plain_loader(self.FIELDS_PLAIN, db_row, self, prefix=prefix)
        _transform_loader(self.FIELDS_TRANSFORM, db_row, self, prefix=prefix)

    @classmethod
    async def load_user(cls, username, db_connection):
        res = await db_connection.execute(
            cls.table.select().where(cls.table.c.username == username)
        )
        if res.rowcount == 0:
            raise DoesNotExist('No user "{}" found'.format(username))
        return cls(await res.first())

    @classmethod
    async def load_trunk(cls, dialed_number, db_connection):
        res = await db_connection.execute(
            cls.table.select()
            .where(
                bindparam("dialed_number", dialed_number).startswith(
                    cls.table.c.username
                )
            )
            .where(cls.table.c.trunk == True)
        )
        if res.rowcount == 0:
            raise DoesNotExist('No trunk for "{}" found'.format(dialed_number))
        elif res.rowcount > 1:
            raise DoesNotExist(
                "Trunk misconfiguration lead to multiple results for {}".format(
                    dialed_number
                )
            )
        return cls(await res.first())


class Registration:
    table = sa.Table(
        "registrations",
        metadata,
        sa.Column(
            "username",
            sa.String(32),
            sa.ForeignKey("users.username", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("location", sa.String(1024), nullable=False),
        sa.Column("oconnection_id", sa.String(1024), nullable=False),
        sa.Column("expires", sa.TIMESTAMP, nullable=False),
        sa.UniqueConstraint(
            "username", "location", "oconnection_id", name="uniq_registrations"
        ),
    )

    FIELDS_PLAIN = ("username", "location", "oconnection_id", "expires")

    def __init__(self, db_row, prefix=None, user=None, dialed_number=None):
        _plain_loader(self.FIELDS_PLAIN, db_row, self, prefix=prefix)
        self._user = user
        self._dialed_number = dialed_number

    @classmethod
    async def load_locations_for(cls, user: User, dialed_number, db_connection):
        result = []
        res = await db_connection.execute(
            cls.table.select().where(cls.table.c.username == user.username)
        )
        return [cls(row, user=user, dialed_number=dialed_number) async for row in res]

    @property
    def call_target(self):
        if self._user is None or self._user.trunk == False:
            return self.location
        # the location field has the format sip/sip:<user>@<ip>:<port>;<param>=<val>,...
        # for a trunk we want to exchange user by the actually dialed number

        return self.location.replace(
            f"{self._user.username}@", f"{self._dialed_number}@", 1
        )


class ActiveCall:
    table = sa.Table(
        "active_calls",
        metadata,
        sa.Column(
            "username",
            sa.String(32),
            sa.ForeignKey("users.username", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("x_eventphone_id", sa.String(64), nullable=False),
    )

    @classmethod
    async def is_active_call(cls, username, x_eventphone_id, db_connection):
        return (
            await db_connection.scalar(
                cls.table.count()
                .where(cls.table.c.username == username)
                .where(cls.table.c.x_eventphone_id == x_eventphone_id)
            )
        ) > 0


class Yate:
    table = sa.Table(
        "Yate",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("hostname", sa.String(256), nullable=False),
        sa.Column("guru3_identifier", sa.String(32), nullable=False),
        sa.Column("voip_listener", sa.String(256), nullable=False),
    )
    FIELDS_PLAIN = ("id", "hostname", "guru3_identifier", "voip_listener")

    def __init__(self, db_row):
        _plain_loader(self.FIELDS_PLAIN, db_row, self)

    @classmethod
    async def load_yates_dict(cls, db_connection):
        yates_dict = {}
        result = await db_connection.execute(cls.table.select())
        async for row in result:
            yates_dict[row.id] = cls(row)
        return yates_dict


class RoutingTreeNode:
    class LogEntry:
        def __init__(self, msg, level, related_node=None):
            self.msg = msg
            self.level = level
            self.related_node = related_node

        def serialize(self):
            return {
                "msg": self.msg,
                "level": self.level,
                "related_node": self.related_node.tree_identifier,
            }

    def __init__(self):
        self._log = []
        self._tree_identifier = None

    @property
    def tree_identifier(self):
        return self._tree_identifier

    @tree_identifier.setter
    def tree_identifier(self, ti):
        self._tree_identifier = ti

    def routing_log(self, msg, level, related_node=None):
        self._log.append(RoutingTreeNode.LogEntry(msg, level, related_node))

    def serialize(self):
        data = {key: getattr(self, key) for key in self.FIELDS_PLAIN}
        data.update({key: str(getattr(self, key)) for key, _ in self.FIELDS_TRANSFORM})
        data["tree_identifier"] = self.tree_identifier
        data["logs"] = [entry.serialize() for entry in self._log]
        return data


class Extension(RoutingTreeNode):
    table = sa.Table(
        "Extension",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("yate_id", sa.Integer, sa.ForeignKey("Yate.id")),
        sa.Column("extension", sa.String(32), nullable=False, unique=True),
        sa.Column("name", sa.String(64)),
        sa.Column("short_name", sa.String(8)),
        sa.Column(
            "type",
            ENUM(
                "SIMPLE",
                "MULTIRING",
                "GROUP",
                "EXTERNAL",
                "TRUNK",
                name="extension_type",
            ),
            nullable=False,
        ),
        sa.Column("outgoing_extension", sa.String(32)),
        sa.Column("outgoing_name", sa.String(64)),
        sa.Column("dialout_allowed", sa.Boolean, server_default="0"),
        sa.Column("ringback", sa.String(128)),
        sa.Column(
            "forwarding_mode",
            ENUM(
                "DISABLED",
                "ENABLED",
                "ON_BUSY",
                "ON_UNAVAILABLE",
                name="forwarding_mode",
            ),
            nullable=False,
        ),
        sa.Column("forwarding_delay", sa.Integer),
        sa.Column(
            "forwarding_extension_id",
            sa.Integer,
            sa.ForeignKey("Extension.id", ondelete="SET NULL"),
        ),
        sa.Column("lang", sa.String(6), nullable=False),
        sa.CheckConstraint(
            "(forwarding_extension_id IS NOT NULL) OR (forwarding_mode = 'DISABLED')",
            name="fwd_correct",
        ),
        sa.CheckConstraint(
            "(forwarding_mode != 'ENABLED') OR (forwarding_delay IS NOT NULL)",
            name="fwd_delay_correct",
        ),
        sa.CheckConstraint(
            "(yate_id IS NOT NULL) OR (type != 'SIMPLE' AND type != 'MULTIRING')",
            name="yate_id_not_null_for_direct_ring",
        ),
    )
    FIELDS_PLAIN = (
        "id",
        "yate_id",
        "extension",
        "name",
        "short_name",
        "outgoing_extension",
        "outgoing_name",
        "dialout_allowed",
        "ringback",
        "forwarding_delay",
        "forwarding_extension_id",
        "lang",
    )
    FIELDS_TRANSFORM = (
        ("type", lambda x: Extension.Type[x]),
        ("forwarding_mode", lambda x: Extension.ForwardingMode[x]),
    )

    class Type(Enum):
        SIMPLE = 0
        MULTIRING = 1
        GROUP = 2
        EXTERNAL = 3
        TRUNK = 4

    class ForwardingMode(Enum):
        DISABLED = 0
        ENABLED = 1
        ON_BUSY = 2
        ON_UNAVAILABLE = 3

    def __init__(self, db_row, prefix=None):
        super().__init__()
        _plain_loader(self.FIELDS_PLAIN, db_row, self, prefix=prefix)
        _transform_loader(self.FIELDS_TRANSFORM, db_row, self, prefix=prefix)

        self.fork_ranks = []
        self.forwarding_extension = None

    def serialize(self):
        data = super().serialize()
        if self.forwarding_extension is not None:
            data["forwarding_extension"] = self.forwarding_extension.serialize()
        if self.fork_ranks:
            data["fork_ranks"] = [rank.serialize() for rank in self.fork_ranks]
        return data

    def __repr__(self):
        return "<Extension {}, name={}, type={}>".format(
            self.extension, self.name, self.type
        )

    @classmethod
    def create_external(cls, extension, external_name=None):
        if external_name is None:
            external_name = "External"
        params = {
            "id": None,
            "yate_id": None,
            "extension": extension,
            "name": external_name,
            "short_name": None,
            "outgoing_extension": None,
            "outgoing_name": None,
            "dialout_allowed": False,
            "ringback": None,
            "forwarding_delay": None,
            "forwarding_extension_id": None,
            "lang": None,
            "type": "EXTERNAL",
            "forwarding_mode": "DISABLED",
        }
        return cls(namedtuple("Ext_row", params.keys())(*params.values()))

    @classmethod
    def create_unknown(cls, extension):
        params = {
            "id": None,
            "yate_id": None,
            "extension": extension,
            "name": "Unknown",
            "short_name": None,
            "outgoing_extension": None,
            "outgoing_name": None,
            "dialout_allowed": False,
            "ringback": None,
            "forwarding_delay": None,
            "forwarding_extension_id": None,
            "lang": None,
            "type": "SIMPLE",
            "forwarding_mode": "DISABLED",
        }
        return cls(namedtuple("Ext_row", params.keys())(*params.values()))

    @classmethod
    async def load_extension(cls, extension, db_connection):
        res = await db_connection.execute(
            cls.table.select().where(cls.table.c.extension == extension)
        )
        if res.rowcount == 0:
            raise DoesNotExist('No extension "{}" found'.format(extension))
        return cls(await res.first())

    @classmethod
    async def load_trunk_extension(cls, dialed_number, db_connection):
        res = await db_connection.execute(
            cls.table.select()
            .where(
                bindparam("dialed_number", dialed_number).startswith(
                    cls.table.c.extension
                )
            )
            .where(cls.table.c.type == "TRUNK")
        )
        if res.rowcount == 0:
            raise DoesNotExist('No trunk for "{}" found'.format(dialed_number))
        elif res.rowcount > 1:
            raise DoesNotExist(
                "Trunk misconfiguration lead to multiple results for {}".format(
                    dialed_number
                )
            )
        return cls(await res.first())

    async def load_forwarding_extension(self, db_connection):
        if self.forwarding_extension_id is None:
            raise DoesNotExist("This extension has no forwarding extension")
        res = await db_connection.execute(
            self.table.select().where(self.table.c.id == self.forwarding_extension_id)
        )
        # this always exists and is unique by db constraints
        self.forwarding_extension = Extension(await res.first())
        if self.tree_identifier is not None:
            self.forwarding_extension.tree_identifier = (
                self.tree_identifier + "-" + str(self.forwarding_extension.id)
            )

    async def populate_fork_ranks(self, db_connection):
        result = await db_connection.execute(
            sa.select(
                [ForkRank.table, ForkRank.member_table, Extension.table],
                use_labels=True,
            )
            .where(ForkRank.table.c.extension_id == self.id)
            .where(ForkRank.member_table.c.extension_id == Extension.table.c.id)
            .where(ForkRank.table.c.id == ForkRank.member_table.c.forkrank_id)
            .order_by(ForkRank.table.c.index)
        )
        self.fork_ranks = []
        current_rank_id = None
        current_rank = None
        async for row in result:
            if current_rank_id != row.ForkRank_id:
                current_rank_id = row.ForkRank_id
                current_rank = ForkRank(row, prefix="ForkRank_")
                if self.tree_identifier is not None:
                    current_rank.tree_identifier = (
                        self.tree_identifier + "-fr" + str(current_rank.id)
                    )
                self.fork_ranks.append(current_rank)
            member = ForkRank.Member(
                ForkRank.RankMemberType[row.ForkRankMember_rankmember_type],
                row.ForkRankMember_active,
                Extension(row, prefix="Extension_"),
            )
            if self.tree_identifier is not None:
                member.extension.tree_identifier = (
                    current_rank.tree_identifier + "-" + str(member.extension.id)
                )
            current_rank.members.append(member)

    @property
    def immediate_forward(self):
        return (
            self.forwarding_mode == Extension.ForwardingMode.ENABLED
            and self.forwarding_delay == 0
        )

    @property
    def has_active_group_members(self):
        for rank in self.fork_ranks:
            if any([m.active for m in rank.members]):
                return True
        return False


class ForkRank(RoutingTreeNode):
    table = sa.Table(
        "ForkRank",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column(
            "extension_id",
            sa.Integer,
            sa.ForeignKey("Extension.id", ondelete="CASCADE"),
            nullable=False,
            index=True,
        ),
        sa.Column("index", sa.Integer, nullable=False),
        sa.Column(
            "mode",
            ENUM("DEFAULT", "NEXT", "DROP", name="fork_rank_mode"),
            nullable=False,
        ),
        sa.Column("delay", sa.Integer),
        sa.CheckConstraint(
            "(mode = 'DEFAULT') OR (delay IS NOT NULL)", name="delay_correct"
        ),
    )
    FIELDS_PLAIN = ("id", "extension_id", "index", "delay")
    FIELDS_TRANSFORM = (("mode", lambda x: ForkRank.Mode[x]),)
    member_table = sa.Table(
        "ForkRankMember",
        metadata,
        sa.Column(
            "forkrank_id",
            sa.Integer,
            sa.ForeignKey("ForkRank.id", ondelete="CASCADE"),
            nullable=False,
            index=True,
        ),
        sa.Column(
            "extension_id",
            sa.Integer,
            sa.ForeignKey("Extension.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "rankmember_type",
            ENUM("DEFAULT", "AUXILIARY", "PERSISTENT", name="fork_rankmember_type"),
            nullable=False,
        ),
        sa.Column("active", sa.Boolean, nullable=False),
        sa.UniqueConstraint("forkrank_id", "extension_id", name="uniq1"),
    )

    class Mode(Enum):
        DEFAULT = 0
        NEXT = 1
        DROP = 2

    class RankMemberType(Enum):
        DEFAULT = 0
        AUXILIARY = 1
        PERSISTENT = 2

        @property
        def is_special_calltype(self):
            return self != ForkRank.RankMemberType.DEFAULT

        @property
        def fork_calltype(self):
            return self.name.lower()

    class Member:
        def __init__(self, type, active, extension):
            self.type = type
            self.active = active
            self.extension = extension

        def __repr__(self):
            res = "<({})Member: {}".format(self.type, self.extension)
            if self.active:
                return res + ">"
            else:
                return res + " (inactive)>"

    def __init__(self, db_row, prefix=None):
        super().__init__()
        _plain_loader(self.FIELDS_PLAIN, db_row, self, prefix=prefix)
        _transform_loader(self.FIELDS_TRANSFORM, db_row, self, prefix=prefix)
        self.members = []

    def __repr__(self):
        return "<ForkRank id={}, extension_id={}, index={}, mode={}, delay={}>".format(
            self.id, self.extension_id, self.mode, self.index, self.mode, self.delay
        )

    def serialize(self):
        data = super().serialize()
        data["members"] = [
            {
                "type": str(member.type),
                "active": member.active,
                "extension": member.extension.serialize(),
            }
            for member in self.members
        ]
        return data


async def initialize_database(connection, stage2_only=False, stage1_only=False):
    if not stage2_only:
        await connection.execute(
            "CREATE TYPE extension_type AS ENUM('SIMPLE', 'MULTIRING', 'GROUP', 'EXTERNAL', "
            "'TRUNK')"
        )
        await connection.execute(
            "CREATE TYPE forwarding_mode AS ENUM('DISABLED', 'ENABLED', 'ON_BUSY', "
            "'ON_UNAVAILABLE')"
        )
        await connection.execute(
            "CREATE TYPE fork_rank_mode AS ENUM('DEFAULT', 'NEXT', 'DROP')"
        )
        await connection.execute(
            "CREATE TYPE fork_rankmember_type AS ENUM('DEFAULT', 'AUXILIARY', 'PERSISTENT')"
        )

    if not stage1_only:
        await connection.execute(
            "CREATE TYPE dect_displaymode AS ENUM('NUMBER', 'NUMBER_AND_NAME', 'NAME')"
        )

    if not stage2_only:
        await connection.execute(CreateTable(Yate.table))
        await connection.execute(CreateTable(Extension.table))
        await connection.execute(CreateTable(ForkRank.table))
        await connection.execute(CreateTable(ForkRank.member_table))

    if not stage1_only:
        await connection.execute(CreateTable(User.table))
        await connection.execute(CreateTable(Registration.table))
        await connection.execute(CreateTable(ActiveCall.table))


async def regenerate_database_objects(connection, stage2_only=False, stage1_only=False):
    if not stage2_only:
        await connection.execute('DROP TABLE IF EXISTS "Yate" CASCADE')
        await connection.execute('DROP TABLE IF EXISTS "Extension" CASCADE')
        await connection.execute('DROP TABLE IF EXISTS "ForkRank" CASCADE')
        await connection.execute('DROP TABLE IF EXISTS "ForkRankMember" CASCADE')

    if not stage1_only:
        await connection.execute("DROP TABLE IF EXISTS users CASCADE")
        await connection.execute("DROP TABLE IF EXISTS active_calls CASCADE")
        await connection.execute("DROP TABLE IF EXISTS registrations CASCADE")

        await connection.execute("DROP TYPE IF EXISTS dect_displaymode")

    if not stage2_only:
        await connection.execute("DROP TYPE IF EXISTS extension_type")
        await connection.execute("DROP TYPE IF EXISTS forwarding_mode")
        await connection.execute("DROP TYPE IF EXISTS fork_rank_mode")
        await connection.execute("DROP TYPE IF EXISTS fork_rankmember_type")

    await initialize_database(connection, stage2_only, stage1_only)


def main():
    asyncio.run(amain())


async def amain():
    # init database
    import argparse
    import sys

    from aiopg.sa import create_engine

    import ywsd.settings

    parser = argparse.ArgumentParser(description="Yate Routing Engine")
    parser.add_argument(
        "--config", type=str, help="Config file to use.", default="routing_engine.yaml"
    )
    parser.add_argument(
        "--stage2", help="Only setup tables for stage2 routing", action="store_true"
    )
    parser.add_argument(
        "--stage1", help="Only setup tables for stage1 routing", action="store_true"
    )
    parser.add_argument(
        "--regenerate", help="Drop tables if they already exist", action="store_true"
    )

    args = parser.parse_args()
    settings = ywsd.settings.Settings(args.config)

    async with create_engine(**settings.DB_CONFIG) as engine:
        async with engine.acquire() as conn:
            if args.regenerate:
                await regenerate_database_objects(conn, args.stage2, args.stage1)
            else:
                await initialize_database(conn, args.stage2, args.stage1)


if __name__ == "__main__":
    main()
