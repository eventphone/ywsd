from enum import Enum

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import ENUM
from sqlalchemy.sql.ddl import CreateTable

metadata = sa.MetaData()


def _plain_loader(fields, source, target, prefix=None):
    for field in fields:
        setattr(target, field, getattr(source, field if prefix is None else prefix+field))


def _transform_loader(fields, source, target, prefix=None):
    for field, transform in fields:
        setattr(target, field, transform(getattr(source, field if prefix is None else prefix+field)))


class DoesNotExist(Exception):
    pass


class Yate:
    table = sa.Table("Yate", metadata,
                     sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
                     sa.Column("hostname", sa.String(256), nullable=False),
                     sa.Column("voip_listener", sa.String(256), nullable=False)
                     )
    FIELDS_PLAIN = ("id", "hostname", "voip_listener")

    def __init__(self, db_row):
        _plain_loader(self.FIELDS_PLAIN, db_row, self)


class Extension:
    table = sa.Table("Extension", metadata,
                     sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
                     sa.Column("yate_id", sa.Integer, sa.ForeignKey("Yate.id")),
                     sa.Column("extension", sa.String(256), nullable=False, unique=True),
                     sa.Column("name", sa.String(256)),
                     sa.Column("type", ENUM("SIMPLE", "MULTIRING", "GROUP", "EXTERNAL", name="extension_type"),
                               nullable=False),
                     sa.Column("outgoing_extension", sa.String(256)),
                     sa.Column("outgoing_name", sa.String(256)),
                     sa.Column("dect_displaymode", ENUM("NUMBER", "NUMBER_AND_NAME", "NAME", name="dect_displaymode")),
                     sa.Column("ringback", sa.String(128)),
                     sa.Column("forwarding_mode", ENUM("DISABLED", "ENABLED", "ON_BUSY", name="forwarding_mode"),
                               nullable=False),
                     sa.Column("forwarding_delay", sa.Integer),
                     sa.Column("forwarding_extension_id", sa.Integer, sa.ForeignKey("Extension.id")),
                     sa.Column("lang", sa.String(8), nullable=False)
                     )
    FIELDS_PLAIN = ("id", "yate_id", "extension", "name", "outgoing_extension", "outgoing_name", "ringback",
                    "forwarding_delay", "forwarding_extension_id", "lang")
    FIELDS_TRANSFORM = (
        ("type", lambda x: Extension.Type[x]),
        ("dect_displaymode", lambda x: Extension.DectDisplaymode[x] if x is not None else None),
        ("forwarding_mode", lambda x: Extension.ForwardingMode[x]),
    )

    class Type(Enum):
        SIMPLE = 0
        MULTIRING = 1
        GROUP = 2
        EXTERNAL = 3

    class DectDisplaymode(Enum):
        NUMBER = 0
        NUMBER_AND_NAME = 1
        NAME = 3

    class ForwardingMode(Enum):
        DISABLED = 0
        ENABLED = 1
        ON_BUSY = 2

    def __init__(self, db_row, prefix=None):
        _plain_loader(self.FIELDS_PLAIN, db_row, self, prefix=prefix)
        _transform_loader(self.FIELDS_TRANSFORM, db_row, self, prefix=prefix)

        self.callgroup_ranks = []
        self.forwarding_extension = None

    def __repr__(self):
        return "<Extension {}, name={}, type={}>".format(self.extension, self.name, self.type)

    @classmethod
    async def load_extension(cls, extension, db_connection):
        res = await db_connection.execute(cls.table.select().where(cls.table.c.extension == extension))
        if res.rowcount == 0:
            raise DoesNotExist("No extension \"{}\" found".format(extension))
        return cls(await res.first())

    async def load_forwarding_extension(self, db_connection):
        if self.forwarding_extension_id is None:
            raise DoesNotExist("This extension has no forwarding extension")
        res = await db_connection.execute(self.table.select().where(self.table.c.id == self.forwarding_extension_id))
        # this always exists and is unique by db constraints
        self.forwarding_extension = Extension(await res.first())

    async def populate_callgroup_ranks(self, db_connection):
        result = await db_connection.execute(
              sa.select([CallgroupRank.table, CallgroupRank.member_table, Extension.table], use_labels=True)
                .where(CallgroupRank.table.c.extension_id == self.id)
                .where(CallgroupRank.member_table.c.extension_id == Extension.table.c.id)
                .where(CallgroupRank.table.c.id == CallgroupRank.member_table.c.callgrouprank_id)
                .order_by(CallgroupRank.table.c.index)
        )
        self.callgroup_ranks = []
        current_rank_id = None
        current_rank = None
        async for row in result:
            if current_rank_id != row.CallgroupRank_id:
                print("There is a new rank!")
                current_rank_id = row.CallgroupRank_id
                current_rank = CallgroupRank(row, prefix="CallgroupRank_")
                self.callgroup_ranks.append(current_rank)
            member = CallgroupRank.Member(CallgroupRank.RankMemberType[row.CallgroupRankMember_rankmember_type],
                                          row.CallgroupRankMember_active,
                                          Extension(row, prefix="Extension_"))
            current_rank.members.append(member)

    @property
    def immediate_forward(self):
        return self.forwarding_mode == Extension.ForwardingMode.ENABLED and self.forwarding_delay == 0

    @property
    def has_active_group_members(self):
        for rank in self.callgroup_ranks:
            if any([m.active for m in rank.members]):
                return True
        return False


class CallgroupRank:
    table = sa.Table("CallgroupRank", metadata,
                     sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
                     sa.Column("extension_id", sa.Integer, sa.ForeignKey("Extension.id"), nullable=False, index=True),
                     sa.Column("index", sa.Integer, nullable=False),
                     sa.Column("mode", ENUM("DEFAULT", "NEXT", "DROP", name="callgroup_rank_mode"), nullable=False),
                     sa.Column("delay", sa.Integer)
                     )
    FIELDS_PLAIN = ("id", "extension_id", "index", "delay")
    FIELDS_TRANSFORM = (
        ("mode", lambda x: CallgroupRank.Mode[x]),
    )
    member_table = sa.Table("CallgroupRankMember", metadata,
                            sa.Column("callgrouprank_id", sa.Integer, sa.ForeignKey("CallgroupRank.id"),
                                      nullable=False, index=True),
                            sa.Column("extension_id", sa.Integer, sa.ForeignKey("Extension.id"),
                                      nullable=False),
                            sa.Column("rankmember_type", ENUM("DEFAULT", "AUXILIARY", "PERSISTENT",
                                                              name="callgroup_rankmember_type"), nullable=False),
                            sa.Column("active", sa.Boolean, nullable=False),
                            sa.UniqueConstraint("callgrouprank_id", "extension_id", name="uniq1")
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
            return self != CallgroupRank.RankMemberType.DEFAULT

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
        _plain_loader(self.FIELDS_PLAIN, db_row, self, prefix=prefix)
        _transform_loader(self.FIELDS_TRANSFORM, db_row, self, prefix=prefix)
        self.members = []

    def __repr__(self):
        return "<CallgroupRank id={}, extension_id={}, index={}, mode={}, delay={}>"\
            .format(self.id, self.extension_id, self.mode, self.index, self.mode, self.delay)


async def regenerate_database_objects(connection):
    await connection.execute("DROP TABLE IF EXISTS \"Yate\" CASCADE")
    await connection.execute("DROP TABLE IF EXISTS \"Extension\" CASCADE")
    await connection.execute("DROP TABLE IF EXISTS \"CallgroupRank\" CASCADE")
    await connection.execute("DROP TABLE IF EXISTS \"CallgroupRankMember\" CASCADE")

    await connection.execute("DROP TYPE IF EXISTS extension_type")
    await connection.execute("CREATE TYPE extension_type AS ENUM('SIMPLE', 'MULTIRING', 'GROUP', 'EXTERNAL')")
    await connection.execute("DROP TYPE IF EXISTS dect_displaymode")
    await connection.execute("CREATE TYPE dect_displaymode AS ENUM('NUMBER', 'NUMBER_AND_NAME', 'NAME')")
    await connection.execute("DROP TYPE IF EXISTS forwarding_mode")
    await connection.execute("CREATE TYPE forwarding_mode AS ENUM('DISABLED', 'ENABLED', 'ON_BUSY')")
    await connection.execute("DROP TYPE IF EXISTS callgroup_rank_mode")
    await connection.execute("CREATE TYPE callgroup_rank_mode AS ENUM('DEFAULT', 'NEXT', 'DROP')")
    await connection.execute("DROP TYPE IF EXISTS callgroup_rankmember_type")
    await connection.execute("CREATE TYPE callgroup_rankmember_type AS ENUM('DEFAULT', 'AUXILIARY', 'PERSISTENT')")

    await connection.execute(CreateTable(Yate.table))
    await connection.execute(CreateTable(Extension.table))
    await connection.execute(CreateTable(CallgroupRank.table))
    await connection.execute(CreateTable(CallgroupRank.member_table))
