'''SQLAlchemy dialect for Netezza'''

from sqlalchemy.dialects import registry
from sqlalchemy.engine import reflection
from sqlalchemy.connectors.pyodbc import PyODBCConnector
from sqlalchemy.dialects.postgresql.base import (
    PGDialect, PGTypeCompiler, PGCompiler, PGDDLCompiler, DOUBLE_PRECISION,
    INTERVAL, TIME, TIMESTAMP)
import sqlalchemy.types as sqltypes
from sqlalchemy.schema import DDLElement, SchemaItem
from sqlalchemy.sql import text, bindparam
import pyodbc
import re
from sqlalchemy.schema import DDLElement
from sqlalchemy.ext.compiler import compiles


# pylint:disable=R0901,W0212


class ST_GEOMETRY(sqltypes.Binary):
    __visit_name__ = 'ST_GEOMETRY'


class BYTEINT(sqltypes.INTEGER):
    __visit_name__ = 'BYTEINT'


class NVARCHAR(sqltypes.NVARCHAR):
    '''Netezza NVARCHAR'''
    def __init__(self, length=None, collation=None,
                 convert_unicode='force',
                 unicode_error=None):
        super(NVARCHAR, self).__init__(
            length,
            collation=collation,
            convert_unicode=convert_unicode,
            unicode_error='ignore')


class OID(sqltypes.BigInteger):
    '''System table only type'''
    __visit_name__ = 'OID'


class NAME(NVARCHAR):
    '''System table only type'''
    __visit_name__ = 'NAME'


class ABSTIME(sqltypes.TIME):
    '''System table only type'''
    __visit_name__ = 'ABSTIME'


# Weird types gleaned from _v_datatype
ischema_names = {
    'st_geometry': ST_GEOMETRY,
    'byteint': BYTEINT,
    'oid': OID,
    'name': NAME,
}


class NetezzaTypeCompiler(PGTypeCompiler):
    '''Fills out unique netezza types'''

    def visit_ST_GEOMETRY(self, type_):
        return 'ST_GEOMETRY({})'.format(type_.length)

    def visit_BYTEINT(self, _type):
        return 'BYTEINT'

    def visit_OID(self, _type):
        return 'OID'

    def visit_NAME(self, _type):
        return 'NAME'

    def visit_ABSTIME(self, _type):
        return 'ABSTIME'


class NetezzaCompiler(PGCompiler):
    '''Handles some quirks of netezza queries'''

    def limit_clause(self, select):
        '''Netezza doesn't allow sql params in the limit/offset piece'''
        text = ""
        if select._limit is not None:
            text += " \n LIMIT {limit}".format(limit=int(select._limit))
        if select._offset is not None:
            if select._limit is None:
                text += " \n LIMIT ALL"
            text += " OFFSET {offset}".format(offset=int(select._offset))
        return text


class DistributeOn(SchemaItem):
    '''Represents a distribute on clause'''

    def __init__(self, *column_names):
        '''Use like:
        my_table_1 = Table('my_table_1', metadata,
            Column('id_key', BIGINT),
            Column('nbr', BIGINT),
            DistributeOn('id_key')
        )
        my_table_2 = Table('my_table_2', metadata,
            Column('id_key', BIGINT),
            Column('nbr', BIGINT),
            DistributeOn('random')
        )
        '''
        self.column_names = column_names if column_names else ('RANDOM',)

    def _set_parent(self, parent):
        self.parent = parent
        parent.distribute_on = self


class NetezzaDDLCompiler(PGDDLCompiler):
    '''Adds Netezza specific DDL clauses'''

    def post_create_table(self, table):
        '''Adds the `distribute on` clause to create table expressions'''
        clause = ' DISTRIBUTE ON {columns}'
        if hasattr(table, 'distribute_on') and \
           table.distribute_on.column_names[0].lower() != 'random':
            column_list = ','.join(table.distribute_on.column_names)
            columns = '({})'.format(column_list)
        else:
            columns = 'RANDOM'
        return clause.format(columns=columns)

# Maps type ids to sqlalchemy types, plus whether they have variable precision
oid_datatype_map = {
    16: (sqltypes.Boolean, False),
    18: (sqltypes.CHAR, False),
    19: (NAME, False),
    20: (sqltypes.BigInteger, False),
    21: (sqltypes.SmallInteger, False),
    23: (sqltypes.Integer, False),
    25: (sqltypes.TEXT, False),
    26: (OID, False),
    700: (sqltypes.REAL, False),
    701: (DOUBLE_PRECISION, False),
    702: (ABSTIME, False),
    1042: (sqltypes.CHAR, True),
    1043: (sqltypes.String, True),
    1082: (sqltypes.Date, False),
    1083: (TIME, False),
    1184: (TIMESTAMP, False),
    1186: (INTERVAL, False),
    1266: (TIMESTAMP, False),
    1700: (sqltypes.Numeric, False),
    2500: (BYTEINT, False),
    2522: (sqltypes.NCHAR, True),
    2530: (sqltypes.NVARCHAR, True),
    2552: (ST_GEOMETRY, True),
    2568: (sqltypes.VARBINARY, True),
}


class NetezzaODBC(PyODBCConnector, PGDialect):
    '''Attempts to reuse as much as possible from the postgresql and pyodbc
    dialects.
    '''

    name = 'netezza'
    encoding = 'latin9'
    default_paramstyle = 'qmark'
    returns_unicode_strings = False
    supports_native_enum = False
    supports_sequences = True
    sequences_optional = False
    isolation_level = 'READ COMMITTED'
    max_identifier_length = 128
    type_compiler = NetezzaTypeCompiler
    statement_compiler = NetezzaCompiler
    ddl_compiler = NetezzaDDLCompiler
    description_encoding = None

    def initialize(self, connection):
        super(NetezzaODBC, self).initialize(connection)
        # PyODBC connector tries to set these to true...
        self.supports_unicode_statements = False
        self.supports_unicode_binds = False
        self.returns_unicode_strings = True
        self.convert_unicode = 'ignore'
        self.encoding = 'latin9'
        self.ischema_names.update(ischema_names)

    def has_table(self, connection, tablename, schema=None):
        '''Checks if the table exists in the current database'''
        # Have to filter by database name because the table could exist in
        # another database on the same machine
        dbname = connection.connection.getinfo(pyodbc.SQL_DATABASE_NAME)
        sql = ('select count(*) from _v_object_data where objname = ? '
               'and dbname = ?')
        result = connection.execute(sql, (str(tablename), dbname)).scalar()
        return bool(result)

    def get_table_names(self, connection, schema=None, **kw):
        result = connection.execute(
            "select tablename as name from _v_table "
            "where tablename not like '_t_%'")
        table_names = [r[0] for r in result]
        return table_names

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        SQL_COLS = """
            SELECT CAST(a.attname AS VARCHAR(128)) as name,
                   a.atttypid as typeid,
                   not a.attnotnull as nullable,
                   a.attcolleng as length,
                   a.format_type
            FROM _v_relation_column a
            WHERE a.name = :tablename
            ORDER BY a.attnum
        """
        s = text(SQL_COLS,
                 bindparams=[bindparam('tablename', type_=sqltypes.String)],
                 typemap={'name': NAME,
                          'typeid': sqltypes.Integer,
                          'nullable': sqltypes.Boolean,
                          'length': sqltypes.Integer,
                          'format_type': sqltypes.String,
                          })
        c = connection.execute(s, tablename=table_name)
        rows = c.fetchall()
        # format columns
        columns = []
        for name, typeid, nullable, length, format_type in rows:
            coltype_class, has_length = oid_datatype_map[typeid]
            if coltype_class is sqltypes.Numeric:
                precision, scale = re.match(
                    r'numeric\((\d+),(\d+)\)', format_type).groups()
                coltype = coltype_class(int(precision), int(scale))
            elif has_length:
                coltype = coltype_class(length)
            else:
                coltype = coltype_class()
            columns.append({
                'name': name,
                'type': coltype,
                'nullable': nullable,
            })
        return columns

    @reflection.cache
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        '''Netezza doesn't have PK/unique constraints'''
        return {'constrained_columns': [], 'name': None}

    @reflection.cache
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        '''Netezza doesn't have foreign keys'''
        return []

    @reflection.cache
    def get_indexes(self, connection, table_name, schema=None, **kw):
        '''Netezza doesn't have indexes'''
        return []

    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        result = connection.execute(
            "select viewname as name from _v_view"
            "where viewname not like '_v_%'")
        return [r[0] for r in result]

    def get_isolation_level(self, connection):
        return self.isolation_level

    def _get_default_schema_name(self, connection):
        '''Netezza doesn't use schemas'''
        raise NotImplementedError

    def _check_unicode_returns(self, connection):
        '''Netezza doesn't *do* unicode (except in nchar & nvarchar)'''
        pass


class CreateTableAs(DDLElement):
    """Create a CREATE TABLE AS SELECT ... statement."""

    def __init__(self,
                 new_table_name,
                 selectable,
                 temporary=False,
                 distribute_on='random'):
        '''Distribute_on may be a tuple of column names'''
        super(CreateTableAs, self).__init__()
        self.selectable = selectable
        self.temporary = temporary
        self.new_table_name = new_table_name
        self.distribute_on = distribute_on

    def distribute_clause(self):
        if self.distribute_on.lower() != 'random':
            column_list = ','.join(self.distribute_on)
            return '({})'.format(column_list)
        else:
            return 'RANDOM'


@compiles(CreateTableAs)
def visit_create_table_as(element, compiler, **_kwargs):
    '''compiles a ctas statement'''
    return """
        CREATE {tmp} TABLE {name} AS (
        {select}
        ) DISTRIBUTE ON {distribute}
    """.format(
        tmp='TEMP' if element.temporary else '',
        name=element.new_table_name,
        select=compiler.sql_compiler.process(element.selectable),
        distribute=element.distribute_clause(),
    )


registry.register("netezza", "netezza_dialect", "NetezzaODBC")
registry.register(
    "netezza.pyodbc", "netezza_dialect", "NetezzaODBC")
