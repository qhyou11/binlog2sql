# -*- coding: utf-8 -*-
import struct
import sys
import os

import pymysql

from pymysql.cursors import DictCursor
from pymysql.util import int2byte
from pymysql.connections import MysqlPacket

from pymysqlreplication.packet import BinLogPacketWrapper
from pymysqlreplication.constants.BINLOG import TABLE_MAP_EVENT, ROTATE_EVENT
from pymysqlreplication.event import (
    QueryEvent, RotateEvent, FormatDescriptionEvent,
    XidEvent, GtidEvent, StopEvent,
    BeginLoadQueryEvent, ExecuteLoadQueryEvent,
    HeartbeatLogEvent, NotImplementedEvent)
from pymysqlreplication.row_event import (
    UpdateRowsEvent, WriteRowsEvent, DeleteRowsEvent, TableMapEvent)

# 2013 Connection Lost
# 2006 MySQL server has gone away
MYSQL_EXPECTED_ERROR_CODES = [2013, 2006]


def to_uint8(byts) :
    retval,= struct.unpack('B' , byts)
    return retval

def to_uint16(byts) :
    retval,= struct.unpack('H' , byts)
    return retval

def to_uint32(byts) :
    retval,= struct.unpack('I' , byts)
    return retval

def to_uint64(byts) :
    retval,= struct.unpack('Q' , byts)
    return retval

def to_char(byts) :
    retval,= struct.unpack('c' , byts)
    return retval

def dump_packet(data):  # pragma: no cover
    def printable(data):
        if 32 <= data < 127:
            return chr(data)
        return "."

    try:
        print("packet length:", len(data))
        for i in range(1, 7):
            f = sys._getframe(i)
            print("call[%d]: %s (line %d)" % (i, f.f_code.co_name, f.f_lineno))
        print("-" * 66)
    except ValueError:
        pass
    dump_data = [data[i : i + 16] for i in range(0, min(len(data), 256), 16)]
    for d in dump_data:
        print(
            " ".join("{:02X}".format(x) for x in d)
            + "   " * (16 - len(d))
            + " " * 2
            + "".join(printable(x) for x in d)
        )
    print("-" * 66)
    print() 

class Constant :
    BINLOG_HEADER = b'\xfebin'
    BINLOG_HEADER_LEN = 4
    EVENT_HEADER_LEN = 19

class EventType :
    UNKNOWN_EVENT= 0
    START_EVENT_V3= 1
    QUERY_EVENT= 2
    STOP_EVENT= 3
    ROTATE_EVENT= 4
    INTVAR_EVENT= 5
    LOAD_EVENT= 6
    SLAVE_EVENT= 7
    CREATE_FILE_EVENT= 8
    APPEND_BLOCK_EVENT= 9
    EXEC_LOAD_EVENT= 10
    DELETE_FILE_EVENT= 11
    NEW_LOAD_EVENT= 12
    RAND_EVENT= 13
    USER_VAR_EVENT= 14
    FORMAT_DESCRIPTION_EVENT= 15
    XID_EVENT= 16
    BEGIN_LOAD_QUERY_EVENT= 17
    EXECUTE_LOAD_QUERY_EVENT= 18
    TABLE_MAP_EVENT = 19
    PRE_GA_WRITE_ROWS_EVENT = 20
    PRE_GA_UPDATE_ROWS_EVENT = 21
    PRE_GA_DELETE_ROWS_EVENT = 22
    WRITE_ROWS_EVENT = 23
    UPDATE_ROWS_EVENT = 24
    DELETE_ROWS_EVENT = 25
    INCIDENT_EVENT= 26
    HEARTBEAT_LOG_EVENT= 27

class ErrCode :
    ERR_BAD_ARGS = 1
    ERR_OPEN_FAILED = 2
    ERR_NOT_BINLOG = 3
    ERR_READ_FDE_FAILED = 4
    ERR_READ_RE_FAILED = 5


class BinLogFileReader(object):

    """Read event from binlog file
    """

    def __init__(self, connection_settings, server_id,ctl_connection_settings=None, 
                 blocking=False, only_events=None, log_file=None,file_size=0, log_pos=None,
                 filter_non_implemented_events=True,
                 ignored_events=None, auto_position=None,
                 only_tables=None, ignored_tables=None,
                 only_schemas=None, ignored_schemas=None,
                 freeze_schema=False, skip_to_timestamp=None,
                 pymysql_wrapper=None,
                 fail_on_table_metadata_unavailable=False):
        """
        Attributes:
            ctl_connection_settings: Connection settings for cluster holding schema information
            blocking: Read on stream is blocking
            only_events: Array of allowed events
            ignored_events: Array of ignored events
            log_file: Set replication start log file
            log_pos: Set replication start log pos (resume_stream should be true)
            auto_position: Use master_auto_position gtid to set position
            only_tables: An array with the tables you want to watch
            ignored_tables: An array with the tables you want to skip
            only_schemas: An array with the schemas you want to watch
            ignored_schemas: An array with the schemas you want to skip
            freeze_schema: If true do not support ALTER TABLE. It's faster.
            skip_to_timestamp: Ignore all events until reaching specified timestamp.
            fail_on_table_metadata_unavailable: Should raise exception if we can't get
                                                table information on row_events
        """

        self.__connection_settings = connection_settings
        self.__connection_settings.setdefault("charset", "utf8")

        self.__connected_ctl = False
        self.__blocking = blocking
        self._ctl_connection_settings = ctl_connection_settings
        if ctl_connection_settings:
            self._ctl_connection_settings.setdefault("charset", "utf8")

        self.__only_tables = only_tables
        self.__ignored_tables = ignored_tables
        self.__only_schemas = only_schemas
        self.__ignored_schemas = ignored_schemas
        self.__freeze_schema = freeze_schema
        self.__allowed_events = self._allowed_event_list(
            only_events, ignored_events, filter_non_implemented_events)
        self.__fail_on_table_metadata_unavailable = fail_on_table_metadata_unavailable

        # We can't filter on packet level TABLE_MAP and rotate event because
        # we need them for handling other operations
        self.__allowed_events_in_packet = frozenset(
            [TableMapEvent, RotateEvent]).union(self.__allowed_events)

        # Store table meta information
        self.table_map = {}
        self.log_pos = log_pos
        self.log_file = log_file
        self.auto_position = auto_position
        self.skip_to_timestamp = skip_to_timestamp

        if pymysql_wrapper:
            self.pymysql_wrapper = pymysql_wrapper
        else:
            self.pymysql_wrapper = pymysql.connect
        self.mysql_binlog = log_file
        self.file_size = file_size
        
        self.reader = BinFileReader(self.mysql_binlog,self.file_size)
        binlog_header = self.reader.chars(Constant.BINLOG_HEADER_LEN)
        if binlog_header != Constant.BINLOG_HEADER :
            raise Exception("It's not a mysql binlog file")


    def close(self):
        if self.__connected_ctl:
            # break reference cycle between stream reader and underlying
            # mysql connection object
            self._ctl_connection._get_table_information = None
            self._ctl_connection.close()
            self.__connected_ctl = False

    def __connect_to_ctl(self):
        if not self._ctl_connection_settings:
            self._ctl_connection_settings = dict(self.__connection_settings)
        self._ctl_connection_settings["db"] = "information_schema"
        self._ctl_connection_settings["cursorclass"] = DictCursor
        self._ctl_connection = self.pymysql_wrapper(**self._ctl_connection_settings)
        self._ctl_connection._get_table_information = self.__get_table_information
        
        self.__connected_ctl = True


    def fetchone(self) :
        if not self.__connected_ctl:
            self.__connect_to_ctl() 

        while True:
            binlog_event = None
            try :
                preview_pos = self.reader.tell()
                if self.reader.check_eof():
                    print('eof')
                    break

                event_header = EventHeader(self.reader)
 
                self.reader.seek(preview_pos)
                buf = self.reader.read(event_header.event_length)
                pad = struct.pack('<c',b'\x00')
                packet = MysqlPacket(pad+buf,'utf8')

                binlog_event = BinLogPacketWrapper(packet,self.table_map,
                                    self._ctl_connection,
                                    True,
                                    self.__allowed_events_in_packet,
                                    self.__only_tables,
                                    self.__ignored_tables,
                                    self.__only_schemas,
                                    self.__ignored_schemas,
                                    self.__freeze_schema,
                                    self.__fail_on_table_metadata_unavailable)
                if binlog_event.event_type == ROTATE_EVENT:
                    self.log_pos = binlog_event.event.position
                    self.log_file = binlog_event.event.next_binlog
                    print('next bin log file :{}'.format(self.log_file))
                    self.table_map = {}
                elif binlog_event.log_pos:
                    self.log_pos = binlog_event.log_pos
                if self.skip_to_timestamp and binlog_event.timestamp < self.skip_to_timestamp:
                    continue
    
                if binlog_event.event_type == TABLE_MAP_EVENT and \
                        binlog_event.event is not None:
                    self.table_map[binlog_event.event.table_id] = \
                        binlog_event.event.get_table()
                if binlog_event.event is None or (binlog_event.event.__class__ not in self.__allowed_events):
                    continue
                return binlog_event.event
            except Exception as err:
                print(err)
                if self.reader.stream.read() == '' :
                    pass
                else :
                    raise Exception('Error when read mysql binlog,filename is %s '\
                        %(self.mysql_binlog , ) )
            


    def _allowed_event_list(self, only_events, ignored_events,
                            filter_non_implemented_events):
        if only_events is not None:
            events = set(only_events)
        else:
            events = set((
                QueryEvent,
                RotateEvent,
                StopEvent,
                FormatDescriptionEvent,
                XidEvent,
                GtidEvent,
                BeginLoadQueryEvent,
                ExecuteLoadQueryEvent,
                UpdateRowsEvent,
                WriteRowsEvent,
                DeleteRowsEvent,
                TableMapEvent,
                HeartbeatLogEvent,
                NotImplementedEvent,
                ))
        if ignored_events is not None:
            for e in ignored_events:
                events.remove(e)
        if filter_non_implemented_events:
            try:
                events.remove(NotImplementedEvent)
            except KeyError:
                pass
        return frozenset(events)

    def __get_table_information(self, schema, table):
        for i in range(1, 3):
            try:
                if not self.__connected_ctl:
                    self.__connect_to_ctl()

                cur = self._ctl_connection.cursor()
                cur.execute("""
                    SELECT
                        COLUMN_NAME, COLLATION_NAME, CHARACTER_SET_NAME,
                        COLUMN_COMMENT, COLUMN_TYPE, COLUMN_KEY
                    FROM
                        information_schema.columns
                    WHERE
                        table_schema = %s AND table_name = %s
                    """, (schema, table))

                return cur.fetchall()
            except pymysql.OperationalError as error:
                code, message = error.args
                if code in MYSQL_EXPECTED_ERROR_CODES:
                    self.__connected_ctl = False
                    continue
                else:
                    raise error
    def __iter__(self):
        return iter(self.fetchone, None)


class BinFileReader(object) :
    def __init__(self , bin_filename,bin_filesize) :
        if bin_filesize == -1:
            # bin_filename is a file name 
            self.stream = open(bin_filename,'rb')
            self.file_size = os.fstat(self.stream.fileno()).st_size
        else:
            self.stream = bin_filename
            self.file_size =bin_filesize

    def header(self) :
        buf = self.stream.read(19)
        print(buf)
        unpack = struct.unpack('<IBIIIH', buf)
        print(unpack)

    def uint8(self) :
        buf = self.stream.read(1)
        if buf is None or buf=='' :
            raise Exception('End of file.')
        return to_uint8(buf)
    
    def uint16(self) :
        buf = self.stream.read(2)
        if buf is None or buf=='' :
            raise Exception('End of file.')
        return to_uint16(buf)
    
    def uint32(self) :
        buf = self.stream.read(4)
        if buf is None or buf=='' :
            raise Exception('End of file.')
        return to_uint32(buf)
    
    def uint64(self) :
        buf = self.stream.read(8)
        if buf is None or buf=='' :
            raise Exception('End of file.')
        return to_uint64(buf)
    
    def char(self) :
        buf = self.stream.read(1)
        if buf is None or buf=='' :
            raise Exception('End of file.')
        return to_char(buf)
    
    def chars(self , byte_size=1) :
        buf = self.stream.read(byte_size)
        if buf is None or buf=='' :
            raise Exception('End of file.')
        return buf
    
    def close(self) :
        self.stream.close()
    
    def seek(self , p , seek_type=0) :
        self.stream.seek(p , seek_type)

    def tell(self ) :
        return self.stream.tell()

    def check_eof(self ) :
        return self.stream.tell() == self.file_size

    def read(self,length ) :
        return self.stream.read(length)


class EventHeader :
    EVENT_HEADER_LEN = 19
    
    def __init__(self , reader) :
        # reader.header()
        self.timestamp = reader.uint32()
        self.type_code = reader.uint8()
        self.server_id = reader.uint32()
        self.event_length = reader.uint32()
        self.next_position = reader.uint32()
        self.flags = reader.uint16()
    
    def __repr__(self) :
        msg = 'EventHeader: Timestamp=%s type_code=%s server_id=%s event_length=%s next_position=%s flags=%s' % \
            (self.timestamp , self.type_code , self.server_id , self.event_length , self.next_position ,self.flags)
        return msg
