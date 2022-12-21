#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import datetime
import getpass
import pymysql
from binlog_oss import BinLogFiles
from binlog_file_reader import BinLogFileReader
from pymysqlreplication.event import QueryEvent, RotateEvent, FormatDescriptionEvent
from binlog2sql_util import parse_args, concat_sql_from_binlog_event, create_unique_file, temp_open, \
    reversed_lines, is_dml_event, event_type


def command_line_args(args):
    need_print_help = False if args else True
    parser = parse_args()
    parser.add_argument('-i', '--instance-id', dest='instance_id', type=str,
                                 help='Instance id for RDS', default='')
    args = parser.parse_args(args)
    if args.help or need_print_help:
        parser.print_help()
        sys.exit(1)
    if not args.start_file:
        raise ValueError('Lack of parameter: start_file')
    if args.flashback and args.stop_never:
        raise ValueError('Only one of flashback or stop-never can be True')
    if args.flashback and args.no_pk:
        raise ValueError('Only one of flashback or no_pk can be True')
    if (args.start_time and not is_valid_datetime(args.start_time)) or \
            (args.stop_time and not is_valid_datetime(args.stop_time)):
        raise ValueError('Incorrect datetime argument')
    if not args.password:
        args.password = getpass.getpass()
    else:
        args.password = args.password[0]
    return args

class Binlog2sql(object):

    def __init__(self, connection_settings,instance_id=None, start_file=None, start_pos=None, end_file=None, end_pos=None,
                 start_time=None, stop_time=None, only_schemas=None, only_tables=None, no_pk=False,
                 flashback=False, stop_never=False, back_interval=1.0, only_dml=True, sql_type=None):
        """
        conn_setting: {'host': 127.0.0.1, 'port': 3306, 'user': user, 'passwd': passwd, 'charset': 'utf8'}
        """

        # if not start_file:
        #     raise ValueError('Lack of parameter: start_file')

        self.conn_setting = connection_settings
        self.start_file = start_file
        self.start_pos = start_pos if start_pos else 4    # use binlog v4
        self.end_file = end_file
        self.end_pos = end_pos
        if start_time:
            self.start_time = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
            self.start_time_str = start_time 
        else:
            self.start_time = datetime.datetime.strptime('1980-01-01 00:00:00', "%Y-%m-%d %H:%M:%S")
            self.start_time_str = '1980-01-01 00:00:00' 
        if stop_time:
            self.stop_time = datetime.datetime.strptime(stop_time, "%Y-%m-%d %H:%M:%S")
            self.stop_time_str = stop_time
        else:
            self.stop_time = datetime.datetime.strptime('2999-12-31 00:00:00', "%Y-%m-%d %H:%M:%S")
            self.stop_time_str = '2999-12-31 00:00:00' 
        
        binloglist = BinLogFiles(instance_id)
        result_files = binloglist.get_files_by_time_range(self.start_time_str,self.stop_time_str)

        if not self.start_file:
            self.start_file = result_files[-1][0]
            
        if not self.end_file:
            self.end_file = result_files[0][0]

        self.end_file = self.end_file if self.end_file else start_file
        
        # print('startfile:{},endfile:{}'.format(self.start_file,self.end_file))
        
        append_flag = False

        self.binlog_files = list()
        for log_tuple in result_files:
            if not log_tuple:
                continue
            if log_tuple[0] == self.end_file:
                append_flag = True
            if append_flag:
                self.binlog_files.append(log_tuple)
            if log_tuple[0] == self.start_file:
                break

        self.only_schemas = only_schemas if only_schemas else None
        self.only_tables = only_tables if only_tables else None
        self.no_pk, self.flashback, self.stop_never, self.back_interval = (no_pk, flashback, stop_never, back_interval)
        self.only_dml = only_dml
        self.sql_type = [t.upper() for t in sql_type] if sql_type else []

        
        self.connection = pymysql.connect(**self.conn_setting)
        with self.connection as cursor:
            cursor.execute("SHOW MASTER STATUS")
            self.eof_file, self.eof_pos = cursor.fetchone()[:2]

            cursor.execute("SELECT @@server_id")
            self.server_id = cursor.fetchone()[0]
            if not self.server_id:
                raise ValueError('missing server_id in %s:%s' % (self.conn_setting['host'], self.conn_setting['port']))

    def pop_rowid(self,binlog_event):
        has_primary = False
        for column in binlog_event.columns:
            if column.data.get('is_primary',False):
                has_primary = True
        if not has_primary:
            print('----')
            for row in binlog_event.rows:
                values = row.get('values',{})
                drop_key = ''
                for key in values:
                    if key.startswith('__dropped_col_'):
                        drop_key = key
                if drop_key:
                    del values[drop_key]
        pass

    def process_binlogs(self):
        for binlog_tuple in self.binlog_files:
            if len(binlog_tuple) != 3:
                continue
            filename = binlog_tuple[0]
            file_size = binlog_tuple[2]
            file_object = BinLogFiles.fetch_file_object_by_link(binlog_tuple[1])
            self.process_binlog_file(filename,file_object,file_size)
        # file_obj,stream_filename,file_size = BinLogFileList.get_file_obj()

    def process_binlog_file(self,filename,file_object,file_size):
        stream_filename = filename
        stream = BinLogFileReader(connection_settings=self.conn_setting, server_id=self.server_id,
                                    log_file=file_object,file_size=file_size, log_pos=self.start_pos, only_schemas=self.only_schemas,
                                    only_tables=self.only_tables, blocking=True)

        flag_last_event = False
        e_start_pos, last_pos = stream.log_pos, stream.log_pos
        # to simplify code, we do not use flock for tmp_file.
        tmp_file = create_unique_file('%s.%s' % (self.conn_setting['host'], self.conn_setting['port']))
        with temp_open(tmp_file, "w") as f_tmp, self.connection as cursor:
            for binlog_event in stream:
                if not self.stop_never:
                    # print(stream.log_pos)
                    try:
                        event_time = datetime.datetime.fromtimestamp(binlog_event.timestamp)
                    except OSError:
                        event_time = datetime.datetime(1980, 1, 1, 0, 0)
                    if (stream_filename == self.end_file and stream.log_pos == self.end_pos) or \
                            (stream_filename == self.eof_file and stream.log_pos == self.eof_pos):
                        flag_last_event = True
                    elif event_time < self.start_time:
                        if not (isinstance(binlog_event, RotateEvent)
                                or isinstance(binlog_event, FormatDescriptionEvent)):
                            last_pos = binlog_event.packet.log_pos
                        continue
                    elif  (self.end_pos and stream_filename == self.end_file and stream.log_pos > self.end_pos) or \
                            (stream_filename == self.eof_file and stream.log_pos > self.eof_pos) or \
                            (event_time >= self.stop_time):
                        break
                    # else:
                    #     raise ValueError('unknown binlog file or position')

                
                if isinstance(binlog_event, QueryEvent) and binlog_event.query == 'BEGIN':
                    e_start_pos = last_pos

                if isinstance(binlog_event, QueryEvent) and not self.only_dml:
                    sql = concat_sql_from_binlog_event(cursor=cursor, binlog_event=binlog_event,
                                                       flashback=self.flashback, no_pk=self.no_pk)
                    if sql:
                        print(sql)
                elif is_dml_event(binlog_event) and event_type(binlog_event) in self.sql_type:
                    self.pop_rowid(binlog_event)
                    for row in binlog_event.rows:
                        sql = concat_sql_from_binlog_event(cursor=cursor, binlog_event=binlog_event, no_pk=self.no_pk,
                                                           row=row, flashback=self.flashback, e_start_pos=e_start_pos)
                        if self.flashback:
                            f_tmp.write(sql + '\n')
                        else:
                            print(sql)

                if not (isinstance(binlog_event, RotateEvent) or isinstance(binlog_event, FormatDescriptionEvent)):
                    last_pos = binlog_event.packet.log_pos
                if flag_last_event:
                    break

            stream.close()
            f_tmp.close()
            if self.flashback:
                self.print_rollback_sql(filename=tmp_file)
        return True

    def print_rollback_sql(self, filename):
        """print rollback sql from tmp_file"""
        with open(filename, "rb") as f_tmp:
            batch_size = 1000
            i = 0
            for line in reversed_lines(f_tmp):
                print(line.rstrip())
                if i >= batch_size:
                    i = 0
                    if self.back_interval:
                        print('SELECT SLEEP(%s);' % self.back_interval)
                else:
                    i += 1

    def __del__(self):
        pass


if __name__ == '__main__':
    args = command_line_args(sys.argv[1:])
    conn_setting = {'host': args.host, 'port': args.port, 'user': args.user, 'passwd': args.password, 'charset': 'utf8'}
    instance_id = args.instance_id

    if not args.instance_id:
        instance_id = args.host.split('.')[0]

    binlog2sql = Binlog2sql(connection_settings=conn_setting,instance_id=instance_id, start_file=args.start_file, start_pos=args.start_pos,
                            end_file=args.end_file, end_pos=args.end_pos, start_time=args.start_time,
                            stop_time=args.stop_time, only_schemas=args.databases, only_tables=args.tables,
                            no_pk=args.no_pk, flashback=args.flashback, stop_never=args.stop_never,
                            back_interval=args.back_interval, only_dml=args.only_dml, sql_type=args.sql_type)
    binlog2sql.process_binlogs()
