# -*- coding: utf-8 -*-

import sys
import time
import tempfile

from typing import List

import requests

from alibabacloud_rds20140815.client import Client as Rds20140815Client
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_rds20140815 import models as rds_20140815_models
from alibabacloud_tea_util import models as util_models
from alibabacloud_tea_util.client import Client as UtilClient

from alibabacloud_credentials.client import Client as CredClient
from alibabacloud_credentials.models import Config as CredConfig


def localtime_to_gmtime(time_string: str) ->str:
    time_object = time.strptime(time_string,'%Y-%m-%d %H:%M:%S')
    test_object_t = int(time.mktime(time_object))
    gm_time_object = time.gmtime(test_object_t)
    return time.strftime('%Y-%m-%dT%H:%M:%SZ',gm_time_object)


class BinLogFiles:
    def __init__(self,dbinstance_id: str):
        self._db_instance_id = dbinstance_id
        pass

    @staticmethod
    def create_client(
    ) -> Rds20140815Client:
        """
        Using AK&SK to initialize RAM Client
        @param access_key_id:
        @param access_key_secret:
        @return: Client
        @throws Exception
        """
        cred_config = CredConfig(
            # role_name=ecs_role_name,      # RoleName, if ignored, will automaticly get from ECS meta.
            type='ecs_ram_role'      # Ram role type
 
        )

        cred = CredClient(cred_config)
        config = open_api_models.Config(
            credential=cred
        )
        # The domain of the service to be accessed
        config.endpoint = f'rds.aliyuncs.com'
        return Rds20140815Client(config)

    def get_files_by_time_range(self, start_time: str, end_time: str) -> None:
        # initialize Client
        start_time = localtime_to_gmtime(start_time)
        end_time = localtime_to_gmtime(end_time)

        client = BinLogFiles.create_client()
        describe_binlog_files_request = rds_20140815_models.DescribeBinlogFilesRequest(
            start_time=start_time,
            end_time=end_time,
            dbinstance_id= self._db_instance_id
        )
        runtime = util_models.RuntimeOptions()
        last_link = ''
        file_size = 0
        binlog_filename = ''
        result_files = list()
        last_filename = ''
        try:
            result = client.describe_binlog_files_with_options(describe_binlog_files_request, runtime)
            binlog_list = result.body.items.bin_log_file
            for binlogfile in binlog_list:
                #print('log_file_name:{},host_instance_id:{},log_begin_time:{},log_end_time:{},remote_status:{},file_size:{}'.format(binlogfile.log_file_name,binlogfile.host_instance_id,binlogfile.log_begin_time,binlogfile.log_end_time,binlogfile.remote_status,binlogfile.file_size))
                result_files.append((binlogfile.log_file_name,binlogfile.intranet_download_link,binlogfile.file_size))
        except Exception as error:
            print(error)
        return result_files

    @staticmethod
    def fetch_file_object_by_link(link: str ) -> Rds20140815Client:
        tempfile_obj = None
        # file_size = 0
        try:
            if link:
                response = requests.get(link, stream=True)
                tempfile_obj = tempfile.TemporaryFile()
                data = response.raw.read()
                # print(len(data))
                # file_size = len(data)
                tempfile_obj.write(data)
                tempfile_obj.seek(0)
        except Exception as error:
            print(error)
        finally:
            return tempfile_obj

