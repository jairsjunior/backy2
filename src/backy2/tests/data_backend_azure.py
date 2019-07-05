#!/usr/bin/env python3
# -*- encoding: utf-8 -*-

import configparser
from backy2.data_backends.azureblob import DataBackend
from collections import namedtuple

class Test_AzureDataBackend():

    config = configparser.ConfigParser()
    config['azure'] = {
        'azure_access_key_id': '',
        'azure_secret_access_key': '',
        'bucket_name': ''
    }
    data_backend = DataBackend(config['azure'])
#
    def write(self, text):
        return self.data_backend.save(text, True)
#
    def read(self, uid):
        BlockModel = namedtuple('Block', ['uid', 'version_uid', 'id', 'date', 'checksum', 'size', 'valid'])
        recoveredData = self.data_backend.read(BlockModel(uid=uid, version_uid='', id='', date='', checksum='', size='', valid=''), True)
        return recoveredData
#
    def list_all(self):
        return self.data_backend.get_all_blob_uids()
#
    def remove_list(self, uids):
        return self.data_backend.rm_many(uids)
