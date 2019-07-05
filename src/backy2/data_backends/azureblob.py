#!/usr/bin/env python
# -*- encoding: utf-8 -*-
from backy2.backy import blocks_from_hints
from backy2.data_backends import DataBackend as _DataBackend
from backy2.logging import logger
from backy2.utils import TokenBucket
from io import BytesIO
from azure.storage.blob import BlockBlobService
import hashlib
import os
import queue
import shortuuid
import socket
import threading
import time

import configparser
from collections import namedtuple


class DataBackend(_DataBackend):
    """ A DataBackend which stores in AzureBlob compatible storages. The files are
    stored in a configurable bucket. """

    WRITE_QUEUE_LENGTH = 20
    READ_QUEUE_LENGTH = 20

    _SUPPORTS_PARTIAL_READS = False
    _SUPPORTS_PARTIAL_WRITES = False
    fatal_error = None

    def __init__(self, config):

        azure_access_key_id = config.get('azure_access_key_id')
        azure_secret_access_key = config.get('azure_secret_access_key')
        container_name = config.get('bucket_name', 'backy2')
        simultaneous_writes = config.getint('simultaneous_writes', 1)
        simultaneous_reads = config.getint('simultaneous_reads', 1)
        bandwidth_read = config.getint('bandwidth_read', 0)
        bandwidth_write = config.getint('bandwidth_write', 0)

        self.read_throttling = TokenBucket()
        self.read_throttling.set_rate(bandwidth_read)  # 0 disables throttling
        self.write_throttling = TokenBucket()
        self.write_throttling.set_rate(bandwidth_write)  # 0 disables throttling
        self.container_name = container_name

        # print('Databackend: Azure blob')
        # print('azure_access_key_id: ', azure_access_key_id)
        # print('azure_secret_access_key: ', azure_secret_access_key)
        # print('container_name: ', container_name)

        self.conn = BlockBlobService(
            account_name=azure_access_key_id,
            account_key=azure_secret_access_key
        )

        # create our bucket
        try:
            self.conn.create_container(container_name)
        # except boto.exception.S3CreateError:
        #     # exists...
        #     pass
        except (OSError, Exception)  as e:
            # no route to host
            self.fatal_error = e
            logger.error('Fatal error, dying: {}'.format(e))
            print('Fatal error: {}'.format(e))
            exit(10)

        self.write_queue_length = simultaneous_writes + self.WRITE_QUEUE_LENGTH
        self.read_queue_length = simultaneous_reads + self.READ_QUEUE_LENGTH
        self._write_queue = queue.Queue(self.write_queue_length)
        self._read_queue = queue.Queue()
        self._read_data_queue = queue.Queue(self.read_queue_length)
        self._writer_threads = []
        self._reader_threads = []
        for i in range(simultaneous_writes):
            _writer_thread = threading.Thread(target=self._writer, args=(i,))
            _writer_thread.daemon = True
            _writer_thread.start()
            self._writer_threads.append(_writer_thread)
        for i in range(simultaneous_reads):
            _reader_thread = threading.Thread(target=self._reader, args=(i,))
            _reader_thread.daemon = True
            _reader_thread.start()
            self._reader_threads.append(_reader_thread)


    def _writer(self, id_):
        """ A threaded background writer """
        while True:
            entry = self._write_queue.get()
            if entry is None or self.fatal_error:
                logger.debug("Writer {} finishing.".format(id_))
                break
            uid, data = entry
            time.sleep(self.write_throttling.consume(len(data)))
            t1 = time.time()

            try:
                # res = self.conn.create_blob_from_text(
                #     container_name=self.container_name,
                #     blob_name=uid,
                #     text=data,
                #     validate_content=True,
                #     encoding='ascii'
                # )
                string_data = data
                if not isinstance(string_data, bytes):
                    string_data = string_data.encode("utf-8")
                fp = BytesIO(string_data)
                res = self.conn.create_blob_from_bytes(
                    container_name=self.container_name,
                    blob_name=uid,
                    blob=fp.getvalue(),
                    validate_content=True,
                )
            except (OSError, Exception) as e:
                # We let the backup job die here fataly.
                self.fatal_error = e
                logger.error('Fatal error, dying: {}'.format(e))
                print('Error on Write File', e)
                #exit('Fatal error: {}'.format(e))  # this only raises SystemExit
                os._exit(11)
            t2 = time.time()
            self._write_queue.task_done()
            logger.debug('Writer {} wrote data async. uid {} in {:.2f}s (Queue size is {})'.format(id_, uid, t2-t1, self._write_queue.qsize()))


    def _reader(self, id_):
        """ A threaded background reader """
        while True:
            block = self._read_queue.get()  # contains block
            if block is None or self.fatal_error:
                logger.debug("Reader {} finishing.".format(id_))
                break
            t1 = time.time()
            try:
                data = self.read_raw(block.uid)
            except FileNotFoundError:
                self._read_data_queue.put((block, None))  # catch this!
            else:
                self._read_data_queue.put((block, data))
                t2 = time.time()
                self._read_queue.task_done()
                logger.debug('Reader {} read data async. uid {} in {:.2f}s (Queue size is {})'.format(id_, block.uid, t2-t1, self._read_queue.qsize()))


    def read_raw(self, block_uid):
        while True:
            try:
                data = self.conn.get_blob_to_bytes(
                    container_name=self.container_name,
                    blob_name=block_uid,
                    validate_content=True,
                )
                data = data.content
            except (OSError, Exception) as e:
                # TODO: Check what is the exact exception throwed here to show if has error
                logger.error('Timeout while fetching from azure - error is "{}"'.format(str(e)))
                pass
            else:
                break
        time.sleep(self.read_throttling.consume(len(data)))
        return data

    def _uid(self):
        # 32 chars are allowed and we need to spread the first few chars so
        # that blobs are distributed nicely. And want to avoid hash collisions.
        # So we create a real base57-encoded uuid (22 chars) and prefix it with
        # its own md5 hash[:10].
        suuid = shortuuid.uuid()
        hash = hashlib.md5(suuid.encode('ascii')).hexdigest()
        return hash[:10] + suuid

    def _remove_many(self, uids):
        resultErrors = []
        for uid in uids:
            try:
                self.rm(uid)
            except Exception as e:
                print('Remove Many Exception -> UID:', uid, ' Exception: ', e)
                resultErrors.append(uid)
        return resultErrors


    def save(self, data, _sync=False):
        if self.fatal_error:
            print('error fatal self')
            raise self.fatal_error
        uid = self._uid()
        self._write_queue.put((uid, data))
        if _sync:
            self._write_queue.join()
        return uid


    def rm(self, uid):
        try:
            self.conn.delete_blob(self.container_name, uid)
        except (OSError, Exception) as e:
            raise FileNotFoundError('UID {} not found.'.format(uid))

    def rm_many(self, uids):
        """ Deletes many uids from the data backend and returns a list
        of uids that couldn't be deleted.
        """
        errors = self._remove_many(uids)
        if len(errors) > 0:
            return errors


    def read(self, block, sync=False):
        self._read_queue.put(block)
        if sync:
            rblock, offset, length, data = self.read_get()
            if rblock.id != block.id:
                raise RuntimeError('Do not mix threaded reading with sync reading!')
            if data is None:
                raise FileNotFoundError('UID {} not found.'.format(block.uid))
            return data


    def read_get(self):
        block, data = self._read_data_queue.get()
        offset = 0
        length = len(data)
        self._read_data_queue.task_done()
        return block, offset, length, data


    def read_queue_size(self):
        return self._read_queue.qsize()


    def get_all_blob_uids(self, prefix=None):
        return self.conn.list_blob_names(self.container_name, prefix)


    def close(self):
        for _writer_thread in self._writer_threads:
            self._write_queue.put(None)  # ends the thread
        for _writer_thread in self._writer_threads:
            _writer_thread.join()
        for _reader_thread in self._reader_threads:
            self._read_queue.put(None)  # ends the thread
        for _reader_thread in self._reader_threads:
            _reader_thread.join()

