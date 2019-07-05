import pytest
import os
import sys
import backy2.backy
import shutil
#import time
import random
import uuid
# from backy2.tests.data_backend_azure import Test_AzureDataBackend

BLOCK_SIZE = 1024*4096

@pytest.yield_fixture
def argv():
    original = sys.argv
    new = original[:1]
    sys.argv = new
    yield new
    sys.argv = original


@pytest.fixture(scope="function")
def test_path(request):
    path = '_testbackup'
    os.mkdir(path)
    def fin():
        shutil.rmtree(path)
    request.addfinalizer(fin)
    return path


@pytest.fixture(scope="function")
def backy(request):
    _test_path = test_path(request)
    TESTLEN = 10
    BLOCK_SIZE = 4096
    meta_backend = backy2.backy.SQLBackend('sqlite:///'+_test_path+'/backy.sqlite')
    data_backend = backy2.backy.FileBackend(_test_path)
    backy = backy2.backy.Backy(meta_backend=meta_backend, data_backend=data_backend, block_size=BLOCK_SIZE)

    version_name = 'backup'
    snapshot_name = 'snapname'
    version_uid = backy.meta_backend.set_version(version_name, snapshot_name, TESTLEN, BLOCK_SIZE*TESTLEN, 1)
    block_uids = [uuid.uuid1().hex for i in range(TESTLEN)]
    checksums = [uuid.uuid1().hex for i in range(TESTLEN)]

    for id in range(TESTLEN):
        backy.meta_backend.set_block(id, version_uid, block_uids[id], checksums[id], BLOCK_SIZE, 1)

    def fin():
        backy.close()

    return backy


def test_blocks_from_hints():
    hints = [
        (10, 100, True),
        (1024, 2048, True),
        (4096, 3000, True),
        (14000, 10, True),
        (16383, 1025, True),
        (8657, 885, True),
        #(35458871, 3624441, True),
        ]
    #         0          1, 2          4, 5, 6       13,          15, 16
    block_size = 1024
    cfh = backy2.backy.blocks_from_hints(hints, block_size)
    assert sorted(list(cfh)) == [0, 1, 2, 4, 5, 6, 8, 9, 13, 15, 16]


def test_FileBackend_path(test_path):
    uid = 'c2cac25a7afd11e5b45aa44e314f9270'

    backend = backy2.backy.FileBackend(test_path)
    backend.DEPTH = 2
    backend.SPLIT = 2
    path = backend._path(uid)
    assert path == 'c2/ca'

    backend.DEPTH = 3
    backend.SPLIT = 2
    path = backend._path(uid)
    assert path == 'c2/ca/c2'

    backend.DEPTH = 3
    backend.SPLIT = 3
    path = backend._path(uid)
    assert path == 'c2c/ac2/5a7'

    backend.DEPTH = 3
    backend.SPLIT = 1
    path = backend._path(uid)
    assert path == 'c/2/c'

    backend.DEPTH = 1
    backend.SPLIT = 2
    path = backend._path(uid)
    assert path == 'c2'

    backend.close()


def test_FileBackend_save_read(test_path):
    backend = backy2.backy.FileBackend(test_path)
    uid = backend.save(b'test')
    backend.close()
    assert backend.read(uid) == b'test'
    backend.close()


def test_metabackend_set_version(test_path):
    backend = backy2.backy.SQLBackend('sqlite:///'+test_path+'/backy.sqlite')
    name = 'backup-mysystem1-20150110140015'
    snapshot_name = 'snapname'
    uid = backend.set_version(name, snapshot_name, 10, 5000, 1)
    assert(uid)
    version = backend.get_version(uid)
    assert version.name == name
    assert version.size == 10
    assert version.size_bytes == 5000
    assert version.uid == uid
    assert version.valid == 1
    backend.close()


def test_metabackend_version_not_found(test_path):
    backend = backy2.backy.SQLBackend('sqlite:///'+test_path+'/backy.sqlite')
    with pytest.raises(KeyError) as e:
        backend.get_version('123')
    assert str(e.exconly()) == "KeyError: 'Version 123 not found.'"
    backend.close()


def test_metabackend_block(test_path):
    backend = backy2.backy.SQLBackend('sqlite:///'+test_path+'/backy.sqlite')
    name = 'backup-mysystem1-20150110140015'
    snapshot_name = 'snapname'
    block_uid = 'asdfgh'
    checksum = '1234567890'
    size = 5000
    id = 0
    version_uid = backend.set_version(name, snapshot_name, 10, 5000, 1)
    backend.set_block(id, version_uid, block_uid, checksum, size, 1)

    block = backend.get_block(block_uid)

    assert block.checksum == checksum
    assert block.uid == block_uid
    assert block.id == id
    assert block.size == size
    assert block.version_uid == version_uid

    backend.close()


def test_metabackend_blocks_by_version(test_path):
    TESTLEN = 10
    backend = backy2.backy.SQLBackend('sqlite:///'+test_path+'/backy.sqlite')
    version_name = 'backup-mysystem1-20150110140015'
    snapshot_name = 'snapname'
    version_uid = backend.set_version(version_name, snapshot_name, TESTLEN, 5000, 1)
    block_uids = [uuid.uuid1().hex for i in range(TESTLEN)]
    checksums = [uuid.uuid1().hex for i in range(TESTLEN)]
    size = 5000

    for id in range(TESTLEN):
        backend.set_block(id, version_uid, block_uids[id], checksums[id], size, 1)

    blocks = backend.get_blocks_by_version(version_uid)
    assert len(blocks) == TESTLEN

    # blocks are always ordered by id
    for id in range(TESTLEN):
        block = blocks[id]
        assert block.id == id
        assert block.checksum == checksums[id]
        assert block.uid == block_uids[id]
        assert block.size == size
        assert block.version_uid == version_uid

    backend.close()



def _patch(filename, offset, data=None):
    """ write data into a file at offset """
    if not os.path.exists(filename):
        open(filename, 'wb')
    with open(filename, 'r+b') as f:
        f.seek(offset)
        f.write(data)


def test_backystore_readlist(backy):
    store = backy2.backy.BackyStore(backy, cachedir='/tmp')
    version_uid = backy.ls()[0].uid
    offset = random.randint(0, 6500)
    length = random.randint(0, 15000)
    read_list = store._block_list(version_uid, offset, length)
    assert read_list[0][1] == offset % backy.block_size
    read_list_length = 0
    for entry in read_list:
        read_list_length += entry[2]
    assert read_list_length == length
    print('Trying with offset {} and length {} resulted in {} blocks.'.format(offset, length, len(read_list)))

# def test_data_backend_azure_write_read():
#     data_backend = Test_AzureDataBackend()
#     text= b'\xa4%|\xf8j\xca\xab\xe3\xe4$\xda\xd4\xee\x9a\xd1\xf3\x88\\\x18\xff\x0fG\xd4\xdb\x7fp/R\xd2\x1d\x97\xcf\x15HX\x8dOH{\xdf\x88.\xa9\xb1\xd8\xa8\xa5\xb16\xfb\x96\xfe/_=\xab\x9b\x83\xe7CGh\x98k\x85\xf6\xdfYb~T\x97K\xa3\xb7\x88\x93\xf2\x1a\'\xe4$\xd5\x8cg\xad\x90\xf8\x00b\x1ad\xfe\x9cE\xa2\xe0\xa9<\xf8\\\x84\xe2T+\x8f\xf2\x94\x19\xe6w:^\xb7\x11hsB\xb6\xb6\xb0\xddg\xb7\x13\xcbQ\xcd\x97\'\x06\xef\xf2M\xce\x06}#\xb1\xddk\xa1>\xe3\x9e\xbf\x8b\xb7\'\x10\r\xdf\xe4\xdf_2\'w%\xdd\xa46L\xb4\x07\xcb=]\xc4w\xab\xecb\xb8\x7f$+\xec\xa1A\xcev\x02\x05\x9a\x1cm\rT\x12\x0c\x13\x93\x05A\xe03EYs\xbb!4\xc2\xe7ns~\xbd;\xff\x80\xf3\xab\x85\xd6a\xdd\x80\xf1\xf4\x05\xa7\xb2\tX\xdf\x8f\xd6\xe5\xd7s\xf7{\xc9\xd6\xd0"r\xa2Mf\xa4F\x9a\xef\xf2\x0e\xc4\xbaT\xdcS\xc7\xf1\xe4\xdd\x924\xcb\x8dE;\x8e\x1bs\xb4\x9fP\x80t\xc01\xe5\xf5,^Z\xf3\\\x8efPwa\x8b8\x8f\xdf\x15<\x80\xc0h\xdbo\xd1\xf3G\x03z4&\xf2\xcdE0\x10\x8di\xb5\x83w\xc5s\x81#A\xec\xfa\xddLB\x0c\x18-\xbb\xdb\xf1\xb3\xd6\x9b\xa2\xa4\xaf\xc9xN\x802ux\xb9\x0eJH\xa3\xa5\xee\xf5R\t\x93.\xda\xff\xd2\xa39\xa7\xac\xcd\xe5\x04W&&e8 \x02A\x8f\x05\xe6\xc1\xae\x7f\x86\x99|-\x98\x00J\xa3&\xdc\x16\xe0\xf0W\xe0\xf9bo \xd9t\xe2\xaab\x1a#\r\xfd\x7f\xed\x18-\x86\xe0\xca\xb0\xbb\xce\x1cVd\x1f\x08\xe3\xe8\xf9\x83\x82e2\x07\xec\xd3\x85\xe0n,b\x1f\x95\xb3\xc6G"\xed\xce$H\xe7\xf3\xf1\xb7\xf9s\xc2\xa0\x177\x94\xab\r\x085\xd9w\nN\x10dT>_\xbf\xb6q\xbb\x80\t\x98z\x92j\x9e\xab_\x94^\x01E\x83.\xa7\xfeM\x88\xea}-\x14\x84\x0c\xf3>\xf2Nf{\x88>3@\x07->\x87\xfd\xc86\xd1y\xb4\x17\xd8#\xe7-(h\xe7\xa84}\xd2\xfb\x96A\xf1=\xb3To\x84<CIb\xc5/\x97\x83\xb0\xedi&k\x88\xb77/J\x80{F\x9a\xb8\xa4p\xcd\xecNS"\x1b\x07\x85l\xab`\xe1\x96\xfd\x12r\x13\x9b5\xde\xf1\x992\x8dy\x06y\xdb[n\x17\xec\xc2q\xf4_\xd8I\t\xb4C\x9e>\xf1%ci\x9c\x15\xd14~\xc9\xacA\x81\xe3Z\xbc{b\x03\x11-\nR\x0e"$\xf9\x9e\x05tsB:\x88}\xbc/~\xffI\xca\xca\xaf\xaeG\x8a=A\xa1\x17\x1ck\x1e\xa3\xc1sv\\\xc8\xfd\x0b\x01\x7f\\0\xe4\x1aw\x08\xb3\x1cU\x0c< -t\x8b\xa1\xe9^\x07\xe3]u=\x0f\x87A\x82F(d\xf09\xe7\xca\x90*\xea\xdc\x8aC\xce"\xb8\xbb\x9d\xc3+J\x87\xa7w\x9f\xaf\x7f\xb6\xdb_\xa4\x05\xbd\x87v\xfb/\xeeAD\xf8\x02\x1d\x9b\x90P6Y\xee\x18\xca\xe4\x8d\x1c\xb4\xfb\xe5\xea\xc9\x0f\xff\x05\xa3\r\x92\xf6\\\x8a=\t\x0e\xae\x0eG\xe8peF-\x8c^/\x1c\x1a\x01\xfam\xa8\x14\x13I\xba\x89\xd0{\xeappg@r\xdc;Z\xf0\xf4\x86\x953}z\xde^\xa3\x9di.S"f\xdbp\xde\xe4k\xc0[\xa2\xe6>\xa5\xec\x91\\\x16\x88\xb1c\xa1\xf9,\xe4\x7f\xa0A\x03obK%\xca\xc1Xmj\x96\xce\xb3N\x02{\x14\x96\xc7\x0c\xecl\xb5\x9bM\xb3\xee\x0c\xab\xfd\x19\xfa\xa7\x0c\xa8\xa8\x82\xdbA\xb4\x16\x8b4\xf9e\x16\x8e\xdc\xfc\xd0\x17\x8a\xefk\xc2\x00\xce]\x8e#\xf6qAv\x8f$\x1bl\xb0\x01\xb2\x8a:\x03[f\xd3\xc93\x7f\x17\xa5)\r\xc3\xd2J1\xb5\xbb\x04\xb8\xe38\xda\x99\xe4\xfc\x87\xce\xd0>\xcd,\x85\x00$\x9eN\xfdE\x87v\x05\x0c\xdb\xb3\xc0k\xa7&)\x1c\x18\x18z\xda\x1b\xc8\x9d\xfb\xbf\xb4!!>3\xc6\xa8f;\x81\xd7\x8aMe\xe2s\xb3\xb3\xf8.\t^</\xc2\xbfL\xb8\\8g\xb6\xd4\xab#H\x13D;\xa4?\xa4;\xf4\xbei\x884c'
#     uid = data_backend.write(text)
#     print('Persisted UUID: ', uid)
#     dataReaded = data_backend.read(uid)
#     print('Data Readed: ', dataReaded)
#     assert text == dataReaded

# def test_data_backend_azure_list_remove():
#     data_backend = Test_AzureDataBackend()
#     uids = data_backend.list_all()
#     for uid in uids:
#         print('-> ', uid)
#     errors = data_backend.remove_list(uids)
#     if len(errors) > 0:
#         for uid in errors:
#             print('err -> ', uid)
#     assert len(errors) == 0
