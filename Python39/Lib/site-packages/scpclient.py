#######################################################################
# scp client for use with paramiko.
#
# Copyright 2011-2014 True Blade Systems, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Notes:
#
# The scp protocol is poorly documented. For one explanation, see:
# http://blogs.oracle.com/janp/entry/how_the_scp_protocol_works
#
# Protocol weaknesses:
#  - Inextensible.
#  - Must decide on recursive or not at startup. For a given
#    run of the remote scp program, you cannot switch between
#    recursive or not.
#  - You must launch a new remote scp instance for each unrelated
#    you want to read or write.
#  - When writing a file, you must specify the size in advance.
#    This makes it impossible to send arbitrary streams.
#######################################################################


import os as _os
from socket import timeout as _SocketTimeout
from collections import namedtuple as _namedtuple
from contextlib import closing

__all__ = ['SCPError', 'SCPTimeoutError', 'Write', 'WriteDir',
           'Read', 'ReadDir']

class SCPError(Exception): pass
class SCPTimeoutError(SCPError): pass

########################################################################
# Misc. support routines.

# Retrieve just the file stats that scp cares about, in the correct
# format: mode, size, mtime, atime
def _read_stats(st):
    # convert 'mode' to something like '0755'
    # convert times to ints from floats
    return (oct(st.st_mode)[-4:], st.st_size,
            int(st.st_mtime), int(st.st_atime))


def _extract_file_times(msg):
    # msg contains 'mtime 0 atime 0'
    parts = msg.split()

    if len(parts) != 4 or parts[1] != '0' or parts[3] != '0':
        raise SCPError('Invalid time header: {0!r}'.format(msg))

    mtime = int(parts[0])
    atime = int(parts[2]) or mtime
    return mtime, atime


def _extract_file_info(msg):
    # msg contains 'mode size pathname'
    parts = msg.split()

    if len(parts) != 3:
        raise SCPError('Invalid file receive header: {0!r}'.format(msg))

    try:
        mode = int(parts[0], 8)
    except ValueError:
        raise SCPError('Bad file mode: {0!r}'.format(parts[0]))

    try:
        size = int(parts[1])
    except ValueError:
        raise SCPError('Bad file size: {0!r}'.format(parts[1]))

    return mode, size, parts[2]


########################################################################
# scp protocol support routines.

_MISC_BUF_LEN = 16 * 1024

def _ssh_open_channel(transport, timeout):
    channel = transport.open_session()
    if timeout is not None:
        channel.settimeout(timeout)
    return channel


def _scp_recv(channel, max_size):
    try:
        return channel.recv(max_size)
    except _SocketTimeout:
        raise SCPTimeoutError('Timout waiting for scp response')


def _scp_send_time(channel, mtime, atime):
    channel.sendall('T{0} 0 {1} 0\n'.format(mtime, atime))
    _scp_read_response(channel)


def _scp_send_pushd(channel, directory, preserve_times):
    (mode, size, mtime, atime) = _read_stats(_os.stat(directory))
    basename = _os.path.basename(directory)
    if preserve_times:
        _scp_send_time(channel, mtime, atime)
    channel.sendall('D{0} 0 {1}\n'.format(mode, basename))
    _scp_read_response(channel)


def _scp_send_popd(channel):
    channel.sendall('E\n')
    _scp_read_response(channel)


def _scp_read_response(channel):
    # Read an scp response. Translate to an exception if needed.
    msg = _scp_recv(channel, _MISC_BUF_LEN)

    if not msg:
        raise SCPError('Empty response')

    if msg[0] == '\x00':
        # Normal result.
        return
    elif msg[0] == '\x01':
        raise SCPError('Server error: {0!r}'.format(msg[1:]))
    else:
        raise SCPError('Invalid response: {0!r}'.format(msg))


def _scp_receive_file(channel, msg, buf_size):
    # Return (path, mode, size, read_fn). read_fn()
    #  yields parts of the file, until EOF.

    mode, size, path = _extract_file_info(msg)

    try:
        # Tell the remote side we're ready to read
        channel.sendall('\x00')

        def file_reader():
            bytes_read = 0
            while bytes_read < size:
                # Compute the max to read.
                bytes_to_read = buf_size
                if size - bytes_read <= buf_size:
                    bytes_to_read = size - bytes_read

                s = _scp_recv(channel, bytes_to_read)

                yield s

                bytes_read += len(s)

            msg = _scp_recv(channel, _MISC_BUF_LEN)
            if len(msg) == 0 or msg[0] != '\x00':
                raise SCPError('Error on end of read: {0!r} {1!r}'.format(
                    msg[0], msg[1:]))

        return path, mode, size, file_reader

    except _SocketTimeout:
        raise SCPTimeoutError('Timeout on file read')


_read_file      = _namedtuple('_read_file', 'path mode mtime atime size read_fn')
_read_dir_start = _namedtuple('_read_dir_start', 'path mode')
_read_dir_end   = _namedtuple('_read_dir_end', 'path mode mtime atime')

def _scp_receive_loop(transport, timeout, scp_command, buf_size):
    # Open a channel; just use it here.
    with closing(_ssh_open_channel(transport, timeout)) as channel:

        # Execute the scp command on the far side.
        channel.exec_command(scp_command)

        # Process each scp command.
        mtime = None
        atime = None

        # Keep track of the directory push/pops.
        dir_stack = []

        while not channel.closed:
            # Read until the channel is closed.
            channel.sendall('\x00')

            # This read needs to be large enough to read the largest
            #  possible filename. Just pick something very large.
            msg = _scp_recv(channel, _MISC_BUF_LEN)
            #print 'scp command ' + repr(msg)
            if not msg:
                break
            cmd = msg[0]
            msg = msg[1:]

            if cmd == 'T':
                # File time. Just remember it for later.
                mtime, atime = _extract_file_times(msg)

            elif cmd == 'C':
                # Receive file.
                path, mode, size, read_fn = _scp_receive_file(channel, msg,
                                                              buf_size)

                # Get the directory on top of the stack in order to compute
                #  our entire path name.
                if len(dir_stack) != 0:
                    path = _os.path.join(dir_stack[-1].path, path)

                yield _read_file(path, mode, mtime, atime, size, read_fn)

                mtime = None
                atime = None

            elif cmd == 'D':
                # Push directory.
                mode, size, path = _extract_file_info(msg)

                # Get the directory on top of the stack in order to compute
                #  our entire path name.
                if len(dir_stack) != 0:
                    path = _os.path.join(dir_stack[-1].path, path)

                # Push an "end" on to the stack, so it can be returned later
                #  with the relavent info.
                dir_stack.append(_read_dir_end(path, mode, mtime, atime))

                mtime = None
                atime = None

                yield _read_dir_start(path, mode)

            elif cmd == 'E':
                # Pop directory.

                # Notify caller. The objects on the stack or _read_dir_end's.
                yield dir_stack.pop()

            elif cmd == '\x01':
                raise SCPError('scp error: {0!r}'.format(msg))
            else:
                raise SCPError('Unknown scp reply: {0!r} {1!r}'.format(cmd, msg))


########################################################################


# The default number of bytes to read per call. Can be overridden.
_DEFAULT_BUF_SIZE = 64 * 1024

_REMOTE_SCP_COMMAND = 'scp'


########################################################################
# File writers.
class _WriteBase(object):
    def __init__(self,
                 transport,
                 remote_path,
                 timeout,
                 remote_scp_command,
                 buf_size,
                 command_arg):
        self.buf_size = buf_size

        self._channel = _ssh_open_channel(transport, timeout)

        scp_command = '{0}{1} -t {2}'.format(remote_scp_command,
                                             command_arg,
                                             remote_path)
        self._channel.exec_command(scp_command)
        _scp_read_response(self._channel)



    def close(self):
        self._channel.close()


    def _send_file(self, local_filename, preserve_times=False,
                  override_mode=None, remote_filename=None, progress=None):
        '''Call once for each file you want to send to the remote side'''

        if remote_filename is None:
            remote_filename = _os.path.basename(local_filename)

        with open(local_filename, 'rb') as fl:
            mode, size, mtime, atime = _read_stats(_os.fstat(fl.fileno()))
            if not preserve_times:
                mtime = None
                atime = None
            self._send(fl, remote_filename, mode, size, mtime, atime, progress)


    def _send(self, fl, remote_filename, mode, size, mtime=None, atime=None,
              progress=None):
        '''Can be called with a file or file-like object'''

        if mtime is not None and atime is not None:
            _scp_send_time(self._channel, mtime, atime)

        self._channel.sendall('C{0} {1} {2}\n'.format(mode, size, remote_filename))
        _scp_read_response(self._channel)

        file_pos = 0
        while file_pos < size:
            s = fl.read(self.buf_size)
            if len(s) == 0:
                raise SCPError('Unable to read input file after {0} bytes'.format(file_pos))

            self._channel.sendall(s)
            file_pos += len(s)

            if progress:
                progress(remote_filename, size, file_pos)

        self._channel.sendall('\x00')


class Write(_WriteBase):
    def __init__(self,
                 transport,
                 remote_path,
                 timeout=None,
                 remote_scp_command=_REMOTE_SCP_COMMAND,
                 buf_size=_DEFAULT_BUF_SIZE):
        _WriteBase.__init__(self, transport, remote_path, timeout,
                            remote_scp_command, buf_size, '')

    # expose these names
    send_file = _WriteBase._send_file
    send = _WriteBase._send


class WriteDir(_WriteBase):
    def __init__(self,
                 transport,
                 remote_path,
                 timeout=None,
                 remote_scp_command=_REMOTE_SCP_COMMAND,
                 buf_size=_DEFAULT_BUF_SIZE):

        # Note the leading space in ' -r'. It's important.
        _WriteBase.__init__(self, transport, remote_path, timeout,
                            remote_scp_command, buf_size, ' -r')


    def send_dir(self, local_dirname, preserve_times=False,
                 override_mode=None, progress=None):
        '''Call once for each directory you want to send to the remote side'''

        # local_filename should be a directory.
        prev_dir = local_dirname
        for curr_dir, dirs, files in _os.walk(local_dirname):
            # Issue pops until we're back up to this level
            while prev_dir != _os.path.commonprefix([prev_dir, curr_dir]):
                _scp_send_popd(self._channel)
                prev_dir = _os.path.split(prev_dir)[0]

            # Now push the current directory.
            _scp_send_pushd(self._channel, curr_dir, preserve_times)

            # And process the files in this directory
            for f in files:
                self._send_file(_os.path.join(curr_dir, f), preserve_times,
                                override_mode, None, progress)

            prev_dir = curr_dir

########################################################################

########################################################################
# File readers.

class _ReadBase(object):
    # Unlike WriteBase, there's really not much point of making this
    #  a class. It could just be a single function. But for consistency,
    #  make the Read interface look like Write.

    def __init__(self, transport, remote_path, timeout, remote_scp_command,
                 buf_size, recursive):
        self._transport = transport
        self._remote_path = remote_path
        self._timeout = timeout
        self._remote_scp_command = remote_scp_command
        self._buf_size = buf_size
        self._recursive = recursive

    # This is just here to make this look like WriteBase. There's no state
    #  to manage since we open and close channels within the receive routines.
    def close(self):
        pass

    def _receive(self, remote_filename):

        scp_command = '{cmd}{recurse} -p -f {path}'.format(
            cmd=self._remote_scp_command,
            recurse=' -r' if self._recursive else '',
            path=_os.path.join(self._remote_path, remote_filename))

        for reader in _scp_receive_loop(self._transport, self._timeout,
                                        scp_command, self._buf_size):
            yield reader


class Read(_ReadBase):
    def __init__(self,
                 transport,
                 remote_path,
                 timeout=None,
                 remote_scp_command=_REMOTE_SCP_COMMAND,
                 buf_size=_DEFAULT_BUF_SIZE):
        _ReadBase.__init__(self, transport, remote_path, timeout,
                          remote_scp_command, buf_size, False)


    def receive_file(self, local_filename,
                     preserve_times=False,
                     override_mode=None,
                     remote_filename=None,
                     progress=None):

        if remote_filename is None:
            remote_filename = _os.path.basename(local_filename)

        for reader in self._receive(remote_filename):
            # This should only return once, so we'll just use the first
            #  iteration.
            assert isinstance(reader, _read_file)

            with open(local_filename, 'wb') as fl:
                for s in reader.read_fn():
                    fl.write(s)

                _os.fchmod(fl.fileno(),
                           reader.mode if override_mode is None else override_mode)

            if preserve_times:
                # Set the file times.
                _os.utime(local_filename, (reader.atime, reader.mtime))

            return


    def receive(self, remote_filename=None, progress=None):

        for reader in self._receive(remote_filename):
            # This should only return once, so we'll just use the first
            #  iteration.
            assert isinstance(reader, _read_file)

            return ''.join(s for s in reader.read_fn())


class ReadDir(_ReadBase):
    def __init__(self,
                 transport,
                 remote_path,
                 timeout=None,
                 remote_scp_command=_REMOTE_SCP_COMMAND,
                 buf_size=_DEFAULT_BUF_SIZE):
        _ReadBase.__init__(self, transport, remote_path, timeout,
                          remote_scp_command, buf_size, True)

    def receive_dir(self, local_dirname,
                    preserve_times=False,
                    override_mode=None,
                    remote_dirname='',
                    progress=None):

        if not _os.path.isdir(local_dirname):
            raise SCPError('local directory {0!r} does not exist'.format(
                                                          local_dirname))

        for op in self._receive(remote_dirname):
            if _os.path.isabs(op.path):
                raise SCPError('{0!r} is an absolute path'.format(op.path))
            if '..' in op.path:
                raise SCPError('{0!r} contains ".."'.format(op.path))
            path = _os.path.join(local_dirname, op.path)
            if isinstance(op, _read_file):
                with open(path, 'wb') as fl:
                    for s in op.read_fn():
                        fl.write(s)
                _os.chmod(path,
                          op.mode if override_mode is None else override_mode)
                if preserve_times:
                    _os.utime(path, (op.atime, op.mtime))
            elif isinstance(op, _read_dir_start):
                # Create the directory.
                if not _os.path.exists(path):
                    _os.mkdir(path, op.mode)
                elif _os.path.isdir(path):
                    _os.chmod(path,
                              op.mode if override_mode is None
                                   else override_mode)
                else:
                    raise SCPError('{0!r} is not a directory'.format(path))
            elif isinstance(op, _read_dir_end):
                # chmod the directory, if needed.
                if preserve_times:
                    _os.utime(path, (op.atime, op.mtime))
            else:
                raise RuntimeError('Unknown program state {0!r}'.format(op))


########################################################################

if __name__ == '__main__':
    # need tests
    pass
