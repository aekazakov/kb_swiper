# -*- coding: utf-8 -*-
#BEGIN_HEADER
import copy
import errno
import ftplib
import gzip
import io
import json
import os
import re
import shutil
import subprocess
import tarfile
import tempfile
import time
import uuid
import zipfile

import bz2file
import magic
import requests
#import semver
from requests_toolbelt.multipart.encoder import MultipartEncoder

from lib.AbstractHandleClient import AbstractHandle as HandleService
from lib.WorkspaceClient import Workspace
from lib.baseclient import ServerError as HandleError
from lib.baseclient import ServerError as WorkspaceError


class ShockException(Exception):
    pass

#END_HEADER


class DataFileUtil:
    '''
    Module Name:
    DataFileUtil

    Module Description:
    Contains utilities for saving and retrieving data to and from KBase data
services. Requires Shock 0.9.6+ and Workspace Service 0.4.1+.

Note that some calls may create files or directories in the root of the scratch space (typically
/kb/module/work/tmp). For this reason client programmers should not request that DFU archive from
the root of the scratch space - always create a new directory (e.g. using a UUID for a name or a
standard library temporary directory utility) and add the target files to that directory when
archiving.
    '''

    ######## WARNING FOR GEVENT USERS ####### noqa
    # Since asynchronous IO can lead to methods - even the same method -
    # interrupting each other, you must be *very* careful when using global
    # state. A method could easily clobber the state set by another while
    # the latter method is running.
    ######################################### noqa
    VERSION = "0.1.2"
    GIT_URL = "https://github.com/mrcreosote/DataFileUtil"
    GIT_COMMIT_HASH = "f816c1d3ab84c9cee6a83b3d7200a44b4de112ef"

    #BEGIN_CLASS_HEADER

    GZ = '.gz'
    GZIP = '.gzip'
    TGZ = '.tgz'

    DECOMPRESS_EXT_MAP = {GZ: '',
                          GZIP: '',
                          '.bz': '',
                          '.bz2': '',
                          '.bzip': '',
                          '.bzip2': '',
                          TGZ: '.tar',
                          '.tbz': '.tar'
                          }

    ROOT = re.compile(r'^[\\' + os.sep + ']+$')

    # staging file prefix
    STAGING_GLOBAL_FILE_PREFIX = '/data/bulk/'
    STAGING_USER_FILE_PREFIX = '/staging/'

    def log(self, message, prefix_newline=False):
        print(('\n' if prefix_newline else '') +
              str(time.time()) + ': ' + str(message))

    def endswith(self, string, suffixes):
        strl = string.lower()
        for s in suffixes:
            if strl.endswith(s):
                return True
        return False

    # it'd be nice if you could just open the file and gzip on the fly but I
    # don't see a way to do that
    def gzip(self, oldfile):
        if self.endswith(oldfile, [self.GZ, self.GZIP, self.TGZ]):
            self.log('File {} is already gzipped, skipping'.format(oldfile))
            return oldfile
        newfile = oldfile + self.GZ
        self.log('gzipping {} to {}'.format(oldfile, newfile))
        with open(oldfile, 'rb') as s, gzip.open(newfile, 'wb') as t:
            shutil.copyfileobj(s, t)
        return newfile

    # alternate drop-in replacement for the above gzip function using
    # the pigz parallel compression program
    def _pigz_compress(self, oldfile, n_proc=None, compression_level=None):
        if self.endswith(oldfile, [self.GZ, self.GZIP, self.TGZ]):
            self.log('File {} is already gzipped, skipping'.format(oldfile))
            return oldfile
        newfile = oldfile + self.GZ
        self.log('gzipping (with pigz) {} to {}'.format(oldfile, newfile))

        # -f to force overwrite
        # --keep to retain the original file
        # --fast optimizes speed over compression level (we lose a few % in compression size apparantly)
        # --processes to limit the number of processes
        # --stdout to print compressed file to stdout (necessary to select specific filename)
        if not n_proc:
            n_proc = self.PIGZ_N_PROCESSES
        if not compression_level:
            compression_level = self.PIGZ_COMPRESSION_LEVEL

        command = ['pigz', '-f', '--keep', '-' + str(compression_level), '--processes', str(n_proc),
                   '--stdout', oldfile]

        newfile_handle = open(newfile, "w")
        p = subprocess.Popen(command, shell=False, stdout=newfile_handle)
        exitCode = p.wait()
        newfile_handle.close()

        if (exitCode != 0):
            raise ValueError('Error running command: ' + ' '.join(command) + '\n' +
                             'Exit Code: ' + str(exitCode))
        return newfile

    def _pack(self, file_path, pack):
        if pack not in ['gzip', 'targz', 'zip']:
            raise ValueError('Invalid pack value: ' + pack)
        if pack == 'gzip':
            return self._pigz_compress(file_path)
            # return self.gzip(file_path)
        if os.path.isdir(file_path):
            file_path = file_path + os.sep  # double seps ok here
        d, f = os.path.split(file_path)  # will return dir as f if no / at end
        if not d:
            d = '.'
        # note abspath removes trailing slashes incl. double seps
        # but does NOT remove multiple slashes at the start of the path. FFS.
        d = os.path.abspath(os.path.expanduser(d))
        if self.ROOT.match(os.path.splitdrive(d)[1]):
            raise ValueError('Packing root is not allowed')
        if not os.listdir(d):
            raise ValueError('Directory {} is empty'.format(d))
        if not f:
            f = os.path.basename(d)
        file_path = d + os.sep + f
        self.log('Packing {} to {}'.format(d, pack))
        # tar is smart enough to not pack its own archive file into the new archive, zip isn't.
        # TODO is there a designated temp files dir in the scratch space? Nope.
        # check dir to archive is not self.tmp or its parent dir for zip
        (fd, tf) = tempfile.mkstemp(dir=self.tmp)
        os.close(fd)
        if pack == 'targz':
            ctf = shutil.make_archive(tf, 'gztar', d)
            suffix = ctf.replace(tf, '', 1)
            shutil.move(ctf, file_path + suffix)
        else:
            if os.path.commonprefix([d, self.tmp]) == d:
                error_msg = 'Directory to zip [{}] is parent of result archive file'.format(d)
                raise ValueError(error_msg)
            suffix = '.zip'
            with zipfile.ZipFile(tf + suffix, 'w',
                                 zipfile.ZIP_DEFLATED,
                                 allowZip64=True) as zip_file:
                for root, dirs, files in os.walk(d):
                    for file in files:
                        filepath = os.path.join(root, file).replace(d, '')
                        zip_file.write(os.path.join(root, file), filepath)

            shutil.move(tf + suffix, file_path + suffix)

        os.remove(tf)

        return file_path + suffix

    def _decompress_file_name(self, file_path):
        for ext in self.DECOMPRESS_EXT_MAP:
            if file_path.endswith(ext):
                return file_path[0: -len(ext)] + self.DECOMPRESS_EXT_MAP[ext]
        return file_path

    def _decompress(self, openfn, file_path, unpack):
        new_file = self._decompress_file_name(file_path)
        self.log('decompressing {} to {} ...'.format(file_path, new_file))
        with openfn(file_path, 'rb') as s, tempfile.NamedTemporaryFile(
                dir=self.tmp, delete=False) as tf:
            # don't create the target file until it's done decompressing
            shutil.copyfileobj(s, tf)
            s.close()
            tf.flush()
            shutil.move(tf.name, new_file)
        t = magic.from_file(new_file, mime=True)
        self._unarchive(new_file, unpack, t)
        return new_file

    # almost drop-in replacement for _decompress which always uses pigz instead
    # of the passed in file open function
    def _pigz_decompress(self, file_path, unpack, n_proc=None):
        new_file = self._decompress_file_name(file_path)
        self.log('decompressing (with pigz) {} to {} ...'.format(file_path, new_file))

        # --keep to retain the original file
        # --processes to limit the number of processes
        # --stdout to print compressed file to stdout (necessary to select specific filename)
        if not n_proc:
            n_proc = self.PIGZ_N_PROCESSES
        command = ['pigz', '--decompress', '--keep', '--processes', str(n_proc), '--stdout', file_path]

        # seems like an odd case, but the decompressed file name, if it can't be mapped
        # is the same name as the original file. We can't do this when piping stdout from
        # pigz, so instead we pipe to a temporary file, then move it to overwrite the original
        output_file = new_file
        if new_file == file_path:
            output_file = new_file + '.temp'

        newfile_handle = open(output_file, "w")
        p = subprocess.Popen(command, shell=False, stdout=newfile_handle)
        exitCode = p.wait()
        newfile_handle.close()

        if (exitCode != 0):
            raise ValueError('Error running command: ' + ' '.join(command) + '\n' +
                             'Exit Code: ' + str(exitCode))

        if new_file != output_file:
            shutil.move(output_file, new_file)

        t = magic.from_file(new_file, mime=True)
        self._unarchive(new_file, unpack, t)
        return new_file

    def _unarchive(self, file_path, unpack, file_type):
        file_dir = os.path.dirname(file_path)
        if file_type in ['application/' + x for x in ('x-tar', 'tar', 'x-gtar')]:
            if not unpack:
                raise ValueError(
                    'File {} is tar file but only uncompress was specified'
                    .format(file_path))
            self.log('unpacking {} ...'.format(file_path))
            with tarfile.open(file_path) as tf:
                self._check_members(tf.getnames())
                tf.extractall(file_dir)
        if file_type in ['application/' + x for x in ('zip', 'x-zip-compressed')]:
                        # x-compressed is apparently both .Z and .zip?
            if not unpack:
                raise ValueError(
                    'File {} is zip file but only uncompress was specified'
                    .format(file_path))
            self.log('unpacking {} ...'.format(file_path))
            with zipfile.ZipFile(file_path) as zf:
                self._check_members(zf.namelist())
                zf.extractall(file_dir)

    def _check_members(self, member_list):
        # How the hell do I test this? Adding relative paths outside a zip is
        # easy, but the other 3 cases aren't
        for m in member_list:
            n = os.path.normpath(m)
            if n.startswith('/') or n.startswith('..'):
                err = ('Dangerous archive file - entry [{}] points to a ' +
                       'file outside the archive directory').format(m)
                self.log(err)
                raise ValueError(err)

    def _unpack(self, file_path, unpack):
        t = magic.from_file(file_path, mime=True)
        if t in ['application/' + x for x in ('x-gzip', 'gzip')]:
            return self._pigz_decompress(file_path, unpack)
            # return self._decompress(gzip.open, file_path, unpack)
        # probably most of these aren't needed, but hard to find a definite
        # source
        if t in ['application/' + x for x in
                 ('x-bzip', 'x-bzip2', 'bzip', 'bzip2')]:
            return self._decompress(bz2file.BZ2File, file_path, unpack)

        self._unarchive(file_path, unpack, t)
        return file_path

    # http://stackoverflow.com/a/600612/643675
    def mkdir_p(self, path):
        if not path:
            return
        try:
            os.makedirs(path)
        except OSError as exc:
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else:
                raise

    def check_shock_response(self, response, errtxt):
        if not response.ok:
            try:
                err = json.loads(response.content)['error'][0]
            except:
                # this means shock is down or not responding.
                self.log("Couldn't parse response error content from Shock: " +
                         response.content)
                response.raise_for_status()
            raise ShockException(errtxt + str(err))

    def make_handle(self, shock_data, token):
        hs = HandleService(self.handle_url, token=token)
        handle = {'id': shock_data['id'],
                  'type': 'shock',
                  'url': self.shock_url,
                  'file_name': shock_data['file']['name'],
                  'remote_md5': shock_data['file']['checksum']['md5']
                  }
        hid = hs.persist_handle(handle)
        handle['hid'] = hid
        return handle

    def make_ref(self, object_info):
        return str(object_info[6]) + '/' + str(object_info[0]) + \
            '/' + str(object_info[4])

    def _get_staging_file_path(self, token_user, staging_file_subdir_path):
        """
        _get_staging_file_path: return staging area file path

        directory pattern:
            perfered to return user specific path: /staging/sub_dir/file_name
            if this path is not visible to user, use global bulk path: /data/bulk/user_name/sub_dir/file_name
        """

        user_path = os.path.join(self.STAGING_USER_FILE_PREFIX, staging_file_subdir_path.strip('/'))

        if os.path.exists(user_path):
            return user_path
        else:
            return os.path.join(self.STAGING_GLOBAL_FILE_PREFIX, token_user,
                                staging_file_subdir_path.strip('/'))

    def _download_file(self, download_type, file_url):
        """
        _download_file: download execution distributor

        params:
        download_type: download type for web source file
        file_url: file URL
        """
        if download_type == 'Direct Download':
            copy_file_path = self._download_direct_download_link(file_url)
        elif download_type == 'DropBox':
            copy_file_path = self._download_dropbox_link(file_url)
        elif download_type == 'FTP':
            copy_file_path = self._download_ftp_link(file_url)
        elif download_type == 'Google Drive':
            copy_file_path = self._download_google_drive_link(file_url)
        else:
            valid_download_types = ['Direct Download', 'FTP',
                                    'DropBox', 'Google Drive']
            error_msg = "[{}] download_type is invalid.\n".format(download_type)
            error_msg += "Please use one of {}".format(valid_download_types)
            raise ValueError(error_msg)

        return copy_file_path

    def _retrieve_filepath(self, file_url, cookies=None):
        """
        _retrieve_filepath: retrieve file name from download URL and return local file path

        """

        try:
            with requests.get(file_url, cookies=cookies, stream=True) as response:
                try:
                    content_disposition = response.headers['content-disposition']
                except KeyError:
                    self.log('Parsing file name directly from URL')
                    file_name = file_url.split('/')[-1]
                else:
                    file_name = content_disposition.split('filename="')[-1].split('";')[0]
        except BaseException as error:
            error_msg = 'Cannot connect to URL: {}\n'.format(file_url)
            error_msg += 'Exception: {}'.format(error)
            raise ValueError(error_msg)

        self.log('Retrieving file name from url: {}'.format(file_name))
        copy_file_path = os.path.join(self.tmp, file_name)

        return copy_file_path

    def _wget_dl(self, url, destination_file, try_number=20, time_out=1800):
        """
        _wget_dl: run wget command tool
        """

        download_state = 1
        command=["wget", "-c", "--no-verbose", "-O", destination_file,
                 "-t", str(try_number), "-T", str(time_out), url]
        try:
            download_state=subprocess.call(command)
        except Exception as e:
            self.log('Error running wget')
            self.log(e)

        return download_state

    def _download_to_file(self, file_url):
        """
        _download_to_file: download url content to file

        params:
        file_url: direct download URL

        """
        copy_file_path = self._retrieve_filepath(file_url)

        self.log('Connecting and downloading web source: {}'.format(
                                                                file_url))

        success = False
        attempts = 0
        while attempts < 3 and not success:
            try:
                self._wget_dl(file_url, copy_file_path)
                success = True
            except Exception as e:
                print('Exception Error: {}'.format(e))
                print('Failed to download. Attempting to rerun')
                attempts += 1

        if not success:
            raise ValueError('Dowload Failed!')

        self.log('Downloaded file to {}'.format(copy_file_path))

        return copy_file_path

    def _download_direct_download_link(self, file_url):
        """
        _download_direct_download_link: direct download link handler

        params:
        file_url: direct download URL
        copy_file_path: output file saving path

        """
        copy_file_path = self._download_to_file(file_url)
        copy_file_path = self._unpack(copy_file_path, True)

        return copy_file_path

    def _download_dropbox_link(self, file_url):
        """
        _download_dropbox_link: dropbox download link handler
                                file needs to be shared publicly

        params:
        file_url: dropbox download link

        """
        if not file_url.startswith('https://www.dropbox.com/'):
            raise ValueError('Invalid DropBox Link: {}'.format(file_url))

        self.log('Connecting DropBox link: {}'.format(file_url))
        # translate dropbox URL for direct download
        if "?" not in file_url:
            force_download_link = file_url + '?raw=1'
        else:
            force_download_link = file_url.partition('?')[0] + '?raw=1'

        self.log('Generating DropBox direct download link\n' +
                 ' from: {}\n to: {}'.format(file_url, force_download_link))

        copy_file_path = self._download_to_file(force_download_link)
        copy_file_path = self._unpack(copy_file_path, True)

        return copy_file_path

    def _get_google_confirm_token(self, response):
        """
        _get_google_confirm_token: get Google drive confirm token for large file
        """
        for key, value in response.cookies.items():
            if key.startswith('download_warning'):
                return value

        return None

    def _download_google_drive_to_file(self, file_url, cookies=None):
        """
        _download_google_drive_to_file: download url content to file
        params:
        file_url: direct download URL
        """
        copy_file_path = self._retrieve_filepath(file_url, cookies)

        self.log('Connecting and downloading web source: {}'.format(
                                                                file_url))

        try:
            with requests.get(file_url, cookies=cookies, stream=True) as response:
                with open(copy_file_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=1024):
                        f.write(chunk)
                self.log('Downloaded file to {}'.format(copy_file_path))
        except BaseException as error:
            error_msg = 'Cannot connect to URL: {}\n'.format(file_url)
            error_msg += 'Exception: {}'.format(error)
            raise ValueError(error_msg)

        return copy_file_path

    def _download_google_drive_link(self, file_url):
        """
        _download_google_drive_link: Google Drive download link handler
                                     file needs to be shared publicly

        params:
        file_url: Google Drive download link

        """
        if not file_url.startswith('https://drive.google.com/'):
            raise ValueError('Invalid Google Drive Link: {}'.format(file_url))

        self.log('Connecting Google Drive link: {}'.format(file_url))
        # translate Google Drive URL for direct download
        force_download_link_prefix = 'https://docs.google.com/uc?export=download'

        if file_url.find('drive.google.com/file/d/') != -1:
            file_id = file_url.partition('/d/')[-1].partition('/')[0]
        elif file_url.find('drive.google.com/open?id=') != -1:
            file_id = file_url.partition('id=')[-1]
        else:
            error_msg = 'Unexpected Google Drive share link.\n'
            error_msg += 'URL: {}'.format(file_url)
            raise ValueError(error_msg)

        force_download_link = force_download_link_prefix + '&id={}'.format(file_id)

        with requests.Session() as session:
            response = session.get(force_download_link)
            confirm_token = self._get_google_confirm_token(response)

            if confirm_token:
                force_download_link = force_download_link_prefix + '&confirm={}&id={}'.format(confirm_token, file_id)

            self.log('Generating Google Drive direct download link\n' +
                     ' from: {}\n to: {}'.format(file_url, force_download_link))

            copy_file_path = self._download_google_drive_to_file(force_download_link, response.cookies)
            copy_file_path = self._unpack(copy_file_path, True)

        return copy_file_path

    def _download_ftp_link(self, file_url):
        """
        _download_ftp_link: FTP download link handler
                            URL fomat: ftp://anonymous:email@ftp_link
                                    or ftp://ftp_link
                            defualt user_name: 'anonymous'
                                    password: 'anonymous@domain.com'

                            Note: Currenlty we only support anonymous FTP due to securty reasons.

        params:
        file_url: FTP download link

        """
        if not file_url.startswith('ftp://'):
            raise ValueError('Invalid FTP Link: {}'.format(file_url))

        self.log('Connecting FTP link: {}'.format(file_url))
        ftp_url_format = re.match(r'ftp://.*:.*@.*/.*', file_url)
        # process ftp credentials
        if ftp_url_format:
            ftp_user_name = re.search('ftp://(.+?):', file_url).group(1)
            if ftp_user_name.lower() != 'anonymous':
                raise ValueError("Currently we only support anonymous FTP")
            ftp_password = file_url.rpartition('@')[0].rpartition(':')[-1]
            ftp_domain = re.search(
                'ftp://.*:.*@(.+?)/', file_url).group(1)
            ftp_file_path = file_url.partition(
                'ftp://')[-1].partition('/')[-1].rpartition('/')[0]
            ftp_file_name = re.search(
                'ftp://.*:.*@.*/(.+$)', file_url).group(1)
        else:
            self.log('Setting anonymous FTP user_name and password')
            ftp_user_name = 'anonymous'
            ftp_password = 'anonymous@domain.com'
            ftp_domain = re.search('ftp://(.+?)/', file_url).group(1)
            ftp_file_path = file_url.partition(
                'ftp://')[-1].partition('/')[-1].rpartition('/')[0]
            ftp_file_name = re.search('ftp://.*/(.+$)', file_url).group(1)

        self._check_ftp_connection(ftp_user_name, ftp_password,
                                   ftp_domain, ftp_file_path, ftp_file_name)

        with ftplib.FTP(ftp_domain) as ftp_connection:
            ftp_connection.login(ftp_user_name, ftp_password)
            ftp_connection.cwd(ftp_file_path)

            copy_file_path = os.path.join(self.tmp, ftp_file_name)

            with open(copy_file_path, 'wb') as output:
                ftp_connection.retrbinary('RETR {}'.format(ftp_file_name),
                                          output.write)
            self.log('Copied FTP file to: {}'.format(copy_file_path))

        copy_file_path = self._unpack(copy_file_path, True)

        return copy_file_path

    def _check_ftp_connection(self, user_name, password, domain, file_path, file_name):
        """
        _check_ftp_connection: ftp connection checker

        params:
        user_name: FTP user name
        password: FTP user password
        domain: FTP domain
        file_path: target file directory
        file_name: target file name

        """

        try:
            with ftplib.FTP(domain) as ftp:
                try:
                    ftp.login(user_name, password)
                except ftplib.all_errors as error:
                    raise ValueError("Cannot login: {}".format(error))
                else:
                    ftp.cwd(file_path)
                    if file_name in ftp.nlst():
                        pass
                    else:
                        raise ValueError(
                          "File {} does NOT exist in FTP path: {}".format(
                                    file_name, domain + '/' + file_path))
        except ftplib.all_errors as error:
            raise ValueError("Cannot connect: {}".format(error))

    def _gen_tmp_path(self):
        return os.path.join(self.scratch, 'temp')  # str(uuid.uuid4()))

    #END_CLASS_HEADER

    # config contains contents of config file in a hash or None if it couldn't
    # be found
    def __init__(self, config):
        #BEGIN_CONSTRUCTOR
        self.shock_url = config['shock-url']
        self.shock_effective = self.shock_url
        # note that the unit tests cannot easily test this. Be careful with changes here
        with requests.get(config['kbase-endpoint'] + '/shock-direct', allow_redirects=False) as r:
            if r.status_code == 302:
                self.log('Using direct shock url for transferring files')
                self.shock_effective = r.headers['Location']
        self.log('Shock url: ' + self.shock_effective)
        self.handle_url = config['handle-service-url']
        self.ws_url = config['workspace-url']
        self.scratch = config['scratch']
        self.tmp = self._gen_tmp_path()
        self.mkdir_p(self.tmp)

        # Number of processors used by PIGZ, and a compression level (1=fastest, 9=best)
        self.PIGZ_N_PROCESSES = config['pigz_n_processes']
        self.PIGZ_COMPRESSION_LEVEL = config['pigz_compression_level']
        #END_CONSTRUCTOR
        pass


    def shock_to_file(self, ctx, params):
        """
        Download a file from Shock.
        :param params: instance of type "ShockToFileParams" (Input for the
           shock_to_file function. Required parameters: shock_id | handle_id
           - the ID of the Shock node, or the Handle to a shock node.
           file_path - the location to save the file output. If this is a
           directory, the file will be named as per the filename in Shock.
           Optional parameters: unpack - either null, 'uncompress', or
           'unpack'. 'uncompress' will cause any bzip or gzip files to be
           uncompressed. 'unpack' will behave the same way, but it will also
           unpack tar and zip archive files (uncompressing gzipped or bzipped
           archive files if necessary). If 'uncompress' is specified and an
           archive file is encountered, an error will be thrown. If the file
           is an archive, it will be unbundled into the directory containing
           the original output file. Note that if the file name (either as
           provided by the user or by Shock) without the a decompression
           extension (e.g. .gz, .zip or .tgz -> .tar) points to an existing
           file and unpack is specified, that file will be overwritten by the
           decompressed Shock file.) -> structure: parameter "shock_id" of
           String, parameter "handle_id" of String, parameter "file_path" of
           String, parameter "unpack" of String
        :returns: instance of type "ShockToFileOutput" (Output from the
           shock_to_file function. node_file_name - the filename of the file
           as stored in Shock. file_path - the path to the downloaded file.
           If a directory was specified in the input, this will be the
           directory appended with the shock file name. If a file was
           specified, it will be that file path. In either case, if the file
           is uncompressed any compression file extensions will be removed
           (e.g. .gz) and or altered (e.g. .tgz -> .tar) as appropriate. size
           - the size of the file in bytes as stored in Shock, prior to
           unpacking. attributes - the file attributes, if any, stored in
           Shock.) -> structure: parameter "node_file_name" of String,
           parameter "file_path" of String, parameter "size" of Long,
           parameter "attributes" of mapping from String to unspecified object
        """
        # ctx is the context object
        # return variables are: out
        #BEGIN shock_to_file
        token = ctx['token']
        if not token:
            raise ValueError('Authentication token required.')
        headers = {'Authorization': 'OAuth ' + token}
        shock_id = params.get('shock_id')
        handle_id = params.get('handle_id')
        if not shock_id and not handle_id:
            raise ValueError('Must provide shock ID or handle ID')
        if shock_id and handle_id:
            raise ValueError(
                'Must provide either a shock ID or handle ID, not both')

        shock_url = self.shock_effective
        if handle_id:
            self.log('Fetching info for handle: '+handle_id)
            hs = HandleService(self.handle_url, token=token)
            handles = hs.hids_to_handles([handle_id])
            # don't override direct url if provided
            #shock_url = handles[0]['url']
            shock_id = handles[0]['id']

        file_path = params.get('file_path')
        if not file_path:
            raise ValueError('Must provide file path')
        self.mkdir_p(os.path.dirname(file_path))
        node_url = shock_url + '/node/' + shock_id
        r = requests.get(node_url, headers=headers, allow_redirects=True)
        errtxt = ('Error downloading file from shock ' +
                  'node {}: ').format(shock_id)
        self.check_shock_response(r, errtxt)
        resp_obj = r.json()
        size = resp_obj['data']['file']['size']
        if not size:
            raise ShockException('Node {} has no file'.format(shock_id))
        node_file_name = resp_obj['data']['file']['name']
        attributes = resp_obj['data']['attributes']
        if os.path.isdir(file_path):
            file_path = os.path.join(file_path, node_file_name)
        self.log('downloading shock node ' + shock_id + ' into file: ' + str(file_path))
        with open(file_path, 'wb') as fhandle:
            with requests.get(node_url + '?download_raw', stream=True,
                              headers=headers, allow_redirects=True) as r:
                self.check_shock_response(r, errtxt)
                for chunk in r.iter_content(1024):
                    if not chunk:
                        break
                    fhandle.write(chunk)
        unpack = params.get('unpack')
        if unpack:
            if unpack not in ['unpack', 'uncompress']:
                raise ValueError('Illegal unpack value: ' + str(unpack))
            file_path = self._unpack(file_path, unpack == 'unpack')
        out = {'node_file_name': node_file_name,
               'attributes': attributes,
               'file_path': file_path,
               'size': size}
        self.log('downloading done')
        #END shock_to_file

        # At some point might do deeper type checking...
        if not isinstance(out, dict):
            raise ValueError('Method shock_to_file return value ' +
                             'out is not type dict as required.')
        # return the results
        return [out]

    def shock_to_file_mass(self, ctx, params):
        """
        Download multiple files from Shock.
        :param params: instance of list of type "ShockToFileParams" (Input
           for the shock_to_file function. Required parameters: shock_id |
           handle_id - the ID of the Shock node, or the Handle to a shock
           node. file_path - the location to save the file output. If this is
           a directory, the file will be named as per the filename in Shock.
           Optional parameters: unpack - either null, 'uncompress', or
           'unpack'. 'uncompress' will cause any bzip or gzip files to be
           uncompressed. 'unpack' will behave the same way, but it will also
           unpack tar and zip archive files (uncompressing gzipped or bzipped
           archive files if necessary). If 'uncompress' is specified and an
           archive file is encountered, an error will be thrown. If the file
           is an archive, it will be unbundled into the directory containing
           the original output file. Note that if the file name (either as
           provided by the user or by Shock) without the a decompression
           extension (e.g. .gz, .zip or .tgz -> .tar) points to an existing
           file and unpack is specified, that file will be overwritten by the
           decompressed Shock file.) -> structure: parameter "shock_id" of
           String, parameter "handle_id" of String, parameter "file_path" of
           String, parameter "unpack" of String
        :returns: instance of list of type "ShockToFileOutput" (Output from
           the shock_to_file function. node_file_name - the filename of the
           file as stored in Shock. file_path - the path to the downloaded
           file. If a directory was specified in the input, this will be the
           directory appended with the shock file name. If a file was
           specified, it will be that file path. In either case, if the file
           is uncompressed any compression file extensions will be removed
           (e.g. .gz) and or altered (e.g. .tgz -> .tar) as appropriate. size
           - the size of the file in bytes as stored in Shock, prior to
           unpacking. attributes - the file attributes, if any, stored in
           Shock.) -> structure: parameter "node_file_name" of String,
           parameter "file_path" of String, parameter "size" of Long,
           parameter "attributes" of mapping from String to unspecified object
        """
        # ctx is the context object
        # return variables are: out
        #BEGIN shock_to_file_mass
        if type(params) != list:
            raise ValueError('expected list input')
        out = []
        # in the future, could make this rather silly implementation smarter
        # although probably bottlenecked by disk & network so parallelization
        # may not help
        for p in params:
            out.append(self.shock_to_file(ctx, p)[0])
        #END shock_to_file_mass

        # At some point might do deeper type checking...
        if not isinstance(out, list):
            raise ValueError('Method shock_to_file_mass return value ' +
                             'out is not type list as required.')
        # return the results
        return [out]

    def file_to_shock(self, ctx, params):
        """
        Load a file to Shock.
        :param params: instance of type "FileToShockParams" (Input for the
           file_to_shock function. Required parameters: file_path - the
           location of the file (or directory if using the pack parameter) to
           load to Shock. Optional parameters: attributes - DEPRECATED:
           attributes are currently ignored by the upload function and will
           be removed entirely in a future version. User-specified attributes
           to save to the Shock node along with the file. make_handle - make
           a Handle Service handle for the shock node. Default false. pack -
           compress a file or archive a directory before loading to Shock.
           The file_path argument will be appended with the appropriate file
           extension prior to writing. For gzips only, if the file extension
           denotes that the file is already compressed, it will be skipped.
           If file_path is a directory and tarring or zipping is specified,
           the created file name will be set to the directory name, possibly
           overwriting an existing file. Attempting to pack the root
           directory is an error. Do not attempt to pack the scratch space
           root as noted in the module description. The allowed values are:
           gzip - gzip the file given by file_path. targz - tar and gzip the
           directory specified by the directory portion of the file_path into
           the file specified by the file_path. zip - as targz but zip the
           directory.) -> structure: parameter "file_path" of String,
           parameter "attributes" of mapping from String to unspecified
           object, parameter "make_handle" of type "boolean" (A boolean - 0
           for false, 1 for true. @range (0, 1)), parameter "pack" of String
        :returns: instance of type "FileToShockOutput" (Output of the
           file_to_shock function. shock_id - the ID of the new Shock node.
           handle - the new handle, if created. Null otherwise.
           node_file_name - the name of the file stored in Shock. size - the
           size of the file stored in shock.) -> structure: parameter
           "shock_id" of String, parameter "handle" of type "Handle" (A
           handle for a file stored in Shock. hid - the id of the handle in
           the Handle Service that references this shock node id - the id for
           the shock node url - the url of the shock server type - the type
           of the handle. This should always be shock. file_name - the name
           of the file remote_md5 - the md5 digest of the file.) ->
           structure: parameter "hid" of String, parameter "file_name" of
           String, parameter "id" of String, parameter "url" of String,
           parameter "type" of String, parameter "remote_md5" of String,
           parameter "node_file_name" of String, parameter "size" of String
        """
        # ctx is the context object
        # return variables are: out
        #BEGIN file_to_shock
        token = ctx['token']
        if not token:
            raise ValueError('Authentication token required.')
        headers = {'Authorization': 'Oauth ' + token}
        file_path = params.get('file_path')
        if not file_path:
            raise ValueError('No file(s) provided for upload to Shock.')
        pack = params.get('pack')
        if pack:
            file_path = self._pack(file_path, pack)
        self.log('uploading file ' + str(file_path) + ' into shock node')
        with open(os.path.abspath(file_path), 'rb') as data_file:
            # Content-Length header is required for transition to
            # https://github.com/kbase/blobstore
            files = {'upload': (os.path.basename(file_path), data_file, None,
                {'Content-Length': os.path.getsize(file_path)})}
            mpe = MultipartEncoder(fields=files)
            headers['content-type'] = mpe.content_type
            response = requests.post(
                self.shock_effective + '/node', headers=headers, data=mpe,
                stream=True, allow_redirects=True)
        self.check_shock_response(
            response, ('Error trying to upload file {} to Shock: '
                       ).format(file_path))
        shock_data = response.json()['data']
        shock_id = shock_data['id']
        out = {'shock_id': shock_id,
               'handle': None,
               'node_file_name': shock_data['file']['name'],
               'size': shock_data['file']['size']}
        if params.get('make_handle'):
            out['handle'] = self.make_handle(shock_data, token)
        self.log('uploading done into shock node: ' + shock_id)
        #END file_to_shock

        # At some point might do deeper type checking...
        if not isinstance(out, dict):
            raise ValueError('Method file_to_shock return value ' +
                             'out is not type dict as required.')
        # return the results
        return [out]

    def unpack_file(self, ctx, params):
        """
        Using the same logic as unpacking a Shock file, this method will cause
        any bzip or gzip files to be uncompressed, and then unpack tar and zip
        archive files (uncompressing gzipped or bzipped archive files if
        necessary). If the file is an archive, it will be unbundled into the
        directory containing the original output file.
        :param params: instance of type "UnpackFileParams" -> structure:
           parameter "file_path" of String
        :returns: instance of type "UnpackFileResult" -> structure: parameter
           "file_path" of String
        """
        # ctx is the context object
        # return variables are: out
        #BEGIN unpack_file
        del ctx
        file_path = params.get('file_path')
        if not file_path:
            raise ValueError('Must provide file path')

        unpack = True
        new_file_path = self._unpack(file_path, unpack)
        out = {'file_path': new_file_path}
        #END unpack_file

        # At some point might do deeper type checking...
        if not isinstance(out, dict):
            raise ValueError('Method unpack_file return value ' +
                             'out is not type dict as required.')
        # return the results
        return [out]

    def pack_file(self, ctx, params):
        """
        Pack a file or directory into gzip, targz, or zip archives.
        :param params: instance of type "PackFileParams" (Input for the
           pack_file function. Required parameters: file_path - the location
           of the file (or directory if using the pack parameter) to load to
           Shock. pack - The format into which the file or files will be
           packed. The file_path argument will be appended with the
           appropriate file extension prior to writing. For gzips only, if
           the file extension denotes that the file is already compressed, it
           will be skipped. If file_path is a directory and tarring or
           zipping is specified, the created file name will be set to the
           directory name, possibly overwriting an existing file. Attempting
           to pack the root directory is an error. Do not attempt to pack the
           scratch space root as noted in the module description. The allowed
           values are: gzip - gzip the file given by file_path. targz - tar
           and gzip the directory specified by the directory portion of the
           file_path into the file specified by the file_path. zip - as targz
           but zip the directory.) -> structure: parameter "file_path" of
           String, parameter "pack" of String
        :returns: instance of type "PackFileResult" (Output from the
           pack_file function. file_path - the path to the packed file.) ->
           structure: parameter "file_path" of String
        """
        # ctx is the context object
        # return variables are: out
        #BEGIN pack_file
        del ctx
        file_path = params.get('file_path')
        if not file_path:
            raise ValueError('file_path is required')
        out = {'file_path': self._pack(file_path, params.get('pack'))}
        #END pack_file

        # At some point might do deeper type checking...
        if not isinstance(out, dict):
            raise ValueError('Method pack_file return value ' +
                             'out is not type dict as required.')
        # return the results
        return [out]

    def package_for_download(self, ctx, params):
        """
        :param params: instance of type "PackageForDownloadParams" (Input for
           the package_for_download function. Required parameters: file_path
           - the location of the directory to compress as zip archive before
           loading to Shock. This argument will be appended with the '.zip'
           file extension prior to writing. If it is a directory, file name
           of the created archive will be set to the directory name followed
           by '.zip', possibly overwriting an existing file. Attempting to
           pack the root directory is an error. Do not attempt to pack the
           scratch space root as noted in the module description. ws_ref -
           list of references to workspace objects which will be used to
           produce info-files in JSON format containing workspace metadata
           and provenance structures. It produces new files in folder pointed
           by file_path (or folder containing file pointed by file_path if
           it's not folder). Optional parameters: attributes - DEPRECATED:
           attributes are currently ignored by the upload function and will
           be removed entirely in a future version. User-specified attributes
           to save to the Shock node along with the file.) -> structure:
           parameter "file_path" of String, parameter "attributes" of mapping
           from String to unspecified object, parameter "ws_refs" of list of
           String
        :returns: instance of type "PackageForDownloadOutput" (Output of the
           package_for_download function. shock_id - the ID of the new Shock
           node. node_file_name - the name of the file stored in Shock. size
           - the size of the file stored in shock.) -> structure: parameter
           "shock_id" of String, parameter "node_file_name" of String,
           parameter "size" of String
        """
        # ctx is the context object
        # return variables are: returnVal
        #BEGIN package_for_download
        if ctx['token'] is None:
            raise ValueError('Authentication token required!')
        file_path = params.get('file_path')
        if not file_path:
            raise ValueError('No file/directory provided.')
        ws_refs = params.get('ws_refs')
        if not ws_refs:
            raise ValueError('No workspace references provided.')
        dir_path = file_path
        if not os.path.isdir(dir_path):
            dir_path, _ = os.path.split(dir_path)
        if not dir_path:
            dir_path = '.'
        dir_path = os.path.abspath(os.path.expanduser(dir_path))
        objects = [{'ref': x} for x in ws_refs]
        ws = Workspace(self.ws_url, token=ctx['token'])
        items = ws.get_objects2({'no_data': 1, 'ignoreErrors': 0,
                                 'objects': objects})['data']
        for item in items:
            item_info = item['info']
            info_to_save = {'metadata': [item_info],
                            'provenance': [item]}
            ws_name = item_info[7]
            obj_name = item_info[1]
            obj_ver = item_info[4]
            info_file_name = 'KBase_object_details_' + ws_name.replace(':', '_') + '_' + \
                             obj_name + '_v' + str(obj_ver) + '.json'
            info_file_path = os.path.join(dir_path, info_file_name)
            with io.open(info_file_path, 'w', encoding="utf-8") as writer:
                text = json.dumps(info_to_save, sort_keys=True,
                                  indent=4, ensure_ascii=False)
                writer.write(text)
        fts_input = {'file_path': file_path, 'ws_refs': ws_refs,
                     'pack': 'zip'}
        returnVal = self.file_to_shock(ctx, fts_input)[0]
        #END package_for_download

        # At some point might do deeper type checking...
        if not isinstance(returnVal, dict):
            raise ValueError('Method package_for_download return value ' +
                             'returnVal is not type dict as required.')
        # return the results
        return [returnVal]

    def file_to_shock_mass(self, ctx, params):
        """
        Load multiple files to Shock.
        :param params: instance of list of type "FileToShockParams" (Input
           for the file_to_shock function. Required parameters: file_path -
           the location of the file (or directory if using the pack
           parameter) to load to Shock. Optional parameters: attributes -
           DEPRECATED: attributes are currently ignored by the upload
           function and will be removed entirely in a future version.
           User-specified attributes to save to the Shock node along with the
           file. make_handle - make a Handle Service handle for the shock
           node. Default false. pack - compress a file or archive a directory
           before loading to Shock. The file_path argument will be appended
           with the appropriate file extension prior to writing. For gzips
           only, if the file extension denotes that the file is already
           compressed, it will be skipped. If file_path is a directory and
           tarring or zipping is specified, the created file name will be set
           to the directory name, possibly overwriting an existing file.
           Attempting to pack the root directory is an error. Do not attempt
           to pack the scratch space root as noted in the module description.
           The allowed values are: gzip - gzip the file given by file_path.
           targz - tar and gzip the directory specified by the directory
           portion of the file_path into the file specified by the file_path.
           zip - as targz but zip the directory.) -> structure: parameter
           "file_path" of String, parameter "attributes" of mapping from
           String to unspecified object, parameter "make_handle" of type
           "boolean" (A boolean - 0 for false, 1 for true. @range (0, 1)),
           parameter "pack" of String
        :returns: instance of list of type "FileToShockOutput" (Output of the
           file_to_shock function. shock_id - the ID of the new Shock node.
           handle - the new handle, if created. Null otherwise.
           node_file_name - the name of the file stored in Shock. size - the
           size of the file stored in shock.) -> structure: parameter
           "shock_id" of String, parameter "handle" of type "Handle" (A
           handle for a file stored in Shock. hid - the id of the handle in
           the Handle Service that references this shock node id - the id for
           the shock node url - the url of the shock server type - the type
           of the handle. This should always be shock. file_name - the name
           of the file remote_md5 - the md5 digest of the file.) ->
           structure: parameter "hid" of String, parameter "file_name" of
           String, parameter "id" of String, parameter "url" of String,
           parameter "type" of String, parameter "remote_md5" of String,
           parameter "node_file_name" of String, parameter "size" of String
        """
        # ctx is the context object
        # return variables are: out
        #BEGIN file_to_shock_mass
        if type(params) != list:
            raise ValueError('expected list input')
        out = []
        # in the future, could make this rather silly implementation smarter
        # although probably bottlenecked by disk & network so parallelization
        # may not help
        for p in params:
            out.append(self.file_to_shock(ctx, p)[0])
        #END file_to_shock_mass

        # At some point might do deeper type checking...
        if not isinstance(out, list):
            raise ValueError('Method file_to_shock_mass return value ' +
                             'out is not type list as required.')
        # return the results
        return [out]

    def copy_shock_node(self, ctx, params):
        """
        Copy a Shock node.
        :param params: instance of type "CopyShockNodeParams" (Input for the
           copy_shock_node function. Required parameters: shock_id - the id
           of the node to copy. Optional parameters: make_handle - make a
           Handle Service handle for the shock node. Default false.) ->
           structure: parameter "shock_id" of String, parameter "make_handle"
           of type "boolean" (A boolean - 0 for false, 1 for true. @range (0,
           1))
        :returns: instance of type "CopyShockNodeOutput" (Output of the
           copy_shock_node function. shock_id - the id of the new Shock node.
           handle - the new handle, if created. Null otherwise.) ->
           structure: parameter "shock_id" of String, parameter "handle" of
           type "Handle" (A handle for a file stored in Shock. hid - the id
           of the handle in the Handle Service that references this shock
           node id - the id for the shock node url - the url of the shock
           server type - the type of the handle. This should always be shock.
           file_name - the name of the file remote_md5 - the md5 digest of
           the file.) -> structure: parameter "hid" of String, parameter
           "file_name" of String, parameter "id" of String, parameter "url"
           of String, parameter "type" of String, parameter "remote_md5" of
           String
        """
        # ctx is the context object
        # return variables are: out
        #BEGIN copy_shock_node
        token = ctx['token']
        if token is None:
            raise ValueError('Authentication token required!')
        header = {'Authorization': 'Oauth {}'.format(token)}
        source_id = params.get('shock_id')
        if not source_id:
            raise ValueError('Must provide shock ID')
        mpdata = MultipartEncoder(fields={'copy_data': source_id})
        header['Content-Type'] = mpdata.content_type

        with requests.post(self.shock_url + '/node', headers=header, data=mpdata,
                           allow_redirects=True) as response:
            self.check_shock_response(
                response, ('Error copying Shock node {}: '
                           ).format(source_id))
            shock_data = response.json()['data']
        shock_id = shock_data['id']
        out = {'shock_id': shock_id, 'handle': None}
        if params.get('make_handle'):
            out['handle'] = self.make_handle(shock_data, token)
        #END copy_shock_node

        # At some point might do deeper type checking...
        if not isinstance(out, dict):
            raise ValueError('Method copy_shock_node return value ' +
                             'out is not type dict as required.')
        # return the results
        return [out]

    def own_shock_node(self, ctx, params):
        """
        Gain ownership of a Shock node.
        Returns a shock node id which is owned by the caller, given a shock
        node id.
        If the shock node is already owned by the caller, returns the same
        shock node ID. If not, the ID of a copy of the original node will be
        returned.
        If a handle is requested, the node is already owned by the caller, and
        a handle already exists, that handle will be returned. Otherwise a new
        handle will be created and returned.
        :param params: instance of type "OwnShockNodeParams" (Input for the
           own_shock_node function. Required parameters: shock_id - the id of
           the node for which the user needs ownership. Optional parameters:
           make_handle - make or find a Handle Service handle for the shock
           node. Default false.) -> structure: parameter "shock_id" of
           String, parameter "make_handle" of type "boolean" (A boolean - 0
           for false, 1 for true. @range (0, 1))
        :returns: instance of type "OwnShockNodeOutput" (Output of the
           own_shock_node function. shock_id - the id of the (possibly new)
           Shock node. handle - the handle, if requested. Null otherwise.) ->
           structure: parameter "shock_id" of String, parameter "handle" of
           type "Handle" (A handle for a file stored in Shock. hid - the id
           of the handle in the Handle Service that references this shock
           node id - the id for the shock node url - the url of the shock
           server type - the type of the handle. This should always be shock.
           file_name - the name of the file remote_md5 - the md5 digest of
           the file.) -> structure: parameter "hid" of String, parameter
           "file_name" of String, parameter "id" of String, parameter "url"
           of String, parameter "type" of String, parameter "remote_md5" of
           String
        """
        # ctx is the context object
        # return variables are: out
        #BEGIN own_shock_node
        token = ctx['token']
        if token is None:
            raise ValueError('Authentication token required!')
        header = {'Authorization': 'Oauth {}'.format(token)}
        source_id = params.get('shock_id')
        if not source_id:
            raise ValueError('Must provide shock ID')
        with requests.get(self.shock_url + '/node/' + source_id + '/acl/?verbosity=full',
                          headers=header, allow_redirects=True) as res:
            self.check_shock_response(
                res, 'Error getting ACLs for Shock node {}: '.format(source_id))
            owner = res.json()['data']['owner']['username']
        if owner != ctx['user_id']:
            out = self.copy_shock_node(ctx, params)[0]
        elif params.get('make_handle'):
            hs = HandleService(self.handle_url, token=token)
            handles = hs.ids_to_handles([source_id])
            if handles:
                h = handles[0]
                del h['created_by']
                del h['creation_date']
                del h['remote_sha1']
                out = {'shock_id': source_id, 'handle': h}
            else:
                # possibility of race condition here, but highly unlikely, so
                # meh
                with requests.get(self.shock_url + '/node/' + source_id,
                                  headers=header, allow_redirects=True) as r:
                    errtxt = ('Error downloading attributes from shock ' +
                              'node {}: ').format(source_id)
                    self.check_shock_response(r, errtxt)
                    out = {'shock_id': source_id,
                           'handle': self.make_handle(r.json()['data'], token)}
        else:
            out = {'shock_id': source_id}
        #END own_shock_node

        # At some point might do deeper type checking...
        if not isinstance(out, dict):
            raise ValueError('Method own_shock_node return value ' +
                             'out is not type dict as required.')
        # return the results
        return [out]

    def ws_name_to_id(self, ctx, name):
        """
        Translate a workspace name to a workspace ID.
        :param name: instance of String
        :returns: instance of Long
        """
        # ctx is the context object
        # return variables are: id
        #BEGIN ws_name_to_id
        ws = Workspace(self.ws_url, token=ctx['token'])
        id = ws.get_workspace_info(  # @ReservedAssignment
            {'workspace': name})[0]
        #END ws_name_to_id

        # At some point might do deeper type checking...
        if not isinstance(id, int):
            raise ValueError('Method ws_name_to_id return value ' +
                             'id is not type int as required.')
        # return the results
        return [id]

    def save_objects(self, ctx, params):
        """
        Save objects to the workspace. Saving over a deleted object undeletes
        it.
        :param params: instance of type "SaveObjectsParams" (Input parameters
           for the "save_objects" function. Required parameters: id - the
           numerical ID of the workspace. objects - the objects to save. The
           object provenance is automatically pulled from the SDK runner.) ->
           structure: parameter "id" of Long, parameter "objects" of list of
           type "ObjectSaveData" (An object and associated data required for
           saving. Required parameters: type - the workspace type string for
           the object. Omit the version information to use the latest
           version. data - the object data. Optional parameters: One of an
           object name or id. If no name or id is provided the name will be
           set to 'auto' with the object id appended as a string, possibly
           with -\d+ appended if that object id already exists as a name.
           name - the name of the object. objid - the id of the object to
           save over. meta - arbitrary user-supplied metadata for the object,
           not to exceed 16kb; if the object type specifies automatic
           metadata extraction with the 'meta ws' annotation, and your
           metadata name conflicts, then your metadata will be silently
           overwritten. hidden - true if this object should not be listed
           when listing workspace objects. extra_provenance_input_refs -
           (optional) if set, these refs will be appended to the primary
           ProveanceAction input_ws_objects reference list. In general, if
           the input WS object ref was passed in from a narrative App, this
           will be set for you. However, there are cases where the object ref
           passed to the App is a container, and you are operating on a
           member or subobject of the container, in which case to maintain
           that direct mapping to those subobjects in the provenance of new
           objects, you can provide additional object refs here. For example,
           if the input is a ReadsSet, and your App creates a new WS object
           for each read library in the set, you may want a direct reference
           from each new WS object not only to the set, but also to the
           individual read library.) -> structure: parameter "type" of
           String, parameter "data" of unspecified object, parameter "name"
           of String, parameter "objid" of Long, parameter "meta" of mapping
           from String to String, parameter "hidden" of type "boolean" (A
           boolean - 0 for false, 1 for true. @range (0, 1)), parameter
           "extra_provenance_input_refs" of list of String
        :returns: instance of list of type "object_info" (Information about
           an object, including user provided metadata. objid - the numerical
           id of the object. name - the name of the object. type - the type
           of the object. save_date - the save date of the object. ver - the
           version of the object. saved_by - the user that saved or copied
           the object. wsid - the id of the workspace containing the object.
           workspace - the name of the workspace containing the object. chsum
           - the md5 checksum of the object. size - the size of the object in
           bytes. meta - arbitrary user-supplied metadata about the object.)
           -> tuple of size 11: parameter "objid" of Long, parameter "name"
           of String, parameter "type" of String, parameter "save_date" of
           String, parameter "version" of Long, parameter "saved_by" of
           String, parameter "wsid" of Long, parameter "workspace" of String,
           parameter "chsum" of String, parameter "size" of Long, parameter
           "meta" of mapping from String to String
        """
        # ctx is the context object
        # return variables are: info
        #BEGIN save_objects
        prov = ctx.provenance()
        objs = params.get('objects')
        if not objs:
            raise ValueError('Required parameter objects missing')
        wsid = params.get('id')
        if not wsid:
            raise ValueError('Required parameter id missing')
        objs_to_save = []
        for o in objs:
            obj_to_save = {}

            prov_to_save = prov
            if 'extra_provenance_input_refs' in o:
                prov_to_save = copy.deepcopy(prov)  # need to make a copy so we don't clobber other objects
                extra_input_refs = o['extra_provenance_input_refs']
                if extra_input_refs:
                    if len(prov) > 0:
                        if 'input_ws_objects' in prov[0]:
                            prov_to_save[0]['input_ws_objects'].extend(extra_input_refs)
                        else:
                            prov_to_save[0]['input_ws_objects'] = extra_input_refs
                    else:
                        prov_to_save = [{'input_ws_objects': extra_input_refs}]

            keys = ['type', 'data', 'name', 'objid', 'meta', 'hidden']
            for k in keys:
                if k in o:
                    obj_to_save[k] = o[k]

            obj_to_save['provenance'] = prov_to_save
            objs_to_save.append(obj_to_save)

        ws = Workspace(self.ws_url, token=ctx['token'])
        try:
            info = ws.save_objects({'id': wsid, 'objects': objs_to_save})
        except WorkspaceError as e:
            self.log('Logging workspace error on save_objects: {}\n{}'.format(
                e.message, e.data))
            raise
        #END save_objects

        # At some point might do deeper type checking...
        if not isinstance(info, list):
            raise ValueError('Method save_objects return value ' +
                             'info is not type list as required.')
        # return the results
        return [info]

    def get_objects(self, ctx, params):
        """
        Get objects from the workspace.
        :param params: instance of type "GetObjectsParams" (Input parameters
           for the "get_objects" function. Required parameters: object_refs -
           a list of object references in the form X/Y/Z, where X is the
           workspace name or id, Y is the object name or id, and Z is the
           (optional) object version. In general, always use ids rather than
           names if possible to avoid race conditions. A reference path may
           be specified by separating references by a semicolon, e.g.
           4/5/6;5/7/2;8/9/4 specifies that the user wishes to retrieve the
           fourth version of the object with id 9 in workspace 8, and that
           there exists a reference path from the sixth version of the object
           with id 5 in workspace 4, to which the user has access. The user
           may or may not have access to workspaces 5 and 8. Optional
           parameters: ignore_errors - ignore any errors that occur when
           fetching an object and instead insert a null into the returned
           list.) -> structure: parameter "object_refs" of list of String,
           parameter "ignore_errors" of type "boolean" (A boolean - 0 for
           false, 1 for true. @range (0, 1))
        :returns: instance of type "GetObjectsResults" (Results from the
           get_objects function. list<ObjectData> data - the returned
           objects.) -> structure: parameter "data" of list of type
           "ObjectData" (The data and supplemental info for an object.
           UnspecifiedObject data - the object's data or subset data.
           object_info info - information about the object.) -> structure:
           parameter "data" of unspecified object, parameter "info" of type
           "object_info" (Information about an object, including user
           provided metadata. objid - the numerical id of the object. name -
           the name of the object. type - the type of the object. save_date -
           the save date of the object. ver - the version of the object.
           saved_by - the user that saved or copied the object. wsid - the id
           of the workspace containing the object. workspace - the name of
           the workspace containing the object. chsum - the md5 checksum of
           the object. size - the size of the object in bytes. meta -
           arbitrary user-supplied metadata about the object.) -> tuple of
           size 11: parameter "objid" of Long, parameter "name" of String,
           parameter "type" of String, parameter "save_date" of String,
           parameter "version" of Long, parameter "saved_by" of String,
           parameter "wsid" of Long, parameter "workspace" of String,
           parameter "chsum" of String, parameter "size" of Long, parameter
           "meta" of mapping from String to String
        """
        # ctx is the context object
        # return variables are: results
        #BEGIN get_objects
        ignore_err = params.get('ignore_errors')
        objlist = params.get('object_refs')
        if not objlist:
            raise ValueError('No objects specified for retrieval')
        input_ = {'objects': [{'ref': x} for x in objlist]}
        if ignore_err:
            input_['ignoreErrors'] = 1
        ws = Workspace(self.ws_url, token=ctx['token'])
        try:
            retobjs = ws.get_objects2(input_)['data']
        except WorkspaceError as e:
            self.log('Logging workspace error on get_objects: {}\n{}'.format(
                e.message, e.data))
            raise
        results = []
        for o in retobjs:
            if not o:
                results.append(None)
                continue
            res = {'data': o['data'], 'info': o['info']}
            he = 'handle_error'
            hs = 'handle_stacktrace'
            if he in o or hs in o:
                ref = self.make_ref(o['info'])
                self.log('Handle error for object {}: {}.\nStacktrace: {}'
                         .format(ref, o.get(he), o.get(hs)))
                if ignore_err:
                    res = None
                else:
                    raise HandleError(
                        'HandleError', 0, 'Handle error for object {}: {}'
                        .format(ref, o.get(he)), o.get(hs))
            results.append(res)
        results = {'data': results}
        #END get_objects

        # At some point might do deeper type checking...
        if not isinstance(results, dict):
            raise ValueError('Method get_objects return value ' +
                             'results is not type dict as required.')
        # return the results
        return [results]

    def versions(self, ctx):
        """
        Get the versions of the Workspace service and Shock service.
        :returns: multiple set - (1) parameter "wsver" of String, (2)
           parameter "shockver" of String
        """
        # ctx is the context object
        # return variables are: wsver, shockver
        #BEGIN versions
        del ctx
        wsver = Workspace(self.ws_url).ver()
        with requests.get(self.shock_url, allow_redirects=True) as resp:
            self.check_shock_response(resp, 'Error contacting Shock: ')
            shockver = resp.json()['version']
        #END versions

        # At some point might do deeper type checking...
        if not isinstance(wsver, str):
            raise ValueError('Method versions return value ' +
                             'wsver is not type str as required.')
        if not isinstance(shockver, str):
            raise ValueError('Method versions return value ' +
                             'shockver is not type str as required.')
        # return the results
        return [wsver, shockver]

    def download_staging_file(self, ctx, params):
        """
        Download a staging area file to scratch area
        :param params: instance of type "DownloadStagingFileParams" (Input
           parameters for the "download_staging_file" function. Required
           parameters: staging_file_subdir_path: subdirectory file path e.g.
           for file: /data/bulk/user_name/file_name staging_file_subdir_path
           is file_name for file:
           /data/bulk/user_name/subdir_1/subdir_2/file_name
           staging_file_subdir_path is subdir_1/subdir_2/file_name) ->
           structure: parameter "staging_file_subdir_path" of String
        :returns: instance of type "DownloadStagingFileOutput" (Results from
           the download_staging_file function. copy_file_path: copied file
           scratch area path) -> structure: parameter "copy_file_path" of
           String
        """
        # ctx is the context object
        # return variables are: results
        #BEGIN download_staging_file
        if not params.get('staging_file_subdir_path'):
            error_msg = "missing 'staging_file_subdir_path' parameter"
            raise ValueError(error_msg)

        staging_file_subdir_path = params.get('staging_file_subdir_path')
        staging_file_name = os.path.basename(staging_file_subdir_path)
        staging_file_path = self._get_staging_file_path(ctx['user_id'], staging_file_subdir_path)

        self.log('Start downloading staging file: %s' % staging_file_path)
        shutil.copy2(staging_file_path, self.tmp)
        copy_file_path = os.path.join(self.tmp, staging_file_name)
        self.log('Copied staging file from %s to %s' %
                 (staging_file_path, copy_file_path))

        copy_file_path = self._unpack(copy_file_path, True)

        results = {'copy_file_path': copy_file_path}
        #END download_staging_file

        # At some point might do deeper type checking...
        if not isinstance(results, dict):
            raise ValueError('Method download_staging_file return value ' +
                             'results is not type dict as required.')
        # return the results
        return [results]

    def download_web_file(self, ctx, params):
        """
        Download a web file to scratch area
        :param params: instance of type "DownloadWebFileParams" (Input
           parameters for the "download_web_file" function. Required
           parameters: file_url: file URL download_type: one of ['Direct
           Download', 'FTP', 'DropBox', 'Google Drive']) -> structure:
           parameter "file_url" of String, parameter "download_type" of String
        :returns: instance of type "DownloadWebFileOutput" (Results from the
           download_web_file function. copy_file_path: copied file scratch
           area path) -> structure: parameter "copy_file_path" of String
        """
        # ctx is the context object
        # return variables are: results
        #BEGIN download_web_file

        # check for required parameters
        for p in ['file_url', 'download_type']:
            if p not in params:
                raise ValueError("missing '{}' parameter".format(p))

        file_url = params.get('file_url')
        download_type = params.get('download_type')

        self.log('Start downloading web file from: {}'.format(file_url))
        copy_file_path = self._download_file(download_type, file_url)

        results = {'copy_file_path': copy_file_path}
        #END download_web_file

        # At some point might do deeper type checking...
        if not isinstance(results, dict):
            raise ValueError('Method download_web_file return value ' +
                             'results is not type dict as required.')
        # return the results
        return [results]
    def status(self, ctx):
        #BEGIN_STATUS
        returnVal = {'state': 'OK',
                     'message': '',
                     'version': self.VERSION,
                     'git_url': self.GIT_URL,
                     'git_commit_hash': self.GIT_COMMIT_HASH
                     }
        del ctx
        #END_STATUS
        return [returnVal]
