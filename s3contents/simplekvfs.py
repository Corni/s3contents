"""
Utilities to make SimpleKVFS look like a regular file system
"""
import storefact

from s3contents.ipycompat import HasTraits, Unicode
from s3contents.basefs import BaseFS, NoSuchFileException
from simplekv import CopyMixin
from simplekv.decorator import URLEncodeKeysDecorator


class SimpleKVFS(BaseFS, HasTraits):
    prefix = Unicode("", help="Prefix path inside the specified bucket").tag(config=True)
    delimiter = Unicode("/", help="Path delimiter").tag(config=True)
    store_url = Unicode("hfs://.", help="URL to store in storefact format").tag(config=True, env="JPYNB_SKVFS_STORE_URL")
    store_url = Unicode("hmemory://.", help="URL to store in storefact format").tag(config=True, env="JPYNB_SKVFS_STORE_URL")
    dir_keep_file = Unicode(".s3keep", help="Empty file to create when creating directories").tag(config=True)

    def __init__(self, log, **kwargs):
        super(SimpleKVFS, self).__init__(log, **kwargs)
        self.log = log
        self.delimiter = "/"
        if self.prefix:
            self.mkdir("")
        self.original_store = storefact.get_store_from_url(self.store_url)
        # decorated store to support i.e. slashes in keys.
        self.store = URLEncodeKeysDecorator(self.original_store)

    def get_keys(self, prefix=""):
        return self.store.keys(prefix=prefix)

    def isfile(self, path):
        self.log.debug("S3contents[S3FS] Checking if `%s` is a file", path)
        key = self.as_key(path)
        if key != "" and key in self.store:
            self.log.debug("S3contents[S3FS] `%s` is a file: %s", path, True)
            return True
        self.log.debug("S3contents[S3FS] `%s` is a file: %s", path, False)
        return False

    def cp(self, old_path, new_path):
        self.log.debug("S3contents[S3FS] Copy `%s` to `%s`", old_path, new_path)
        if self.isdir(old_path):
            old_dir_path, new_dir_path = old_path, new_path
            old_dir_key = self.as_key(old_dir_path)
            for key in self.store.iter_keys(prefix=old_dir_key):
                old_item_path = self.as_path(key)
                new_item_path = old_item_path.replace(old_dir_path, new_dir_path, 1)
                self.cp(old_item_path, new_item_path)
        elif self.isfile(old_path):
            old_key = self.as_key(old_path)
            new_key = self.as_key(new_path)
            # we can not use self.store here as it is decorated
            if isinstance(self.original_store, CopyMixin):
                    self.store.copy(old_key, new_key)
            else:
                self.store.put(new_key, self.store.get(old_key))

    def rm(self, path):
        self.log.debug("S3contents[S3FS] Deleting: `%s`", path)
        if self.isfile(path):
            key = self.as_key(path)
            self.store.delete(key)
        elif self.isdir(path):
            key = self.as_key(path)
            key += "/"
            for obj in self.get_keys(key):
                self.store.delete(obj)

    def read(self, path):
        key = self.as_key(path)
        if not self.isfile(path):
            raise NoSuchFileException(self.as_path(key))
        return self.store.get(key)

    def write(self, path, content):
        self.store.put(self.as_key(path), content)
