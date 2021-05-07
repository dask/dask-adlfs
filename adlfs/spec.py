# -*- coding: utf-8 -*-


from __future__ import absolute_import, division, print_function

import asyncio
from glob import has_magic
import io
import logging
import os
import warnings
import weakref

from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError
from azure.storage.blob._shared.base_client import create_configuration
from azure.datalake.store import AzureDLFileSystem, lib
from azure.datalake.store.core import AzureDLFile, AzureDLPath
from azure.storage.blob.aio import BlobServiceClient as AIOBlobServiceClient
from azure.storage.blob import generate_blob_sas, BlobSasPermissions
from azure.storage.blob.aio._list_blobs_helper import BlobPrefix
from azure.storage.blob._models import BlobBlock, BlobProperties, BlobType
from fsspec import AbstractFileSystem
from fsspec.asyn import (
    sync,
    AsyncFileSystem,
    get_loop,
    sync_wrapper,
)
from fsspec.spec import AbstractBufferedFile
from fsspec.utils import infer_storage_options, tokenize
from .utils import (
    filter_blobs,
    get_blob_metadata,
    close_service_client,
    close_container_client,
)

from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

FORWARDED_BLOB_PROPERTIES = [
    "metadata",
    "creation_time",
    "deleted",
    "deleted_time",
    "last_modified",
    "content_time",
    "content_settings",
    "remaining_retention_days",
    "archive_status",
    "last_accessed_on",
    "etag",
    "tags",
    "tag_count",
]
_ROOT_PATH = "/"


class AzureDatalakeFileSystem(AbstractFileSystem):
    """
    Access Azure Datalake Gen1 as if it were a file system.

    This exposes a filesystem-like API on top of Azure Datalake Storage

    Parameters
    -----------
    tenant_id:  string
        Azure tenant, also known as the subscription id
    client_id: string
        The username or serivceprincipal id
    client_secret: string
        The access key
    store_name: string (optional)
        The name of the datalake account being accessed.  Should be inferred from the urlpath
        if using with Dask read_xxx and to_xxx methods.

    Examples
    --------
    >>> adl = AzureDatalakeFileSystem(tenant_id="xxxx", client_id="xxxx",
    ...                               client_secret="xxxx")

    >>> adl.ls('')

    Sharded Parquet & CSV files can be read as

    >>> storage_options = dict(tennant_id=TENNANT_ID, client_id=CLIENT_ID,
    ...                        client_secret=CLIENT_SECRET)  # doctest: +SKIP
    >>> ddf = dd.read_parquet('adl://store_name/folder/filename.parquet',
    ...                       storage_options=storage_options)  # doctest: +SKIP

    >>> ddf = dd.read_csv('adl://store_name/folder/*.csv'
    ...                   storage_options=storage_options)  # doctest: +SKIP


    Sharded Parquet and CSV files can be written as

    >>> ddf.to_parquet("adl://store_name/folder/filename.parquet",
    ...                storage_options=storage_options)  # doctest: +SKIP

    >>> ddf.to_csv('adl://store_name/folder/*.csv'
    ...            storage_options=storage_options)  # doctest: +SKIP
    """

    protocol = "adl"

    def __init__(self, tenant_id, client_id, client_secret, store_name):
        super().__init__()
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.store_name = store_name
        self.do_connect()

    @staticmethod
    def _get_kwargs_from_urls(paths):
        """ Get the store_name from the urlpath and pass to storage_options """
        ops = infer_storage_options(paths)
        out = {}
        if ops.get("host", None):
            out["store_name"] = ops["host"]
        return out

    @classmethod
    def _strip_protocol(cls, path):
        ops = infer_storage_options(path)
        return ops["path"]

    def do_connect(self):
        """Establish connection object."""
        token = lib.auth(
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret,
        )
        self.azure_fs = AzureDLFileSystem(token=token, store_name=self.store_name)

    def ls(self, path, detail=False, invalidate_cache=True, **kwargs):
        files = self.azure_fs.ls(
            path=path, detail=detail, invalidate_cache=invalidate_cache
        )

        for file in (file for file in files if type(file) is dict):
            if "type" in file:
                file["type"] = file["type"].lower()
            if "length" in file:
                file["size"] = file["length"]
        return files

    def info(self, path, invalidate_cache=True, expected_error_code=404, **kwargs):
        info = self.azure_fs.info(
            path=path,
            invalidate_cache=invalidate_cache,
            expected_error_code=expected_error_code,
        )
        info["size"] = info["length"]
        """Azure FS uses upper case type values but fsspec is expecting lower case"""
        info["type"] = info["type"].lower()
        return info

    def _trim_filename(self, fn, **kwargs):
        """ Determine what kind of filestore this is and return the path """
        so = infer_storage_options(fn)
        fileparts = so["path"]
        return fileparts

    def glob(self, path, details=False, invalidate_cache=True, **kwargs):
        """For a template path, return matching files"""
        adlpaths = self._trim_filename(path)
        filepaths = self.azure_fs.glob(
            adlpaths, details=details, invalidate_cache=invalidate_cache
        )
        return filepaths

    def isdir(self, path, **kwargs):
        """Is this entry directory-like?"""
        try:
            return self.info(path)["type"].lower() == "directory"
        except FileNotFoundError:
            return False

    def isfile(self, path, **kwargs):
        """Is this entry file-like?"""
        try:
            return self.azure_fs.info(path)["type"].lower() == "file"
        except Exception:
            return False

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options: dict = {},
        **kwargs,
    ):
        return AzureDatalakeFile(self, path, mode=mode)

    def read_block(self, fn, offset, length, delimiter=None, **kwargs):
        return self.azure_fs.read_block(fn, offset, length, delimiter)

    def ukey(self, path):
        return tokenize(self.info(path)["modificationTime"])

    def size(self, path):
        return self.info(path)["length"]

    def __getstate__(self):
        dic = self.__dict__.copy()
        logger.debug("Serialize with state: %s", dic)
        return dic

    def __setstate__(self, state):
        logger.debug("De-serialize with state: %s", state)
        self.__dict__.update(state)
        self.do_connect()


class AzureDatalakeFile(AzureDLFile):
    # TODO: refoctor this. I suspect we actually want to compose an
    # AbstractBufferedFile with an AzureDLFile.

    def __init__(
        self,
        fs,
        path,
        mode="rb",
        autocommit=True,
        block_size=2 ** 25,
        cache_type="bytes",
        cache_options=None,
        *,
        delimiter=None,
        **kwargs,
    ):
        super().__init__(
            azure=fs.azure_fs,
            path=AzureDLPath(path),
            mode=mode,
            blocksize=block_size,
            delimiter=delimiter,
        )
        self.fs = fs
        self.path = AzureDLPath(path)
        self.mode = mode

    def seek(self, loc: int, whence: int = 0, **kwargs):
        """Set current file location

        Parameters
        ----------
        loc: int
            byte location

        whence: {0, 1, 2}
            from start of file, current location or end of file, resp.
        """
        loc = int(loc)
        if not self.mode == "rb":
            raise ValueError("Seek only available in read mode")
        if whence == 0:
            nloc = loc
        elif whence == 1:
            nloc = self.loc + loc
        elif whence == 2:
            nloc = self.size + loc
        else:
            raise ValueError("invalid whence (%s, should be 0, 1 or 2)" % whence)
        if nloc < 0:
            raise ValueError("Seek before start of file")
        self.loc = nloc
        return self.loc


class AzureBlobFileSystem(AsyncFileSystem):
    """
    Access Azure Datalake Gen2 and Azure Storage if it were a file system using Multiprotocol Access

    Parameters
    ----------
    account_name: str
        The storage account name. This is used to authenticate requests
        signed with an account key and to construct the storage endpoint. It
        is required unless a connection string is given, or if a custom
        domain is used with anonymous authentication.
    account_key: str
        The storage account key. This is used for shared key authentication.
        If any of account key, sas token or client_id is specified, anonymous access
        will be used.
    sas_token: str
        A shared access signature token to use to authenticate requests
        instead of the account key. If account key and sas token are both
        specified, account key will be used to sign. If any of account key, sas token
        or client_id are specified, anonymous access will be used.
    request_session: Session
        The session object to use for http requests.
    connection_string: str
        If specified, this will override all other parameters besides
        request session. See
        http://azure.microsoft.com/en-us/documentation/articles/storage-configure-connection-string/
        for the connection string format.
    socket_timeout: int
        If specified, this will override the default socket timeout. The timeout specified is in
        seconds.
        See DEFAULT_SOCKET_TIMEOUT in _constants.py for the default value.
    credential: TokenCredential or SAS token
        The credentials with which to authenticate.  Optional if the account URL already has a SAS token.
        Can include an instance of TokenCredential class from azure.identity
    blocksize: int
        The block size to use for download/upload operations. Defaults to the value of
        ``BlockBlobService.MAX_BLOCK_SIZE``
    client_id: str
        Client ID to use when authenticating using an AD Service Principal client/secret.
    client_secret: str
        Client secret to use when authenticating using an AD Service Principal client/secret.
    tenant_id: str
        Tenant ID to use when authenticating using an AD Service Principal client/secret.
    default_fill_cache: bool = True
        Whether to use cache filling with opoen by default
    default_cache_type: string ('bytes')
        If given, the default cache_type value used for "open()".  Set to none if no caching
        is desired.  Docs in fsspec

    Pass on to fsspec:

    skip_instance_cache:  to control reuse of instances
    use_listings_cache, listings_expiry_time, max_paths: to control reuse of directory listings

    Examples
    --------
    Authentication with an account_key
    >>> abfs = AzureBlobFileSystem(account_name="XXXX", account_key="XXXX", container_name="XXXX")
    >>> abfs.ls('')

    **  Sharded Parquet & csv files can be read as: **
        ------------------------------------------
        ddf = dd.read_csv('abfs://container_name/folder/*.csv', storage_options={
        ...    'account_name': ACCOUNT_NAME, 'account_key': ACCOUNT_KEY})

        ddf = dd.read_parquet('abfs://container_name/folder.parquet', storage_options={
        ...    'account_name': ACCOUNT_NAME, 'account_key': ACCOUNT_KEY,})

    Authentication with an Azure ServicePrincipal
    >>> abfs = AzureBlobFileSystem(account_name="XXXX", tenant_id=TENANT_ID,
        ...    client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
    >>> abfs.ls('')

    Authentication with environment based identity which can be  EnvironmentCredential, ManagedIdentityCredential,
    SharedTokenCache(Windows only), AzureCLI identity
    >>> abfs = AzureBlobFileSystem(account_name="XXXX", tenant_id=TENANT_ID)


    **  Read files as: **
        -------------
        ddf = dd.read_csv('abfs://container_name/folder/*.csv', storage_options={
            'account_name': ACCOUNT_NAME, 'tenant_id': TENANT_ID, 'client_id': CLIENT_ID,
            'client_secret': CLIENT_SECRET})
        })
    """

    protocol = "abfs"

    def __init__(
        self,
        account_name: str = None,
        account_key: str = None,
        connection_string: str = None,
        credential: str = None,
        sas_token: str = None,
        request_session=None,
        socket_timeout: int = None,
        blocksize: int = create_configuration(storage_sdk="blob").max_block_size,
        client_id: str = None,
        client_secret: str = None,
        tenant_id: str = None,
        location_mode: str = "primary",
        loop=None,
        asynchronous: bool = False,
        default_fill_cache: bool = True,
        default_cache_type: str = "bytes",
        **kwargs,
    ):
        super_kwargs = {
            k: kwargs.pop(k)
            for k in ["use_listings_cache", "listings_expiry_time", "max_paths"]
            if k in kwargs
        }  # pass on to fsspec superclass
        super().__init__(asynchronous=asynchronous, loop=loop, **super_kwargs)
        self.account_name = account_name or os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
        self.account_key = account_key or os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
        self.connection_string = connection_string or os.getenv(
            "AZURE_STORAGE_CONNECTION_STRING"
        )
        self.sas_token = sas_token or os.getenv("AZURE_STORAGE_SAS_TOKEN")
        self.client_id = client_id or os.getenv("AZURE_STORAGE_CLIENT_ID")
        self.client_secret = client_secret or os.getenv("AZURE_STORAGE_CLIENT_SECRET")
        self.tenant_id = tenant_id or os.getenv("AZURE_STORAGE_TENANT_ID")
        self.location_mode = location_mode
        self.credential = credential
        self.request_session = request_session
        self.socket_timeout = socket_timeout
        self.blocksize = blocksize
        self.default_fill_cache = default_fill_cache
        self.default_cache_type = default_cache_type
        if (
            self.credential is None
            and self.account_key is None
            and self.sas_token is None
        ):
            if self.client_id is not None:
                (
                    self.credential,
                    self.sync_credential,
                ) = self._get_credential_from_service_principal()
            else:
                (
                    self.credential,
                    self.sync_credential,
                ) = self._get_default_credential()
        else:
            self.sync_credential = None
        self.do_connect()
        weakref.finalize(self, sync, self.loop, close_service_client, self)

    @classmethod
    def _strip_protocol(cls, path: str):
        """
        Remove the protocol from the input path

        Parameters
        ----------
        path: str
            Path to remove the protocol from

        Returns
        -------
        str
            Returns a path without the protocol
        """
        logger.debug(f"_strip_protocol for {path}")
        ops = infer_storage_options(path)

        # we need to make sure that the path retains
        # the format {host}/{path}
        # here host is the container_name
        if ops.get("host", None):
            ops["path"] = ops["host"] + ops["path"]
        ops["path"] = ops["path"].lstrip("/")

        logger.debug(f"_strip_protocol({path}) = {ops}")
        return ops["path"]

    def _get_credential_from_service_principal(self):
        """
        Create a Credential for authentication.  This can include a TokenCredential
        client_id, client_secret and tenant_id

        Returns
        -------
        Tuple of (Async Credential, Sync Credential).
        """
        from azure.identity import ClientSecretCredential
        from azure.identity.aio import (
            ClientSecretCredential as AIOClientSecretCredential,
        )

        async_credential = AIOClientSecretCredential(
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret,
        )

        sync_credential = ClientSecretCredential(
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret,
        )

        return (async_credential, sync_credential)

    def _get_default_credential(self):
        """
        Use azure identity default credential resolver method. This is useful both for local development
        (eg. to use VS Code token cache) and on Azure by using managed identity.
        https://docs.microsoft.com/en-us/python/api/azure-identity/azure.identity.defaultazurecredential

        Returns
        -------
        Tuple of (Async Credential, Sync Credential).
        """
        from azure.identity import DefaultAzureCredential
        from azure.identity.aio import (
            DefaultAzureCredential as AIODefaultAzureCredential,
        )

        return (AIODefaultAzureCredential(), DefaultAzureCredential())

    def do_connect(self):
        """Connect to the BlobServiceClient, using user-specified connection details.
        Tries credentials first, then connection string and finally account key.

        Raises
        ------
        ValueError if none of the connection details are available
        """
        try:
            if self.connection_string is not None:
                self.service_client = AIOBlobServiceClient.from_connection_string(
                    conn_str=self.connection_string
                )
            elif self.account_name:
                self.account_url: str = f"https://{self.account_name}.blob.core.windows.net"
                creds = [self.credential, self.account_key]
                if any(creds):
                    self.service_client = [
                        AIOBlobServiceClient(
                            account_url=self.account_url,
                            credential=cred,
                            _location_mode=self.location_mode,
                        )
                        for cred in creds
                        if cred is not None
                    ][0]
                elif self.sas_token is not None:
                    if not self.sas_token.startswith("?"):
                        self.sas_token = f"?{self.sas_token}"
                    self.service_client = AIOBlobServiceClient(
                        account_url=self.account_url + self.sas_token,
                        credential=None,
                        _location_mode=self.location_mode,
                    )
                else:
                    self.service_client = AIOBlobServiceClient(
                        account_url=self.account_url
                    )
            else:
                raise ValueError(
                    "Must provide either a connection_string or account_name with credentials!!"
                )

        except Exception as e:
            raise ValueError(f"unable to connect to account for {e}")

    def split_path(self, path, delimiter="/", return_container: bool = False, **kwargs):
        """
        Normalize ABFS path string into bucket and key.

        Parameters
        ----------
        path : string
            Input path, like `abfs://my_container/path/to/file`

        delimiter: string
            Delimiter used to split the path

        return_container: bool

        Examples
        --------
        >>> split_path("abfs://my_container/path/to/file")
        ['my_container', 'path/to/file']
        """

        if path in ["", delimiter]:
            return "", ""

        path = self._strip_protocol(path)
        path = path.lstrip(delimiter)
        if "/" not in path:
            # this means path is the container_name
            return path, ""
        else:
            return path.split(delimiter, 1)

    def info(self, path, refresh=False, **kwargs):
        try:
            fetch_from_azure = (path and self._ls_from_cache(path) is None) or refresh
        except Exception:
            fetch_from_azure = True
        if fetch_from_azure:
            return sync(self.loop, self._info, path, refresh)
        return super().info(path)

    async def _info(self, path, refresh=False, **kwargs):
        """Give details of entry at path
        Returns a single dictionary, with exactly the same information as ``ls``
        would with ``detail=True``.
        The default implementation should calls ls and could be overridden by a
        shortcut. kwargs are passed on to ```ls()``.
        Some file systems might not be able to measure the file's size, in
        which case, the returned dict will include ``'size': None``.
        Returns
        -------
        dict with keys: name (full path in the FS), size (in bytes), type (file,
        directory, or something else) and other FS-specific keys.
        """
        if refresh:
            invalidate_cache = True
        else:
            invalidate_cache = False
        path = self._strip_protocol(path)
        out = await self._ls(
            self._parent(path), invalidate_cache=invalidate_cache, **kwargs
        )
        out = [o for o in out if o["name"].rstrip("/") == path]
        if out:
            return out[0]
        out = await self._ls(path, invalidate_cache=invalidate_cache, **kwargs)
        path = path.rstrip("/")
        out1 = [o for o in out if o["name"].rstrip("/") == path]
        if len(out1) == 1:
            if "size" not in out1[0]:
                out1[0]["size"] = None
            return out1[0]
        elif len(out1) > 1 or out:
            return {"name": path, "size": None, "type": "directory"}
        else:
            raise FileNotFoundError

    def glob(self, path, **kwargs):
        return sync(self.loop, self._glob, path)

    async def _glob(self, path, **kwargs):
        """
        Find files by glob-matching.
        If the path ends with '/' and does not contain "*", it is essentially
        the same as ``ls(path)``, returning only files.
        We support ``"**"``,
        ``"?"`` and ``"[..]"``.
        kwargs are passed to ``ls``.
        """
        import re

        ends = path.endswith("/")
        path = self._strip_protocol(path)
        indstar = path.find("*") if path.find("*") >= 0 else len(path)
        indques = path.find("?") if path.find("?") >= 0 else len(path)
        indbrace = path.find("[") if path.find("[") >= 0 else len(path)

        ind = min(indstar, indques, indbrace)

        detail = kwargs.pop("detail", False)

        if not has_magic(path):
            root = path
            depth = 1
            if ends:
                path += "/*"
            elif await self._exists(path):
                if not detail:
                    return [path]
                else:
                    return {path: await self._info(path)}
            else:
                if not detail:
                    return []  # glob of non-existent returns empty
                else:
                    return {}
        elif "/" in path[:ind]:
            ind2 = path[:ind].rindex("/")
            root = path[: ind2 + 1]
            depth = 20 if "**" in path else path[ind2 + 1 :].count("/") + 1
        else:
            root = ""
            depth = 20 if "**" in path else 1

        allpaths = await self._glob_find(
            root, maxdepth=depth, withdirs=True, detail=True, **kwargs
        )
        pattern = (
            "^"
            + (
                path.replace("\\", r"\\")
                .replace(".", r"\.")
                .replace("+", r"\+")
                .replace("//", "/")
                .replace("(", r"\(")
                .replace(")", r"\)")
                .replace("|", r"\|")
                .rstrip("/")
                .replace("?", ".")
            )
            + "$"
        )
        pattern = re.sub("[*]{2}", "=PLACEHOLDER=", pattern)
        pattern = re.sub("[*]", "[^/]*", pattern)
        pattern = re.compile(pattern.replace("=PLACEHOLDER=", ".*"))
        out = {
            p: allpaths[p]
            for p in sorted(allpaths)
            if pattern.match(p.replace("//", "/").rstrip("/"))
        }
        if detail:
            return out
        else:
            return list(out)

    def ls(
        self,
        path: str,
        detail: bool = False,
        invalidate_cache: bool = False,
        delimiter: str = "/",
        return_glob: bool = False,
        **kwargs,
    ):

        files = sync(
            self.loop,
            self._ls,
            path=path,
            invalidate_cache=invalidate_cache,
            delimiter=delimiter,
            return_glob=return_glob,
        )
        if detail:
            return files
        else:
            return list(sorted(set([f["name"] for f in files])))

    async def _ls(
        self,
        path: str,
        invalidate_cache: bool = False,
        delimiter: str = "/",
        return_glob: bool = False,
        **kwargs,
    ):
        """
        Create a list of blob names from a blob container

        Parameters
        ----------
        path: str
            Path to an Azure Blob with its container name

        detail: bool
            If False, return a list of blob names, else a list of dictionaries with blob details

        invalidate_cache:  bool
            If True, do not use the cache

        delimiter: str
            Delimiter used to split paths

        return_glob: bool

        """
        logger.debug(f"abfs.ls() is searching for {path}")
        target_path = path.strip("/")
        container, path = self.split_path(path)

        if invalidate_cache:
            self.dircache.clear()
        if (container in ["", ".", "*", delimiter]) and (path in ["", delimiter]):
            if _ROOT_PATH not in self.dircache or invalidate_cache or return_glob:
                # This is the case where only the containers are being returned
                logger.info(
                    "Returning a list of containers in the azure blob storage account"
                )
                contents = self.service_client.list_containers(include_metadata=True)
                containers = [c async for c in contents]
                files = await self._details(containers)
                self.dircache[_ROOT_PATH] = files

            return self.dircache[_ROOT_PATH]
        else:
            if target_path not in self.dircache or invalidate_cache or return_glob:
                if container not in ["", delimiter]:
                    # This is the case where the container name is passed
                    async with self.service_client.get_container_client(
                        container=container
                    ) as cc:
                        path = path.strip("/")
                        blobs = cc.walk_blobs(
                            include=["metadata"], name_starts_with=path
                        )

                    # Check the depth that needs to be screened
                    depth = target_path.count("/")
                    outblobs = []
                    try:
                        async for next_blob in blobs:
                            if depth in [0, 1] and path == "":
                                outblobs.append(next_blob)
                            elif isinstance(next_blob, BlobProperties):
                                if next_blob["name"].count("/") == depth:
                                    outblobs.append(next_blob)
                                elif not next_blob["name"].endswith("/") and (
                                    next_blob["name"].count("/") == (depth - 1)
                                ):
                                    outblobs.append(next_blob)
                            else:
                                async for blob_ in next_blob:
                                    if isinstance(blob_, BlobProperties) or isinstance(
                                        blob_, BlobPrefix
                                    ):
                                        if blob_["name"].endswith("/"):
                                            if (
                                                blob_["name"].rstrip("/").count("/")
                                                == depth
                                            ):
                                                outblobs.append(blob_)
                                            elif (
                                                blob_["name"].count("/") == depth
                                                and blob_["size"] == 0
                                            ):
                                                outblobs.append(blob_)
                                            else:
                                                pass
                                        elif blob_["name"].count("/") == (depth):
                                            outblobs.append(blob_)
                                        else:
                                            pass
                    except ResourceNotFoundError:
                        raise FileNotFoundError
                    finalblobs = await self._details(
                        outblobs, target_path=target_path, return_glob=return_glob
                    )
                    if return_glob:
                        return finalblobs
                    finalblobs = await self._details(outblobs, target_path=target_path)
                    if not finalblobs:
                        if not await self._exists(target_path):
                            raise FileNotFoundError
                        return []
                    self.dircache[target_path] = finalblobs

            return self.dircache[target_path]

    async def _details(
        self,
        contents,
        delimiter="/",
        return_glob: bool = False,
        target_path="",
        **kwargs,
    ):
        """
        Return a list of dictionaries of specifying details about the contents

        Parameters
        ----------
        contents

        delimiter: str
            Delimiter used to separate containers and files

        return_glob: bool

        Returns
        -------
        List of dicts
            Returns details about the contents, such as name, size and type
        """
        output = []
        for content in contents:
            data = {
                key: content[key]
                for key in FORWARDED_BLOB_PROPERTIES
                if content.has_key(key)  # NOQA
            }
            if content.has_key("container"):  # NOQA
                fname = f"{content.container}{delimiter}{content.name}"
                fname = fname.rstrip(delimiter)
                if content.has_key("size"):  # NOQA
                    data.update({"name": fname})
                    data.update({"size": content.size})
                    data.update({"type": "file"})
                else:
                    data.update({"name": fname})
                    data.update({"size": None})
                    data.update({"type": "directory"})
            else:
                fname = f"{content.name}"
                data.update({"name": fname})
                data.update({"size": None})
                data.update({"type": "directory"})
            if "metadata" in data.keys():
                if data["metadata"] is not None:
                    if (
                        "is_directory" in data["metadata"].keys()
                        and data["metadata"]["is_directory"] == "true"
                    ):
                        data.update({"type": "directory"})
                        data.update({"size": None})
                    elif (
                        "is_directory" in data["metadata"].keys()
                        and data["metadata"]["is_directory"] == "false"
                    ):
                        data.update({"type": "file"})
                    else:
                        pass
            if return_glob:
                data.update({"name": data["name"].rstrip("/")})
            output.append(data)
        if target_path:
            if len(output) == 1 and output[0]["type"] == "file":
                # This handles the case where path is a file passed to ls
                return output
            output = await filter_blobs(output, target_path)

        return output

    def find(self, path, withdirs=False, prefix="", **kwargs):
        return sync(
            self.loop, self._find, path=path, withdirs=withdirs, prefix=prefix, **kwargs
        )

    async def _find(self, path, withdirs=False, prefix="", with_parent=False, **kwargs):
        """List all files below path.
        Like posix ``find`` command without conditions
        Parameters
        ----------
        path : str
            The path (directory) to list from
        withdirs: bool
            Whether to include directory paths in the output. This is True
            when used by glob, but users usually only want files.
        prefix: str
            Only return files that match `^{path}/{prefix}`
        kwargs are passed to ``ls``.
        """
        path = self._strip_protocol(path)
        parent_path = path.strip("/") + "/"
        target_path = f"{parent_path}{(prefix or '').lstrip('/')}"
        container, path = self.split_path(target_path)

        async with self.service_client.get_container_client(
            container=container
        ) as container_client:
            blobs = container_client.list_blobs(
                include=["metadata"], name_starts_with=path
            )
        files = {}
        dir_set = set()
        dirs = {}
        detail = kwargs.pop("detail", False)
        try:
            infos = await self._details([b async for b in blobs])
            for info in infos:
                name = info["name"]
                if name == target_path:
                    continue
                parent_dir = self._parent(name).rstrip("/") + "/"
                if parent_dir not in dir_set and parent_dir != parent_path.strip("/"):
                    dir_set.add(parent_dir)
                    dirs[parent_dir] = {
                        "name": parent_dir,
                        "type": "directory",
                        "size": 0,
                    }
                if info["type"] == "directory":
                    dirs[name] = info
                if info["type"] == "file":
                    files[name] = info

        except ResourceNotFoundError:
            # find doesn't raise but returns [] or {} instead
            pass
        if withdirs:
            if not with_parent:
                dirs.pop(target_path, None)
            files.update(dirs)
        names = sorted(files)
        if not detail:
            return names
        return {name: files[name] for name in names}

    async def _glob_find(self, path, maxdepth=None, withdirs=False, **kwargs):
        """List all files below path in a recusrsive manner.
        Like posix ``find`` command without conditions
        Parameters
        ----------
        path : str
        maxdepth: int or None
            If not None, the maximum number of levels to descend
        withdirs: bool
            Whether to include directory paths in the output. This is True
            when used by glob, but users usually only want files.
        kwargs are passed to ``ls``.
        """
        # TODO: allow equivalent of -name parameter
        path = self._strip_protocol(path)
        out = dict()
        detail = kwargs.pop("detail", False)
        async for path, dirs, files in self._async_walk(
            path, maxdepth, detail=True, **kwargs
        ):
            if files == []:
                files = {}
                dirs = {}
            if withdirs:
                files.update(dirs)
            out.update({info["name"]: info for name, info in files.items()})
        if await self._isfile(path) and path not in out:
            # walk works on directories, but find should also return [path]
            # when path happens to be a file
            out[path] = {}
        names = sorted(out)
        if not detail:
            return names
        else:
            return {name: out[name] for name in names}

    def _walk(self, path, dirs, files):
        for p, d, f in zip([path], [dirs], [files]):
            yield p, d, f

    async def _async_walk(self, path: str, maxdepth=None, **kwargs):
        """Return all files belows path

        List all files, recursing into subdirectories; output is iterator-style,
        like ``os.walk()``. For a simple list of files, ``find()`` is available.

        Note that the "files" outputted will include anything that is not
        a directory, such as links.

        Parameters
        ----------
        path: str
            Root to recurse into

        maxdepth: int
            Maximum recursion depth. None means limitless, but not recommended
            on link-based file-systems.

        **kwargs are passed to ``ls``
        """
        path = self._strip_protocol(path)
        full_dirs = {}
        dirs = {}
        files = {}

        detail = kwargs.pop("detail", False)
        try:
            listing = await self._ls(path, return_glob=True, **kwargs)
        except (FileNotFoundError, IOError):
            listing = []

        for info in listing:
            # each info name must be at least [path]/part , but here
            # we check also for names like [path]/part/
            pathname = info["name"].rstrip("/")
            name = pathname.rsplit("/", 1)[-1]
            if info["type"] == "directory" and pathname != path:
                # do not include "self" path
                full_dirs[pathname] = info
                dirs[name] = info
            elif pathname == path:
                # file-like with same name as give path
                files[""] = info
            else:
                files[name] = info

        if detail:
            for p, d, f in self._walk(path, dirs, files):
                yield p, d, f
        else:
            yield path, list(dirs), list(files)

        if maxdepth is not None:
            maxdepth -= 1
            if maxdepth < 1:
                return

        for d in full_dirs:
            async for path, dirs, files in self._async_walk(
                d, maxdepth=maxdepth, detail=detail, **kwargs
            ):
                yield path, dirs, files

    async def _container_exists(self, container_name):
        try:
            async with self.service_client.get_container_client(
                container_name
            ) as client:
                await client.get_container_properties()
        except ResourceNotFoundError:
            return False
        else:
            return True

    async def _mkdir(self, path, create_parents=True, delimiter="/", **kwargs):
        """
        Mkdir is a no-op for creating anything except top-level containers.
        This aligns to the Azure Blob Filesystem flat hierarchy

        Parameters
        ----------
        path: str
            The path to create

        create_parents: bool
            If True (default), create the Azure Container if it does not exist

        delimiter: str
            Delimiter to use when splitting the path

        """
        fullpath = path
        container_name, path = self.split_path(path, delimiter=delimiter)
        container_exists = await self._container_exists(container_name)
        if not create_parents and not container_exists:
            raise PermissionError(
                "Azure Container does not exist.  Set create_parents=True to create!!"
            )

        if container_exists and not kwargs.get("exist_ok", True):
            raise FileExistsError(
                f"Cannot overwrite existing Azure container -- {container_name} already exists."
            )

        if not container_exists:
            await self.service_client.create_container(container_name)
            self.invalidate_cache(_ROOT_PATH)

        self.invalidate_cache(self._parent(fullpath))

    mkdir = sync_wrapper(_mkdir)

    def makedir(self, path, exist_ok=False):
        """
        Create directory entry at path

        Parameters
        ----------
        path: str
            The path to create

        delimiter: str
            Delimiter to use when splitting the path

        exist_ok: bool
            If False (default), raise an error if the directory already exists.
        """
        try:
            self.mkdir(path, create_parents=True, exist_ok=exist_ok)
        except FileExistsError:
            if exist_ok:
                pass
            else:
                raise

    async def _rm(self, path, recursive=False, maxdepth=None, **kwargs):
        """Delete files.
        Parameters
        ----------
        path: str or list of str
            File(s) to delete.
        recursive: bool
            If file(s) are directories, recursively delete contents and then
            also remove the directory
        maxdepth: int or None
            Depth to pass to walk for finding files to delete, if recursive.
            If None, there will be no limit and infinite recursion may be
            possible.
        """
        path = await self._expand_path(
            path, recursive=recursive, maxdepth=maxdepth, with_parent=True
        )
        for p in reversed(path):
            await self._rm_file(p)
        self.invalidate_cache()

    rm = sync_wrapper(_rm)

    async def _rm_file(self, path, delimiter="/", **kwargs):
        """
        Delete a given file

        Parameters
        ----------
        path: str
            Path to file to delete

        delimiter: str
            Delimiter to use when splitting the path
        """
        try:
            kind = await self._info(path)
            container_name, path = self.split_path(path, delimiter=delimiter)
            kind = kind["type"]
            if path != "":
                async with self.service_client.get_container_client(
                    container=container_name
                ) as cc:
                    await cc.delete_blob(path.rstrip(delimiter))
            elif kind == "directory":
                await self._rmdir(container_name)
            else:
                raise RuntimeError(f"Unable to remove {path}")
        except ResourceNotFoundError:
            pass
        except FileNotFoundError:
            pass
        except Exception as e:
            raise RuntimeError(f"Failed to remove {path} for {e}")

        self.invalidate_cache(self._parent(path))

    sync_wrapper(_rm_file)

    def rmdir(self, path: str, delimiter="/", **kwargs):
        sync(self.loop, self._rmdir, path, delimiter=delimiter, **kwargs)

    async def _rmdir(self, path: str, delimiter="/", **kwargs):
        """
        Remove a directory, if empty

        Parameters
        ----------
        path: str
            Path of directory to remove

        delimiter: str
            Delimiter to use when splitting the path

        """

        container_name, path = self.split_path(path, delimiter=delimiter)
        container_exists = await self._container_exists(container_name)
        if container_exists and not path:
            await self.service_client.delete_container(container_name)
            self.invalidate_cache(_ROOT_PATH)

    def size(self, path):
        return sync(self.loop, self._size, path)

    async def _size(self, path):
        """Size in bytes of file"""
        res = await self._info(path)
        size = res.get("size", None)
        return size

    def isfile(self, path):
        return sync(self.loop, self._isfile, path)

    async def _isfile(self, path):
        """Is this entry file-like?"""
        try:
            path_ = path.split("/")[:-1]
            path_ = "/".join([p for p in path_])
            if self.dircache[path_]:
                for fp in self.dircache[path_]:
                    if fp["name"] == path and fp["type"] == "file":
                        return True
        except KeyError:
            pass
        try:
            info = await self._info(path)
            return info["type"] == "file"
        except:  # noqa: E722
            return False

    def isdir(self, path):
        return sync(self.loop, self._isdir, path)

    async def _isdir(self, path):
        """Is this entry directory-like?"""

        if path in self.dircache:
            for fp in self.dircache[path]:
                # Files will contain themselves in the cache, but
                # a directory can not contain itself
                if fp["name"] != path:
                    return True
        try:
            info = await self._info(path)
            return info["type"] == "directory"
        except IOError:
            return False

    def exists(self, path):
        return sync(self.loop, self._exists, path)

    async def _exists(self, path):
        """Is there a file at the given path"""
        try:
            if self._ls_from_cache(path):
                return True
        except FileNotFoundError:
            pass
        except KeyError:
            pass

        container_name, path = self.split_path(path)

        if not path:
            if container_name:
                return await self._container_exists(container_name)
            else:
                # Empty paths exist by definition
                return True

        async with self.service_client.get_blob_client(container_name, path) as bc:
            exists = await bc.exists()
        return exists

    async def _pipe_file(self, path, value, overwrite=True, **kwargs):
        """Set the bytes of given file"""
        container_name, path = self.split_path(path)
        async with self.service_client.get_blob_client(
            container=container_name, blob=path
        ) as bc:
            result = await bc.upload_blob(
                data=value, overwrite=overwrite, metadata={"is_directory": "false"}
            )
        self.invalidate_cache(self._parent(path))
        return result

    pipe_file = sync_wrapper(_pipe_file)

    def cat(self, path, recursive=False, on_error="raise", **kwargs):
        """Fetch (potentially multiple) paths' contents
        Returns a dict of {path: contents} if there are multiple paths
        or the path has been otherwise expanded
        on_error : "raise", "omit", "return"
            If raise, an underlying exception will be raised (converted to KeyError
            if the type is in self.missing_exceptions); if omit, keys with exception
            will simply not be included in the output; if "return", all keys are
            included in the output, but the value will be bytes or an exception
            instance.
        """
        paths = self.expand_path(path, recursive=recursive)
        if (
            len(paths) > 1
            or isinstance(path, list)
            or paths[0] != self._strip_protocol(path)
        ):
            out = {}
            for path in paths:
                try:
                    out[path] = self.cat_file(path, **kwargs)
                except Exception as e:
                    if on_error == "raise":
                        raise
                    if on_error == "return":
                        out[path] = e
            return out
        else:
            return self.cat_file(paths[0])

    def url(self, path, expires=3600, **kwargs):
        return sync(self.loop, self._url, path, expires, **kwargs)

    async def _url(self, path, expires=3600, **kwargs):
        """Generate presigned URL to access path by HTTP

        Parameters
        ----------
        path : string
            the key path we are interested in
        expires : int
            the number of seconds this signature will be good for.
        """
        container_name, blob = self.split_path(path)

        sas_token = generate_blob_sas(
            account_name=self.account_name,
            container_name=container_name,
            blob_name=blob,
            account_key=self.account_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.utcnow() + timedelta(seconds=expires),
        )

        async with self.service_client.get_blob_client(container_name, blob) as bc:
            url = f"{bc.url}?{sas_token}"
        return url

    def expand_path(self, path, recursive=False, maxdepth=None):
        return sync(self.loop, self._expand_path, path, recursive, maxdepth)

    async def _expand_path(self, path, recursive=False, maxdepth=None, **kwargs):
        """Turn one or more globs or directories into a list of all matching files"""

        with_parent = kwargs.get(
            "with_parent", False
        )  # Sets whether to return the parent dir

        if isinstance(path, list):
            path = [f"{p.strip('/')}" for p in path if not p.endswith("*")]
        else:
            if not path.endswith("*"):
                path = f"{path.strip('/')}"
        if isinstance(path, str):
            out = await self._expand_path(
                [path], recursive, maxdepth, with_parent=with_parent
            )
        else:
            out = set()
            path = [self._strip_protocol(p) for p in path]
            for p in path:
                if has_magic(p):
                    bit = set(await self._glob(p))
                    out |= bit
                    if recursive:
                        bit2 = set(await self._expand_path(p))
                        out |= bit2
                    continue
                elif recursive:
                    rec = set(
                        await self._find(p, withdirs=True, with_parent=with_parent)
                    )
                    out |= rec

                if p not in out and (
                    recursive is False
                    or await self._exists(p)
                    or await self._exists(p.rstrip("/"))
                ):
                    if not await self._exists(p):
                        # This is to verify that we don't miss files
                        p = p.rstrip("/")
                        if not await self._exists(p):
                            continue
                    out.add(p)

        if not out:
            raise FileNotFoundError
        return list(sorted(out))

    async def _put_file(self, lpath, rpath, delimiter="/", overwrite=False, **kwargws):
        """
        Copy single file to remote

        :param lpath: Path to local file
        :param rpath: Path to remote file
        :param delimitier: Filepath delimiter
        :param overwrite: Boolean (False).  If True, overwrite the existing file present
        """

        container_name, path = self.split_path(rpath, delimiter=delimiter)

        if os.path.isdir(lpath):
            self.makedirs(rpath, exist_ok=True)
        else:
            try:
                with open(lpath, "rb") as f1:
                    async with self.service_client.get_blob_client(
                        container_name, path
                    ) as bc:
                        await bc.upload_blob(
                            f1, overwrite=overwrite, metadata={"is_directory": "false"}
                        )
                self.invalidate_cache()
            except ResourceExistsError:
                raise FileExistsError("File already exists!")
            except ResourceNotFoundError:
                if not await self._exists(container_name):
                    raise FileNotFoundError("Container does not exist.")
                await self._put_file(lpath, rpath, delimiter, overwrite)
                self.invalidate_cache()

    put_file = sync_wrapper(_put_file)

    async def _cp_file(self, path1, path2, **kwargs):
        """ Copy the file at path1 to path2 """
        container1, path1 = self.split_path(path1, delimiter="/")
        container2, path2 = self.split_path(path2, delimiter="/")

        cc1 = self.service_client.get_container_client(container1)
        blobclient1 = cc1.get_blob_client(blob=path1)
        if container1 == container2:
            blobclient2 = cc1.get_blob_client(blob=path2)
        else:
            cc2 = self.service_client.get_container_client(container2)
            blobclient2 = cc2.get_blob_client(blob=path2)
        await blobclient2.start_copy_from_url(blobclient1.url)
        self.invalidate_cache(container1)
        self.invalidate_cache(container2)

    cp_file = sync_wrapper(_cp_file)

    def upload(self, lpath, rpath, recursive=False, **kwargs):
        """Alias of :ref:`FilesystemSpec.put`."""
        return self.put(lpath, rpath, recursive=recursive, **kwargs)

    def download(self, rpath, lpath, recursive=False, **kwargs):
        """Alias of :ref:`FilesystemSpec.get`."""
        return self.get(rpath, lpath, recursive=recursive, **kwargs)

    async def _get_file(self, rpath, lpath, recursive=False, delimiter="/", **kwargs):
        """ Copy single file remote to local """
        files = await self._ls(rpath)
        files = [f["name"] for f in files]
        container_name, path = self.split_path(rpath, delimiter=delimiter)
        try:
            if await self._isdir(rpath):
                os.makedirs(lpath, exist_ok=True)
            else:
                async with self.service_client.get_blob_client(
                    container_name, path.rstrip(delimiter)
                ) as bc:
                    with open(lpath, "wb") as my_blob:
                        stream = await bc.download_blob()
                        data = await stream.readall()
                        my_blob.write(data)
        except Exception as e:
            raise FileNotFoundError(f"File not found for {e}")

    get_file = sync_wrapper(_get_file)

    def getxattr(self, path, attr):
        meta = self.info(path).get("metadata", {})
        return meta[attr]

    async def _setxattrs(self, rpath, **kwargs):
        container_name, path = self.split_path(rpath)
        try:
            async with self.service_client.get_blob_client(container_name, path) as bc:
                await bc.set_blob_metadata(metadata=kwargs)
            self.invalidate_cache(self._parent(rpath))
        except Exception as e:
            raise FileNotFoundError(f"File not found for {e}")

    setxattrs = sync_wrapper(_setxattrs)

    def invalidate_cache(self, path=None):
        if path is None:
            self.dircache.clear()
        else:
            self.dircache.pop(path, None)
        super(AzureBlobFileSystem, self).invalidate_cache(path)

    def _open(
        self,
        path: str,
        mode: str = "rb",
        block_size: int = None,
        autocommit: bool = True,
        cache_options: dict = {},
        cache_type="readahead",
        metadata=None,
        **kwargs,
    ):
        """Open a file on the datalake, or a block blob

        Parameters
        ----------
        path: str
            Path to file to open

        mode: str
            What mode to open the file in - defaults to "rb"

        block_size: int
            Size per block for multi-part downloads.

        autocommit: bool
            Whether or not to write to the destination directly

        cache_type: str
            One of "readahead", "none", "mmap", "bytes", defaults to "readahead"
            Caching policy in read mode.
            See the definitions here:
            https://filesystem-spec.readthedocs.io/en/latest/api.html#readbuffering
        """
        logger.debug(f"_open:  {path}")
        return AzureBlobFile(
            fs=self,
            path=path,
            mode=mode,
            block_size=block_size,
            autocommit=autocommit,
            cache_options=cache_options,
            cache_type=cache_type,
            metadata=metadata,
            **kwargs,
        )


class AzureBlobFile(AbstractBufferedFile):
    """ File-like operations on Azure Blobs """

    DEFAULT_BLOCK_SIZE = 5 * 2 ** 20

    def __init__(
        self,
        fs: AzureBlobFileSystem,
        path: str,
        mode: str = "rb",
        block_size="default",
        autocommit: bool = True,
        cache_type: str = "bytes",
        cache_options: dict = {},
        metadata=None,
        **kwargs,
    ):
        """
        Represents a file on AzureBlobStorage that implements buffered reading and writing

        Parameters
        ----------
        fs: AzureBlobFileSystem
            An instance of the filesystem

        path: str
            The location of the file on the filesystem

        mode: str
            What mode to open the file in. Defaults to "rb"

        block_size: int, str
            Buffer size for reading and writing. The string "default" will use the class
            default

        autocommit: bool
            Whether or not to write to the destination directly

        cache_type: str
            One of "readahead", "none", "mmap", "bytes", defaults to "readahead"
            Caching policy in read mode. See the definitions in ``core``.

        cache_options : dict
            Additional options passed to the constructor for the cache specified
            by `cache_type`.

        kwargs: dict
            Passed to AbstractBufferedFile
        """

        from fsspec.core import caches

        container_name, blob = fs.split_path(path)
        self.fs = fs
        self.path = path
        self.mode = mode
        self.container_name = container_name
        self.blob = blob
        self.block_size = block_size
        self.container_client = (
            fs.service_client.get_container_client(self.container_name)
            or self.connect_client()
        )
        self.blocksize = (
            self.DEFAULT_BLOCK_SIZE if block_size in ["default", None] else block_size
        )
        self.loc = 0
        self.autocommit = autocommit
        self.end = None
        self.start = None
        self.closed = False
        self.loop = self.fs.loop or get_loop()

        if cache_options is None:
            cache_options = {}

        if "trim" in kwargs:
            warnings.warn(
                "Passing 'trim' to control the cache behavior has been deprecated. "
                "Specify it within the 'cache_options' argument instead.",
                FutureWarning,
            )
            cache_options["trim"] = kwargs.pop("trim")
        self.metadata = None
        self.kwargs = kwargs

        if self.mode not in {"ab", "rb", "wb"}:
            raise NotImplementedError("File mode not supported")
        if self.mode == "rb":
            if not hasattr(self, "details"):
                self.details = self.fs.info(self.path)
            self.size = self.details["size"]
            self.cache = caches[cache_type](
                self.blocksize, self._fetch_range, self.size, **cache_options
            )
            self.metadata = sync(
                self.loop, get_blob_metadata, self.container_client, self.blob
            )

        else:
            self.metadata = metadata or {"is_directory": "false"}
            self.buffer = io.BytesIO()
            self.offset = None
            self.forced = False
            self.location = None

    def close(self):
        """Close file and azure client."""
        asyncio.run_coroutine_threadsafe(close_container_client(self), loop=self.loop)
        super().close()

    def connect_client(self):
        """Connect to the Asynchronous BlobServiceClient, using user-specified connection details.
        Tries credentials first, then connection string and finally account key

        Raises
        ------
        ValueError if none of the connection details are available
        """
        try:
            self.fs.account_url: str = (
                f"https://{self.fs.account_name}.blob.core.windows.net"
            )
            creds = [self.fs.sync_credential, self.fs.account_key, self.fs.credential]
            if any(creds):
                self.container_client = [
                    AIOBlobServiceClient(
                        account_url=self.fs.account_url,
                        credential=cred,
                        _location_mode=self.fs.location_mode,
                    ).get_container_client(self.container_name)
                    for cred in creds
                    if cred is not None
                ][0]
            elif self.fs.connection_string is not None:
                self.container_client = AIOBlobServiceClient.from_connection_string(
                    conn_str=self.fs.connection_string
                ).get_container_client(self.container_name)
            elif self.fs.sas_token is not None:
                self.container_client = AIOBlobServiceClient(
                    account_url=self.fs.account_url + self.fs.sas_token, credential=None
                ).get_container_client(self.container_name)
            else:
                self.container_client = AIOBlobServiceClient(
                    account_url=self.fs.account_url
                ).get_container_client(self.container_name)

        except Exception as e:
            raise ValueError(
                f"Unable to fetch container_client with provided params for {e}!!"
            ) from e

    async def _async_fetch_range(self, start: int, end: int, **kwargs):
        """
        Download a chunk of data specified by start and end

        Parameters
        ----------
        start: int
            Start byte position to download blob from
        end: int
            End byte position to download blob from
        """
        async with self.container_client:
            stream = await self.container_client.download_blob(
                blob=self.blob, offset=start, length=end
            )
            blob = await stream.readall()
        return blob

    _fetch_range = sync_wrapper(_async_fetch_range)

    async def _reinitiate_async_upload(self, **kwargs):
        pass

    async def _async_initiate_upload(self, **kwargs):
        """Prepare a remote file upload"""
        self._block_list = []
        if self.mode == "wb":
            try:
                await self.container_client.delete_blob(self.blob)
            except ResourceNotFoundError:
                pass
            else:
                await self._reinitiate_async_upload()

        elif self.mode == "ab":
            if not await self.fs._exists(self.path):
                async with self.container_client.get_blob_client(blob=self.blob) as bc:
                    await bc.create_append_blob(metadata=self.metadata)
        else:
            raise ValueError(
                "File operation modes other than wb are not yet supported for writing"
            )

    _initiate_upload = sync_wrapper(_async_initiate_upload)

    async def _async_upload_chunk(self, final: bool = False, **kwargs):
        """
        Write one part of a multi-block file upload

        Parameters
        ----------
        final: bool
            This is the last block, so should complete file, if
            self.autocommit is True.

        """
        data = self.buffer.getvalue()
        length = len(data)
        block_id = len(self._block_list)
        block_id = f"{block_id:07d}"
        if self.mode == "wb":
            try:
                async with self.container_client.get_blob_client(blob=self.blob) as bc:
                    await bc.stage_block(
                        block_id=block_id, data=data, length=length,
                    )
                self._block_list.append(block_id)

                if final:
                    block_list = [BlobBlock(_id) for _id in self._block_list]
                    async with self.container_client.get_blob_client(
                        blob=self.blob
                    ) as bc:
                        await bc.commit_block_list(
                            block_list=block_list, metadata=self.metadata
                        )
            except Exception as e:
                # This step handles the situation where data="" and length=0
                # which is throws an InvalidHeader error from Azure, so instead
                # of staging a block, we directly upload the empty blob
                # This isn't actually tested, since Azureite behaves differently.
                if block_id == "0000000" and length == 0 and final:
                    async with self.container_client.get_blob_client(
                        blob=self.blob
                    ) as bc:
                        await bc.upload_blob(data=data, metadata=self.metadata)
                elif length == 0 and final:
                    # just finalize
                    block_list = [BlobBlock(_id) for _id in self._block_list]
                    async with self.container_client.get_blob_client(
                        blob=self.blob
                    ) as bc:
                        await bc.commit_block_list(
                            block_list=block_list, metadata=self.metadata
                        )
                else:
                    raise RuntimeError(f"Failed to upload block{e}!") from e
        elif self.mode == "ab":
            async with self.container_client.get_blob_client(blob=self.blob) as bc:
                await bc.upload_blob(
                    data=data,
                    length=length,
                    blob_type=BlobType.AppendBlob,
                    metadata=self.metadata,
                )
        else:
            raise ValueError(
                "File operation modes other than wb or ab are not yet supported for upload_chunk"
            )

    _upload_chunk = sync_wrapper(_async_upload_chunk)

    def __del__(self):
        try:
            if not self.closed:
                self.close()
        except TypeError:
            pass
