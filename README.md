Filesystem interface to Azure-Datalake Gen1 and Gen2 Storage 
------------------------------------------------------------


[![PyPI version shields.io](https://img.shields.io/pypi/v/adlfs.svg)](https://pypi.python.org/pypi/adlfs/)
[![Latest conda-forge version](https://img.shields.io/conda/vn/conda-forge/adlfs?logo=conda-forge)](https://anaconda.org/conda-forge/aldfs)

Quickstart
----------

This package can be installed using:

`pip install adlfs`

or

`conda install -c conda-forge adlfs`

The `adl://` and `abfs://` protocols are included in fsspec's known_implementations registry 
in fsspec > 0.6.1, otherwise users must explicitly inform fsspec about the supported adlfs protocols.


To use the Gen1 filesystem:

```python
import dask.dataframe as dd

storage_options={'tenant_id': TENANT_ID, 'client_id': CLIENT_ID, 'client_secret': CLIENT_SECRET}

dd.read_csv('adl://{STORE_NAME}/{FOLDER}/*.csv', storage_options=storage_options)
```

To use the Gen2 filesystem you can use the protocol `abfs` or `az`:

```python
import dask.dataframe as dd

storage_options={'account_name': ACCOUNT_NAME, 'account_key': ACCOUNT_KEY}

ddf = dd.read_csv('abfs://{CONTAINER}/{FOLDER}/*.csv', storage_options=storage_options)
ddf = dd.read_parquet('az://{CONTAINER}/folder.parquet', storage_options=storage_options)

or optionally, if AZURE_STORAGE_ACCOUNT_NAME and an AZURE_STORAGE_<CREDENTIAL> is 
set as an environmental variable, then storage_options will be read from the environmental
variables. In case none of them is specified, it will fall back to the azure identity library [default authentication methods](https://docs.microsoft.com/en-us/python/api/azure-identity/azure.identity.defaultazurecredential)

```

To read from a public storage blob you are required to specify the `'account_name'`.
For example, you can access [NYC Taxi & Limousine Commission](https://azure.microsoft.com/en-us/services/open-datasets/catalog/nyc-taxi-limousine-commission-green-taxi-trip-records/) as:

```python
storage_options = {'account_name': 'azureopendatastorage'}
ddf = dd.read_parquet('az://nyctlc/green/puYear=2019/puMonth=*/*.parquet', storage_options=storage_options)
```


Details
-------
The package includes pythonic filesystem implementations for both 
Azure Datalake Gen1 and Azure Datalake Gen2, that facilitate 
interactions between both Azure Datalake implementations and Dask.  This is done leveraging the 
[intake/filesystem_spec](https://github.com/intake/filesystem_spec/tree/master/fsspec) base class and Azure Python SDKs.

Operations against both Gen1 Datalake currently only work with an Azure ServicePrincipal
with suitable credentials to perform operations on the resources of choice.

Operations against the Gen2 Datalake are implemented by leveraging [Azure Blob Storage Python SDK](https://github.com/Azure/azure-sdk-for-python).

    The filesystem can be instantiated with a variety of credentials, including:
        account_name
        account_key
        sas_token
        connection_string
        Azure ServicePrincipal credentials (which requires tenant_id, client_id, client_secret)
        location_mode:  valid value are "primary" or "secondary" and apply to RA-GRS accounts

    The following enviornmental variables can also be set and picked up for authentication:
        "AZURE_STORAGE_CONNECTION_STRING"
        "AZURE_STORAGE_ACCOUNT_NAME"
        "AZURE_STORAGE_ACCOUNT_KEY"
        "AZURE_STORAGE_SAS_TOKEN"
        "AZURE_STORAGE_CLIENT_SECRET"
        "AZURE_STORAGE_CLIENT_ID"
        "AZURE_STORAGE_TENANT_ID"


The AzureBlobFileSystem accepts [all of the Async BlobServiceClient arguments](https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python).

    By default, write operations create BlockBlobs in Azure, which, once written can not be appended.  It is possible to create an AppendBlob using an `mode="ab"` when creating, and then when operating on blobs.  Currently AppendBlobs are not available if hierarchical namespaces are enabled.
