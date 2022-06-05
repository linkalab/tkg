import os
import csv
from typing import Tuple
from dagster import root_input_manager
from dagster.builtins import String
from pandas import DataFrame
import pandas as pd

## default setting used if keys read_csv_options and write_csv_options are not specified in the asset configuratiion

default_read_csv_options =  {  
    "quoting": csv.QUOTE_MINIMAL,
    "escapechar": "\\",
    "doublequote": False,
    }

### unused read and write defaults should be the same!
# default_write_csv_options = {  
#     "quoting": csv.QUOTE_NONE,
#     "escapechar": "\\",
#     "doublequote": False,
#     "index": False
#     }

def get_fs_path_and_extension(asset_config: dict, root_path: str) -> Tuple[str, str, dict]:

    relative_path = asset_config["path"]
    filename = asset_config["filename"]
    extension = asset_config["extension"]        
    read_csv_options = asset_config.get("read_csv_options", default_read_csv_options)
    
    configured_path = relative_path + [filename]
    rpath = os.path.join(root_path, *configured_path)
    if extension != "":
        rpath = rpath + "." + extension
    return rpath, extension, read_csv_options

def make_table_loder(asset_key: str, table_config: dict, loader_type: str):
    """This factory creates RootInputManager classes that can retrieve the data from
    data lake tables (on local filesystem or online) and pass it as pandas dataframe to solids.
    """
    def _get_fs_path_and_extension(context):
        root_path = context.resources.dl_config["base_path"]
        fpath, extension, read_csv_options = get_fs_path_and_extension(table_config, root_path)
        # d = {'col1': [1, 2], 'col2': ["asset_key", "config.name"], "col3": [asset_key, table_config["filename"]]}
        # df = DataFrame(data=d)
        if not os.path.isfile(fpath):
            context.log.warn("file does not exist: %s" %fpath)
            return None, None, None
        else:
            return fpath, extension, read_csv_options
        

    @root_input_manager(required_resource_keys={"dl_config"})
    def _local_file_handler(context) -> String:
        fpath, _, _ = _get_fs_path_and_extension(context)
        if fpath is None:
            raise(Exception("File not found for asset: %s" % asset_key))
        return fpath


    @root_input_manager(required_resource_keys={"dl_config"})
    def _local_loader(context) -> DataFrame:
        fpath, extension, read_csv_options = _get_fs_path_and_extension(context)
        if fpath is None:
            context.log.warn("File for asset not found for %s" % asset_key)
            return None
            #raise(Exception("File for asset not found for %s" % asset_key))
        if extension == 'csv':
            context.log.info("try reading path: %s" % fpath)
            df = pd.read_csv(fpath, **read_csv_options)
            context.log.info("done reading: %s" % fpath)
        elif extension == 'parquet':
            df = pd.read_parquet(fpath)
        else:
            raise("Unknown extension '%s'" % extension)
        return df
    
    if loader_type == "local_df":
        return _local_loader
    elif loader_type == "local_fh":
        return _local_file_handler
    else:
        raise Exception("Unknown loader type %s" % loader_type)