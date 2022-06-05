import os, io
import shutil
from typing import Any, Dict, Union, List
from dagster.utils import mkdir_p

from kg_tourism.assets.dl_assets import DLAssetKey
from kg_tourism.resources.knowledge_graph import KnowledgeGraph
from ..resources.table_loader_factory import get_fs_path_and_extension

import pandas as pd
import pyspark
from dagster import AssetKey, EventMetadataEntry, IOManager, OutputContext, check, io_manager



class DlIOManager(IOManager):
    """
    This IOManager will take in a pandas or pyspark dataframe and store it in parquet at the
    specified path.

    Downstream solids can either load this dataframe into a spark session, a pandas Dataframe or simply retrieve a path
    to where the data is stored.
    """
    def _get_fs_path_and_extension(self, context) -> str:
        root_path = context.resources.dl_config["base_path"]
        table_config = context.metadata["table_config"]
        return get_fs_path_and_extension( table_config, root_path)
    
    # def get_output_asset_key(self, context: OutputContext):
    #     table_config = context.metadata["table_config"]
    #     return AssetKey([*table_config['path']])

    def handle_output(
        self, context: OutputContext, obj: Union[pd.DataFrame, pyspark.sql.DataFrame, io.TextIOWrapper, str, Dict, DLAssetKey]
    ):
        table_meta = context.metadata["table_config"]
        path, extension, read_csv_options = self._get_fs_path_and_extension(context)

        # Ensure path exists
        mkdir_p(os.path.dirname(path))

        if isinstance(obj, pd.DataFrame):
            context.log.info("Salvo un dataframe pandas con path %s" % path)
            row_count = len(obj)
            if extension == 'parquet':
                obj.to_parquet(path=path, index=False)
            elif extension == "csv":
                obj.to_csv(path, **read_csv_options,index=False)
            else:
                raise("Unknown extension '%s'" % extension)
        elif isinstance(obj, pyspark.sql.DataFrame):
            context.log.info("Salvo un dataframe spark")
            row_count = obj.count()
            if extension == 'parquet':
                obj.write.parquet(path=path, mode="overwrite")
            elif extension == "csv":
                obj.write.csv(path=path, mode="overwrite")
            else:
                raise("Unknown extension '%s'" % extension)
        elif isinstance(obj, dict):
            shutil.copy2(obj["file_path"], path)
            row_count = sum(1 for line in open(path))                  
        elif isinstance(obj, DLAssetKey):
            shutil.copy2(obj.file_path, path)
            row_count = sum(1 for line in open(path))     
        elif isinstance(obj, str):
            shutil.copy2(obj, path)
            row_count = sum(1 for line in open(path))

        elif isinstance(obj, io.TextIOWrapper):
            row_count=0
            with obj as input_file:
                with open(path, "w") as out_file:
                    for line in input_file:
                        if "ROW" in line:
                            out_file.write(line)
                            row_count = row_count + 1
        else:
            raise Exception(f"Outputs of type {type(obj)} not supported.")
        
        agent_meta = {
            "name": context.solid_def.name,
            "version": context.solid_def.version,
            "tags": context.solid_def.tags
        }

        yield EventMetadataEntry.int(value=row_count, label="row_count")
        yield EventMetadataEntry.path(path=path, label="path")
        yield EventMetadataEntry.json(data=table_meta, label="table_meta", description="Metadata for the data lake table.")
        yield EventMetadataEntry.json(data=agent_meta, label="agent_meta", description="Metadata for the Solid that produces the output.")


    def load_input(self, context) -> Union[pyspark.sql.DataFrame, pd.DataFrame, str]:
        # In this load_input function, we vary the behavior based on the type of the downstream input
        #context.log.info("data_lake %s " % context.upstream_output.metadata["datalake"])
        
        context.log.info("Dagster type: %s" % context.dagster_type.typing_type)
        if context.dagster_type.typing_type == DLAssetKey:
            asset_key = context.upstream_output.metadata["asset_key"]
            context.log.info("We have to produce an object: %s" % asset_key)
            return DLAssetKey(asset_key)
            
        path, extension, read_csv_options = self._get_fs_path_and_extension(context.upstream_output)
   
        # path = self._get_path(context.upstream_output)
        if context.dagster_type.typing_type == pyspark.sql.DataFrame:
            # return pyspark dataframe
            context.log.info("Devo produrre un dataframe spark in input al solid a partire dal path: %s" % path)
            if extension == "parquet":
                return context.resources.pyspark.spark_session.read.parquet(path)
            elif extension == "csv":
                return context.resources.pyspark.spark_session.read.csv(path)
            else:
                raise Exception("Unsupported file extension %s " % extension)
        elif context.dagster_type.typing_type == pd.DataFrame:
            # return pyspark dataframe
            context.log.info("Devo produrre un dataframe pandas in input al solid a partire dal path: %s" % path)
            if extension == "parquet":
                return pd.read_parquet(path)
            elif extension == "csv":
                return pd.read_csv(path, **read_csv_options)
            elif extension == "ttl":
                kg = KnowledgeGraph()
                kg.load_rdf(path)
                return kg.export_df()
            else:
                raise Exception("Unsupported file extension %s " % extension)
        elif context.dagster_type.typing_type == str:
            # return path to parquet files
            context.log.info("Devo produrre una stringa con un path che finirà in input al solid a valle: %s" % path)
            return path

        return check.failed(
            f"Inputs of type {context.dagster_type} not supported. Please specify a valid type "
            "for this input either in the solid signature or on the corresponding InputDefinition."
        )

@io_manager(
    required_resource_keys={"dl_config"},
)
def dl_io_manager(_):
    return DlIOManager()
