import os
from typing import Union
from dagster.utils import mkdir_p

import pandas
import pyspark
from dagster import AssetKey, EventMetadataEntry, IOManager, OutputContext, check, io_manager


class ParquetIOManager(IOManager):
    """
    This IOManager will take in a pandas or pyspark dataframe and store it in parquet at the
    specified path.

    Downstream solids can either load this dataframe into a spark session or simply retrieve a path
    to where the data is stored.
    """

    def _get_path(self, context: OutputContext):

        base_path = context.resource_config["base_path"]

        return os.path.join(base_path, f"{context.name}.pq")

    def get_output_asset_key(self, context: OutputContext):
        return AssetKey([*context.resource_config["base_path"].split("://"), context.name])

    def handle_output(
        self, context: OutputContext, obj: Union[pandas.DataFrame, pyspark.sql.DataFrame]
    ):

        path = self._get_path(context)
        
        # Ensure path exists
        mkdir_p(os.path.dirname(path))

        if isinstance(obj, pandas.DataFrame):
            context.log.info("Salvo un dataframe pandas con path %s" % path)
            row_count = len(obj)
            obj.to_parquet(path=path, index=False)
        elif isinstance(obj, pyspark.sql.DataFrame):
            context.log.info("Salvo un dataframe spark")
            row_count = obj.count()
            obj.write.parquet(path=path, mode="overwrite")
        else:
            raise Exception(f"Outputs of type {type(obj)} not supported.")
        yield EventMetadataEntry.int(value=row_count, label="row_count")
        yield EventMetadataEntry.path(path=path, label="path")

    def load_input(self, context) -> Union[pyspark.sql.DataFrame, pandas.DataFrame, str]:
        # In this load_input function, we vary the behavior based on the type of the downstream input
        #context.log.info("data_lake %s " % context.upstream_output.metadata["datalake"])
        path = self._get_path(context.upstream_output)
        if context.dagster_type.typing_type == pyspark.sql.DataFrame:
            # return pyspark dataframe
            context.log.info("Devo produrre un dataframe spark in input al solid a partire dal path: %s" % path)
            return context.resources.pyspark.spark_session.read.parquet(path)
        elif context.dagster_type.typing_type == pandas.DataFrame:
            # return pyspark dataframe
            context.log.info("Devo produrre un dataframe pandas in input al solid a partire dal path: %s" % path)
            return pandas.read_parquet(path)
        elif context.dagster_type.typing_type == str:
            # return path to parquet files
            context.log.info("Devo produrre una stringa con un path in input al solid a partire dalla stringa: %s" % path)
            return path
        return check.failed(
            f"Inputs of type {context.dagster_type} not supported. Please specify a valid type "
            "for this input either in the solid signature or on the corresponding InputDefinition."
        )


class PartitionedParquetIOManager(ParquetIOManager):
    """
    This works similarly to the normal ParquetIOManager, but stores outputs for different partitions
    in different filepaths.
    """

    def _get_path(self, context: OutputContext):
        try:
            meta = context.metadata
            print("meta: ",meta)
            print("context.name",context.config)
            dl_path = meta["dl_path"]
        except Exception as e:
            context.log.error(str(e))
            dl_path = ""
        base_path = context.resource_config["base_path"]

        # filesystem-friendly string that is scoped to the start/end times of the data slice
        print(context.resources.partition_list)
        partition_list = context.resources.partition_list["partitions"] 
        for partition in partition_list:
            context.log.info("partition: %s" % partition)
        start = "".join(c for c in f"{context.resources.partition_start}" if c == "_" or c == "=" or c.isdigit())
        end = "".join(c for c in f"{context.resources.partition_end}" if c == "_" or c == "=" or c.isdigit())
        partition_str = f"start={start}/end={end}"
        #partition_str = "".join(c for c in partition_str if c == "_" or c == "=" or c.isdigit())

        


        # if local fs path, store all outptus in same directory
        if "://" not in base_path:
            context.log.info("Using data lake path: '%s'" % dl_path)
            # return os.path.join(base_path, dl_path, f"{context.name}-{partition_str}.pq")
            return os.path.join(base_path, dl_path, f"{partition_str}/{context.name}.pq")
        # otherwise seperate into different dirs
        return os.path.join(base_path, context.name, f"{partition_str}.pq")

    def get_output_asset_partitions(self, context: OutputContext):
        return set([context.resources.partition_start])


@io_manager(
    config_schema={"base_path": str},
    required_resource_keys={"pyspark"},
)
def parquet_io_manager(_):
    return ParquetIOManager()


@io_manager(
    config_schema={"base_path": str},
    required_resource_keys={"pyspark", "partition_start", "partition_end", "partition_list"},
)
def partitioned_parquet_io_manager(_):
    return PartitionedParquetIOManager()
