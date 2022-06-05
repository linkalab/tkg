from abc import ABC, abstractmethod
import ntpath
import os, subprocess
from os.path import exists 

from typing import Any, Optional
from dagster.config.field import Field
from dagster.utils import mkdir_p

from dagster.core.definitions import resource
#from ..resources.dl_config import dl_config
from ..resources.table_loader_factory import get_fs_path_and_extension
from spacy.util import load_config


RML_MAPPER_JAVA_DEFAULT_CONFIG = {}

def path_leaf(path):
    head, tail = ntpath.split(path)
    return tail or ntpath.basename(head)

class AbstractRMLMapper(ABC):
    @abstractmethod 
    def produce_mapping(self, rml_tag: str, rml_file: str, tmp_dir_name: str, assets: dict):
        pass


class RMLMapperJava(AbstractRMLMapper):
    def __init__(self, dl_config: dict, rml_config: Optional[dict], logger: Any ) -> None:
        self._logger = logger
        self._dl_config = dl_config
        if rml_config is None:
            rml_config = RML_MAPPER_JAVA_DEFAULT_CONFIG
    
    def _prepare_execution(self, rml_tag: str, tmp_dir: str, assets: str) -> None:
        rml_tag_config = self._dl_config["rml_config"][rml_tag] 
        if not os.path.exists(tmp_dir):
            os.makedirs(tmp_dir)

        #create a symlink for each of the csv files that are referred to in the rml mapping file
        for source in rml_tag_config["sources"]:
            source_asset = source["asset"]
            src_file = assets[source_asset]
            link_name = os.path.join(tmp_dir, source["rml_source"])
            os.symlink(src_file, link_name)

    def _execute_mapping(self, rml_tag: str, tmp_dir: str, rml_file: str) -> str:
        ### SEE: https://janakiev.com/blog/python-shell-commands/ 
        # process = subprocess.Popen(['ping', '-c 4', 'python.org'], 
        jar = self._dl_config["rml_config"][rml_tag]["rml_executor"]["jar"]
        java_opts = self._dl_config["rml_config"][rml_tag]["rml_executor"]["java_opts"]
        rml_opts = self._dl_config["rml_config"][rml_tag]["rml_executor"]["rml_opts"]
        out_asset_key = self._dl_config["rml_config"][rml_tag]["output"][0]
        out_asset = self._dl_config["assets"][out_asset_key]
        base_path = self._dl_config["base_path"]
        fpath, _ , _ = get_fs_path_and_extension(out_asset, base_path)
        # Ensure output path exists
        mkdir_p(os.path.dirname(fpath))
        temp_out_file_name = path_leaf(fpath) ## we get the filename only to create it in the temp directory
        temp_out_path = os.path.join(tmp_dir, temp_out_file_name)
        self._logger.info("Temp out file path %s" % temp_out_path)

        instruction = [
            'java'] + \
            java_opts + \
            [
            '-jar', jar] + \
            rml_opts + \
            ['-m', rml_file,
            '-o', temp_out_path]
        self._logger.info(" ".join(instruction))
        process = subprocess.Popen(instruction, 
        # java -jar ../bin/rmlmapper.jar -v -m ../map_booking.ttl -o ../output/kg_triples_out.ttl
        # java -jar rmlmapper.jar -v -m map_lb_and_reviews.ttl -o kg_triples_out.ttl
            stdout = subprocess.PIPE,
            stderr = subprocess.PIPE,
            cwd = tmp_dir,
            universal_newlines=True)
        while True:
            output = process.stdout.readline()
            err = process.stderr.readline()
            self._logger.info(output.rstrip())
            self._logger.debug(err.rstrip())
            # Do something else
            return_code = process.poll()
            if return_code is not None:
                # Process has finished, read rest of the output 
                for output in process.stdout.readlines():
                    self._logger.info(output.strip())                
                for err in process.stderr.readlines():
                    self._logger.debug(err.strip())
                
                if return_code != 0: ## PROBLEM! return code for RMLMapperJava is always 0 even when an error occurs 
                    raise Exception("RDF mapper error, return code %s" % return_code)
                if not exists(temp_out_path): ## WORKAROUND! we check if output file exists because return code for RMLMapperJava is always 0 even when an error occurs 
                    raise Exception("RDF mapper error, output file was not generated: %s DOES NOT EXIST" % temp_out_path)
                self._logger.info('RETURN CODE %s' % return_code)
                break
            self._logger.info("Mapper return code %s" % return_code)
        return temp_out_path
    
    def produce_mapping(self, rml_tag: str, rml_file: str, tmp_dir_name: str, assets: dict, prepare = True):
        tmp_dir = os.path.join(self._dl_config["tmp_path"],tmp_dir_name)
        # print("rml_config for tag %s:  " %rml_tag , self._dl_config["rml_config"][rml_tag])
        # print(assets)
        # print("tmp_dir: ", tmp_dir)
        if prepare:
            self._prepare_execution(rml_tag, tmp_dir, assets)
        output_path = self._execute_mapping(rml_tag, tmp_dir, rml_file)
        return output_path
        

@resource(
    config_schema={"rml_config": Field(dict, is_required=False) },
    required_resource_keys = {"dl_config"},
    description="A RML processor that produces triples from source csv and rml mapping file.",
)
def rml_mapper(context):
    rml_config = context.resource_config.get("rml_config", None)
    dl_config = context.resources.dl_config
    
    return RMLMapperJava(dl_config, rml_config,  context.log)
