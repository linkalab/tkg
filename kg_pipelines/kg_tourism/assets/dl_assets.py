"""Asset definitions for booking.com source."""
# import pandas as pd

import json
import os
import pandas as pd
from typing import Dict, Iterator, Union, List
from dagster import AssetKey, AssetMaterialization, DagsterType, InputDefinition, OutputDefinition, EventMetadataEntry, Output, usable_as_dagster_type
from dagster.builtins import String
from pandas.core.frame import DataFrame
from dagster import solid

from kg_tourism.resources.knowledge_graph import KnowledgeGraph


from ..resources.table_loader_factory import get_fs_path_and_extension, make_table_loder
#import csv

import csv
from typing import Tuple

READER_SOLID_VERSION = "1.0.0"

################# Data lake asset key class

class DLAssetKey():
    def __init__(self, asset_key: str):
        pass
        self.asset_key = asset_key
        self.asset_path = dl_root_asset_config[asset_key]["path"]
        self.asset_config = dl_root_asset_config[asset_key]
    
    def toJson(self):
        return json.dumps("{}")
        #return json.dumps(self, default=lambda o: o.__dict__)
        
def is_dl_asset_key(_, value):
    return isinstance(value, DLAssetKey)

DLAssetKeyType = DagsterType(
    name="DLAssetKeyType",
    type_check_fn=is_dl_asset_key,
    description="A naive representation of a data frame, e.g., as returned by csv.DictReader.",
)

def dl_output_definitions(names: Union[str,List[str]]) ->  Iterator[OutputDefinition]:
    if type(names) is str:
        names = [names]
    for name in names:
        dagster_type =dl_root_asset_config[name].get("dagster_type", None)
        yield OutputDefinition(dagster_type=dagster_type, name=name, asset_key = AssetKey([*dl_root_asset_config[name]['path']]), metadata={"table_config": dl_root_asset_config[name], "asset_key": name})
      

default_read_csv_options =  {  
    "quoting": csv.QUOTE_MINIMAL,
    "escapechar": "\\",
    "doublequote": False,
    }

def get_writer_asset_solid_factory(asset_key):
    @solid(
        name=asset_key+"_writer",
        input_defs=[InputDefinition(name="df", dagster_type=DataFrame )],
        output_defs = list(dl_output_definitions(asset_key)),
        required_resource_keys={"dl_config"},
    )      
    def _get_asset(context, df):
        def _store_asset(context, df: DataFrame) -> DataFrame:
            context.log.info("Loading asset with key %s" % asset_key)
            return df
            #return locals(asset_key)
        print("Create solid for asset %", asset_key)
        return _store_asset(context, df)
    return _get_asset


### Class to create a factory to produce solids to read assets in the data lake
class ReaderAssetSolidFactory:
    def __init__(self):
        self.solids = {}

    def get_reader_asset_solid_factory(self, asset_key, output_type = pd.DataFrame):
        asset_config = dl_root_asset_config[asset_key]
        description = asset_config.get("description", "No description")

        @solid(
            name=asset_key+"_reader",
            version=READER_SOLID_VERSION,
            output_defs = [OutputDefinition(output_type, io_manager_key="temporary_io_manager", description=description, asset_key=AssetKey([*dl_root_asset_config[asset_key]['path']]))],  ## the asset key is the composition of the full path of the asset in the data lake        
            required_resource_keys={"dl_config"},
        )       
        def _get_asset(context):
            def _get_fs_path_and_extension(context):
                root_path = context.resources.dl_config["base_path"]
                table_config = dl_root_asset_config[asset_key]
                fpath, extension, read_csv_options = get_fs_path_and_extension(table_config, root_path)
                # d = {'col1': [1, 2], 'col2': ["asset_key", "config.name"], "col3": [asset_key, table_config["filename"]]}
                # df = DataFrame(data=d)
                if not os.path.isfile(fpath):
                    context.log.warn("file does not exist: %s" %fpath)
                    return None, None, None
                else:
                    return fpath, extension, read_csv_options

            def _local_loader(context, output_type) -> DataFrame:
                fpath, extension, read_csv_options = _get_fs_path_and_extension(context)
                table_meta = asset_config
                agent_meta = {
                    "name": context.solid_def.name,
                    "version": context.solid_def.version,
                    "tags": context.solid_def.tags
                }
                if fpath is None:
                    context.log.warn("File for asset not found for %s" % asset_key)
                    return None
                    #raise(Exception("File for asset not found for %s" % asset_key))
                if output_type is pd.DataFrame:
                    if extension == 'csv':
                        context.log.info("try reading path: %s" % fpath)
                        df = pd.read_csv(fpath, **read_csv_options)
                        context.log.info("done reading: %s" % fpath)
                    elif extension == 'parquet':
                        df = pd.read_parquet(fpath)
                    elif extension == "ttl":
                        kg = KnowledgeGraph()
                        kg.load_rdf(fpath)
                        df = kg.export_df()
                    else:
                        raise("Unknown extension '%s'" % extension)

                    row_count = df.shape[0]
                    print("%s shape: " % asset_key, df.shape)
                    yield Output(
                        value=df, 
                        metadata_entries= [
                            EventMetadataEntry.int(value=row_count, label="row_count"),
                            EventMetadataEntry.path(path=fpath, label="path"),
                            EventMetadataEntry.json(data=table_meta, label="table_meta", description="Metadata for the data lake table."),
                            EventMetadataEntry.json(data=agent_meta, label="agent_meta", description="Metadata for the Solid that produces the output.")
                        ]) 

                    # return df
                elif output_type is str:
                    row_count = sum(1 for line in open(fpath))
                    yield Output(
                        value=fpath, 
                        metadata_entries= [
                            EventMetadataEntry.int(value=row_count, label="row_count"),
                            EventMetadataEntry.path(path=fpath, label="path"),
                            EventMetadataEntry.json(data=table_meta, label="table_meta", description="Metadata for the data lake table."),
                            EventMetadataEntry.json(data=agent_meta, label="agent_meta", description="Metadata for the Solid that produces the output.")
                        ])

                else:
                    raise("Unkmanaged output type '%s'" % output_type)

            # manager = dl_root_input_managers[asset_key]
            # df = manager(context)
            #context.log.info("Create solid for asset %" % (asset_key,))
            print("Create solid for asset %", asset_key)
            return _local_loader(context, output_type)
        
        asset_reader = self.solids.get(asset_key, None)
        if asset_reader is None:
            return _get_asset
        else:
            return asset_reader
 

## Data lake assets used as root input manager to other solids
## see: https://docs.dagster.io/concepts/io-management/unconnected-inputs
## The dictionary is used to map all data lake resources (either locally or on cloud storage).
## Each key is a unique identifier for the data lake resource, which is associated to its metadata.
           
 
### dictionary of assets that should be used as data frames
dl_root_df_asset_config = {

    ######################### Ontology assets ################################

    "onto_lf_mapping_table": {
        "path": ["management","cleaned","ontology","lodging_facility_mapping"],
        "filename": "lf_ontology_mapping",
        "extension": "csv",
        "read_csv_options": {}, ## use pandas defaults
        "write_csv_options": {},
        "partitions": [  ### TODO: add support for partitions in the LocalFileSystemStorage class
            {"name": "dumpdate",
            "value": "20210701"}
        ]

    },    

    "onto_la_mapping_table": {
        "path": ["management","cleaned","ontology","location_amenity_mapping"],
        "filename": "la_ontology_mapping",
        "extension": "csv",
        "read_csv_options": {}, ## use pandas defaults
        "write_csv_options": {},
        "partitions": [  ### TODO: add support for partitions in the LocalFileSystemStorage class
            {"name": "dumpdate",
            "value": "20210701"}
        ]

    },    


    "bkg_facilities_class_mappings_table": {
        "path": ["management","cleaned","booking","facilities_class_mappings"],
        "filename": "booking_facilities",
        "extension": "csv",
        "read_csv_options": {}, ## use pandas defaults
        "write_csv_options": {},
        "partitions": [  ### TODO: add support for partitions in the LocalFileSystemStorage class
            {"name": "dumpdate",
            "value": "20210701"}
        ]

    },

        "air_facilities_class_mappings_table": {
        "path": ["management","cleaned","airbnb","facilities_class_mappings"],
        "filename": "airbnb_facilities",
        "extension": "csv",
        "read_csv_options": {}, ## use pandas defaults
        "write_csv_options": {},
        "partitions": [  ### TODO: add support for partitions in the LocalFileSystemStorage class
            {"name": "dumpdate",
            "value": "20210701"}
        ]

    },


    "all_facilities_class_mappings_table": {
        "path": ["management","integration","ontology","facilities_class_mappings"],
        "filename": "all_facilities",
        "extension": "csv",
        "partitions": [  ### TODO: add support for partitions in the LocalFileSystemStorage class
            {"name": "dumpdate",
            "value": "20210701"}
        ]
    },   

    "fake_asset": {
        "path": ["management","cleaned","booking","facilities_class_mappings"],
        "filename": "booking_facilities_copy",
        "extension": "csv",
        "read_csv_options": {}, ## use pandas defaults
        "write_csv_options": {},
        "partitions": [  ### TODO: add support for partitions in the LocalFileSystemStorage class
            {"name": "dumpdate",
            "value": "20210701"}
        ]

    },    

    ######################### Booking.com assets ################################
    
    "bkg_virt_pages_flat": {
        "path": ["management","integration","booking","pages_flat"],
        "filename": "pages_flat",
        "extension": "parquet",
        "partitions": [  ### TODO: add support for partitions in the LocalFileSystemStorage class
            {"name": "dumpdate",
            "value": "20210701"}
        ]

    },
    "bkg_virt_reviews_flat": {
        "label": "Booking reviews table fromn data lake.",
        "label-lang": "en",        
        "path": ["management","integration","booking","reviews_flat"],
        "filename": "reviews_flat",
        "extension": "parquet",
        "partitions": [  ### TODO: add support for partitions in the LocalFileSystemStorage class
            {"name": "dumpdate",
            "value": "20210701"}
        ]

    },
    "bkg_virt_acco_flat": {
        "label": "Booking accommodation table fromn data lake.",
        "label-lang": "en",        
        "path": ["management","integration","booking","acco_flat"],
        "filename": "acco_flat",
        "extension": "parquet",
        "partitions": [  ### TODO: add support for partitions in the LocalFileSystemStorage class
            {"name": "dumpdate",
            "value": "20210701"}
        ]

    },
    "bkg_virt_availability_pages_flat": {
        "label": "Booking availability table fromn data lake.",
        "label-lang": "en",        
        "path": ["management","integration","booking","availability_pages_flat"],
        "filename": "availability_pages_flat",
        "extension": "parquet",
        "partitions": [  ### TODO: add support for partitions in the LocalFileSystemStorage class
            {"name": "dumpdate",
            "value": "20210701"}
        ]

    },

    "bkg_lf_onto_aligned_table": {
        "label": "Booking lodging facilities aligned to ontology classes.",
        "label-lang": "en",        
        "path": ["management","integration","booking","lf_onto_aligned"],
        "filename": "lf_onto_aligned",
        "extension": "parquet",
        "partitions": [  ### TODO: add support for partitions in the LocalFileSystemStorage class
            {"name": "dumpdate",
            "value": "20210701"}
        ]
    },


    "bkg_amenity_onto_aligned_table": {
        "label": "Booking amenities aligned with ontology classes.",
        "label-lang": "en",        
        "path": ["management","integration","booking","amenity_onto_aligned"],
        "filename": "amenity_onto_aligned",
        "extension": "parquet",
        "partitions": [  ### TODO: add support for partitions in the LocalFileSystemStorage class
            {"name": "dumpdate",
            "value": "20210701"}
        ]
    },

    "bkg_lb_table": {
        "path": ["management","integration","booking","lodging_business"],
        "filename": "lodging_business",
        "extension": "csv",
        "partitions": [  ### TODO: add support for partitions in the LocalFileSystemStorage class
            {"name": "dumpdate",
            "value": "20210701"}
        ]

    },    
    "bkg_lb_descriptions_table": {
        "path": ["management","integration","booking","lodging_business_desc"],
        "filename": "lodging_business_descriptions",
        "extension": "csv",
        "partitions": [  ### TODO: add support for partitions in the LocalFileSystemStorage class
            {"name": "dumpdate",
            "value": "20210701"}
        ]

    },       
    "bkg_amenities_entities_table": {
        "path": ["management","integration","booking","amenities_entities"],
        "filename": "amenities_entities",
        "extension": "csv",
        "partitions": [  ### TODO: add support for partitions in the LocalFileSystemStorage class
            {"name": "dumpdate",
            "value": "20210701"}
        ]

    },      
    "bkg_reviews_filtered_table": {
        "label": "Booking.com user reviews filtered.",
        "label-lang": "en",                
        "path": ["management","integration","booking","reviews_filtered"],
        "filename": "reviews_filtered",
        "extension": "csv",
        "partitions": [  ### TODO: add support for partitions in the LocalFileSystemStorage class
            {"name": "dumpdate",
            "value": "20210701"}
        ]

    },      
    "bkg_acco_filtered_table": {
        "label": "Booking.com accommodation filtered table used for triple creation.",
        "label-lang": "en",                
        "path": ["management","integration","booking","acco_filtered"],
        "filename": "acco_filtered",
        "extension": "csv",
        "partitions": [  ### TODO: add support for partitions in the LocalFileSystemStorage class
            {"name": "dumpdate",
            "value": "20210701"}
        ]

    },      
    "bkg_offers_table": {
        "label": "Booking.com offers filtered table used for triple creation.",
        "label-lang": "en",                
        "path": ["management","integration","booking","offers"],
        "filename": "offers",
        "extension": "csv",
        "partitions": [  ### TODO: add support for partitions in the LocalFileSystemStorage class
            {"name": "dumpdate",
            "value": "20210701"}
        ]

    },  
    "bkg_reviews_enriched_table": {
        "label": "Booking.com reviews enriched table used for tripel creation.",
        "label-lang": "en",                
        "path": ["management","integration","booking","reviews_enriched"],
        "filename": "reviews_enriched",
        "extension": "parquet",
        "partitions": [  ### TODO: add support for partitions in the LocalFileSystemStorage class
            {"name": "dumpdate",
            "value": "20210701"}
        ]

    },    
    "bkg_lf_enriched_table": {
        "path": ["management","integration","booking","lf_enriched"],
        "filename": "lf_enriched",
        "extension": "parquet",
        "partitions": [  ### TODO: add support for partitions in the LocalFileSystemStorage class
            {"name": "dumpdate",
            "value": "20210701"}
        ]
    },    
    "bkg_reviews_dbps_entities": {
        "label": "Booking.com - DBpedia entities extracted from reviews.",
        "label-lang": "en",                
        "path": ["management","integration","booking","reviews_dbps_entities"],
        "filename": "reviews_dbps_entities",
        "extension": "csv",
        "partitions": [  ### TODO: add support for partitions in the LocalFileSystemStorage class
            {"name": "dumpdate",
            "value": "20210701"}
        ]
    },    
    "bkg_reviews_gn_entities": {
        "label": "Booking.com - Geonames entities extracted from reviews.",
        "label-lang": "en",                
        "path": ["management","integration","booking","reviews_gn_entities"],
        "filename": "reviews_gn_entities",
        "extension": "csv",
        "partitions": [  ### TODO: add support for partitions in the LocalFileSystemStorage class
            {"name": "dumpdate",
            "value": "20210701"}
        ]
    },   
    "bkg_descriptions_dbps_entities": {
        "label": "Booking.com - DBpedia entities extracted from lodging facility description.",
        "label-lang": "en",                
        "path": ["management","integration","booking","descriptions_dbps_entities"],
        "filename": "descriptions_dbps_entities",
        "extension": "csv",
        "partitions": [  ### TODO: add support for partitions in the LocalFileSystemStorage class
            {"name": "dumpdate",
            "value": "20210701"}
        ]
    },   
    "bkg_descriptions_gn_entities": {
        "label": "Booking.com - Geonames entities extracted from lodging facility description.",
        "label-lang": "en",   
        "path": ["management","integration","booking","descriptions_gn_entities"],
        "filename": "descriptions_gn_entities",
        "extension": "csv",
        "partitions": [  ### TODO: add support for partitions in the LocalFileSystemStorage class
            {"name": "dumpdate",
            "value": "20210701"}
        ]
    },
    ######################### AirBnB assets ################################

    "air_virt_reviews_flat_table": {
        "path": ["management","integration","airbnb","reviews_flat"],
        "filename": "reviews_flat",
        "extension": "parquet",
        "partitions": [  ### TODO: add support for partitions in the LocalFileSystemStorage class
            {"name": "dumpdate",
            "value": "20210701"}
        ]

    },
    "air_reviews_filtered_table": {
        "label": "AirBnB user reviews filtered.",
        "label-lang": "en",                
        "path": ["management","integration","airbnb","reviews_filtered"],
        "filename": "reviews_filtered",
        "extension": "parquet",
        "partitions": [  ### TODO: add support for partitions in the LocalFileSystemStorage class
            {"name": "dumpdate",
            "value": "20210701"}
        ]

    },     
    "air_virt_pages_flat": {
        "path": ["management","integration","airbnb","pages_flat"],
        "filename": "pages_flat",
        "extension": "parquet",
        "partitions": [  ### TODO: add support for partitions in the LocalFileSystemStorage class
            {"name": "dumpdate",
            "value": "20210701"}
        ]

    },

    "air_lf_onto_aligned_table": {
        "label": "AirBnB lodging facilities aligned to ontology classes.",
        "label-lang": "en",        
        "path": ["management","integration","airbnb","lf_onto_aligned"],
        "filename": "lf_onto_aligned",
        "extension": "parquet",
        "partitions": [  ### TODO: add support for partitions in the LocalFileSystemStorage class
            {"name": "dumpdate",
            "value": "20210701"}
        ]
    },  

    "air_amenity_onto_aligned_table": {
        "label": "AirBnB amenities aligned with ontology classes.",
        "label-lang": "en",        
        "path": ["management","integration","airbnb","amenity_onto_aligned"],
        "filename": "amenity_onto_aligned",
        "extension": "parquet",
        "partitions": [  ### TODO: add support for partitions in the LocalFileSystemStorage class
            {"name": "dumpdate",
            "value": "20210701"}
        ]
    },

    "air_lf_enriched_table": {
        "path": ["management","integration","airbnb","lf_enriched"],
        "filename": "lf_enriched",
        "extension": "parquet",
        "partitions": [  ### TODO: add support for partitions in the LocalFileSystemStorage class
            {"name": "dumpdate",
            "value": "20210701"}
        ]

    },   
    "air_reviews_enriched_table": {
        "path": ["management","integration","airbnb","reviews_enriched"],
        "filename": "reviews_enriched",
        "extension": "parquet",
        "partitions": [  ### TODO: add support for partitions in the LocalFileSystemStorage class
            {"name": "dumpdate",
            "value": "20210701"}
        ]

    },   
    "air_reviews_dbps_entities": {
        "path": ["management","integration","airbnb","reviews_dbps_entities"],
        "filename": "reviews_dbps_entities",
        "extension": "parquet",
        "partitions": [  ### TODO: add support for partitions in the LocalFileSystemStorage class
            {"name": "dumpdate",
            "value": "20210701"}
        ]
    },   
    "air_reviews_gn_entities": {
        "path": ["management","integration","airbnb","reviews_gn_entities"],
        "filename": "reviews_gn_entities",
        "extension": "parquet",
        "partitions": [  ### TODO: add support for partitions in the LocalFileSystemStorage class
            {"name": "dumpdate",
            "value": "20210701"}
        ]
    },  
    "air_descriptions_dbps_entities": {
        "path": ["management","integration","airbnb","descriptions_dbps_entities"],
        "filename": "descriptions_dbps_entities",
        "extension": "parquet",
        "partitions": [  ### TODO: add support for partitions in the LocalFileSystemStorage class
            {"name": "dumpdate",
            "value": "20210701"}
        ]
    },      
    "air_descriptions_gn_entities": {
        "path": ["management","integration","airbnb","descriptions_gn_entities"],
        "filename": "descriptions_gn_entities",
        "extension": "parquet",
        "partitions": [  ### TODO: add support for partitions in the LocalFileSystemStorage class
            {"name": "dumpdate",
            "value": "20210701"}
        ]
    },   
}

dl_root_fh_asset_config = {
    "bkg_rml_map_lf_reviews": { ### RML mapping file for lodging facilities and reviews
        "label": "Booking.com RML mapping for lodging facilities and review",
        "label-lang": "en",
        "path": ["metadata", "management","integration","booking","rml","mapping","lf_and_reviews"],
        "filename": "map_booking_lf_and_reviews",
        "extension": "ttl"
    },
    "bkg_triples_lf_reviews": { ### RDF nquad file with triple created for lodging facilities and reviews 
        "label": "Booking.com base triples for lodging facilities and review",
        "label-lang": "en",    
        "path": ["consumption","kg","booking","lf_and_reviews","nquads"],
        "filename": "nquads_booking_lf_and_reviews",
        "extension": "ttl"
    },      
    "bkg_rml_map_el_lf_reviews": { ### RML mapping file for lodging facilities and reviews entity linking
        "label": "Booking.com RML mapping for DBpedia entities extracted from lodging facilities and review",
        "label-lang": "en",
        "path": ["metadata", "management","integration","booking","rml","mapping","el_lf_reviews"],
        "filename": "map_booking_el_lf_reviews",
        "extension": "ttl"
    },
    "bkg_triples_el_dbps_lf_reviews": { ### RDF nquad file with triple created for lodging facilities and reviews entity linking
        "label": "Booking.com triples of DBpedia entities extracted from lodging facilities and review",
        "label-lang": "en",
        "path": ["consumption","kg","booking","el_dbps_lf_reviews","nquads"],
        "filename": "nquads_booking_el_dbps_lf_reviews",
        "extension": "ttl"
    },   
    "bkg_triples_el_gn_lf_reviews": { ### RDF nquad file with triple created for lodging facilities and reviews entity linking
        "label": "Booking.com triples of Geonames entities extracted from lodging facilities and review",
        "label-lang": "en",
        "path": ["consumption","kg","booking","el_gn_lf_reviews","nquads"],
        "filename": "nquads_booking_el_gn_lf_reviews",
        "extension": "ttl"
    },         
    ### TODO: add RML and triple assets for geonames entity linking of Booking lodging facilities and reviews

    "air_rml_map_lf_reviews": { ### RML mapping file for lodging facilities and reviews
        "label": "AirBnB RML mapping for lodging facilities and review",
        "label-lang": "en",
        "path": ["metadata", "management","integration","airbnb","rml","mapping","lf_and_reviews"],
        "filename": "map_airbnb_lf_and_reviews",
        "extension": "ttl"
    },
    "air_triples_lf_reviews": { ### RDF nquad file with triple created for lodging facilities and reviews 
        "label": "AirBnB base triples for lodging facilities and review",
        "label-lang": "en",    
        "path": ["consumption","kg","airbnb","lf_and_reviews","nquads"],
        "filename": "nquads_airbnb_lf_and_reviews",
        "extension": "ttl",
        #"dagster_type": Dict
        #"dagster_type": DLAssetKey
    },         
    "air_rml_map_el_lf_reviews": { ### RML mapping file for lodging facilities and reviews entity linking
        "label": "AirBnB RML mapping for DBpedia entities extracted from lodging facilities and review",
        "label-lang": "en",
        "path": ["metadata", "management","integration","airbnb","rml","mapping","el_lf_reviews"],
        "filename": "map_airbnb_el_lf_reviews",
        "extension": "ttl"
    },
    "air_triples_el_dbps_lf_reviews": { ### RDF nquad file with triple created for lodging facilities and reviews entity linking
        "label": "AirBnB triples of DBpedia entities extracted from lodging facilities and review",
        "label-lang": "en",
        "path": ["consumption","kg","airbnb","el_dbps_lf_reviews","nquads"],
        "filename": "nquads_airbnb_el_dbps_lf_reviews",
        "extension": "ttl"
    },   
    "air_triples_el_gn_lf_reviews": { ### RDF nquad file with triple created for lodging facilities and reviews entity linking
        "label": "AirBnB triples of Geonames entities extracted from lodging facilities and review",
        "label-lang": "en",
        "path": ["consumption","kg","airbnb","el_gn_lf_reviews","nquads"],
        "filename": "nquads_airbnb_el_gn_lf_reviews",
        "extension": "ttl"
    },      
}


dl_root_asset_config = {}
dl_root_asset_config.update(dl_root_df_asset_config)
dl_root_asset_config.update(dl_root_fh_asset_config)

####################### AirBnB column selection for each table

air_lf_columns = ['amenities', 'amenity_ids', 'avg_rating', 'bathrooms', 'bedrooms', 'beds', 'city', 'country', 'description', 'id', 'latitude', 'longitude', 'name', 'person_capacity', 'place_id', 'price_rate', 'price_rate_type', 'review_count', 'room_and_property_type', 'room_type', 'room_type_category', 'satisfaction_guest', 'state', 'url']
air_lf_columns_for_triples = ['avg_rating', 'avg_rating_norm', 'acco_class', 'lf_class', 'bathrooms', 'bedrooms', 'beds', 'city', 'country', 'id', 'latitude', 'longitude', 'name', 'person_capacity', 'place_id', 'price_rate', 'price_rate_type', 'review_count', 'room_and_property_type', 'room_type', 'room_type_category', 'satisfaction_guest', 'state', 'url']


air_filtered_reviews_columns = ['id', 'review_id', 'created_at', 'rating', 'rating_norm','language']


####################### Booking.com column selection for each table

## lodging businees columns we must take from Booking.com bkg_virt_pages_flat table
bkg_lf_columns = [  
    'id', 'url', 'name', 'structure_type', 'avg_rating','address', 'latitude', 'longitude' ]  

bkg_lf_columns_for_triples = [  
    'id', 'url', 'name', 'structure_type', 'lf_class', 'avg_rating_norm','address', 'latitude', 'longitude' ]  

bkg_lb_descriptions_columns = [
    'id', 'url', 'name', 'description', ]

bkg_filtered_reviews_columns = [
    'lf_id', 'lf_url', 'review_id', 'review_nationality', 'rating', 'rating_norm','created_at'    
#    'review_tags', ## TODO: review tags can better describe the user who's making the review
]

bkg_acco_columns = [
    #'lf_id', 'lf_url', 'acco_id', 'name', 'acco_name', 'bed_types', 'checkin', 'checkout', 'beds'
    #TODO: fix problemi in bed_types column where new lines are found 
    'lf_id', 'lf_url', 'acco_id', 'name', 'acco_name', 'checkin', 'checkout', 'beds', 'acco_class'
#    'acco_amenities',  ### TODO: amenities can be connected to accommodation also
]

bkg_offers_columns = [
    'acco_id', 'lf_id', 'lf_url', 'offer_id', 'name', 'checkin', 'checkout',
    'acco_name', 'room_scarcity', 'beds',
    'max_person', 'original_price', 'current_price', 'currency', 'price', 'u_offer_id'
#    'acco_amenities',  ## TODO: remove from athena view because these are the same amenities whe found in the accommodation entity
#    'bed_types', ## TODO: 
 #    'condition', ## TODO: it's a text, use ML to extract entities

]
####################### data lake tables definition for Dagster input manager

#root_path = "/home/rancas/local_datalake"
dl_root_input_managers = {}
for (asset_name, asset_config) in dl_root_df_asset_config.items():
    dl_root_input_managers[asset_name] = make_table_loder(asset_name, asset_config, "local_df"   )
for (asset_name, asset_config) in dl_root_fh_asset_config.items():
    dl_root_input_managers[asset_name] = make_table_loder(asset_name, asset_config, "local_fh"   )


dl_root_input_definitions ={}
for (asset_name, asset_config) in dl_root_df_asset_config.items():
    dl_root_input_definitions[asset_name] = InputDefinition(asset_name, root_manager_key=asset_name)
for (asset_name, asset_config) in dl_root_fh_asset_config.items():
    dl_root_input_definitions[asset_name] = InputDefinition(asset_name, dagster_type = String, root_manager_key=asset_name)



####################### data lake tables reader and writer solids for Dagster
reader_asset_solid_factory = ReaderAssetSolidFactory()
dl_root_asset_readers = {}
for (asset_key, _) in dl_root_df_asset_config.items():
    print("create solid with output of type DataFrame for: ", ">".join(dl_root_asset_config[asset_key]["path"]))
    reader_asset_solid = reader_asset_solid_factory.get_reader_asset_solid_factory(asset_key)
    dl_root_asset_readers[asset_key] = reader_asset_solid

for (asset_key, _) in dl_root_fh_asset_config.items():
    print("create solid with output of type str for: ", ">".join(dl_root_asset_config[asset_key]["path"]))
    reader_asset_solid = reader_asset_solid_factory.get_reader_asset_solid_factory(asset_key, str)
    dl_root_asset_readers[asset_key] = reader_asset_solid

dl_root_asset_writers = {}
for (asset_key, _) in dl_root_df_asset_config.items():
    writer_asset_solid = get_writer_asset_solid_factory(asset_key)
    dl_root_asset_writers[asset_key] = writer_asset_solid



