import os
from dagster import resource
from ..assets.dl_assets import dl_root_asset_config





@resource(config_schema={"base_path": str, "tmp_path": str})
def dl_config(init_context):
    base_path = init_context.resource_config["base_path"]
    
    rml_config = {
        "bkg_base_rml_map": {
            "sources": [
                {"rml_source": "booking_lf.csv", "asset": "bkg_lf_entities"},
                {"rml_source": "booking_amenities.csv", "asset": "bkg_amenities_entities"},
                {"rml_source": "booking_reviews.csv", "asset": "bkg_reviews_entities"},
                {"rml_source": "booking_acco.csv", "asset": "bkg_acco_entities"},
                {"rml_source": "booking_offers.csv", "asset": "bkg_offers_entities"},

            ],
            "output": ["bkg_triples_lf_reviews"],
            "rml_executor": {
                "jar": os.path.join(base_path,"code","rmlmapper.jar"),
                "java_opts": ['-Xmx4g', '-Xms4g'],
                "rml_opts": ['-v']
            }
        },
        "bkg_dbps_rml_map": {
            "sources": [
                {"rml_source": "booking_lf_entities.csv", "asset": "bkg_descriptions_dbps_entities"},
                {"rml_source": "booking_reviews_entities.csv", "asset": "bkg_reviews_dbps_entities"},
            ],
            "output": ["bkg_triples_el_dbps_lf_reviews"],
            "rml_executor": {
                "jar": os.path.join(base_path,"code","rmlmapper.jar"),
                "java_opts": ['-Xmx4g', '-Xms4g'],
                "rml_opts": ['-v','-d']
            }
        },
        "bkg_gn_rml_map": {
            "sources": [
                {"rml_source": "booking_lf_entities.csv", "asset": "bkg_descriptions_gn_entities"},
                {"rml_source": "booking_reviews_entities.csv", "asset": "bkg_reviews_gn_entities"},
            ],
            "output": ["bkg_triples_el_gn_lf_reviews"],
            "rml_executor": {
                "jar": os.path.join(base_path,"code","rmlmapper.jar"),
                "java_opts": ['-Xmx4g', '-Xms4g'],
                "rml_opts": ['-v','-d']
            }
        },        
        "air_base_rml_map": {
            "sources": [
                {"rml_source": "airbnb_lf.csv", "asset": "air_lf_entities"},
                {"rml_source": "airbnb_amenities.csv", "asset": "air_amenities_entities"},
                {"rml_source": "airbnb_reviews.csv", "asset": "air_reviews_entities"},
            ],
            "output": ["air_triples_lf_reviews"],
            "rml_executor": {
                "jar": os.path.join(base_path,"code","rmlmapper.jar"),
                "java_opts": ['-Xmx4g', '-Xms4g'],
                "rml_opts": ['-v']
            }
        },        
        "air_dbps_rml_map": {
            "sources": [
                {"rml_source": "airbnb_lf_entities.csv", "asset": "air_descriptions_dbps_entities"},
                {"rml_source": "airbnb_reviews_entities.csv", "asset": "air_reviews_dbps_entities"},
            ],
            "output": ["air_triples_el_dbps_lf_reviews"],
            "rml_executor": {
                "jar": os.path.join(base_path,"code","rmlmapper.jar"),
                "java_opts": ['-Xmx4g', '-Xms4g'],
                "rml_opts": ['-v','-d']
            }
        },
        "air_gn_rml_map": {
            "sources": [
                {"rml_source": "airbnb_lf_entities.csv", "asset": "air_descriptions_gn_entities"},
                {"rml_source": "airbnb_reviews_entities.csv", "asset": "air_reviews_gn_entities"},
            ],
            "output": ["air_triples_el_gn_lf_reviews"],
            "rml_executor": {
                "jar": os.path.join(base_path,"code","rmlmapper.jar"),
                "java_opts": ['-Xmx4g', '-Xms4g'],
                "rml_opts": ['-v','-d']
            }
        }
    }
    
    config = {
        "base_path": base_path,
        "tmp_path": init_context.resource_config["tmp_path"],
        "assets": dl_root_asset_config,
        "rml_config": rml_config
        }
    return config