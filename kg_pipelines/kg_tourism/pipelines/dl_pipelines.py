from typing import Callable
from dagster import ModeDefinition, PresetDefinition
from dagster import fs_io_manager
from dagster import pipeline
from dagster_pandas import DataFrame

from kg_tourism.resources.provenance_extractor import provenance_extractor

from ..resources.geonames_el import geonames_entity_linker
from ..resources.dbp_spotlight_el import dbp_entity_linker
from ..resources.lf_acco_classifier import lf_acco_classifier

from ..resources.rml_mapper import rml_mapper
from ..solids.emit_text_chunks import dynamic_text_chunks
from ..resources.dl_io_manager import dl_io_manager

from ..resources.language_detector import language_detector
from ..assets.dl_assets import dl_root_input_managers, dl_root_asset_readers, dl_root_asset_writers
from ..solids.dl_solids import classify_air_amenities, classify_air_lf_and_acco, classify_bkg_acco, classify_bkg_amenities, classify_bkg_lf, create_air_base_triples_rml, create_air_el_dbps_triples_rml, create_air_el_gn_triples_rml, create_air_lf_enriched_table, create_air_reviews_enriched_table, create_air_reviews_filtered_table, create_bkg_base_triples_rml, create_bkg_el_dbps_triples_rml, create_bkg_el_gn_triples_rml, create_bkg_offers_table, create_bkg_reviews_filtered_table, dynamic_gn_entity_linking, create_bkg_lb_enriched_table, create_bkg_reviews_enriched_table,  dynamic_dbps_entity_linking, join_entities_out, prepare_text_for_entity_linking, provenance_tracer
from ..resources.dl_config import dl_config

import os

BASE_PATH =  os.getenv('DATALAKE_PATH', "/opt/local_datalake")
TMP_PATH = os.getenv('DATALAKE_TMP', "/tmp") 

default_resources_preset = {"dl_config": { "config": {"base_path": BASE_PATH, "tmp_path": TMP_PATH}}}
default_dynamic_text_chunks_preset = { "config": {"batch_size": 1000}}
default_prepare_text_for_description_el_preset = { "config": {"id_column": "id", "lang_column": "lang", "lang_filter": "en", "text_column": "description"}}
default_prepare_text_for_review_el_preset = { "config": {"id_column": "review_id", "lang_column": "lang", "lang_filter": "en", "text_column": "comment"}}


resource_defs = {}

for (res_name, root_input_manager) in dl_root_input_managers.items():
    resource_defs[res_name] = root_input_manager

resource_defs["dl_config"] = dl_config
resource_defs["io_manager"] = dl_io_manager
resource_defs["dl_io_manager"] = dl_io_manager
resource_defs["temporary_io_manager"] = fs_io_manager
resource_defs["language_detector"] = language_detector
resource_defs["lf_acco_classifier"] = lf_acco_classifier
resource_defs["dbp_entity_linker"] = dbp_entity_linker
resource_defs["geonames_entity_linker"] = geonames_entity_linker
resource_defs["rml_mapper"] = rml_mapper
resource_defs["provenance_extractor"] = provenance_extractor

def core_entity_linking(input_asset_key: str, output_asset_key: str, dynamic_entity_linking_function: Callable ):
    input_solid = dl_root_asset_readers[input_asset_key]
    input_df = input_solid()
    prepared_text = prepare_text_for_entity_linking(input_df)
    data_chunks = dynamic_text_chunks(prepared_text)
    linked_entities = data_chunks.map(dynamic_entity_linking_function)
    raw_df = join_entities_out(linked_entities.collect())
    store_solid = dl_root_asset_writers[output_asset_key]
    
    store_solid(raw_df)

def core_entity_linking_embed(input_df: DataFrame, dynamic_entity_linking_function: Callable ):
    prepared_text = prepare_text_for_entity_linking(input_df)
    data_chunks = dynamic_text_chunks(prepared_text)
    linked_entities = data_chunks.map(dynamic_entity_linking_function)
    raw_df = join_entities_out(linked_entities.collect())
    return raw_df




@pipeline(
    mode_defs=[ModeDefinition(name="local",resource_defs=resource_defs)],
    preset_defs=[PresetDefinition(
        "local",
        run_config={
            "resources": default_resources_preset,
            "solids": {
                "create_bkg_reviews_enriched_table": {"config": {"update": True}},
                "create_bkg_lb_enriched_table": {"config": {"update": True}},                
                "prepare_text_for_entity_linking": default_prepare_text_for_review_el_preset,
                "prepare_text_for_entity_linking_2": default_prepare_text_for_description_el_preset,
                "prepare_text_for_entity_linking_3": default_prepare_text_for_review_el_preset,
                "prepare_text_for_entity_linking_4": default_prepare_text_for_description_el_preset,
                "dynamic_text_chunks": default_dynamic_text_chunks_preset,
                "dynamic_text_chunks_2": default_dynamic_text_chunks_preset,
                "dynamic_text_chunks_3": { "config": {"batch_size": 10000}},
                "dynamic_text_chunks_4": default_dynamic_text_chunks_preset,

            },
            "execution": {"multiprocess": {"config": {"max_concurrent": 6}}}
        },
        mode="local")]
    )
def kg_booking_entity_linking_triples_pipeline():
    """This pipeline produces triples for entity linking using DBPedia and Geonames.
    It has tree phases: 
        1) detects the language used in text extracted from reviews and lodging facility descriptions;
        2) uses DBpedia Spotlight and Mordecai (Geonames) do extract and link named entities from the text (english only);
        3) creates RDF triples to associate each reviews and each londing facility description to DBpedia/Geonames entities. 
    """
    bkg_reviews_enriched_table_df = create_bkg_reviews_enriched_table()
    bkg_lf_enriched_table_df = create_bkg_lb_enriched_table()

    bkg_reviews_dbps_entities_df = core_entity_linking_embed(bkg_reviews_enriched_table_df, dynamic_dbps_entity_linking)
    dl_root_asset_writers["bkg_reviews_dbps_entities"](bkg_reviews_dbps_entities_df) 

    bkg_descriptisons_dbps_entities_df = core_entity_linking_embed(bkg_lf_enriched_table_df, dynamic_dbps_entity_linking)
    dl_root_asset_writers["bkg_descriptions_dbps_entities"](bkg_descriptisons_dbps_entities_df) 

    bkg_reviews_gn_entities_df = core_entity_linking_embed(bkg_reviews_enriched_table_df, dynamic_gn_entity_linking)
    dl_root_asset_writers["bkg_reviews_gn_entities"](bkg_reviews_gn_entities_df) 


    bkg_descriptisons_gn_entities_df = core_entity_linking_embed(bkg_lf_enriched_table_df, dynamic_gn_entity_linking)
    dl_root_asset_writers["bkg_descriptions_gn_entities"](bkg_descriptisons_gn_entities_df) 

    dbps_triple_asset_name = create_bkg_el_dbps_triples_rml(bkg_descriptions_dbps_entities_prepared=bkg_descriptisons_dbps_entities_df, bkg_reviews_dbps_entities_prepared=bkg_reviews_dbps_entities_df)
    provenance_tracer(dbps_triple_asset_name)

    gn_triple_asset_name = create_bkg_el_gn_triples_rml(bkg_descriptions_gn_entities_prepared=bkg_descriptisons_gn_entities_df, bkg_reviews_gn_entities_prepared=bkg_reviews_gn_entities_df)
    provenance_tracer(gn_triple_asset_name)


@pipeline(
    mode_defs=[ModeDefinition(name="local",resource_defs=resource_defs)],
    preset_defs=[PresetDefinition(
        "local",
        run_config={ "resources": default_resources_preset},
        mode="local")]
)
def kg_booking_base_triples_pipeline():
    bkg_rml_map_lf_reviews = dl_root_asset_readers["bkg_rml_map_lf_reviews"]()
    onto_lf_mapping_table_df = dl_root_asset_readers["onto_lf_mapping_table"]()
    bkg_virt_pages_flat_df = dl_root_asset_readers["bkg_virt_pages_flat"]()
    bkg_virt_reviews_flat_df = dl_root_asset_readers["bkg_virt_reviews_flat"]()
    bkg_virt_acco_flat_df = dl_root_asset_readers["bkg_virt_acco_flat"]()
    bkg_virt_availability_pages_flat_df = dl_root_asset_readers["bkg_virt_availability_pages_flat"]()


    ### apply the classification of lodging facilities with ontology
    bkg_lf_onto_aligned_table_df = classify_bkg_lf(onto_lf_mapping_table_df, bkg_virt_pages_flat_df)
    
    ### filter reviews to take just those related to the lodging facilities we are considering
    bkg_reviews_filtered_table_df = create_bkg_reviews_filtered_table(bkg_virt_reviews_flat_df, bkg_lf_onto_aligned_table_df)
    
    ### filter accommodations to take just those related to the lodging facilities we are considering
    bkg_acco_filtered_table_df = classify_bkg_acco(
        onto_lf_mapping_table_df,bkg_virt_acco_flat_df, bkg_lf_onto_aligned_table_df
    )

    ### apply the classification of location amenities with ontology
    onto_la_mapping_table_df = dl_root_asset_readers["onto_la_mapping_table"]()
    bkg_amenity_onto_aligned_table_df = classify_bkg_amenities(onto_la_mapping_table_df, bkg_virt_pages_flat_df, bkg_lf_onto_aligned_table_df)

    
    ### create offers table from accommodation filtered table
    bkg_offers_table_df = create_bkg_offers_table(
        bkg_virt_availability_pages_flat_df, bkg_acco_filtered_table_df
    )

    triple_asset_name = create_bkg_base_triples_rml(bkg_rml_map_lf_reviews=bkg_rml_map_lf_reviews, bkg_lf_onto_aligned_table_df = bkg_lf_onto_aligned_table_df, bkg_amenity_onto_aligned_table_df = bkg_amenity_onto_aligned_table_df, bkg_reviews_filtered_table_df = bkg_reviews_filtered_table_df,
    bkg_acco_filtered_table_df = bkg_acco_filtered_table_df,
    bkg_offers_table_df = bkg_offers_table_df
    )
    provenance_tracer(triple_asset_name)



####### AirBnB pipelines

@pipeline(
    mode_defs=[ModeDefinition(name="local",resource_defs=resource_defs)],
    preset_defs=[PresetDefinition(
        "local",
        run_config={ "resources": default_resources_preset},
        mode="local")]
)
def kg_airbnb_base_triples_pipeline():
    air_rml_map_lf_reviews = dl_root_asset_readers["air_rml_map_lf_reviews"]()
    onto_lf_mapping_table_df = dl_root_asset_readers["onto_lf_mapping_table"]()
    air_virt_pages_flat_df = dl_root_asset_readers["air_virt_pages_flat"]()
    air_virt_reviews_flat_table_df = dl_root_asset_readers["air_virt_reviews_flat_table"]()


    air_lf_onto_aligned_table_df = classify_air_lf_and_acco(onto_lf_mapping_table_df, air_virt_pages_flat_df)
    
    air_reviews_filtered_table_df = create_air_reviews_filtered_table(air_lf_onto_aligned_table_df, air_virt_reviews_flat_table_df)

    onto_la_mapping_table_df = dl_root_asset_readers["onto_la_mapping_table"]()
    air_amenity_onto_aligned_table_df = classify_air_amenities(onto_la_mapping_table_df, air_lf_onto_aligned_table_df)
    
    triple_asset_name = create_air_base_triples_rml(
        air_rml_map_lf_reviews=air_rml_map_lf_reviews, 
        air_lf_onto_aligned_table_df = air_lf_onto_aligned_table_df, 
        air_amenity_onto_aligned_table_df = air_amenity_onto_aligned_table_df, 
        air_reviews_filtered_table_df = air_reviews_filtered_table_df)
    provenance_tracer(triple_asset_name)



@pipeline(
    mode_defs=[ModeDefinition(name="local",resource_defs=resource_defs)],
    preset_defs=[PresetDefinition(
        "local",
        run_config={
            "resources": default_resources_preset,
            "solids": {
                "create_air_reviews_enriched_table": {"config": {"update": True}},
                "create_air_lf_enriched_table": {"config": {"update": True}},                
                "prepare_text_for_entity_linking": default_prepare_text_for_review_el_preset,
                "prepare_text_for_entity_linking_2": default_prepare_text_for_description_el_preset,
                "prepare_text_for_entity_linking_3": default_prepare_text_for_review_el_preset,
                "prepare_text_for_entity_linking_4": default_prepare_text_for_description_el_preset,
                "dynamic_text_chunks": default_dynamic_text_chunks_preset,
                "dynamic_text_chunks_2": default_dynamic_text_chunks_preset,
                "dynamic_text_chunks_3": { "config": {"batch_size": 10000}},
                "dynamic_text_chunks_4": default_dynamic_text_chunks_preset,

            },
            "execution": {"multiprocess": {"config": {"max_concurrent": 6}}}
        },
        mode="local")]
    )
def kg_airbnb_entity_linking_triples_pipeline():
    """This pipeline produces triples for entity linking using DBPedia and Geonames.
    It has tree phases: 
        1) detects the language used in text extracted from reviews and lodging facility descriptions;
        2) uses DBpedia Spotlight and Mordecai (Geonames) do extract and link named entities from the text (english only);
        3) creates RDF triples to associate each reviews and each londing facility description to DBpedia/Geonames entities. 
    """
    air_reviews_enriched_table_df = create_air_reviews_enriched_table()
    air_lf_enriched_table_df = create_air_lf_enriched_table()

    air_reviews_dbps_entities_df = core_entity_linking_embed(air_reviews_enriched_table_df, dynamic_dbps_entity_linking)
    dl_root_asset_writers["air_reviews_dbps_entities"](air_reviews_dbps_entities_df) 

    air_descriptisons_dbps_entities_df = core_entity_linking_embed(air_lf_enriched_table_df, dynamic_dbps_entity_linking)
    dl_root_asset_writers["air_descriptions_dbps_entities"](air_descriptisons_dbps_entities_df) 

    air_reviews_gn_entities_df = core_entity_linking_embed(air_reviews_enriched_table_df, dynamic_gn_entity_linking)
    dl_root_asset_writers["air_reviews_gn_entities"](air_reviews_gn_entities_df) 


    air_descriptisons_gn_entities_df = core_entity_linking_embed(air_lf_enriched_table_df, dynamic_gn_entity_linking)
    dl_root_asset_writers["air_descriptions_gn_entities"](air_descriptisons_gn_entities_df) 

    dbps_triple_asset_name = create_air_el_dbps_triples_rml(air_descriptions_dbps_entities_prepared=air_descriptisons_dbps_entities_df, air_reviews_dbps_entities_prepared=air_reviews_dbps_entities_df)
    provenance_tracer(dbps_triple_asset_name)

    
    gn_triple_asset_name = create_air_el_gn_triples_rml(air_descriptions_gn_entities_prepared=air_descriptisons_gn_entities_df, air_reviews_gn_entities_prepared=air_reviews_gn_entities_df)
    provenance_tracer(gn_triple_asset_name)
