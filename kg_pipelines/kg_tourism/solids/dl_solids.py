from multiprocessing.spawn import prepare
from operator import index
import os
from typing import Any,  Iterator, Optional, Tuple, Union, List, Dict
from dagster import AssetKey, Bool,  solid, InputDefinition

from dagster.builtins import String
from dagster.core.definitions.events import RetryRequested
from dagster.core.definitions.output import OutputDefinition
from dagster.config.field import Field 
from dagster.utils import mkdir_p

from pandas.core.frame import DataFrame

from kg_tourism.resources.table_loader_factory import get_fs_path_and_extension
from ..assets.dl_assets import DLAssetKey, DLAssetKeyType, dl_output_definitions, dl_root_input_definitions, dl_root_asset_config, dl_root_input_managers
import pandas as pd 
from ..assets.asset_utils import build_class_name, clean_name
from ..assets.dl_assets import bkg_lf_columns, bkg_lf_columns_for_triples, bkg_acco_columns, bkg_offers_columns, bkg_lb_descriptions_columns, bkg_filtered_reviews_columns
from ..assets.dl_assets import air_lf_columns, air_lf_columns_for_triples, air_filtered_reviews_columns
from ..utils.dataframe_utils import split_old_new
# import module
import traceback
  

REVIEW_TEXT_COLUMN = 'comment'
REVIEW_ID_COLUMN =  "review_id"
LB_TEXT_COLUMN = 'description'
LB_ID_COLUMN = 'id'
AIR_LF_TEXT_COLUMN = 'description'
AIR_LF_ID_COLUMN = 'id'
LANG_COLUMN = 'lang'
LANG_SCORE_COLUMN = 'score'
LOG_BATCH = 1000 ## number of dataframe lines to process before logging the advancement
MAX_AIRBNB_RATING = 5
MAX_BOOKING_RATING = 5

##################################################################################
############################ Generic solids ##################################
##################################################################################


def detect_language(context, df: pd.DataFrame, old_df: pd.DataFrame, 
                    id_column: str, text_column: str, output_asset_name: str, should_update: Bool) -> pd.DataFrame:
    #new_df, old_df = split_old_new(context, "bkg_lf_enriched_table", df, old_df, id_column, should_update)
    new_df, old_df = split_old_new(context, output_asset_name, df, old_df, id_column, should_update)
    
    language_detector = context.resources.language_detector
    df_lang_detected = language_detector.analyze(new_df, text_column, LOG_BATCH, LANG_COLUMN, LANG_SCORE_COLUMN)
    df_lang = df.merge(df_lang_detected, left_index=True, right_index=True)

    enriched_df = pd.concat([df_lang, old_df])
    return enriched_df 


### TODO: add solid to filter text by language and to take a subset of the complete dataframe to make tests

@solid(
    required_resource_keys={"dbp_entity_linker"},
    description="Extracts entityes from text",
    output_defs = [OutputDefinition(io_manager_key="temporary_io_manager")]
)
def dynamic_dbps_entity_linking(context, text_data: DataFrame) -> List[dict]:

    context.log.info("Extracting entities fron text in data frame.")
    text_column = "text"
    id_column = "id"
    #### TODO: we must filter by language before doing entity linking (see other TODO for creating a solid that takes care of the filtering)
    try:
        entities = []
        for _, row in text_data.iterrows():
            for entity in context.resources.dbp_entity_linker.entities_extraction(row[id_column], row[text_column]):
                entities.append(entity)
        return entities
    except Exception as e:
        raise RetryRequested(max_retries=3) from e


@solid(
    required_resource_keys={"geonames_entity_linker"},
    description="Extracts Geonames entityes from text",
    output_defs = [OutputDefinition(io_manager_key="temporary_io_manager")]
)
def dynamic_gn_entity_linking(context, text_data: DataFrame) -> List[dict]:

    context.log.info("Extracting Geonames entities fron text in data frame.")
    text_column = "text"
    id_column = "id"
    print("text_data colums")
    print(text_data.columns)
    print("resource gn entity linker")
    print(context.resources.geonames_entity_linker)
    #### TODO: we must filter by language before doing entity linking (see other TODO for creating a solid that takes care of the filtering)
    try:
        entities = []
        for _, row in text_data.iterrows():
            for entity in context.resources.geonames_entity_linker.entities_extraction(row[id_column], row[text_column]):
                entities.append(entity)

        return entities
    except Exception as e:
        print("exception:", e)
        traceback.print_exc()
        tb = traceback.format_exc()
        context.log.info("Eccezione "+tb)
        raise RetryRequested(max_retries=3) from e



@solid(
    required_resource_keys={"dl_config", "provenance_extractor"},
    description="Extracts entityes from text",
    input_defs=[InputDefinition("asset_key",  DLAssetKey)],  
    output_defs = [OutputDefinition(io_manager_key="temporary_io_manager")]
)
def provenance_tracer(context, asset_key) -> Any:
    provenance_extractor = context.resources.provenance_extractor
    asset_config = asset_key.asset_config
    asset_path = asset_config["path"]
    
    context.log.info("DAGSTER_HOME: %s." % os.getenv('DAGSTER_HOME'))
    context.log.info("Examining provenance for asset: %s." % " > ".join(asset_path))

    base_path = context.resources.dl_config["base_path"]
    filename = "__".join(asset_path)+".provenance.ttl"
    output_file = os.path.join(base_path, "metadata", *asset_path, filename)

    mkdir_p(os.path.dirname(output_file))

    context.log.info("Saving provenance triples in: %s" % output_file)
    
    provenance_extractor.create_triple_provenance(asset_path)
    provenance_extractor.kg.save_rdf(output_file)

##################################################################################
############################ Booking.com solids ##################################
##################################################################################

@solid(
    output_defs = list(dl_output_definitions("bkg_lf_onto_aligned_table")),
    version="1.0.0",
    config_schema={"limit": Field(int, is_required=False)}
    )
def classify_bkg_lf(context,onto_lf_mapping_table_df: DataFrame, bkg_virt_pages_flat_df: DataFrame) -> DataFrame:
    
    lf_mapping_df = onto_lf_mapping_table_df.rename(columns={'class': 'lf_class'})

        ### limit the number of line processed for test purposes
    limit = context.solid_config.get("limit")
    if limit is not None:
        context.log.warn("Limiting number of line to process to: %s" % limit)

    df_hotels_not_classified = bkg_virt_pages_flat_df.drop_duplicates(subset=['hotel_id'])[:limit] ## drop rows with duplicated id for the hotel id
    df_hotels_not_classified["structure_type"] = df_hotels_not_classified["structure_type"].str.lower()
    
    df_hotels = df_hotels_not_classified.merge(lf_mapping_df, left_on="structure_type", right_on="label", how='left')

    df_hotels["avg_rating"] = df_hotels["avg_rating"].apply(pd.to_numeric, args=('coerce',))

    ### If we can't match a structure type we must set it to a generic LodgingFacility class
    df_hotels["lf_class"].fillna("LodgingFacility", inplace=True)
    df_hotels["avg_rating_norm"] = df_hotels["avg_rating"] / 10
    df_hotels["avg_rating_norm"] = df_hotels["avg_rating_norm"].round(decimals=2)
    df_hotels["avg_rating_norm"].astype(str)

    df_hotels.rename(columns={'hotel_id': 'id','hotel_url': 'url'}, inplace=True)

    return df_hotels

@solid(
    output_defs = list(dl_output_definitions("bkg_acco_filtered_table")),
    required_resource_keys={"lf_acco_classifier"},
    version="1.0.0",
    )
def classify_bkg_acco(
        context,
        onto_lf_mapping_table_df: DataFrame, 
        bkg_virt_acco_flat_df: DataFrame, 
        bkg_lf_onto_aligned_table_df: DataFrame) -> DataFrame:
    
    df_acco_not_classified = bkg_virt_acco_flat_df.drop_duplicates(subset=['acco_id'])
    df_acco_not_classified["structure_type"] = df_acco_not_classified["structure_type"].str.lower()

    onto_lf_mapping_table_df.rename(columns={'class': 'lf_class'}, inplace=True)

    df_acco = df_acco_not_classified.merge(onto_lf_mapping_table_df, left_on="structure_type", right_on="label", how='left')    
    
    ### If we can't match a structure type we must set it to a generic LodgingFacility class
    df_acco["lf_class"].fillna("LodgingFacility", inplace=True)
    df_acco = df_acco.drop_duplicates(subset=['acco_id'])

    ## We use the calassify_accommodation function to associate an Accommodation subclass to each accommodation
    ## If a suitable subclass cannot be found the generic Accommodation class is used 
    classifier = context.resources.lf_acco_classifier
    df_acco["acco_class"] = df_acco.apply(lambda row: classifier.bkg_classify_accommodation(row["lf_class"], row["acco_name"]), axis=1) 
    
    ## we take only the accommodation related to the selected lodging businesses 
    df_filtered_acco = bkg_lf_onto_aligned_table_df[['id']].merge(df_acco,                                                               
                            left_on='id', 
                            right_on='lf_id', 
                            how='inner') 

    return df_filtered_acco[bkg_acco_columns]


@solid(
    output_defs = list(dl_output_definitions("bkg_reviews_filtered_table")),
    version="1.0.0",
    )
def create_bkg_reviews_filtered_table(context, 
        bkg_virt_reviews_flat_df: DataFrame, 
        bkg_lf_onto_aligned_table_df: DataFrame) -> DataFrame:
    
    df_reviews = bkg_virt_reviews_flat_df.drop_duplicates(subset=['review_id'])
    
    ## we take only the reviews related to the selected lodging facilities
    filtered_reviews = bkg_lf_onto_aligned_table_df['id'].to_frame().merge(df_reviews, 
                            left_on='id', 
                            right_on='lf_id', 
                            how='inner')
    
    ###  drop the duplicated column id
    filtered_reviews = filtered_reviews.drop(columns=['id'])
    filtered_reviews["rating"] = filtered_reviews["rating"].apply(pd.to_numeric, args=('coerce',))
    filtered_reviews["rating_norm"] = filtered_reviews["rating"] / 10
    filtered_reviews["rating_norm"] = filtered_reviews["rating_norm"].round(decimals=2)
    filtered_reviews["rating_norm"].astype(str)

    return filtered_reviews[bkg_filtered_reviews_columns]

@solid(
    output_defs = list(dl_output_definitions("bkg_offers_table")),
    version="1.0.0",
    )
def create_bkg_offers_table(context, 
        bkg_virt_availability_pages_flat_df: DataFrame, 
        bkg_acco_filtered_table_df: DataFrame) -> DataFrame:
    
    df_offers = bkg_virt_availability_pages_flat_df.drop_duplicates(subset=['u_offer_id'])

    ## we take only the offers related to the selected lodging businesses
    filtered_offers = bkg_acco_filtered_table_df['acco_id'].to_frame().merge(df_offers, 
                        left_on='acco_id', 
                        right_on='acco_id', 
                        how='inner') 
    
    return filtered_offers[bkg_offers_columns]


@solid(
    output_defs = list(dl_output_definitions("bkg_amenity_onto_aligned_table")),
    version="1.0.0",
    )
def classify_bkg_amenities(context, 
        onto_la_mapping_table_df: DataFrame, 
        bkg_virt_pages_flat_df: DataFrame,
        bkg_lf_onto_aligned_table_df: DataFrame) -> DataFrame:
    
    #### List of amenities labels vs classes extracted from ontology
    df_ab2t_o = onto_la_mapping_table_df.rename(columns={'class': 'amenity_class'})
    df_ab2t_o["amenity_id"] = df_ab2t_o["label"].apply(lambda e: clean_name(str(e)).lower().strip())
    
    ## Let's create an exploded dataframe with a row for each relation between a LodgingFacility and one of its associated amenity
    ## We should also clean and normalize the name for the amenity extracted from Booking.com
    df_amenities = bkg_virt_pages_flat_df[['hotel_id','amenities']].set_index(['hotel_id']).apply(pd.Series.explode).reset_index() \
    .set_axis(['lf_id', 'amenity'], axis=1)
    df_amenities["amenity_id"] = df_amenities["amenity"].apply(lambda e: clean_name(str(e)).lower().strip())


    df_amenities_and_class_ontology = df_amenities.merge(df_ab2t_o, 
                                                left_on='amenity_id', 
                                                right_on='amenity_id', 
                                                how='left')[['amenity_id','lf_id','amenity','amenity_class']]

    df_cited_amenities_ontology = bkg_lf_onto_aligned_table_df['id'].to_frame().merge(df_amenities_and_class_ontology, 
                                    left_on='id', 
                                    right_on='lf_id', 
                                    how='inner')[['lf_id','amenity','amenity_id','amenity_class']]

    ## An amenity related to a LodgingFacility is mapped if we have found a AccommodationFeature class (not null), unmapped otherwise
    df_mapped_amenities_ontology = df_cited_amenities_ontology[df_cited_amenities_ontology["amenity_class"].notnull()]
    df_mapped_amenities_ontology["amenity_id"] = df_mapped_amenities_ontology["amenity_id"].apply(lambda e: e.replace(" ", "_")) ## change id to be used in urls


    return df_mapped_amenities_ontology


@solid(
    config_schema={
        "limit": Field(int, is_required=False)
        },
    required_resource_keys={"dl_config","rml_mapper"},
    description="Create Booking.com triples from semi-structured data using RML for Booking.com",
    output_defs = list(dl_output_definitions("bkg_triples_lf_reviews")),
    version="1.0.0",
    tags= {
        "uses_software": [ {"name": "RMLMapper" , "version": "4.10.1", "uri": "https://github.com/RMLio/rmlmapper-java/tree/v4.10.1"}],
        }
)
def create_bkg_base_triples_rml( 
        context, 
        bkg_rml_map_lf_reviews: str,
        bkg_lf_onto_aligned_table_df: DataFrame, 
        bkg_amenity_onto_aligned_table_df: DataFrame, 
        bkg_reviews_filtered_table_df: DataFrame,
        bkg_acco_filtered_table_df: DataFrame,
        bkg_offers_table_df: DataFrame,
        ):

        ### limit the number of line processed for test purposes
    asset_key = "bkg_triples_lf_reviews"
    limit = context.solid_config.get("limit")
    if limit is not None:
        context.log.warn("Limiting number of entities to process to: %s" % limit)
        limit_list = [limit, limit, limit, limit, limit]
    else:
        limit_list = [
            bkg_lf_onto_aligned_table_df.shape[0],
            bkg_amenity_onto_aligned_table_df.shape[0],
            bkg_reviews_filtered_table_df.shape[0],
            bkg_acco_filtered_table_df.shape[0],
            bkg_offers_table_df.shape[0]
        ]

    bkg_lf_onto_aligned_table_df = bkg_lf_onto_aligned_table_df[bkg_lf_columns_for_triples]

    files_to_materialise = {
        "bkg_lf_entities": bkg_lf_onto_aligned_table_df[0:limit_list[0]],
        "bkg_amenities_entities": bkg_amenity_onto_aligned_table_df[0:limit_list[1]],
        "bkg_reviews_entities": bkg_reviews_filtered_table_df[0:limit_list[2]],
        "bkg_acco_entities": bkg_acco_filtered_table_df[0:limit_list[3]],
        "bkg_offers_entities": bkg_offers_table_df[0:limit_list[4]]
    }
    rml_mapper, tmp_dir_name, assets = prepare_csv_files(context, files_to_materialise)
    # assets are actual csv files containing the data to use for triple creation through rml
    
    output_path = rml_mapper.produce_mapping("bkg_base_rml_map" , bkg_rml_map_lf_reviews, tmp_dir_name, assets, prepare = True)
    out = DLAssetKey(asset_key)
    out.file_path = output_path

    return out ### return data lake asset key object


@solid(
    input_defs=[
        dl_root_input_definitions["bkg_virt_pages_flat"],
        dl_root_input_definitions["bkg_lf_enriched_table"] ## the existing version of the table that must be uptdated
    ],
    output_defs = list(dl_output_definitions("bkg_lf_enriched_table")),
    required_resource_keys={"language_detector"},
    config_schema={"update": bool, "limit": Field(int, is_required=False)}
    )
def create_bkg_lb_enriched_table(context,
        bkg_virt_pages_flat: pd.DataFrame,
        bkg_lf_enriched_table: Optional[pd.DataFrame] ) -> pd.DataFrame:    
    
    #language_detector = context.resources.language_detector
    lb_df = bkg_virt_pages_flat.drop_duplicates(subset=['hotel_id']).rename(columns={'hotel_id': 'id','hotel_url': 'url'})
    df = lb_df[bkg_lb_descriptions_columns]
    print("input df columns", list(df.columns))
    
    ### limit the number of line processed for test purposes
    limit = context.solid_config.get("limit")
    if limit is not None:
        context.log.warn("Limiting number of line to process to: %s" % limit)
        df = df[:limit]

    old_df = bkg_lf_enriched_table
    #id_column = "id"
    should_update = context.solid_config["update"]
 
    return detect_language(context, df, old_df, LB_ID_COLUMN,  LB_TEXT_COLUMN, "bkg_lf_enriched_table", should_update)


@solid(
    input_defs=[
        dl_root_input_definitions["bkg_virt_reviews_flat"],
        dl_root_input_definitions["bkg_reviews_enriched_table"] ## the existing version of the table that must be uptdated
    ],
    output_defs = list(dl_output_definitions("bkg_reviews_enriched_table")),
    required_resource_keys={"language_detector"},
    config_schema={"update": bool}
    )
def create_bkg_reviews_enriched_table(context,
        bkg_virt_reviews_flat: pd.DataFrame,
        bkg_reviews_enriched_table: Optional[pd.DataFrame] ) -> pd.DataFrame:
    
    #language_detector = context.resources.language_detector
    df = bkg_virt_reviews_flat.drop_duplicates(subset=['review_id'])
    
    old_df = bkg_reviews_enriched_table
    #id_column = "review_id"
    should_update = context.solid_config["update"]
    return detect_language(context, df, old_df, REVIEW_ID_COLUMN,  REVIEW_TEXT_COLUMN, "bkg_reviews_enriched_table", should_update)


@solid(
    output_defs = [OutputDefinition(io_manager_key="temporary_io_manager")],
    description="Creates a DataFrame with all the entries from combining batch of entities.",
)
def join_entities_out(_, item_batches: List[List[dict]]) -> DataFrame:
    return DataFrame(item for items in item_batches for item in items)


@solid(
    required_resource_keys={"dl_config","rml_mapper"},
    description="Create DBPedia Spotlight triples using RML for Booking.com lodging facilities and reviews",
    input_defs=[
        dl_root_input_definitions["bkg_rml_map_el_lf_reviews"],
    ],
    output_defs = list(dl_output_definitions("bkg_triples_el_dbps_lf_reviews")),
    version="1.0.0",
    tags= {
        "uses_software": [ {"name": "RMLMapper" , "version": "4.10.1", "uri": "https://github.com/RMLio/rmlmapper-java/tree/v4.10.1"}],
        }
)
def create_bkg_el_dbps_triples_rml(  
        context, 
        bkg_rml_map_el_lf_reviews,
        bkg_descriptions_dbps_entities_prepared: DataFrame,
        bkg_reviews_dbps_entities_prepared: DataFrame
        ):
    
    asset_key = "bkg_triples_el_dbps_lf_reviews"

    files_to_materialise = {
        "bkg_descriptions_dbps_entities": bkg_descriptions_dbps_entities_prepared,
        "bkg_reviews_dbps_entities": bkg_reviews_dbps_entities_prepared,
    }
    rml_mapper, tmp_dir_name, assets = prepare_csv_files(context, files_to_materialise)
    
    output_path = rml_mapper.produce_mapping("bkg_dbps_rml_map" , bkg_rml_map_el_lf_reviews, tmp_dir_name, assets, prepare = True)

    out = DLAssetKey(asset_key)
    out.file_path = output_path

    return out ### return data lake asset key object


@solid(
    required_resource_keys={"dl_config","rml_mapper"},
    description="Create Geonames triples using RML for Booking.com lodging facilities and reviews",
    input_defs=[
        dl_root_input_definitions["bkg_rml_map_el_lf_reviews"],
    ],
    output_defs = list(dl_output_definitions("bkg_triples_el_gn_lf_reviews")),
    version="1.0.0",
    tags= {
        "uses_software": [ {"name": "RMLMapper" , "version": "4.10.1", "uri": "https://github.com/RMLio/rmlmapper-java/tree/v4.10.1"}],
        }
)
def create_bkg_el_gn_triples_rml(  
        context, 
        bkg_rml_map_el_lf_reviews,
        bkg_descriptions_gn_entities_prepared: DataFrame,
        bkg_reviews_gn_entities_prepared: DataFrame
        ):

    asset_key = "bkg_triples_el_gn_lf_reviews"

    files_to_materialise = {
        "bkg_descriptions_gn_entities": bkg_descriptions_gn_entities_prepared,
        "bkg_reviews_gn_entities": bkg_reviews_gn_entities_prepared
    }
    rml_mapper, tmp_dir_name, assets = prepare_csv_files(context, files_to_materialise)

    output_path = rml_mapper.produce_mapping("bkg_gn_rml_map" , bkg_rml_map_el_lf_reviews, tmp_dir_name, assets, prepare = True)
    out = DLAssetKey(asset_key)
    out.file_path = output_path

    return out



##################################################################################
############################ AirBnB solids #######################################
##################################################################################

@solid(
    output_defs = list(dl_output_definitions("air_lf_onto_aligned_table")),
    required_resource_keys={"lf_acco_classifier"},
    version="1.0.0",
    config_schema={"limit": Field(int, is_required=False)}
    )
def classify_air_lf_and_acco(context,onto_lf_mapping_table_df: DataFrame, air_virt_pages_flat_df: DataFrame) -> DataFrame:
    
    lf_mapping_df = onto_lf_mapping_table_df.rename(columns={'class': 'lf_class'})

    limit = context.solid_config.get("limit")
    if limit is not None:
        context.log.warn("Limiting number of line to process to: %s" % limit)

    df = air_virt_pages_flat_df.drop_duplicates(subset=['id'])[:limit] ## drop rows with duplicated id for the listing id
    df_acco = df[air_lf_columns] ### we take only the columns we need for the triple creation
    classifier = context.resources.lf_acco_classifier

    ### We add two columns with labels extracted from 'room_and_property_type' and 'room_type_category'
    ### The following attribution of LodgingFacility and Accommodation subclasses to AirBnB listings is based on these labels.
    df_acco_labelled = df_acco.apply(classifier.air_generate_extra_lf_columns, axis=1)

    ### First step: classify with a LodgingFacility subclass
    ### We can use the LodgingFacility mapping table to check how many AirBnB listings we can classify 
    df_acco_classified = df_acco_labelled.merge(lf_mapping_df, left_on="lodging_facility_label", right_on="label", how='left')

    ## Where there is no mapping with the attributed lodging_facility_label we set the generic LodgingFacility class
    df_acco_classified["lf_class"] = df_acco_classified["lf_class"].fillna("LodgingFacility")

   ### Second step: classify with an Accommodation subclass
    df_acco_classified["acco_class"] = df_acco_classified.apply(lambda row: classifier.air_classify_accommodation(row["lf_class"], row["room_type_category"], row["refined_room_and_property"]), axis=1)

    ### add normalized average rating
    df_acco_classified["avg_rating_norm"] = df_acco_classified["avg_rating"] / MAX_AIRBNB_RATING
    df_acco_classified["avg_rating_norm"] = df_acco_classified["avg_rating_norm"].round(decimals=2)
    df_acco_classified["avg_rating_norm"].astype(str)

    ### Remove unecessary columns added during classifications
    df_acco_classified.drop(columns=["lodging_facility_label", "refined_room_and_property", "label"], inplace = True)

    return df_acco_classified

@solid(
    output_defs = list(dl_output_definitions("air_amenity_onto_aligned_table")),
    required_resource_keys={"lf_acco_classifier"},
    version="1.0.0"
    )
def classify_air_amenities(context,onto_la_mapping_table_df: DataFrame, air_lf_onto_aligned_table_df: DataFrame) -> DataFrame:
    df_amenities = air_lf_onto_aligned_table_df[['id','amenities','amenity_ids']].set_index(['id']).apply(pd.Series.explode).reset_index().set_axis(['acco_id', 'amenity', 'airbnb_id'], axis=1) ## expand each listing row into multiple rows, one for each amenity in the amenity and amenity_ids arrays
    df_amenities["amenity_id"] = df_amenities["amenity"].apply(lambda e: clean_name(str(e)).lower().strip()) # create a clean amenity name to match ontology
    df_amenities = df_amenities[pd.isnull(df_amenities["airbnb_id"]) == False] ## filter null amenities

    # prepare ontology labels vs class table
    onto_la_mapping_table_df.rename(columns={'class': 'amenity_class'}, inplace=True)
    onto_la_mapping_table_df["amenity_id"] = onto_la_mapping_table_df["label"].apply(lambda e: clean_name(str(e)).lower().strip()) # create a clean amenity name to match with amenity_id in the data from AirBnB

    ## use ontology label vs class table to associate an amenity class to the data extracted from AirBnB
    df_amenities_and_class = df_amenities.merge(onto_la_mapping_table_df, 
            left_on='amenity_id', 
            right_on='amenity_id', 
            how='left')[['amenity_id','acco_id','amenity','amenity_class']]
    df = df_amenities_and_class.drop_duplicates(subset=['amenity_id','acco_id'])
    df.rename(columns={'acco_id': 'id'}, inplace=True) ##reaname acco_id as id that represents the airbnb listing id
    
    ## We don't want to include an amenity in the kg if we cant map it
    df["amenity_id"] = df["amenity_id"].apply(lambda e: e.replace(" ", "_")) ## change id to be used in urls

    out_df = df[df["amenity_class"].notnull()]
    return out_df

@solid(
    input_defs=[
        dl_root_input_definitions["air_virt_reviews_flat_table"],
        dl_root_input_definitions["air_reviews_enriched_table"] ## the existing version of the table that must be uptdated
    ],
    output_defs = list(dl_output_definitions("air_reviews_enriched_table")),
    required_resource_keys={"language_detector"},
    config_schema={"update": bool}
    )
def create_air_reviews_enriched_table(context,
        air_virt_reviews_flat_table: pd.DataFrame,
        air_reviews_enriched_table: Optional[pd.DataFrame] ) -> pd.DataFrame:
    
    df = air_virt_reviews_flat_table.drop_duplicates(subset=['review_id'])
    

    df[LANG_COLUMN] = df["language"] ## duplicate language column using the correct name for language enriched tables
    df[LANG_SCORE_COLUMN] = 1 ## use a fiexd value of 1 because the text language is given by AirBnB

    return df

@solid(
    input_defs=[
        dl_root_input_definitions["air_lf_onto_aligned_table"],
        dl_root_input_definitions["air_virt_reviews_flat_table"],
    ],
    output_defs = list(dl_output_definitions("air_reviews_filtered_table")),
    version="1.0.0"
    )
def create_air_reviews_filtered_table(
        air_lf_onto_aligned_table: pd.DataFrame,
        air_virt_reviews_flat_table: pd.DataFrame) -> pd.DataFrame:
    df_reviews = air_virt_reviews_flat_table.drop_duplicates(subset=['review_id'])
    filtered_reviews = air_lf_onto_aligned_table['id'].to_frame().merge(df_reviews, 
        left_on='id', 
        right_on='lf_id', 
        how='left') 
    filtered_reviews = filtered_reviews[filtered_reviews['lf_id'].notnull()].drop(columns=['lf_id'])
    filtered_reviews["rating_norm"] = filtered_reviews["rating"] / MAX_AIRBNB_RATING
    return filtered_reviews[air_filtered_reviews_columns]


@solid(
    config_schema={
        "id_column": str, 
        "text_column": str,
        "lang_column": str,
        "lang_filter": str,
        "limit": Field(int, is_required=False)
        },
    output_defs = [OutputDefinition(io_manager_key="temporary_io_manager")]
)
def prepare_text_for_entity_linking(context, text: DataFrame) -> DataFrame:
    id_column = context.solid_config["id_column"]
    text_column = context.solid_config["text_column"]
    lang_column = context.solid_config["lang_column"]
    lang_filter = context.solid_config["lang_filter"]
    print("input df columns", list(text.columns))
    context.log.info("Number of lines before language filter: %s" % text.shape[0])
    text_filtered = text[text[lang_column] == lang_filter][[id_column, text_column]]
    context.log.info("Number of lines after language filter: %s" % text_filtered.shape[0])
    
    ### limit the number of line processed for test purposes
    limit = context.solid_config.get("limit")
    if limit is not None:
        context.log.warn("Limiting number of line to process to: %s" % limit)
        text_filtered = text_filtered[:limit]
    
    return text_filtered

@solid(
    input_defs=[
        dl_root_input_definitions["air_virt_pages_flat"],
        dl_root_input_definitions["air_lf_enriched_table"] ## the existing version of the table that must be uptdated
    ],
    output_defs = list(dl_output_definitions("air_lf_enriched_table")),
    required_resource_keys={"language_detector"},
    config_schema={"update": bool}
    )
def create_air_lf_enriched_table(context,
        air_virt_pages_flat: pd.DataFrame,
        air_lf_enriched_table: Optional[pd.DataFrame] ) -> pd.DataFrame:
    
    #language_detector = context.resources.language_detector
    df = air_virt_pages_flat.drop_duplicates(subset=['id'])
    
    old_df = air_lf_enriched_table
    #id_column = "review_id"
    should_update = context.solid_config["update"]
    return detect_language(context, df, old_df, AIR_LF_ID_COLUMN,  AIR_LF_TEXT_COLUMN, "air_lf_enriched_table", should_update)

@solid(
    config_schema={
        "limit": Field(int, is_required=False)
        },
    required_resource_keys={"dl_config","rml_mapper"},
    description="Create AirBnB triples from semi-structured data using RML for AirBnB",
    output_defs = list(dl_output_definitions("air_triples_lf_reviews")),
    version="1.0.0",
    tags= {
        "uses_software": [ {"name": "RMLMapper" , "version": "4.10.1", "uri": "https://github.com/RMLio/rmlmapper-java/tree/v4.10.1"}],
        }
    #output_defs = [OutputDefinition(io_manager_key="temporary_io_manager", description = "No description", asset_key=AssetKey([*dl_root_asset_config["air_triples_lf_reviews"]['path']]))],
)
def create_air_base_triples_rml( 
        context, 
        air_rml_map_lf_reviews: str,
        air_lf_onto_aligned_table_df: DataFrame, 
        air_amenity_onto_aligned_table_df: DataFrame, 
        air_reviews_filtered_table_df: DataFrame
        ):

    ### take only columns for triple creation
    air_lf_onto_aligned_table_df = air_lf_onto_aligned_table_df[air_lf_columns_for_triples]
    
    ### limit the number of line processed for test purposes
    asset_key = "air_triples_lf_reviews"
    limit = context.solid_config.get("limit")
    if limit is not None:
        context.log.warn("Limiting number of entities to process to: %s" % limit)
        limit_list = [limit, limit, limit]
    else:
        limit_list = [
            air_lf_onto_aligned_table_df.shape[0],
            air_amenity_onto_aligned_table_df.shape[0],
            air_reviews_filtered_table_df.shape[0]
        ]


    files_to_materialise = {
        "air_lf_entities": air_lf_onto_aligned_table_df[0:limit_list[0]],
        "air_amenities_entities": air_amenity_onto_aligned_table_df[0:limit_list[1]],
        "air_reviews_entities": air_reviews_filtered_table_df[0:limit_list[2]]
    }
    rml_mapper, tmp_dir_name, assets = prepare_csv_files(context, files_to_materialise)
    # assets are actual csv files containing the data to use for triple creation through rml
    
    output_path = rml_mapper.produce_mapping("air_base_rml_map" , air_rml_map_lf_reviews, tmp_dir_name, assets, prepare = True)
    out = DLAssetKey(asset_key)
    out.file_path = output_path

    return out ### return data lake asset key object


@solid(
    required_resource_keys={"dl_config","rml_mapper"},
    description="Create DBPedia Spotlight triples using RML for AirBnB lodging facilities and reviews",
    input_defs=[
        dl_root_input_definitions["air_rml_map_el_lf_reviews"],
    ],
    output_defs = list(dl_output_definitions("air_triples_el_dbps_lf_reviews")),
    version="1.0.0",
    tags= {
        "uses_software": [ {"name": "RMLMapper" , "version": "4.10.1", "uri": "https://github.com/RMLio/rmlmapper-java/tree/v4.10.1"}],
        }
)
def create_air_el_dbps_triples_rml(  #TODO: refactor using create_air_base_triples_rml as model
        context, 
        air_rml_map_el_lf_reviews,
        air_descriptions_dbps_entities_prepared: DataFrame,
        air_reviews_dbps_entities_prepared: DataFrame
        ):
    
    asset_key = "air_triples_el_dbps_lf_reviews"

    files_to_materialise = {
        "air_descriptions_dbps_entities": air_descriptions_dbps_entities_prepared,
        "air_reviews_dbps_entities": air_reviews_dbps_entities_prepared,
    }
    rml_mapper, tmp_dir_name, assets = prepare_csv_files(context, files_to_materialise)
    
    output_path = rml_mapper.produce_mapping("air_dbps_rml_map" , air_rml_map_el_lf_reviews, tmp_dir_name, assets, prepare = True)

    out = DLAssetKey(asset_key)
    out.file_path = output_path

    return out ### return data lake asset key object


@solid(
    required_resource_keys={"dl_config","rml_mapper"},
    description="Create Geonames triples using RML for AirBnB lodging facilities and reviews",
    input_defs=[
        dl_root_input_definitions["air_rml_map_el_lf_reviews"],
    ],
    output_defs = list(dl_output_definitions("air_triples_el_gn_lf_reviews")),
    version="1.0.0",
    tags= {
        "uses_software": [ {"name": "RMLMapper" , "version": "4.10.1", "uri": "https://github.com/RMLio/rmlmapper-java/tree/v4.10.1"}],
        }
)
def create_air_el_gn_triples_rml(  #TODO: refactor using create_air_base_triples_rml as model
        context, 
        air_rml_map_el_lf_reviews,
        air_descriptions_gn_entities_prepared: DataFrame,
        air_reviews_gn_entities_prepared: DataFrame
        ):

    asset_key = "air_triples_el_gn_lf_reviews"

    files_to_materialise = {
        "air_descriptions_gn_entities": air_descriptions_gn_entities_prepared,
        "air_reviews_gn_entities": air_reviews_gn_entities_prepared
    }
    rml_mapper, tmp_dir_name, assets = prepare_csv_files(context, files_to_materialise)

    output_path = rml_mapper.produce_mapping("air_gn_rml_map" , air_rml_map_el_lf_reviews, tmp_dir_name, assets, prepare = True)
    out = DLAssetKey(asset_key)
    out.file_path = output_path

    return out


def prepare_csv_files(context, df_dict: Dict[str, DataFrame]) -> Tuple[Any, str, Dict[str, DataFrame]]:
    rml_mapper = context.resources.rml_mapper
    tmp_dir_name =  context.pipeline_name + '__' + context.solid_def.name + '_' + context.run_id

    
    dl_config = context.resources.dl_config
    tmp_dir = os.path.join(dl_config["tmp_path"],tmp_dir_name)
    context.log.info("Temp dir: %s" % tmp_dir)
    if not os.path.exists(tmp_dir):
        context.log.info("Created: %s" % tmp_dir)
        os.makedirs(tmp_dir)
    
    entity_file_prefix = "entities_file_"
    path_dict = {}
    for key, df in df_dict.items():
        csv_name = entity_file_prefix+str(key)+".csv"
        path = os.path.join(*[tmp_dir,csv_name])
        path_dict[key] = path
        context.log.info("Materialising %s: %s" % (key, path))
        df.to_csv(path, index=False)

    return rml_mapper,tmp_dir_name, path_dict
