from dagster import repository

#from .pipelines.dl_pipelines import test, kg_booking_base_triples_pipeline, kg_booking_descriptions_language_detection_pipeline, kg_booking_reviews_language_detection_and_entity_linking_pipeline, kg_booking_reviews_language_detection_pipeline
from .pipelines.dl_pipelines import *

@repository
def kg_tourism_repository():
    pipelines = [
        #test,
        #### Booking.com
        kg_booking_base_triples_pipeline,
        kg_booking_entity_linking_triples_pipeline,
        # kg_booking_dbps_triples_pipeline,
        # kg_booking_descriptions_language_detection_pipeline,
        # kg_booking_descriptions_dbps_entity_linking_pipeline,
        # kg_booking_descriptions_gn_entity_linking_pipeline,
        # kg_booking_reviews_dbps_entity_linking_pipeline,
        # kg_booking_reviews_gn_entity_linking_pipeline,
        # kg_booking_reviews_language_detection_pipeline,

        #### AirBnB
        kg_airbnb_base_triples_pipeline,
        kg_airbnb_entity_linking_triples_pipeline,
        # kg_airbnb_dbps_triples_pipeline,
        # kg_airbnb_gn_triples_pipeline,
        # kg_airbnb_reviews_language_detection_pipeline,
        # kg_airbnb_lf_language_detection_pipeline,
        # kg_airbnb_descriptions_dbps_entity_linking_pipeline,
        # kg_airbnb_descriptions_gn_entity_linking_pipeline, 
        # kg_airbnb_reviews_dbps_entity_linking_pipeline,
        # kg_airbnb_reviews_gn_entity_linking_pipeline
        
        
    ]
    schedules = [

    ]
    sensors = [

    ]

    return pipelines + schedules + sensors
