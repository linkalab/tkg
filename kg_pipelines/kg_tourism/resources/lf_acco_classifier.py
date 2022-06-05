#import spacy
from functools import wraps
import re
from typing import Tuple
from dagster import resource
import pandas as pd




def generate_airbnb_lf_labels(room_and_property_type: str, description: str) -> (str, str):  
    ### We try to generate a tuple with two labels:
    ### lodging_facility_label: can be used to map each listing to a LodgingFacility class with a join
    ### refined_room_and_property: is a modified version of room_and_property_type which can be used in the next processing step aimed at mapping each listing to an Accommodation class
    
    if pd.isnull(room_and_property_type): # check if NaN or None 
        room_and_property_type = ""

    room_and_property_type = room_and_property_type.lower() ## set all lower case
    room_and_property_type = room_and_property_type.replace("home", "house") ## the italian word for casa is wrongly translated in home

    if pd.isnull(description): # check if NaN or None 
        description = ""
    description = description.lower()
    guesessed_type = None
    if any([substring in description for substring in ["apartment","appartament"]]):
        guesessed_type =  "apartment"
    elif any([substring in description for substring in ["house","home","casa"]]):
        guesessed_type = "house"



    ### In some cases room_and_property_type starts with "Entire " substring, we have to remove it.
    ### We don't loose any information because the room_type_category field is used by AirBnB to define if the entire place was offered as an accommodation (using "entire_home" value)
    ### E.g. "entire serviced apartment" becomes "serviced apartment"
    if room_and_property_type.find('entire ') == 0:
        lf_label = room_and_property_type.replace("entire ","")
        if guesessed_type is not None:
            return guesessed_type, room_and_property_type ### lodging_facility_label, refined_room_and_property
        else:
            return lf_label, room_and_property_type ### lodging_facility_label, refined_room_and_property

    ## We can have a text in room_and_property_type like "{accommodation type} in {lodging facility}"
    ## Note: sometimes this type of text is present even when room_type_category is set "entire_home".We are not going to resolve this contradiction in this step but in the next one.
    
    acco_in_lf = room_and_property_type.split(" in ")
    if len(acco_in_lf) > 1:
        ## if we have something like "{accommodation type} in {lodging facility}" we return a tuble accordingly
        if guesessed_type is not None:
            return guesessed_type, acco_in_lf[0] ### lodging_facility_label, refined_room_and_property
        else:
            return acco_in_lf[1], acco_in_lf[0] ### lodging_facility_label, refined_room_and_property
    else:
        ## else we don't have any lodging_facility_label in from the data and set it to the generic "lodging facility"
        ## then we pass along "room_and_property_type" as refined_room_and_property output
        return room_and_property_type, room_and_property_type ### lodging_facility_label, refined_room_and_property


@resource
def lf_acco_classifier(init_context):
    return LfAccoClassifier(init_context.log)


class LfAccoClassifier:
    def __init__(self, logger) -> None:       
        self.logger = logger

    def air_generate_extra_lf_columns(self,input_df):    
        room_and_property_type = input_df["room_and_property_type"]
        description = input_df["description"]
        lodging_facility_label, accommodation_label = generate_airbnb_lf_labels(room_and_property_type, description)
        input_df["lodging_facility_label"] = lodging_facility_label
        input_df["refined_room_and_property"] = accommodation_label
        return input_df

    def air_classify_accommodation(self, lf_class, room_type_category, refined_room_and_property):
        lf_hotel_classes = ["Hotel", "CountryHouseHotel", "BunkerHotel", 
                        "UnderWaterHotel", "TreeHouseHotel", 
                        "IceHotel", "CaveHotel", "CapsuleHotel"] ## TODO: extract form ontology
        lf_apartment_classes = ["Aparthotel", "Apartment"] ## TODO: extract form ontology
        lf_house_classes = ["House", "GuestHouse", "HolidayHome", "Villa", "Bungalow"] ## TODO: extract form ontology
        lf_boat_classes = ["Boat", "Botel", "HouseBoat"] ## TODO: extract form ontology
        lf_loft_classes = ["Loft"]
        lf_other_classes = ["Hostel", "Hostal", "Riad", "BedAndBreakfast", "Inn", "Resort", "SkiResort"]
        acco_words = set([word.lower() for word in re.split('; |, |\n|-|\s',refined_room_and_property)])
        apartment_words = set(["apartment", "apt", "studio", "house", "loft", "maisonette", "villa", "residence"])
        boat_words = set(["boat", "house", "home"])
        room_words = set(["room", "double", "twin"])
        suite_words = set(["suite"]) 
        
        if room_type_category == "entire_home":   
            if lf_class in lf_house_classes:
                return "EntireHouse"
            elif lf_class in lf_apartment_classes:
                return "EntireApartment"
            elif lf_class in lf_boat_classes:
                return "EntireBoat"
            elif lf_class in lf_loft_classes:
                return "EntireLoft"    
            elif len(acco_words.intersection(suite_words))>0: ## There is a misleading case where a suite is offered even if entire_home is used 
                return "Suite"
            else:
                return "EntirePlace"
        
        if room_type_category == "private_room" or room_type_category == "hotel_room":
            if lf_class in lf_hotel_classes:
                return "HotelRoom"
            else:
                return "Room"
        
        if room_type_category == "shared_room":
            return "SharedRoom"
        
        return "Accommodation"

    def bkg_classify_accommodation(self, lf_class, acco_name):
        lf_hotel_classes = ["Hotel", "CountryHouseHotel", "BunkerHotel", 
                        "UnderWaterHotel", "TreeHouseHotel", 
                        "IceHotel", "CaveHotel", "CapsuleHotel"]
        lf_apartment_classes = ["Aparthotel", "Apartment"]
        lf_house_classes = ["House", "GuestHouse", "HolidayHome", "Villa", "Bungalow"]
        lf_boat_classes = ["Boat", "Botel", "HouseBoat"]
        lf_other_classes = ["Hostel", "Hostal", "Riad", "BedAndBreakfast", "Inn", "Resort", "SkiResort"]
        acco_words = set([word.lower() for word in re.split('; |, |\n|-|\s',acco_name)])
        apartment_words = set(["apartment", "apt", "studio", "house", "loft", "maisonette", "villa", "residence"])
        boat_words = set(["boat", "house", "home"])
        room_words = set(["room", "double", "twin"])
        suite_words = set(["suite"]) 
        if lf_class in lf_apartment_classes:
            if len(acco_words.intersection(apartment_words))>0:
                return "EntireApartment"
            if len(acco_words.intersection(suite_words))>0:
                return "Suite"
            if len(acco_words.intersection(room_words))>0:
                if lf_class == "Aparthotel":
                    return "HotelRoom"
                else:
                    return "Room"
            else:
                return "EntireApartment"
            
        if lf_class in lf_house_classes:
            if len(acco_words.intersection(apartment_words))>0:
                return "EntireHouse"
            if len(acco_words.intersection(room_words))>0:
                    return "Room"
        if lf_class in lf_hotel_classes + lf_boat_classes + lf_other_classes:
            if lf_class in lf_boat_classes:
                #print("Boat found: ",boat_words )
                #print("Acco words: ",acco_words)
                if len(acco_words.intersection(boat_words))>0:
                    return "EntireBoat"
            if len(acco_words.intersection(apartment_words))>0:
                return "EntireApartment"
            if len(acco_words.intersection(suite_words))>0:
                return "Suite"
            if len(acco_words.intersection(room_words))>0:
                if lf_class in lf_hotel_classes:
                    return "HotelRoom"
                else:
                    return "Room"       
        ### no specific Avvommodation found    
        return "Accommodation"