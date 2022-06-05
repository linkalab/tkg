import re
from datetime import datetime


def try_parsing_date(time_input):
    time_str = '-'.join(str(time_input).replace(","," ").split())
    date_formats = [
        '%Y-%m-%d', '%d.%m.%Y', '%d/%m/%Y','%A-%B-%d-%Y', '%A-%d-%B-%Y', '%Y-%m-%d-%H:%M:%S',
        '%B-%d-%Y', '%d-%B-%Y'
    ]
    #August-2-2019
    for fmt in date_formats:
        try:
            return datetime.strptime(time_str, fmt)
        except ValueError:
            pass
    print("Error on date: ", time_str)
    raise ValueError('no valid date format found')

def convert_time(time_input):
    time_str = '-'.join(str(time_input).replace(","," ").split())
    try:
        time = datetime.strptime(time_str, '%A-%B-%d-%Y')
    except ValueError:
        #print("errore: ",time_str)
        try:
            time = datetime.strptime(time_str, '%A-%d-%B-%Y')
        except ValueError:
            try:
                time = datetime.strptime(time_str, '%Y-%m-%d-%H:%M:%S')
            except:
            #print(" ---- errore: ",time_str)
                raise(ValueError)
    return time

def camel_caser(base):
    result = ''.join(x for x in base.title() if not x.isspace())
    return result

def clean_name(base):
    ## AirBnb amenities name can have a comment after the real name
    ## e.g. "keypad - check yourself into the home with a door code"
    ## e.g. "clothing storage: closet"
    ## we take just the first part
    
    ## we remove the "'s" strings also
    base_left = base.split(" - ")[0].split(": ")[0].replace("’s","") 
    
    
    result = re.sub("['/,!@#$>()]", ' ', base_left)
    return " ".join(result.split()) ## remove multiple spaces

def build_class_name(entity, class_and_ancestor, map_type):   
    if map_type == "label" or map_type == "same":  ## the mapping just added a label to an existing class let's return the enriched class name
        class_name = str(class_and_ancestor).split(",")[1].strip()
        return class_name
    if map_type == "new_class": ## the mapping added a new class using the camel case version of the entity name
        new_class_name = camel_caser(clean_name(entity))
        return new_class_name
    else:
        raise Exception('Unknown mapping type: %s' % map_type)