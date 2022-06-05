from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

import re
from dagster import resource
from dagster.config.field import Field

import spacy

LinkedEntity = Dict[str, Any]

DBP_EL_DEFAULT_CONFIG = {
    "spotlight_config": {
        "confidence": 0.6,
        "types": "DBpedia:Activity,DBpedia:Food,DBpedia:Holiday,DBpedia:MeanOfTransportation,DBpedia:Place,Schema:Event,Schema:Place"
    },
    "NER_model": "en_core_web_lg"
}

class AbstractEntityLinker(ABC):
    @abstractmethod
    def entities_extraction(self, text_id: str, text: str) -> Optional[LinkedEntity]:
        pass
    
    def pre_process(self, text): 
        ### FROM: https://github.com/kavgan/nlp-in-practice/tree/master/tf-idf 
        # lowercase
        text=text.lower() 
        #remove tags
        text=re.sub("</?.*?>"," <> ",text)
        # remove special characters and digits
        text=re.sub("(\\d|\\W)+"," ",text)   
        
        # Swaps line breaks for spaces, also remove apostrophes & single quotes
        text.replace("\n", " ").replace("'"," ").replace("’"," ")
        return text

class DBPSEntityLinker(AbstractEntityLinker):
    def __init__(self, config: Optional[dict], logger: Any ) -> None:

        if config is None:
            config = DBP_EL_DEFAULT_CONFIG
        nlp_el = spacy.load(config['NER_model'])
        nlp_el.add_pipe('dbpedia_spotlight',config=config["spotlight_config"])
        self._nlp = nlp_el
        self._logger = logger
    
    def entities_extraction(self, text_id, text):
        try:
            doc=self._nlp(self.pre_process(str(text)))
            # entities = []
            # statuses = []
            # errors = []
            for ent in doc.ents:
                try:
                    entity = {}
                    entity["surface_form"] = ent.text
                    entity["url"] = ent.kb_id_
                    dbpedia_raw_result = ent._.dbpedia_raw_result
                    if dbpedia_raw_result is not None:
                        types = dbpedia_raw_result.get('@types')
                        if type(types) is str:
                            entity["types"] = types.split(",")
                    else:
                        continue
                    # entities.append(entity)
                    yield {"source_id": text_id, "uri": entity["url"], "entity_data": entity} 
                except Exception as e:
                    self._logger.error(str(e))
                    # statuses.append("error")
                    # errors.append(str(e))
                # else:
                #     statuses.append("ok")
                #     errors.append(None)
        except Exception as ex:
            self._logger.error(str(ex))
        #     return (text_id,[],["error"],[str(ex)])
        
        # return (text_id,entities,statuses,errors)

@resource(
    config_schema={"spotlight_config": Field(dict, is_required=False) },
    description="A DBPedia spotlight client to execute entity extraction and linking to DBPedia from text.",
)
def dbp_entity_linker(context):
    spotlight_config = context.resource_config.get("spotlight_config", None)
    return DBPSEntityLinker(spotlight_config, context.log)
