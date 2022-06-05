#from abc import ABC, abstractmethod
from .dbp_spotlight_el import AbstractEntityLinker
from typing import Any, Dict, Optional

import re
from dagster import resource
from dagster.config.field import Field

import spacy

LinkedEntity = Dict[str, Any]

GEONAMES_EL_DEFAULT_CONFIG = {
    "NER_model": "en_core_web_lg"
}

from mordecai import Geoparser
from multiprocessing.pool import ThreadPool
import editdistance
import numpy as np

class TGeoparser(Geoparser):

    def geoparse_by_country(self, doc, country, admin1_filter = None, verbose=False):
        """Main geoparsing function. Text to extracted, resolved entities.

        Parameters
        ----------
        doc : str or spaCy
            The document to be geoparsed. Can be either raw text or already spacy processed.
            In some cases, it makes sense to bulk parse using spacy's .pipe() before sending
            through to Mordecai
        contry: Geonames country code used to filter results from elasticsearch (es. GBR for Great Brittain)
        admin1: Geonames admin1 code used to filter results from elasticsearch (es. ENG for England)

        Returns
        -------
        proced : list of dicts
            Each entity gets an entry in the list, with the dictionary including geo info, spans,
            and optionally, the input features.
        """
        if not hasattr(doc, "ents"):
            doc = self.nlp(doc)
        proced = self.set_country(doc, country, admin1_filter)
        if not proced:
            return []
            # logging!
            #print("Nothing came back from infer_country...")
        if self.threads:
            pool = ThreadPool(len(proced))
            results = pool.map(self.proc_lookup_country, proced)
            pool.close()
            pool.join()
        else:
            results = []
            for loc in proced:
                if self.is_country(loc['word']):
                    # if it's a country name, just query that
                    res = self.query_geonames_country(loc['word'], 
                                                      self._just_cts[loc['word']],
                                                      filter_params={"feature_code": "PCLI"}) 
                    results.append(res)
                # if the confidence is too low, don't use the country info
                elif loc['country_conf'] > self.country_threshold:
                    res = self.query_geonames_country(loc['word'], loc['country_predicted'], filter_params={"admin1": "England"})
                    results.append(res)
                else:
                    results.append("")

        for n, loc in enumerate(proced):
            res = results[n]
            try:
                _ = res['hits']['hits']
                # If there's no geonames result, what to do?
                # For now, just continue.
                # In the future, delete? Or add an empty "loc" field?
            except (TypeError, KeyError):
                continue
            # Pick the best place
            X, meta = self.features_for_rank_basic(loc, res)
            if X.shape[1] == 0:
                # This happens if there are no results...
                continue
            all_tasks, sorted_meta, sorted_X = self.format_for_prodigy(X, meta, loc['word'], return_feature_subset=True)
            # fl_pad = np.pad(sorted_X, ((0, 5 - sorted_X.shape[0]), (0, 0)), 'constant')
            # fl_unwrap = np.asmatrix(fl_pad.flatten())
            # prediction = self.rank_model.predict(fl_unwrap)
            # place_confidence = prediction.max()
            # loc['geo'] = sorted_meta[prediction.argmax()]
            # loc['place_confidence'] = place_confidence
            loc['geo'] = sorted_meta[0]
            loc['place_confidence'] = 1
        if not self.verbose:
            proced = self.clean_proced(proced)
        return proced

    def proc_lookup_country(self, loc):
        if self.is_country(loc['word']):
            # if it's a country name, just query that
            loc = self.query_geonames_country(loc['word'], 
                                                self._just_cts[loc['word']],
                                                filter_params={"feature_code": "PCLI"}) 
            return loc
        if loc['country_conf'] >= self.country_threshold:
            if loc['admin1_filter'] is not None:
                loc = self.query_geonames_country(loc['word'], loc['country_predicted'], filter_params={"admin1_code": loc['admin1_filter']})
            else:
                loc = self.query_geonames_country(loc['word'], loc['country_predicted'])
            return loc
        else:
            return ""

    def set_country(self, doc, country, admin1):
        """NLP a doc, find its entities, get their features, and return the model's country guess for each.
        Maybe use a better name.

        Parameters
        -----------
        doc: str or spaCy
            the document to country-resolve the entities in
        contry: Geonames country code used to filter results from elasticsearch (es. GBR for Great Brittain)
        admin1: Geonames admin1 code used to filter results from elasticsearch (es. ENG for England)

        Returns
        -------
        proced: list of dict
            the feature output of "make_country_features" updated with the model's
            estimated country for each entity.
            E.g.:
                {'all_confidence': array([ 0.95783567,  0.03769876,  0.00454875], dtype=float32),
                  'all_countries': array(['SYR', 'USA', 'JAM'], dtype='<U3'),
                  'country_conf': 0.95783567,
                  'country_predicted': 'SYR',
                  'features': {'ct_mention': '',
                       'ct_mention2': '',
                       'ctm_count1': 0,
                       'ctm_count2': 0,
                       'first_back': 'JAM',
                       'maj_vote': 'SYR',
                       'most_alt': 'USA',
                       'most_pop': 'SYR',
                       'word_vec': 'SYR',
                       'wv_confid': '29.3188'},
                  'label': 'Syria',
                  'spans': [{'end': 26, 'start': 20}],
                  'text': "There's fighting in Aleppo and Homs.",
                  'word': 'Aleppo'}

        """
        if not hasattr(doc, "ents"):
            doc = self.nlp(doc)
        proced = self.make_selected_country_features(doc, country, require_maj=False)
        if not proced:
            pass
        feat_list = []

        for loc in proced:
            loc["admin1_filter"] = admin1
            loc['country_predicted'] = country
            loc['country_conf'] = 1
            loc['all_countries'] = np.array([country])
            loc['all_confidence'] = np.array([1])

        return proced

    def make_selected_country_features(self, doc, country, require_maj=False):
        """
        Create features for the country picking model forcing the country passed by argument.

        Parameters
        -----------
        doc : str or spaCy doc

        Returns
        -------
        task_list : list of dicts
            Each entry has the word, surrounding text, span, and the country picking features.
            This output can be put into Prodigy for labeling almost as-is (the "features" key needs
            to be renamed "meta" or be deleted.)
        """
        if not hasattr(doc, "ents"):
            doc = self.nlp(doc)
        # initialize the place to store finalized tasks
        task_list = []

        # get document vector
        #doc_vec = self._feature_word_embedding(text)['country_1']

        # get explicit counts of country names
        #ct_mention, ctm_count1, ct_mention2, ctm_count2 = self._feature_country_mentions(doc)

        #  pull out the place names, skipping empty ones, countries, and known
        #  junk from the skip list (like "Atlanic Ocean"
        ents = []
        for ent in doc.ents:
            if not ent.text.strip():
                continue
            if ent.label_ not in ["GPE", "LOC", "FAC", "ORG"]:
                continue
            # don't include country names (make a parameter)
            if ent.text.strip() in self._skip_list:
                continue
            ents.append(ent)
        if not ents:
            return []
        
        for n, ent in enumerate(ents):
            # look for explicit mentions of feature names
            class_mention, code_mention = self._feature_location_type_mention(ent)

            # We only want all this junk for the labeling task. We just want to straight to features
            # and the model when in production.

            ### We are creating a fake feature structure forcing the selected country to be used
            try:
                start = ent.start_char
                end = ent.end_char
                iso_label = country
                try:
                    text_label = self._inv_cts[iso_label]
                except KeyError:
                    text_label = ""
                task = {"text" : ent.text,
                        "label" : text_label,  # human-readable country name
                        "word" : ent.text,
                        "spans" : [{
                            "start" : start,
                            "end" : end,
                            }  # make sure to rename for Prodigy
                                ],
                        "features" : {
                                "maj_vote" : country,
                                "word_vec" : country,
                                "first_back" : country,
                                #"doc_vec" : doc_vec,
                                "most_alt" : country,
                                "most_pop" : country,
                                "ct_mention" : country,
                                "ctm_count1" : 0,
                                "ct_mention2" : country,
                                "ctm_count2" : 0,
                                "wv_confid" : 0,
                                "class_mention" : class_mention,  # inferred geonames class from mentions
                                "code_mention" : code_mention,
                                #"places_vec" : places_vec,
                                #"doc_vec_sent" : doc_vec_sent
                                }
                        }
                task_list.append(task)
            except Exception as e:
                print(ent.text,)
                print(e)
        return task_list  # rename this var

    def features_for_rank_basic(self, proc, results):
        """Compute features for ranking results from ES/geonames


        Parameters
        ----------
        proc : dict
            One dictionary from the list that comes back from geoparse or from make_country_features (doesn't matter)
        results : dict
            the response from a geonames query

        Returns
        --------
        X : numpy matrix
            holding the computed features

        meta: list of dicts
            including feature information
        """
        feature_list = []
        meta = []
        results = results['hits']['hits']
        search_name = proc['word']
        code_mention = proc['features']['code_mention']
        class_mention = proc['features']['class_mention']

        for rank, entry in enumerate(results):
            # go through the results and calculate some features
            # get population number and exists
            
            pop = 0
            has_pop = 0
            logp = 0
            ### order the results came back
            adj_rank = 1 / np.log(rank + 2)
            #adj_rank = 1
            # alternative names
            #len_alt = 1
            len_alt = len(entry['alternativenames'])
            adj_alt = np.log(len_alt)
            ### feature class (just boost the good ones)
            # if entry['feature_class'] == "A" or entry['feature_class'] == "P":
            #     good_type = 1
            # else:
            #     good_type = 0
            good_type = 0
                #fc_score = 3
            ### feature class/code matching
            if entry['feature_class'] == class_mention:
                good_class_mention = 1
            else:
                good_class_mention = 0
            #good_class_mention = 0
            if entry['feature_code'] == code_mention:
                good_code_mention = 1
            else:
                good_code_mention = 0
            #good_code_mention = 0
            ### edit distance
            ed = editdistance.eval(search_name, entry['name'])
            inv_ed = 1/(1+ed)  # shrug
            # maybe also get min edit distance to alternative names...
            # features = [has_pop, pop, logp, adj_rank, len_alt, adj_alt,
            #             good_type, good_class_mention, good_code_mention, ed]
            features = [has_pop, pop, logp, adj_rank, len_alt, adj_alt,
                        good_type, good_class_mention, good_code_mention, inv_ed]
            m = self.format_geonames(entry)

            feature_list.append(features)
            meta.append(m)

        #meta = geo.format_geonames(results)
        X = np.asmatrix(feature_list)
        return (X, meta)



class GeonamesEntityLinker(AbstractEntityLinker):
    def __init__(self, config: Optional[dict], logger: Any, country: str = "GBR", admin1: str = "ENG" ) -> None:

        if config is None:
            config = GEONAMES_EL_DEFAULT_CONFIG
        nlp_el = spacy.load(config['NER_model'],disable=['parser'])
        geo = TGeoparser(nlp=nlp_el, training="disable") ## pass unhandled training parameter to disable keras module load in parent Geoparser class
        self.country = country
        self.admin1 = admin1
        self._geo = geo
        self._nlp = nlp_el
        self._logger = logger
    
    def entities_extraction(self, text_id, text):
        entities = self._geo.geoparse_by_country(text, self.country, self.admin1)
        
        for geo_ent in entities:
            if geo_ent.get('geo',False):
                try:
                    entity = {}
                    entity["surface_form"] = geo_ent["word"]
                    entity["url"] = "http://sws.geonames.org/{}/".format(geo_ent["geo"]['geonameid'])
                    entity["entity_linker"] = geo_ent
                except Exception as ex:
                    self._logger.error(str(ex))
                yield {"source_id": text_id, "uri": entity["url"], "entity_data": entity}

el = None

@resource(
    config_schema={"geonames_el_config": Field(dict, is_required=False) },
    description="A class to process texts for NER (Named Entity Recognition) and entity linking against Geonames",
)
def geonames_entity_linker(context):
    global el    
    #if "el" not in globals():
    print("el: ", el)
    if el is None:
        geonames_el_config = context.resource_config.get("geonames_el_config", None)
        el = GeonamesEntityLinker(geonames_el_config, context.log)
    else:
        print("Found el in globals")
    return el
