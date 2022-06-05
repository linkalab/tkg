#import spacy
from functools import wraps
from dagster import resource
import en_core_web_lg
from spacy_langdetect import LanguageDetector
from spacy.language import Language
#import math
import pandas as pd

## inspired by: https://stackoverflow.com/questions/18603270/progress-indicator-during-pandas-operations/18603549#18603549
def count_wrapper(func,total, print_at, logger):
    @wraps(func)
    def wrapper(*args):
        wrapper.count += 1
        if wrapper.count % wrapper.print_at == 0:
            logger.info( "completed {0:.0f}% - {1}/{2} ".format(100*wrapper.count/wrapper.total, wrapper.count, wrapper.total) )
        return func(*args)
    wrapper.count = 0
    wrapper.total = total
    wrapper.print_at = print_at

    return wrapper

@Language.factory('language_detector')
def language_detector_spacy(nlp, name):
    return LanguageDetector()

class LocalLanguageDetector:
    def lang_detect(self, field, lang_col,lang_score_col):
        if type(field) is not str and pd.isnull(field):
            return pd.Series({lang_col: float('nan'), lang_score_col: float('nan')})        
        text=str(field)
        text_model = self.nlp(text.lower()) ### if upper case words are used the language detection in less accurate
        return pd.Series({lang_col:text_model._.language["language"], lang_score_col:text_model._.language["score"]})

    def analyze(self, df: pd.DataFrame, text_column: str, log_each_n: int = None, 
        lang_column: str = "lang", lang_score_column = "score") -> pd.DataFrame:
        num_rows = df.shape[0]
        if log_each_n is None:
            lang_detect = self.lang_detect
        else:
            lang_detect = count_wrapper(self.lang_detect, num_rows, log_each_n, self.logger)
        
        # df_lang = df.merge(df[text_column].apply(lambda s: lang_detect(s,lang_column,lang_score_column)), left_index=True, right_index=True)
        df_lang = df[text_column].apply(lambda s: lang_detect(s,lang_column,lang_score_column))
        return df_lang

    def __init__(self, logger) -> None:       
        nlp = en_core_web_lg.load(disable=["tagger","ner","lemmatizer"])
        #TODO: cambiare il caricamento come sotto
        #nlp = spacy.load(config['NER_model'],disable=["tagger","ner","lemmatizer"])
        nlp.max_length = 2000000
        nlp.add_pipe('language_detector', last=True)
        self.nlp = nlp
        self.logger = logger

@resource
def language_detector(init_context):
    return LocalLanguageDetector(init_context.log)