from owlready2 import Thing, AllDisjoint, locstr, comment, owl_equivalentclass, owl, World
from owlready2.annotation import AnnotationPropertyClass
from owlready2.entity import ThingClass
from collections import OrderedDict
import itertools
import pandas as pd
import re

import logging

text_to_remove = [
    """\n<br /><br />\nSee also the <a href="/docs/hotels.html">dedicated document on the use of schema.org for marking up hotels and other forms of accommodations</a>.\n"""
]


def camel_caser(base):
    result = ''.join(x for x in base.title() if not x.isspace())
    return result
    
def camel_case_split(text: str):
    words = [[text[0]]]
  
    for c in text[1:]:
        if words[-1][-1].islower() and c.isupper():
            words.append(list(c))
        else:
            words[-1].append(c)
  
    return [''.join(word) for word in words]

def produce_label(camel_case_label: str) -> str:
        return ' '.join(camel_case_split(camel_case_label)).capitalize()
        
        # new_label = ""
        # for idx, word in enumerate(camel_case_split(camel_case_label)):
        #     if idx > 0:
        #         word = word.lower()
        #     new_label = new_label + word + " "
        # return new_label[:-1] ## chop last space

def clean_name(base):
    result = re.sub("[`’'/,!@#$>()]", ' ', base)
    return " ".join(result.split()) ## remove multiple spaces


def clear_comment(comment: str) -> str:
    for substring in text_to_remove:
        comment = comment.replace(substring,"")
    return comment

def find_class(name_spaces,class_name):
    for ns in name_spaces:
        class_found = ns[class_name]
        if class_found is not None:
            break
    return class_found, ns ## return found class and related namespace

def create_external_resource(resorce_name, namespace):
    with namespace:
        res = owl.Thing(resorce_name)
    return res



def generate_class_hierarchy(namespace):
    #classes_df = pd.DataFrame(columns=["class_name","parent_classes","owlready_storid"])
    classes = []
    for c in namespace.classes():
        parents = ""
        for parent in c.is_a:
            if type(parent) is ThingClass:
                parents = parents + parent.name +"|"
        parents = parents[:-1]
 
        if c.iri.startswith(namespace.base_iri):
            #classes_df = classes_df.append({"class_name": c.name,"parent_classes": parents,"owlready_storid": c.storid }, ignore_index=True)
            classes.append({"class_name": c.name,"parent_classes": parents,"owlready_storid": c.storid })
    
    classes_df = pd.DataFrame.from_records(classes) 
    return classes_df

class TaoBuilder:
    def __init__(self, world: World):
        self.world = world
        self.acco = world.get_namespace("http://purl.org/acco/ns#")
        self.gr = world.get_namespace("http://purl.org/goodrelations/v1#")
        self.dct = world.get_namespace("http://purl.org/dc/terms/")
        self.skos = world.get_namespace("http://www.w3.org/2004/02/skos/core#")
    
    def acco_feature_class(self, owl_class, ancestor, namespace, copy = True):  
        logging.debug(">>>>>>>> wrapper for %s" % owl_class.name)
        properties = { 
            #"defined_class": True,
            "gr_name":  [locstr(' '.join(camel_case_split(owl_class.name)).capitalize(), lang = "en")],
            "acco_value": [locstr("yes", lang = "en")]        
        }
        with namespace:
            new_class = type(owl_class.name, (self.acco.AccommodationFeature,), {})
            new_class.is_a.append(ancestor)
            new_class.equivalent_to.append(self.acco_feature_equivalent_class(produce_label(owl_class.name))[0])
            logging.debug(new_class.equivalent_to)

            # new_class = type(owl_class.name, (self.acco.AccommodationFeature,), properties)
            # new_class.is_a.append(ancestor)
            # logging.debug(new_class.equivalent_to)
        return new_class
    
    def acco_feature_equivalent_class(self, acco_name: str, lang: str = "en"):
        return [self.acco.AccommodationFeature & self.gr.__getitem__('name').value(locstr(acco_name, lang = lang)) & self.acco.value.value(locstr("yes", lang = 'en'))]
        
    def process_entity(self, name_spaces, entity_name, parent_class_full, entity_class_full, enrich_type, is_amenity, lang = 'en', provenance = None, comment_text = None):
        logging.debug("entity: "+ entity_name,"enrich: " + enrich_type, "class: " + entity_class_full, "parent class: " + parent_class_full)
        if type(name_spaces) != list:
            name_spaces = [name_spaces]


        if enrich_type == "label":
            try:
                entity_class_name = entity_class_full.strip()
                entity_class, ns = find_class(name_spaces, entity_class_name)
            except IndexError:
                raise(Exception("Cannot find the class name in %s" % entity_class_full))

            if entity_class is None:
                raise(Exception("Cannot add label to class %s because it does not exist." % entity_class_name))
            logging.debug("add altLabel %s on class %s" % (entity_name, entity_class))
            if comment_text is None:
                entity_class.altLabel.append(locstr(entity_name, lang = lang))
            else:
                logging.debug("class: ",entity_class)
                comment[entity_class, self.skos.altLabel, locstr(entity_name, lang = lang)] = comment_text
        elif enrich_type == "new_class":
            try:
                parent_class_name = parent_class_full.strip()
                parent_class, ns = find_class(name_spaces, parent_class_name)
            except IndexError:
                raise(Exception("Cannot find the class name in %s" % parent_class_full))

            if parent_class is None:
                raise(Exception("Cannot add the new class %s as subClass for %s because it does not exist." % (entity_name,parent_class_name)))
            logging.debug("add new class %s with ancestor %s" % (camel_caser(entity_name), parent_class))
            with ns:
                new_class = type(camel_caser(clean_name(entity_name)), (parent_class,), {"label": locstr(entity_name, lang = lang)})
                if comment_text is None:
                    new_class.source.append(provenance)
                else:
                    comment[new_class, self.dct.source, provenance] = comment_text
                if is_amenity == "yes":
                    new_class.equivalent_to = self.acco_feature_equivalent_class(clean_name(entity_name)) ## we have an amenity and we want to declare it as equivalent to an accommodation class from acco ontology
        elif enrich_type == "new_individual": 
            try:
                entity_class_name = entity_class_full.strip()
                if entity_class_name=="":
                    raise(Exception("Class name is empty, cannot create individual %s" % entity_name))
                
                entity_class, ns = find_class(name_spaces, entity_class_name)
            except IndexError:
                raise(Exception("Cannot find the class name in %s" % entity_class_full))

            if entity_class is None:
                raise(Exception("Cannot create individual for class %s because the class does not exist." % entity_class_full))
            
            instance = ns[entity_class.name](camel_caser(entity_name))
            logging.debug("add new individual %s of class %s" % (instance.name, instance.is_a))         
            
        else:
            logging.debug("Nothing to do")

        return enrich_type

def annotation_copier(original_class, new_class, namespace, merge = True, default_lang = "en", enforce_lang = True):
    with namespace:
        logging.debug("namespace: %s" % namespace)
        logging.debug("original class %s" % original_class)
        logging.debug("new class %s" % new_class)
        labels = list(original_class.label).copy() 
        comments = list(original_class.comment).copy() 
        for idx, comment in enumerate(comments):
            comments[idx] = clear_comment(comment)
        logging.debug("Existing labels: %s" % list(new_class.label))
        logging.debug("Existing comments: %s" % list(new_class.comment))
        
        if merge:
            logging.debug("### Merging labels and comments")
            for l in labels:
                if enforce_lang and type(l) == str:
                    l = locstr(l, lang = default_lang)
                new_class.label.append(l)
            for comment in comments:
                if enforce_lang and type(comment) == str:
                    comment = locstr(comment, lang = default_lang)
                new_class.comment.append(comment) 
        else:
            new_class.label = labels
            new_class.comment = comments
            
        logging.debug("Final labels: %s" % list(new_class.label))
        logging.debug("Final comments: %s" % list(new_class.comment))
        return
    
def labeller_by_class_name(original_class, new_class, namespace, merge = True, default_lang = "en"):
    with namespace:
        logging.debug("adding (%s) label to class %s using original class name %s" % (default_lang, new_class, original_class.name)) 
        new_label = produce_label(original_class.name)
        new_class.label.append(locstr(new_label, lang = default_lang))
        return
        
        
def provenance_annotator(original_class, new_class, namespace, merge = True):
    with namespace:
        logging.debug("Existing dc:source: %s" % list(new_class.source))
        
        if merge:
            logging.debug("### Merging dc:source")
            new_class.source.append(original_class)
        else:
            new_class.source = original_class
        
        logging.debug("Final dc:source: %s " % list(new_class.source))

def base_class(owl_class, ancestor, namespace, copy = True):
    with namespace:
        
        if copy:
            logging.debug("copying")
            new_class = type(owl_class.name, (ancestor,), {})            
        else:
            logging.debug("not coping")
            owl_class.is_a.append(ancestor) ## in this way the subclass relation is created in our namespace
            new_class = owl_class            
    return new_class        

def point_new_to_old_class(owl_class, dummy_ancestor, dummy_namespace, copy = True):
    return owl_class ### we use this wrapper to traverse the original class hierarchy 
    

def base_class_merge(owl_class, ancestor, namespace, copy = True, merge = False):
    with namespace:
        if copy:
            logging.debug("copying")
            existing_class = namespace.__getitem__(owl_class.name)
            if existing_class is None: ## the class does not exist yet   
                logging.debug("The class does not exist. Copying.")
                new_class = type(owl_class.name, (ancestor,), {})
            else:
                logging.debug("#### class already defined with id: %s ###########" % existing_class.storid)
                if not merge:
                    logging.debug("We are not merging but replacing the existing class with the new one")
                    new_class = type(owl_class.name, (ancestor,), {})
                else:
                    logging.debug("Skipping the class creation")
                    new_class = existing_class ## We return the existing class
                    
        else:
            logging.debug("not copying")
            owl_class.is_a.append(ancestor) ## in this way the subclass relation is created in our namespace
            new_class = owl_class            
    return new_class

def apply_generic_properties(owl_class, ancestor, namespace, copy, properties, text_comment = "Created by ontology enricher.", class_builder=base_class):
    new_class = class_builder(owl_class, ancestor, namespace, copy)
    ## We can only use Annotation Properties with a class or it would be considered also
    ## and individual and the ontology bacome OWL FULL - see http://wonderweb.man.ac.uk/owl/rdf-appendix.shtml
    
    if type(properties) is AnnotationPropertyClass:
        properties = [properties]
    if type(properties) is not list:
        raise Exception("parameter properties must be a string (single property) or a list of owlready2.prop.AnnotationPropertyClass")
    with namespace:
        for owl_property in properties:
            if type(owl_property) is not AnnotationPropertyClass:
                raise Exception("%s is not a owlready2.prop.AnnotationPropertyClass" % owl_property)
            logging.debug("Applying property: %s" % owl_property.name)
            if type(text_comment) is str and len(text_comment)>0:
                comment[new_class, owl_property, owl_class] = text_comment
            else:
                getattr(new_class, owl_property.get_python_name()).append(owl_class)
    return new_class

def owl_equivalentclass_wrapper(owl_class, ancestor, namespace, copy = True):
    new_class = base_class(owl_class, ancestor, namespace, copy)
    #comment[new_class, skos.exactMatch, owl_class] = "Created by ontology enricher"
    with namespace:
        comment[new_class, owl_equivalentclass, owl_class.iri] = "Created by ontology enricher"  
        #new_class.skos_exactMatch = original_class
    return new_class

def produce_list(original_list, exclude_list, include_list):
    if include_list is not None:
        #print("original_list: ",original_list)
        filtered_set = set(original_list).intersection(set(include_list))
        #print("filtered_set", filtered_set)
        only_disjoint = set(original_list).intersection(set(exclude_list).intersection(set(include_list))) ### classes excluded from recursion but to use in disjoints
    else:
        filtered_set = set(original_list)
        only_disjoint = set(original_list).intersection(set(exclude_list)) ### classes excluded from recursion but to use in disjoints
    
    difference = list(filtered_set - set(exclude_list)) ### classes to be examined recursivelly
    
    return difference, only_disjoint

def produce_list2(original_list, exclude_list, include_list):
    return list_subtraction(original_list, exclude_list)
    
def list_subtraction(li1, li2):
    difference = list(set(li1) - set(li2))
    return difference

def minimal_disjoin_set(to_disjoin, incompatibles_list):
    incompatibles = set()
    for inc in incompatibles_list:
        relevant_incompatibles = to_disjoin.intersection(set(inc))
        if len(relevant_incompatibles)>0:
            incompatibles.add(frozenset(relevant_incompatibles))
    
    all_together = set().union(*incompatibles) ## merge of all inconmpatible classes
    
    logging.debug("###### Incompatibles: %s " % incompatibles)
    #compatibles = list(itertools.product(*incompatibles)) ## we produce all combination of element which can be disjoint picking one of each incompatible elements in the list of tuples
    
    all_together = set().union(*incompatibles)
    compatibles_candidates = set(itertools.product(*incompatibles)) ## we produce all combination of element which can be disjoint picking one of each incompatible elements in the list of tuples
    

    cleaned_from_incompatibles = to_disjoin - all_together
    all_disjoints = []
    compatibles = []
    for comp in compatibles_candidates:
        reject = False
        for incomp in incompatibles:
            if len(set(incomp).intersection(comp))>1:
                reject = True
                break
        if not reject:
            compatibles.append(set(comp))
            all_disjoints.append(  cleaned_from_incompatibles.union(set(comp)) )
    
    logging.debug("###### Compatibles: %s " % compatibles)
    
    if len(all_disjoints) == 0: ## if there are no additions to the set of class to disjoin we use the original list removing all incompatible classes
        all_disjoints.append(set(cleaned_from_incompatibles))
    
    #all_disjoints = []
    #for comp in compatibles:
    #    all_disjoints.append(set(to_disjoin)-set(comp))
    
    ## Test if we have incompatible classes in the same disjoint set
    for disj in all_disjoints:
        for incomp in incompatibles:
            if len(set(incomp).intersection(disj))>1:
                raise(Exception("Incompatible classes in disjoint set %s" % disj) )
    
    sets = list(set(frozenset(item) for item in all_disjoints)) ## discard all repeated sets
    
    list_by_size = OrderedDict() ### we create a dictionary where the key is the set size and the value a list ok sets with that size
    for l in sets:
        size = len(l)
        if size in list_by_size.keys():  
            list_by_size[size].append(l) ## the key exists so we just append the set to the associated list
        else:
            list_by_size[size] = [l] ## key does not exist so we add it to the dictionary and populate the list with the first set
    
    ## If a set is included in a bigger set we do not use it and use the bigger one instead
    skeys = sorted(list_by_size) ## We create a list of sets' sizes in ascending order
    minimal_sets = []
    for idsk,size_key in enumerate(skeys):
        bigger_sets_keys = [skeys[i] for i in range(idsk+1,len(skeys))] ## bigger sets indexes in the skeys list
        for ssk, smaller_set in enumerate(list_by_size[size_key]): ## each set in the current examined size
            found = False
            for bsk in bigger_sets_keys:
                if found:
                    break
                #logging.debug("Bigger sets key:",bsk)
                for bigger_set in list_by_size[bsk]:
                    intersec = smaller_set.intersection(bigger_set)
                    found = intersec==smaller_set ## the intersection is the same as the smaller set thus it is included in the bigger one
                    #use_set[size_key][ssk] = not(found)
                    #logging.debug("current set:", smaller_set, "-- bigger set:",bigger_set, "-- intersection:", intersec, " -- included:", found)
                    if found:
                        logging.debug("Set %s already included in a bigger one" % smaller_set)
                        break
            if not found:
                minimal_sets.append(smaller_set) ## if the smaller set was not included in any of the bigger ones we use it
    return minimal_sets

def disjoin_class_hyerarchy(old_ancestor, hyerarchy = {}, exclude_disjoint = [], namespace = None, exclude = [], include = None):
    ### We can use duplicate_class_hyerarchy function to just apply disjoint Axioms to a class hyerarchy
    duplicate_class_hyerarchy(old_ancestor, hyerarchy = hyerarchy, new_ancestor = old_ancestor, 
          marshallers = None, 
          class_wrapper = base_class, disjoint = True, exclude_disjoint = exclude_disjoint,
          namespace = namespace, copy = None,
          exclude = exclude,
          include = include)

def duplicate_class_hyerarchy(old_ancestor, hyerarchy = {}, new_ancestor = Thing, 
                              marshallers = [annotation_copier, provenance_annotator], 
                              class_wrapper = base_class, disjoint = False, exclude_disjoint = [],
                              namespace = None, copy = True,
                              exclude = [], include = None):
    if namespace is None:
        raise(Exception("Working namespace for hierarchy copy destination is None."))
    complete_old_ancestor_subclasses = list(old_ancestor.subclasses())
    old_ancestor_subclasses, to_disjoint_set = produce_list(complete_old_ancestor_subclasses, exclude, include) ### get all subclasses of the given starting class
    logging.debug("Complete ancestor subclasses: %s" % complete_old_ancestor_subclasses)
    logging.debug("Escluded classes: %s " % exclude)
    logging.debug("Remaining subclasses: %s" % list(old_ancestor_subclasses))
    #exclude_set = set(exclude)
    
    ## class excluded  from hyerarchy exploration but not for disjoin
    #retain_for_disjoin = exclude_set.intersection(set(complete_old_ancestor_subclasses))
    #disjoint_set = set(retain_for_disjoin) ## prepare a set to collect all classes at this level just once
    disjoint_set = to_disjoint_set              

    if len(old_ancestor_subclasses) > 0: ## we have some subclasses
        hyerarchy[old_ancestor.name+"_subclasses"] = {}  ### add a ne key to the hierarchy dictionary using the starting class name
        sub_hyerarchy = hyerarchy[old_ancestor.name+"_subclasses"] ## pointer to the sub dictionary relative to the starting class
    else:
        sub_hyerarchy = hyerarchy ## we dont't have subcasses just add new keys at the current level of the hierarchy
    logging.debug("########## Start processing children of: %s ##############" % old_ancestor)
    for c in old_ancestor_subclasses:  ## for each subclass of the starting class we go deeper
        logging.debug("-----> processing source class %s, storeid %s" % (c.name, c.storid))
        new_class = class_wrapper(c, new_ancestor, namespace, copy = copy) ## process the subclass copy or just attaching it to the new ancestor
        logging.debug("--- new class storeid %s" % new_class.storid)
        logging.debug("---------> done class %s" % c.name)
        logging.debug("%s is a %s" % (new_class.iri, new_class.is_a))
        sub_hyerarchy[c.name] = new_class ## add the class name as a key in the hierarchy dictionary
        if marshallers is not None: ## apply custom transformation to the new or just attached class
            for marshaller in marshallers:
                marshaller(c, new_class, namespace)
        disjoint_set.add(new_class) ## add the new/attached class to the disjoint set at this level of the hierarchy
        ### we go deeper and use the just created/attached class (new_class) as the new ancestor and the current subclass as the starting class
        duplicate_class_hyerarchy(c, sub_hyerarchy, new_class, 
                                 marshallers = marshallers, class_wrapper = class_wrapper, 
                                 disjoint = disjoint, exclude_disjoint = exclude_disjoint,
                                  namespace = namespace, copy = copy,
                                 exclude = exclude, include = include)
    if disjoint and len(disjoint_set) > 0:
        exclude_set = set(exclude_disjoint["*"])  # remove all classe that must not be used in any disjoin
        cleaned_disjoint_set = disjoint_set - set(exclude_set) 
        logging.debug("disjoint processing for subclasses of: %s" % old_ancestor)
        logging.debug("original disjoint set: %s" % disjoint_set)
        logging.debug("exclude form disjoin: %s" % set(exclude_set))
        logging.debug("processing disjoint set: %s" % cleaned_disjoint_set)
        logging.debug("do not disjoint: %s" % exclude_disjoint["do_not_disjoin"])
        disjoint_subsets = minimal_disjoin_set(cleaned_disjoint_set, exclude_disjoint["do_not_disjoin"])
        for to_disjoin in disjoint_subsets:
            logging.debug("disjoint for subclasses of %s -> %s " % (new_ancestor, to_disjoin))
            with namespace:
                AllDisjoint(to_disjoin)