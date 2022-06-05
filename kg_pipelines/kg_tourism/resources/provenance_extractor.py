from dagster import DagsterInstance
from dagster.core.storage.event_log import EventRecordsFilter
from dagster import AssetKey, DagsterEventType 
from dagster import PipelineRunStatus
from dagster.core.storage.pipeline_run import PipelineRunsFilter

from kg_tourism.resources.dl_io_manager import KnowledgeGraph

from datetime import datetime
import os
import json

import kglab
import rdflib
from icecream import ic

os.environ["DAGSTER_HOME"] = os.getenv('DAGSTER_HOME') #"/opt/dagster/dagster_home_kg_d0.14.3/"

instance = DagsterInstance.get() # needs your DAGSTER_HOME to be set

#assets = instance.get_asset_keys()

named_graph = "http://tourism.kg.linkalab/graph/meta"


resource_name_template = "{resource}-{version_name}"

namespaces = {
    "dataset": "http://tourism.kg.linkalab-cloud.com/meta/dataset/",
    "tmeta": "http://tourism.kg.linkalab-cloud.com/meta/", 
    "void": "http://rdfs.org/ns/void#", # Vocabulary of Interlinked Datasets (VoID) - http://vocab.deri.ie/void
    "pav": "http://pav-ontology.github.io/pav/", #Provenance, Authoring and Versioning Ontology - http://pav-ontology.github.io/pav/
    "swo": "http://www.ebi.ac.uk/swo/", # Software ontology - https://obofoundry.org/ontology/swo.html
    "iao": "http://purl.obolibrary.org/obo/" #  Information Artifact Ontology (IAO) - https://obofoundry.org/ontology/iao.html

    
    }

def get_resource_name(mapping_dict, template = resource_name_template):
    resource_name = template.format(**mapping_dict)
    return resource_name

    
data_provenance_input = {
    "dataset": {
        "input": [],
        "output": [],
    },
    
    "agent": {},
    "activity": {
        
    }
}

class ProvExtractor():
    def __init__(self, kg: KnowledgeGraph = None):
        self.instance = DagsterInstance.get()
        self.provenance_data = {
            "input": [],
            "output": [],
            "agent": {},
            "activity": {}
        }
        if kg is None:
            self.kg = KnowledgeGraph(
                name = "Provenance graph",
                namespaces = namespaces
            )
        else:
            self.kg = kg
        self.n = self.kg.get_ns

    def load_asset_provenance(self, asset):
        am, _, _  = self.get_last_asset_materialization(asset)
        self.get_parent_assets(am)

        self.provenance_data["output"] = [self.get_entity_from_asset(am)]
        #print(entity)

        input_entities = []
        for am_input, _, _ in self.get_parent_assets(am):
            #print(self.get_asset_unique_name(am_input))
            input_entity = self.get_entity_from_asset(am_input)
            input_entities.append(input_entity)

        self.provenance_data["input"] = input_entities
        self.provenance_data["agent"] = self.get_agent_from_asset(am)
        self.provenance_data["activity"] = self.get_activity_from_asset(am)
    
    def create_triple_provenance(self, asset = None):
        if asset is not None:
            self.load_asset_provenance(asset)

        ### Add prov Entities for datasets
        datasets = self.provenance_data["input"] + self.provenance_data["output"]
        for entity_metadata in datasets:
            resource_name = get_resource_name(entity_metadata)
            base_dataset = entity_metadata["resource"]
            try:
                lang = entity_metadata["label-lang"]
            except KeyError:
                lang = "en"
            self.kg.add(self.n("dataset")[base_dataset], self.n("rdf").type, self.n("prov").Entity)
            self.kg.add(self.n("dataset")[resource_name], self.n("rdf").type, self.n("prov").Entity)
            self.kg.add(self.n("dataset")[resource_name], self.n("prov").specializationOf, self.n("dataset")[base_dataset])
            self.kg.add(self.n("dataset")[resource_name], self.n("rdf").label, 
                rdflib.Literal(entity_metadata["label"], lang=lang))
            self.kg.add(self.n("tmeta")[resource_name], self.n("owl").versionInfo, rdflib.Literal(str(entity_metadata["version_number"])))
            self.kg.add(self.n("tmeta")[resource_name], self.n("pav").version, rdflib.Literal(str(entity_metadata["version_number"])))

            
            entity_metadata["rdflib_uri"] = self.n("dataset")[resource_name]
            #print(file, resource_name)
        
        ### Add prov Agent
        agent_metadata = self.provenance_data["agent"]
        resource_name = get_resource_name(agent_metadata)
        try:
            lang = agent_metadata["label-lang"]
        except KeyError:
            lang = "en"
        self.kg.add(self.n("tmeta")[resource_name], self.n("rdf").type, self.n("prov").Agent)
        self.kg.add(self.n("tmeta")[resource_name], self.n("rdf").type, self.n("swo").SWO_0000001) ## Is a software 
        
        self.kg.add(self.n("tmeta")[resource_name], self.n("rdf").label, 
            rdflib.Literal(agent_metadata["label"], lang=lang))
        #TODO: model using the software ontology (https://obofoundry.org/ontology/swo.html) property "uses software" - http://www.ebi.ac.uk/swo/SWO_0000082
        #This property allows the linkage of two different pieces of software such that one directly executes or uses the other. The has_part relationship should instead be used to describe related but independent members of a larger software package, and &apos;uses platform&apos; relationship should be used to describe which operating system(s) a particular piece of software can use.
        
        #self.kg.add(self.n("tmeta")[resource_name], self.n("owl").sameAs, rdflib.URIRef(agent_metadata["software_uri"]))
        
        try:
            softwares = agent_metadata["uses_software"]
            for software in softwares:
                software_name = software["name"]
                software_version = software["version"]
                software_uri = software["uri"]
                self.kg.add(self.n("tmeta")[software_name], self.n("rdf").type, self.n("swo").SWO_0000001)
                self.kg.add(self.n("tmeta")[resource_name], self.n("swo").SWO_0000082, self.n("tmeta")[software_name]) ### Activity - uses software - Software
                self.kg.add(self.n("tmeta")[software_name+"_"+software_version], self.n("rdf").type, self.n("iao").IAO_0000129) ## IAO_0000129: Version number
                self.kg.add(self.n("tmeta")[software_name], self.n("swo").SWO_0004000, self.n("tmeta")[software_name+"_"+software_version]) ## Software - has version - Version number
                self.kg.add(self.n("tmeta")[software_name], self.n("swo").SWO_0004006, rdflib.Literal(str(software_uri))) ## Software - has website homepage - Website url

        except Exception as e:
            raise(e)

        self.kg.add(self.n("tmeta")[resource_name], self.n("owl").versionInfo, rdflib.Literal(agent_metadata["version_number"]))
        self.kg.add(self.n("tmeta")[resource_name], self.n("pav").version, rdflib.Literal(agent_metadata["version_number"]))
        agent_metadata["rdflib_uri"] = self.n("tmeta")[resource_name]

        ### Add prov Activity
        activity_metadata = self.provenance_data["activity"]
        resource_name = activity_metadata["resource"]
        try:
            lang = activity_metadata["label-lang"]
        except KeyError:
            lang = "en"
        self.kg.add(self.n("tmeta")[resource_name], self.n("rdf").type, self.n("prov").Activity)
        self.kg.add(self.n("tmeta")[resource_name], self.n("rdf").label, 
            rdflib.Literal(activity_metadata["label"], lang=lang))
        activity_metadata["rdflib_uri"] = self.n("tmeta")[resource_name]

        self.__add_pipeline_step_provenance()

    def __add_pipeline_step_provenance(self):
        used_entities = [ e["rdflib_uri"] for e in self.provenance_data["input"]]
        generated_entities = [ e["rdflib_uri"] for e in self.provenance_data["output"] ]
        activity = self.provenance_data["activity"]["rdflib_uri"]
        time_start = self.provenance_data["activity"]["startedAtTime"]
        time_end = self.provenance_data["activity"]["endedAtTime"]
        agent = self.provenance_data["agent"]["rdflib_uri"]

        if type(used_entities) is rdflib.term.URIRef:
            used_entities = [used_entities]
        if type(generated_entities) is rdflib.term.URIRef:
            generated_entities = [generated_entities]    
        
        for used_entity in used_entities:
            self.kg.add(activity, self.n("prov").used, used_entity)
            
        for gen_entity in generated_entities:
            if type(activity) is rdflib.term.URIRef:
                self.kg.add(activity, self.n("prov").generated, gen_entity)
            if type(agent) is rdflib.term.URIRef:
                self.kg.add(gen_entity, self.n("prov").wasAttributedTo, agent)
            for used_entity in used_entities:
                ic([gen_entity, used_entity])
                self.kg.add(gen_entity, self.n("prov").wasDerivedFrom, used_entity)
            
        if type(agent) is rdflib.term.URIRef and type(activity) is rdflib.term.URIRef:
            self.kg.add(activity, self.n("prov").wasAsociatedWith, agent)
        
        if time_start is not None and time_end is not None:
            self.kg.add(activity, self.n("prov").startedAtTime, rdflib.Literal(time_start, datatype=self.n("xsd").dateTime))
            self.kg.add(activity, self.n("prov").endedAtTime, rdflib.Literal(time_end, datatype=self.n("xsd").dateTime))  

    def get_step_elapsed(self, step_store_id: int):
        start = instance.get_event_records(
            EventRecordsFilter(
                event_type=DagsterEventType.STEP_START,
                after_cursor = step_store_id - 1,
                before_cursor= step_store_id + 1,
             ),
            ascending=False,
            limit=1, ## takes the most recent materialization
        )
        start_ts = start[0].event_log_entry.timestamp

        end = instance.get_event_records(
            EventRecordsFilter(
                event_type=DagsterEventType.STEP_SUCCESS,
                after_cursor = step_store_id - 1,
                before_cursor= step_store_id + 1,
             ),
            ascending=False,
            limit=1, ## takes the most recent materialization
        )
        end_ts = end[0].event_log_entry.timestamp

        return start_ts, end_ts

    def get_last_asset_materialization(self, asset):
        am = self.instance.get_event_records(
            EventRecordsFilter(
                event_type=DagsterEventType.ASSET_MATERIALIZATION,               
                asset_key=AssetKey(asset),
            ),
            ascending=False,
            limit=1, ## takes the most recent materialization
        )[0]
        am_storage_id = am.storage_id
        am_run_id = am.event_log_entry.run_id

        return am, am_storage_id, am_run_id


    def get_agent_metadata(self, am):
        meta_dict = {}
        for meta_entry in am.event_log_entry.dagster_event.event_specific_data.materialization.metadata_entries:
            if meta_entry.label == "agent_meta":
                meta_dict = meta_entry.entry_data.data

                # try:
                #     meta_dict = json.loads(meta_entry.entry_data.data)
                # except json.decoder.JSONDecodeError:
                #     meta_dict = meta_entry.entry_data.data
        return meta_dict

    def get_asset_metadata(self, am):
        meta_dict = {}
        for meta_entry in am.event_log_entry.dagster_event.event_specific_data.materialization.metadata_entries:
            if meta_entry.label == "table_meta":
                meta_dict = meta_entry.entry_data.data
        return meta_dict

    def get_asset_run_id(self, am):
        return am.event_log_entry.run_id
    
    def get_asset_store_id(self, am):
        return am.storage_id

    def get_asset_key_path(self, am):
        return am.event_log_entry.dagster_event.event_specific_data.materialization.asset_key.path

    def get_asset_unique_name(self, am):
        return "__".join(self.get_asset_key_path(am))

    def get_activity_from_asset(self, am):
        activity = {
            "resource": None,
            "label": None,
            "label-lang": None,
            "startedAtTime": None,
            "endedAtTime": None
        }

        agent_meta = self.get_agent_metadata(am)
        activity["resource"] = am.event_log_entry.pipeline_name + "__" + am.event_log_entry.step_key + "__" + am.event_log_entry.run_id
        activity["label"] = "Run of step " + agent_meta["name"]
        activity["label-lang"] = "en"
        start, end = self.get_step_elapsed(am.storage_id)
        activity["startedAtTime"] = datetime.fromtimestamp(start).isoformat()
        activity["endedAtTime"] = datetime.fromtimestamp(end).isoformat()

        return activity

    def get_agent_from_asset(self, am):
        agent =  {
            "resource": None,
            "version_name": None,
            "version_number": None,
            "label": None,
            "label-lang": None,
            "software_uri": None,
        }

        agent_meta = self.get_agent_metadata(am)
        agent["resource"] = agent_meta["name"]
        agent["version_name"] = agent_meta["version"]
        agent["version_number"] = agent_meta["version"]
        agent["label"] = "Step " + agent_meta["name"]
        agent["label-lang"] = "en"
        uses_software = agent_meta["tags"].get("uses_software", [])
        try:
            agent["uses_software"] = json.loads(uses_software)
        except json.decoder.JSONDecodeError:
            agent["uses_software"] = uses_software
        return agent


    def get_entity_from_asset(self, am):
        entity = {
            "resource": None,
            "resorce_suffix": None,                     
            "label": None,
            "label-lang": None,
            "version_name": None,
            #"prov_base_uri": "http://tourism.kg.linkalab-cloud.com/meta/dataset/",
            "version_number": None,
            "local_filename": None
        }
        
        
        asset_meta = self.get_asset_metadata(am)
        
        #asset_key_path = self.get_asset_key_path(am)
        try:
            entity["resource"] = self.get_asset_unique_name(am)
            entity["label"] = asset_meta["label"]
            entity["label-lang"] = asset_meta["label-lang"]
            entity["resorce_suffix"] = "."+asset_meta["extension"]
            entity["version_name"] = am.event_log_entry.run_id
            entity["version_number"] = am.event_log_entry.timestamp
        except KeyError:
            print("Asset_meta: ",asset_meta)
            raise(Exception("Error getting provenance metadata for asset %s" % entity["resource"] ))

        return entity

    def get_parent_assets(self, am):
        for asset_lineage_info in am.event_log_entry.dagster_event.step_materialization_data.asset_lineage:
            #print(asset_lineage_info.asset_key.path)
            input_asset_key = asset_lineage_info.asset_key.path
            yield self.get_last_asset_materialization(input_asset_key)
    

from dagster.config.field import Field
from dagster.core.definitions import resource

@resource(
    #required_resource_keys = {"dl_config"},
    description="An object that produces provenance triples for an asset.",
)
def provenance_extractor(context): 
    return ProvExtractor()
