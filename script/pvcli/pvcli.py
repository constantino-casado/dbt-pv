import time
import attr  
import sys
import os
import requests, json
import numpy
from enum import Enum
from typing import Union, Type, Optional, Dict, List
from pvcli.run import RunEvent, RunState, Run, Job
import logging
import asyncio
import yaml


from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData


async def send2eventhub(Events: List):
    Connstr = os.environ.get("CONNECTION_STR")
    Ev_name = os.environ.get("EVENTHUB_NAME")
    if Ev_name:
        producer = EventHubProducerClient.from_connection_string(conn_str=Connstr, eventhub_name=Ev_name)
        async with producer:
        # Create a batch.
            event_data_batch = await producer.create_batch()
        # Add events to the batch.
            for event in Events:
                devent = Serde.to_dict(event) 
                devent['SendTime'] = time.localtime()
                event_data_batch.add(EventData(json.dumps(devent)))
        # Send the batch of events to the event hub.
            await producer.send_batch(event_data_batch)

    return

log = logging.getLogger("dbtol")
log.setLevel("INFO")

class Transport:
    kind = None
    config = dict
    def emit(self, event: RunEvent):
        raise NotImplementedError()


class TransportFactory:
    def create(self) -> Transport:  # type: ignore
        pass

class PurviewClient():
    account = "PurviewName"
    token = None
    transport = None
    lineage = {}
    def __init__(self, transport: Optional[Transport] = None,):
        self.account = os.environ.get("PURVIEW_NAME") 
        self.transport = transport
        self.token = self.getToken()
        self.api = f"https://{self.account}.purview.azure.com/catalog/api/"
        return
    def getToken(self):
        client_id = os.environ.get("AZURE_CLIENT_ID") 
        client_secret = os.environ.get("AZURE_CLIENT_SECRET")
        tenant_id = os.environ.get("AZURE_TENANT_ID") 
        resource_url = "https://purview.azure.net"
        url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
        payload = f'grant_type=client_credentials&client_id={client_id}&client_secret={client_secret}&resource={resource_url}'
        headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
        }
        response = requests.request("POST", url, headers=headers, data=payload)
        # print(json.loads(response.text)['access_token'])
        return json.loads(response.text)['access_token']


    
    def send2pv(self,processor):
        itemlist = self.buildpvdata(processor)
        products = processor.loadproducts()
        for item in itemlist:
            attributes = {}
            outputentity = {}
            attributes['qualifiedName'] = item['fqn']
            outputentity['typeName'] = item['type']
            attributes['name'] = item['name']
            attributes['description'] = f"dbt process to generate the model: {item['model']}"
            outputentity['attributes'] = attributes
            outguid = self.getentityguid(outputentity)
            if outguid == -1:
                outputentity['guid'] = '-1'
                outguid = self.writeentity(outputentity)['guidAssignments']['-1']
            inputguids = []
            for entity in item['inputs']:
                iguid = self.getentityguid(entity)
                if iguid == -1:
                    entity['guid'] = '-1'
                    iguid = self.writeentity(entity)['guidAssignments']['-1']
                inputguids.append({"guid":iguid})
            # print("-----------input------------")
            # print(json.dumps(item['inputs']))
            attributes = {}
            pvprocess = {}
            attributes['qualifiedName'] = f"dbtprocess-{item['package']}-{item['target']}-{item['model']}"
            pvprocess['typeName'] = 'databricks_process'
            attributes['name'] = f"dbtprocess-{item['package']}"
            attributes['description'] = f"dbt process to generate the model: {item['model']}"
            attributes['inputs'] = inputguids
            attributes['outputs'] = [{"guid":outguid}]
            pvprocess['attributes'] = attributes
            pvprocess['guid'] = '-1'
            # print(json.dumps(pvprocess))
            procguid = self.writeentity(pvprocess)['guidAssignments']['-1']
            guids2link= attributes['inputs']+attributes['outputs']+[{"guid":procguid}]
            for link in guids2link:
                link['relationshipType'] = "Product_Referenceable_Groups"
            print(item['name'])
            print(products.keys())
            if item['name'].lower() in products.keys():
                attributes = products[item['name'].lower()]
                attributes['qualifiedName'] = f"dbtproduct-{item['package']}-{item['model']}"
                entity = {}
                entity['attributes'] = attributes
                entity['typeName'] = "Purview_Product"
                relationshipAttributes = {}
                relationshipAttributes['Groups_Referenceable'] = guids2link
                entity['relationshipAttributes'] = relationshipAttributes
                result = self.writeentity(entity)
                print(result)
            #['mutatedEntities']['CREATE'][0]['guid']
        # at this point we have a list of 
        return
    
    def buildpvdata(self, processor):
        nodes = processor.nodes
        sources = processor.sources
        itemlist = []
        entity = {}
            #Ideally we must capture package and environment from elsewhere - TBD
        
        for key,item in nodes.items():
            package = item['package_name']
            target = processor.target
            myhost = processor.host
            entity['package'] = package
            entity['target'] = target
            entity['model'] = item['alias']
            entity['name'] = item['name']
            entity['id'] = item['unique_id']
            entity['description'] = item['description']
            entity['process_code'] = item['compiled_code']
            entity['fqn'] = item['schema']+item['name']+'@'+myhost
            if "azuredatabricks.net" in myhost:
                entity['type'] = 'hive_table'
            # must be complitted with types available and defined in the script STORE literals
            inputs = []
            for input in  item['depends_on']['nodes']:
                source = {}
                attributes = {}
                source['attributes'] = attributes
                sourcedict = sources[input]
                attributes['name'] = sourcedict['name']
                myfqn = sourcedict['identifier']
                if myfqn.split('://')[0] == 'abfss':
                    temp =  myfqn.split('://')[1].split('@')
                    filepath = temp[1].split('/',1)
                    attributes['qualifiedName'] = 'https://'+filepath[0]+'/'+temp[0]+'/'+filepath[1]
                    if filepath[1][-1] == '/':
                        source['typeName'] = "azure_datalake_gen2_path"
                    else:
                        source['typeName'] = "azure_datalake_gen2_object"
                else:
                    attributes['qualifiedName'] = myfqn
                    source['typeName'] = entity['type']
                #source['relation_name']=sourcedict['relation_name']
                inputs.append(source)
            entity['inputs'] = inputs
            itemlist.append(entity)
        return itemlist
    
    def getentityguid(self, node):
        #GET {Endpoint}/catalog/api/atlas/v2/entity/uniqueAttribute/type/{typeName}?minExtInfo={minExtInfo}&ignoreRelationships={ignoreRelationships}&attr:qualifiedName={attr:qualifiedName}
        url = f"{self.api}atlas/v2/entity/uniqueAttribute/type/{node['typeName']}?minExtInfo={{minExtInfo}}&ignoreRelationships={{ignoreRelationships}}&attr:qualifiedName={{{node['attributes']['qualifiedName']}}}" 
        headers = {"Authorization": f"Bearer {self.token}", 'Content-Type': 'application/json'}
        request = requests.get(url, headers=headers)
        # print('----------requests for guid------------')
        # print(request.text)
        try:
            guid = json.loads(request.text)['entity']['guid']
            return guid
        except:
            return -1

    def writeentity(self, node):
        # POST {Endpoint}/catalog/api/atlas/v2/entity/bulk
        #
        payload = f'{{"referredEntities": {{}}, "entities": [{json.dumps(node)}]}}'
        #url = f"{self.api}atlas/v2/entity/bulk"
        collection = "purview-con"
        url = f"{self.api}collections/{collection}/entity/bulk?api-version=2022-03-01-preview"
        headers = {"Authorization": f"Bearer {self.token}",
                   'Content-Type': 'application/json'}
        response = requests.request("POST", url, headers=headers, data=payload)
        print('------response from writeentity() function-----')
        print(response.text)
        print("---------------------end-----------------------")
        return json.loads(response.text)
    
    def readproducts(self, node):
        
        return 


    def createtypes(self, dtype, table, name, dlength, dfix):
        global seed
        # dtype = Data type for the column
        # table = tablename
        # name = column name
        # dfix = Dataframe filled with ids with the size expected.
        return
    
    # Client emit event 
    def emit(self, event: RunEvent):
        if not isinstance(event, RunEvent):
            raise ValueError("`emit` only accepts RunEvent class")
        if not self.transport:
            raise ValueError("Tried to emit OpenLineage event, but transport is not configured.")
        else:
            self.transport.emit(event)
        return
    def debug(self):
        eventn = 0
        if isinstance(self.transport,PurviewTransport):
            for eventjson in self.transport.session:
                eventn += 1
                # print(f"----Event {eventn} ----")
                # print(eventjson)
                f = open(f'data_{eventn}.json', 'w',encoding='utf-8')
                f.write(eventjson)
                f.close()
        return
        
    def findpventity(mypventity):
        # 
        id = -1
        return id
    
    def createpventity(mypventity):
        id = -1
        return id
    
    def sendlineage(mylineage):
        # Get the info on the target and source. If not defined create them
        return
        
    @classmethod
    def from_environment(cls):
        _factory = DefaultTransportFactory()
        _factory.register_transport(PurviewTransport.kind, PurviewTransport)
        return cls(transport=_factory.create())
    
class TokenProvider:
    def __init__(self, config: dict):
        pass

    def get_bearer(self) -> Optional[str]:
        return None

class PvEntity():
    fqdn: str
    type: str
    def __init__(self, fqdn,type):
        return

class Serde:
    @classmethod
    def remove_nulls_and_enums(cls, obj):
        if isinstance(obj, Enum):
            return obj.value
        if isinstance(obj, Dict):
            return dict(filter(
                lambda x: x[1] is not None,
                {k: cls.remove_nulls_and_enums(v) for k, v in obj.items()}.items()
            ))
        if isinstance(obj, List):
            return list(filter(lambda x: x is not None and (isinstance(x, dict) and x != {}), [
                cls.remove_nulls_and_enums(v) for v in obj if v is not None
            ]))

        # Pandas can use numpy.int64 object
        if 'numpy' in sys.modules and isinstance(obj, numpy.int64):
            return int(obj)
        return obj

    @classmethod
    def to_dict(cls, obj):
        if not isinstance(obj, dict):
            obj = attr.asdict(obj)
        return cls.remove_nulls_and_enums(obj)

    @classmethod
    def to_json(cls, obj):
        return json.dumps(
            cls.to_dict(obj),
            sort_keys=True,
            default=lambda o: f"<<non-serializable: {type(o).__qualname__}>>"
        )
    

class PurviewTransport(Transport):
    kind = "purview"
    config = {}
    session = []

    def __init__(self, config: dict):
        url = config['url'].strip()
        log.debug(f"Constructing openlineage client to send events to {url}")
        try:
            from urllib3.util import parse_url
            parsed = parse_url(url)
            if not (parsed.scheme and parsed.netloc):  # type: ignore
                raise ValueError(f"Need valid url for OpenLineageClient, passed {url}")
        except Exception as e:
            raise ValueError(f"Need valid url for OpenLineageClient, passed {url}. Exception: {e}")
        self.url = url
        self.timeout = 60
        self.config = config

    def emit(self, event: RunEvent):
        if log.isEnabledFor(logging.DEBUG):
            log.debug(f"Sending openlineage event {event}")
        asyncio.run(send2eventhub([event]))
        self.session.append(event)
        return 

class DefaultTransportFactory(TransportFactory):
    def __init__(self):
        self.transports = {}

    def register_transport(self, type: str, clazz: Union[Type[Transport], str]):
        self.transports[type] = clazz

    def create(self) -> Transport:
        client_id = os.environ.get("AZURE_CLIENT_ID") 
        client_secret = os.environ.get("AZURE_CLIENT_SECRET")
        tenant_id = os.environ.get("AZURE_TENANT_ID")
        purviewname = os.environ.get("PURVIEW_NAME") 
        # Purview URL and a Service principal with permissions on purview account
        config = {"url": f"https://{purviewname}.catalog.purview.azure.com/",
            "auth": {
                "type": "service_principal",
                "client_id": client_id,
                "tenant_id": tenant_id,
                "secret": client_secret
            }}
        mytransport = PurviewTransport(config)
        return mytransport
        # For now there is just HTTP transport, or not Transport
        

    def _create_transport(self, config: dict):
        transport_type = config['type']

        if transport_type in self.transports:
            transport_class = self.transports[transport_type]
        else:
            transport_class = transport_type

        if isinstance(transport_class, str):
            transport_class = try_import_from_string(transport_class)
        if not inspect.isclass(transport_class) or not issubclass(transport_class, Transport):
            raise TypeError(
                f"Transport {transport_class} has to be class, and subclass of Transport"
            )

        config_class = transport_class.config

        if isinstance(config_class, str):
            config_class = try_import_from_string(config_class)
        if not inspect.isclass(config_class) or not issubclass(config_class, Config):
            raise TypeError(f"Config {config_class} has to be class, and subclass of Config")

        return transport_class(config_class.from_dict(config))

    def _try_config_from_yaml(self) -> Optional[dict]:
        file = self._find_yaml()
        if file:
            try:
                with open(file, 'r') as f:
                    config = yaml.safe_load(f)
                    return config['transport']
            except Exception:
                # Just move to read env vars
                pass
        return None


