import random
import xml.etree.ElementTree as ET
import uuid
import logging
from enum import Enum
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from GraphQLRequests import (add_tag, create_catalog_entry, create_relationship, create_tag,
    get_tag, login)

# Logfile zu Beginn leeren
open("logfile.txt", "w").close()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),  # Konsole
        logging.FileHandler("logfile.txt", encoding="utf-8")  # Datei
    ]
)

class EntityType(Enum):
    DICTIONARY = ("Dictionary", "Dictionary", "c1c7016b-f85c-43c7-a696-71e75555062b")
    THEMA = ("Thema", "Subject", "5997da9b-a716-45ae-84a9-e2a7d186bcf9")
    KLASSE = ("Klasse", "Subject", "e9b2cd6d-76f7-4c55-96ab-12d084d21e96")
    MERKMALSGRUPPE = ("Merkmalsgruppe", "Subject", "7c9ffe6e-3c8b-4cd2-b57b-4cd102325603")
    REFERENZDOKUMENT = ("Referenzdokument", "ReferenceDocument", "992c8887-301e-4764-891c-ae954426fc22")
    MERKMAL = ("Merkmal", "Property", "d4b0ba83-eb40-4997-85e0-9d6181e85639")
    WERTELISTE = ("Werteliste", "ValueList", "57172977-a42f-4e05-8109-cd906ec7f43c")
    WERT = ("Wert", "Value", "a5d13c88-7d83-42c1-8da2-5dc6d8e8a749")
    MASSEINHEIT = ("Maßeinheit", "Unit", "09da1ebb-8641-47fa-b82e-8588c7fef09e")

class RelType(str, Enum):
    RELATIONSHIP_TO_SUBJECT = "RelationshipToSubject"
    PROPERTIES = "Properties"
    POSSIBLE_VALUES = "PossibleValues"
    DICTIONARY = "Dictionary"
    VALUES = "Values"


# Enum für alle relevanten XML-Tags
class XmlTag(Enum):
    OBJEKTARTENGRUPPE = ("", "AC_Objektartengruppe")
    FEATURETYPE = ("", "AC_FeatureType")
    DATATYPE = ("", "AC_DataType")
    FEATUREATTRIBUTE = ("", "AC_FeatureAttribute")
    LISTEDVALUE = ("", "AC_ListedValue")
    OBJEKTARTENBEREICH = ("", "AC_Objektartenbereich")
    CONNECTOR = ("gml", "dictionaryEntry")
    ASSOCIATIONROLE = ("", "AC_AssociationRole")

    def tag(self, ns):
        return f"{{{ns[self.value[0]]}}}{self.value[1]}"

def find_datatype(attribute, ns):
    name = getattr(attribute.find("valueTypeName", ns), "text", None)
    if name in ("CharacterString", "URI"):
        return "XTD_STRING"
    elif name == "Integer":
        return "XTD_INTEGER"
    elif name == "Boolean":
        return "XTD_BOOLEAN"
    elif name in ("Real", "Angle", "Length", "Area", "Volume"):
        return "XTD_REAL"
    elif name in ("DateTime", "Date"):
        return "XTD_DATETIME"
    else:
        return None

def prepare_entity_attributes(domain, entity_type, ns):
    name = getattr(domain.find("gml:identifier", ns), "text", None)
    description = getattr(domain.find("gml:description", ns), "text", None)
    id = getattr(domain.find("gml:name", ns), "text", None)
    
    # Erweiterten Lookup-Key mit description erstellen
    lookup_key = (name, entity_type.value[1], description)
    
    # Prüfen, ob Entity bereits existiert
    if lookup_key in entity_lookup:
        existing_id = entity_lookup[lookup_key]
        return {
            "id": existing_id,
            "properties": None,  # Keine Properties, da bereits vorhanden
            "entityType": entity_type,
            "is_new": False
        }
    
    # Entity noch nicht vorhanden, neue erstellen
    properties = {"names": {"languageTag": "de", "value": name}}
    if description is not None:
        properties["descriptions"] = {"languageTag": "de", "value": description}
    # if id is not None:
        # properties["id"] = id + "_" + name.replace(" ", "_").replace("'", '"')
    # else:
    #     properties["id"] = str(uuid.uuid4())
    properties["id"] = str(uuid.uuid4())
    if entity_type.value[1] == EntityType.MERKMAL.value[1]:
        datatype = find_datatype(domain, ns)
        if datatype:
            properties["propertyProperties"] = {"dataType": datatype}
    
    # In die Lookup-Tabelle eintragen
    entity_lookup[lookup_key] = properties["id"]
    return {
        "id": properties["id"],
        "properties": properties,
        "entityType": entity_type,
        "is_new": True
    }

def create_entry(attributes, token, tagId):
    properties = attributes["properties"]
    entityType = attributes["entityType"]
    try:
        result = create_catalog_entry(token, entityType.value[1], properties, [entityType.value[2], tagId])
        if result is None:
            # Retry-Logik für add_tag
            max_retries = 5
            addTag = None
            
            for attempt in range(1, max_retries + 1):
                addTag = add_tag(token, properties["id"], tagId)
                if addTag is not None:
                    logging.info(f"Tag für '{properties['names']['value']}' erfolgreich hinzugefügt nach Versuch {attempt}")
                    break
                
                if attempt < max_retries:
                    wait_time = attempt * 0.5  # Progressiv längere Wartezeit
                    time.sleep(wait_time)
                else:
                    # Alle Versuche fehlgeschlagen
                    error_msg = f"add_tag fehlgeschlagen nach {max_retries} Versuchen für Entity {properties['names']['value']} (ID: {properties['id']})"
                    logging.error(error_msg)
                    raise Exception(error_msg)
        
        if entityType == EntityType.DICTIONARY:
            return None  # Keine ID für Dictionary
        return properties["id"]
    except Exception as e:
        logging.error(f"Fehler in create_entry: {e}, EntityType: {entityType}, Properties: {properties}")
        raise

def process_feature_type(level, collected_class_ids, tasks, relationship_tasks, ns, log_index):
    attrs = prepare_entity_attributes(level, EntityType.KLASSE, ns)
    if attrs.get("is_new", True):
        tasks.append(attrs)
    collected_class_ids.append(attrs["id"])

    collected_property_ids = []
    for element in level.findall(XmlTag.CONNECTOR.tag(ns)):
        sub_level = element[0]
        if sub_level.tag == XmlTag.FEATUREATTRIBUTE.tag(ns) or sub_level.tag == XmlTag.ASSOCIATIONROLE.tag(ns):
            prop_attrs = prepare_entity_attributes(sub_level, EntityType.MERKMAL, ns)
            if prop_attrs.get("is_new", True):
                tasks.append(prop_attrs)
            collected_property_ids.append(prop_attrs["id"])

            valList = False
            name = getattr(sub_level.find("valueTypeName", ns), "text", None)
            # Wertelisten haben keine description, daher None verwenden
            value_list_lookup_key = (name, EntityType.WERTELISTE.value[1], None)
            if name and value_list_lookup_key in entity_lookup:
                valListId = entity_lookup[value_list_lookup_key]
                # Werteliste existiert, nur Relation anlegen nach der Schleife
                skip_value_list_creation = True
            else:
                valListId = str(uuid.uuid4())
                skip_value_list_creation = False
            order = 1
            for sub_element in sub_level.findall(XmlTag.CONNECTOR.tag(ns)):
                val_level = sub_element[0]
                if val_level.tag == XmlTag.LISTEDVALUE.tag(ns):
                    valList = True
                    val_attrs = prepare_entity_attributes(val_level, EntityType.WERT, ns)
                    if val_attrs.get("is_new", True):
                        tasks.append(val_attrs)
                    relationship_tasks.append((RelType.VALUES, {"valueListProperties": {"order": order}}, valListId, [val_attrs["id"]]))
                    order += 1
                else:
                    log_unknown_schema_type(5, val_level.tag)
            if valList:
                if skip_value_list_creation:
                    # Werteliste existiert, nur Relation anlegen
                    relationship_tasks.append((RelType.POSSIBLE_VALUES, None, prop_attrs["id"], [valListId]))
                elif name:
                    vAttrs = {
                        "id": valListId,
                        "properties": {
                            "names": {"languageTag": "de", "value": name},
                            "id": valListId,
                            "valueListProperties": {"languageTag": "de"}
                        },
                        "entityType": EntityType.WERTELISTE,
                        "is_new": True
                    }
                    tasks.append(vAttrs)
                    entity_lookup[value_list_lookup_key] = valListId
                    relationship_tasks.append((RelType.POSSIBLE_VALUES, None, prop_attrs["id"], [valListId]))
                else:
                    logging.info(f"Kein valueTypeName für Werteliste bei Merkmal {prop_attrs['properties']['names']['value']}")
        else:
            log_unknown_schema_type(log_index, sub_level.tag)
    relationship_tasks.append((RelType.PROPERTIES, None, attrs["id"], collected_property_ids))

def log_unknown_schema_type(level, tag):
    logging.info(f"Unbekannter Schema-Typ [{level}]: {tag}")

def dict_to_hashable(obj):
    """
    Konvertiert ein Dictionary (auch verschachtelt) zu einem hashbaren Tupel.
    """
    if obj is None:
        return None
    elif isinstance(obj, dict):
        return tuple(sorted((k, dict_to_hashable(v)) for k, v in obj.items()))
    elif isinstance(obj, list):
        return tuple(dict_to_hashable(item) for item in obj)
    else:
        return obj

def hashable_to_dict(obj):
    """
    Konvertiert ein hashbares Tupel zurück zu einem Dictionary.
    """
    if obj is None:
        return None
    elif isinstance(obj, tuple) and len(obj) > 0 and isinstance(obj[0], tuple) and len(obj[0]) == 2:
        # Es ist ein Dictionary-Tupel
        return {k: hashable_to_dict(v) for k, v in obj}
    else:
        return obj

def optimize_relationship_tasks(relationship_tasks):
    """
    Optimiert relationship_tasks, indem Einträge mit gleichen (relationship_type, props, from_id) 
    zusammengeführt werden und die to_ids in einem Array gesammelt werden.
    """
    # Dictionary zum Sammeln der optimierten Relationships
    # Key: (relationship_type, props_hash, from_id)
    # Value: set of to_ids
    optimized = {}
    
    for rel_type, props, from_id, to_ids in relationship_tasks:
        # Props zu einem hashbaren Wert konvertieren (für Dictionary-Key)
        props_key = dict_to_hashable(props)
        
        # Schlüssel für die Gruppierung erstellen
        key = (rel_type, props_key, from_id)
        
        # to_ids zu einem Set hinzufügen (um Duplikate zu vermeiden)
        if key not in optimized:
            optimized[key] = set()
        
        # Alle to_ids hinzufügen
        if isinstance(to_ids, list):
            optimized[key].update(to_ids)
        else:
            optimized[key].add(to_ids)
    
    # Zurück zu der ursprünglichen Struktur konvertieren
    result = []
    for (rel_type, props_key, from_id), to_ids_set in optimized.items():
        # props_key zurück zu Dictionary konvertieren
        props = hashable_to_dict(props_key)
        
        # Set zurück zu List konvertieren
        to_ids_list = list(to_ids_set)
        
        result.append((rel_type, props, from_id, to_ids_list))
    
    return result

def create_relationship_with_retry(rel_args):
    rel_type, properties, from_id, to_ids = rel_args
    new_to_ids = []
    for to_id in to_ids:
        rel_key = (rel_type, from_id, to_id)
        if rel_key in relation_lookup:
            continue
        relation_lookup.add(rel_key)
        new_to_ids.append(to_id)
    if new_to_ids:
        max_retries = 5
        for attempt in range(1, max_retries + 1):
            try:
                result = create_relationship(token, rel_type, properties, from_id, new_to_ids)
                if result and isinstance(result, dict) and 'errors' in result:
                    error_messages = str(result['errors'])
                    if 'lock' in error_messages.lower() and attempt < max_retries:
                        sleep_time = random.uniform(1, 5)
                        time.sleep(sleep_time)
                        continue
                    logging.error(f"Fehler beim Anlegen der Beziehung: {error_messages}\nBeziehungsparameter: type={rel_type}, fromId={from_id}, toId={new_to_ids}")
                break
            except Exception as e:
                if 'lock' in str(e).lower() and attempt < max_retries:
                    sleep_time = random.uniform(1, 5)
                    time.sleep(sleep_time)
                    continue
                logging.error(f"Fehler beim Anlegen der Beziehung: {e}\nBeziehungsparameter: type={rel_type}, fromId={from_id}, toId={new_to_ids}")
                break
                
if __name__ == "__main__":
    start = time.time()
    # Login
    token = login()
    # logging.info("Login erfolgreich")

    # Find or create tag
    tagId = "GeoInfoDokId"
    tagName = "GeoInfoDok"
    # file_path = "resources/aaa_mini.xml"
    file_path = "resources/aaa.xml"

    tag = get_tag(token, tagId)
    # logging.info("Tag abgerufen:", tag)
    if not tag:
        logging.info("Tag nicht gefunden, erstelle neuen Tag.")
        tag = create_tag(token, tagName, tagId)
        logging.info(f"Tag erstellt: {tag}")
    
    # Validierung, dass tagId korrekt ist
    if tagId is None:
        logging.error("tagId ist None - kann nicht fortfahren")
        exit(1)
    
    logging.info(f"Verwende tagId: {tagId}")

    ns = {}
    for event, elem in ET.iterparse(file_path, events=("start-ns", "start")):
        if event == "start-ns":
            ns[elem[0] or ""] = elem[1]

    tree = ET.parse(file_path)
    root = tree.getroot()

    # Lookup-Tabellen und Task Sammlungen für Entities und Relationen
    entity_lookup = {}  # (name, typ, description) -> id
    relation_lookup = set()  # (relationship_type, from_id, [to_ids])
    tasks = [] # List of entity dictionaries: {id, properties, entityType, is_new}
    relationship_tasks = [] # (relationship_type, props, from_id, [to_ids])
    entry_ids = []  # IDs die eine Dictionary-Beziehung benötigen
    
    rel_to_subj_props = {"relationshipToSubjectProperties": {"relationshipType": "XTD_SCHEMA_LEVEL"}}

    # Create Dictionary from FeatureCatalogue
    dictAttrs = prepare_entity_attributes(root, EntityType.DICTIONARY, ns)
    create_entry(dictAttrs, token, tagId)
    dictionaryId = dictAttrs["id"]
    # logging.info(f"Dictionary ID: {dictionaryId}")

    for child in root.findall(XmlTag.CONNECTOR.tag(ns)):
        level1 = child[0]

        if level1.tag == XmlTag.OBJEKTARTENBEREICH.tag(ns):
            attrs1 = prepare_entity_attributes(level1, EntityType.THEMA, ns)
            if attrs1.get("is_new", True):
                tasks.append(attrs1)
            collected_theme_ids = []
            collected_class_ids = []

            for element1 in level1.findall(XmlTag.CONNECTOR.tag(ns)):
                level2 = element1[0]
                if level2.tag == XmlTag.OBJEKTARTENGRUPPE.tag(ns):
                    attrs2 = prepare_entity_attributes(level2, EntityType.THEMA, ns)
                    if attrs2.get("is_new", True):
                        tasks.append(attrs2)
                    collected_theme_ids.append(attrs2["id"])

                    collected_class_ids = []
                    for element2 in level2.findall(XmlTag.CONNECTOR.tag(ns)):
                        level3 = element2[0]

                        if level3.tag == XmlTag.FEATURETYPE.tag(ns) or level3.tag == XmlTag.DATATYPE.tag(ns):
                            process_feature_type(level3, collected_class_ids, tasks, relationship_tasks, ns, 4)

                        else:
                            log_unknown_schema_type(3, level3.tag)
                
                elif level2.tag == XmlTag.FEATURETYPE.tag(ns) or level2.tag == XmlTag.DATATYPE.tag(ns):
                    process_feature_type(level2, collected_class_ids, tasks, relationship_tasks, ns, 3)

                else:
                    log_unknown_schema_type(2, level2.tag)

                relationship_tasks.append((RelType.RELATIONSHIP_TO_SUBJECT, rel_to_subj_props, attrs2["id"], collected_class_ids))

            relationship_tasks.append((RelType.RELATIONSHIP_TO_SUBJECT, rel_to_subj_props, attrs1["id"], collected_theme_ids))
            relationship_tasks.append((RelType.RELATIONSHIP_TO_SUBJECT, rel_to_subj_props, attrs1["id"], collected_class_ids))

        else:
            log_unknown_schema_type(1, level1.tag)


    logging.info(f"Anzahl der Entities: {len(tasks)}")
    logging.info(f"Anzahl der Relationen vor Optimierung: {len(relationship_tasks)}")

    # Relationship-Tasks optimieren (zusammenführen)
    relationship_tasks = optimize_relationship_tasks(relationship_tasks)
    logging.info(f"Anzahl der Relationen nach Optimierung: {len(relationship_tasks)}")

    # Optimale Anzahl an Threads
    cpu_count = os.cpu_count()
    optimal_workers = min(cpu_count*3, 20)

    logging.info(f"CPU-Kerne: {cpu_count}, Verwende {optimal_workers} Worker")

    start0 = time.time()
    # Parallelisierte Entity-Erstellung
    with ThreadPoolExecutor(max_workers=optimal_workers) as executor:
        # Alle Tasks als Futures starten
        future_to_task = {executor.submit(create_entry, task, token, tagId): task for task in tasks}
        
        # Ergebnisse sammeln
        for future in as_completed(future_to_task):
            try:
                id = future.result()
                if id:
                    entry_ids.append(id)
            except Exception as e:
                task = future_to_task[future]
                logging.error(f"Fehler bei Entity-Erstellung: {e}, Task: {task}")

    logging.info(f"Dauer Erstellung Entities: {time.time() - start0:.2f} Sekunden")
    
    start1 = time.time()
    # Dictionary-Beziehungen erstellen
    for id in entry_ids:
        create_relationship_with_retry((RelType.DICTIONARY, None, id, [dictionaryId]))
    logging.info(f"Dauer Erstellung Dictionary-Relationen: {time.time() - start1:.2f} Sekunden")
    
    start2 = time.time()

    for rel_args in relationship_tasks:
        create_relationship_with_retry(rel_args)

    logging.info(f"Dauer Erstellung Relationen: {time.time() - start2:.2f} Sekunden")
    logging.info(f"Gesamtdauer: {time.time() - start:.2f} Sekunden")
