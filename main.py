import itertools
import random
from GraphQLRequests import (
    create_tag,
    get_tag,
    login,
    create_catalog_entry,
    create_relationship,
)
import xml.etree.ElementTree as ET
import uuid
import logging
from enum import Enum

# Logfile zu Beginn leeren
open("logfile.txt", "w").close()
from concurrent.futures import ThreadPoolExecutor
import time

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

def find_datatype(attribute):
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
    properties = {"names": {"languageTag": "de", "value": name}}
    if description is not None:
        properties["descriptions"] = {"languageTag": "de", "value": description}
    # if id is not None:
        # properties["id"] = id + "_" + name.replace(" ", "_").replace("'", '"')
    # else:
    #     properties["id"] = str(uuid.uuid4())
    properties["id"] = str(uuid.uuid4())
    if entity_type.value[1] == EntityType.MERKMAL.value[1]:
        datatype = find_datatype(domain)
        if datatype:
            properties["propertyProperties"] = {"dataType": datatype}
    lookup_key = (name, entity_type.value[1], id)
    # Direkt in die Lookup-Tabelle eintragen
    entity_lookup[lookup_key] = properties["id"]
    return {
        "id": properties["id"],
        "properties": properties,
        "entityType": entity_type
    }

def create_entry(attributes, relationship_tasks=None):
    properties = attributes["properties"]
    entityType = attributes["entityType"]
    result = create_catalog_entry(token, entityType.value[1], properties, [entityType.value[2], tagId])
    if result and entityType != EntityType.DICTIONARY and relationship_tasks is not None:
        relationship_tasks.append((RelType.DICTIONARY, None, properties["id"], [dictionaryId]))
    return properties["id"]

def process_feature_type(level, collected_class_ids, tasks, relationship_tasks, ns, log_index):
    attrs = prepare_entity_attributes(level, EntityType.KLASSE, ns)
    tasks.append((attrs))
    collected_class_ids.append(attrs["id"])

    collected_property_ids = []
    for element in level.findall(XmlTag.CONNECTOR.tag(ns)):
        sub_level = element[0]
        if sub_level.tag == XmlTag.FEATUREATTRIBUTE.tag(ns) or sub_level.tag == XmlTag.ASSOCIATIONROLE.tag(ns):
            prop_attrs = prepare_entity_attributes(sub_level, EntityType.MERKMAL, ns)
            tasks.append((prop_attrs))
            collected_property_ids.append(prop_attrs["id"])

            valList = False
            name = getattr(sub_level.find("valueTypeName", ns), "text", None)
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
                    tasks.append((val_attrs))
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
                        "entityType": EntityType.WERTELISTE
                    }
                    tasks.append((vAttrs))
                    entity_lookup[value_list_lookup_key] = valListId
                    relationship_tasks.append((RelType.POSSIBLE_VALUES, None, prop_attrs["id"], [valListId]))
                else:
                    logging.info(f"Kein valueTypeName für Werteliste bei Merkmal {prop_attrs['properties']['names']['value']}")
        else:
            log_unknown_schema_type(log_index, sub_level.tag)
    relationship_tasks.append((RelType.PROPERTIES, None, attrs["id"], collected_property_ids))

def log_unknown_schema_type(level, tag):
    logging.info(f"Unbekannter Schema-Typ [{level}]: {tag}")

if __name__ == "__main__":
    start = time.time()
    # Login
    token = login()
    # logging.info("Login erfolgreich")

    # Find or create tag
    tagId = "GeoInfoDokId"
    tagName = "GeoInfoDok"
    file_path = "resources/aaa_mini.xml"

    tag = get_tag(token, tagId)
    # logging.info("Tag abgerufen:", tag)
    if not tag:
        logging.info("Tag nicht gefunden, erstelle neuen Tag.")
        create_tag(token, tagName, tagId)
        logging.info("Tag erstellt:", tag)

    ns = {}
    for event, elem in ET.iterparse(file_path, events=("start-ns", "start")):
        if event == "start-ns":
            ns[elem[0] or ""] = elem[1]

    tree = ET.parse(file_path)
    root = tree.getroot()

    # Lookup-Tabellen und Task Sammlungen für Entities und Relationen
    entity_lookup = {}  # (name, typ) -> id
    relation_lookup = set()  # (from_id, to_id, relationship_type)
    tasks = []
    relationship_tasks = []
    # value_tasks wird nicht mehr benötigt
    
    rel_to_subj_props = {"relationshipToSubjectProperties": {"relationshipType": "XTD_SCHEMA_LEVEL"}}

    # Create Dictionary from FeatureCatalogue
    dictAttrs = prepare_entity_attributes(root, EntityType.DICTIONARY, ns)
    dictionaryId = create_entry(dictAttrs, relationship_tasks)
    # logging.info(f"Dictionary ID: {dictionaryId}")

    for child in root.findall(XmlTag.CONNECTOR.tag(ns)):
        level1 = child[0]

        if level1.tag == XmlTag.OBJEKTARTENBEREICH.tag(ns):
            attrs1 = prepare_entity_attributes(level1, EntityType.THEMA, ns)
            tasks.append((attrs1))
            collected_theme_ids = []
            collected_class_ids = []

            for element1 in level1.findall(XmlTag.CONNECTOR.tag(ns)):
                level2 = element1[0]
                if level2.tag == XmlTag.OBJEKTARTENGRUPPE.tag(ns):
                    attrs2 = prepare_entity_attributes(level2, EntityType.THEMA, ns)
                    tasks.append((attrs2))
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
    logging.info(f"Anzahl der Relationen: {len(relationship_tasks)}")

    # Entities sequentiell anlegen
    for task in tasks:
        create_entry(task, relationship_tasks)

    logging.info(f"Dauer Erstellung Entities: {time.time() - start:.2f} Sekunden")
    start2 = time.time()

    relation_jobs = []
    for rel_args in relationship_tasks:
        rel_type, properties, from_id, to_ids = rel_args
        new_to_ids = []
        for to_id in to_ids:
            rel_key = (rel_type, from_id, to_id)
            if rel_key in relation_lookup:
                continue
            relation_lookup.add(rel_key)
            new_to_ids.append(to_id)
        if new_to_ids:
            relation_jobs.append((rel_type, properties, from_id, new_to_ids))

    def rel_worker(args):
        rel_type, properties, from_id, to_ids = args
        max_retries = 5
        for attempt in range(1, max_retries + 1):
            try:
                result = create_relationship(token, rel_type, properties, from_id, to_ids)
                if result and isinstance(result, dict) and 'errors' in result:
                    error_messages = str(result['errors'])
                    if 'lock' in error_messages.lower():
                        if attempt < max_retries:
                            sleep_time = random.uniform(1, 5)
                            time.sleep(sleep_time)
                            continue
                        else:
                            logging.error(f"Lock-Fehler nach {max_retries} Versuchen nicht behoben: {error_messages}\nBeziehungsparameter: type={rel_type}, fromId={from_id}, toId={to_id}")
                    break
                break
            except Exception as e:
                if 'lock' in str(e).lower():
                    if attempt < max_retries:
                        sleep_time = random.uniform(1, 5)
                        time.sleep(sleep_time)
                        continue
                    else:
                        logging.error(f"Lock-Fehler nach {max_retries} Versuchen nicht behoben: {e}\nBeziehungsparameter: type={rel_type}, fromId={from_id}, toId={to_id}")
                        break
                else:
                    logging.error(f"Fehler beim Anlegen der Beziehung: {e}")
                    break

    for job in relation_jobs:
        rel_worker(job)

    logging.info(f"Dauer Erstellung Relationen: {time.time() - start2:.2f} Sekunden")
    logging.info(f"Gesamtdauer: {time.time() - start:.2f} Sekunden")
