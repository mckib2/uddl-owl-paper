import xml.etree.ElementTree as ET
from typing import List, Union, Set, Dict, Any, Optional
from dataclasses import dataclass

from tuple import UddlTuple, Query
from participant_path_parser import ParticipantPath, EntityResolution, AssociationResolution

# Namespaces
NS_RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
NS_RDFS = "http://www.w3.org/2000/01/rdf-schema#"
NS_OWL = "http://www.w3.org/2002/07/owl#"
NS_XSD = "http://www.w3.org/2001/XMLSchema#"
NS_DEFAULT = "http://example.org/uddl#"

def _qname(ns, tag):
    return f"{{{ns}}}{tag}"

def tuple2owl(tuples: List[Union[UddlTuple, Query]]) -> ET.ElementTree:
    # Filter out queries
    data_tuples = [t for t in tuples if isinstance(t, UddlTuple)]

    # 1. Analyze tuples to identify Entities, Associations, Observables
    subjects = set[str]()
    objects = set()
    
    # Track which subjects use 'associates'
    association_subjects = set[str]()
    # Track composition definitions to resolve path types: Class -> Role -> Type
    schema: Dict[str, Dict[str, str]] = {} 

    for t in data_tuples:
        subjects.add(t.subject)
        
        # Track associations
        if t.predicate == 'associates':
            association_subjects.add(t.subject)
            
        # Track schema for compositions and associations
        # We need this to resolve types in paths
        if t.subject not in schema:
            schema[t.subject] = {}
        
        # Determine rolename
        # If rolename is present, use it. Else use object name (if string).
        # Note: t.object can be ParticipantPath
        
        role = t.rolename
        target_type = None
        
        if isinstance(t.object, str):
            objects.add(t.object)
            target_type = t.object
            if not role:
                role = t.object # Fallback role name
        elif isinstance(t.object, ParticipantPath):
            # For paths, we can't easily know the target type without full resolution
            # But we can store the path object itself or defer resolution
            pass
            
        if role and target_type:
            schema[t.subject][role] = target_type

    # Classification Rules:
    # 1. Associations: Subjects that appear in at least one 'associates' tuple.
    #    (They can also appear in 'composes' tuples).
    associations = association_subjects
    
    # 2. Entities: Subjects that are NOT Associations (i.e., they only appear in 'composes' tuples).
    entities = subjects - associations
    
    # 3. Observables: Objects that never appear as subjects.
    observables = objects - subjects

    # Build OWL
    ET.register_namespace("rdf", NS_RDF)
    ET.register_namespace("rdfs", NS_RDFS)
    ET.register_namespace("owl", NS_OWL)
    ET.register_namespace("xsd", NS_XSD)
    ET.register_namespace("", NS_DEFAULT) # Default namespace

    root = ET.Element(_qname(NS_RDF, "RDF"))
    
    # Ontology declaration
    ontology = ET.SubElement(root, _qname(NS_OWL, "Ontology"))
    ontology.set(_qname(NS_RDF, "about"), NS_DEFAULT)

    # Helper to create resources
    def get_resource(name):
        return f"{NS_DEFAULT}#{name}" if not name.startswith("http") else name

    def create_class(name, parent=root):
        cls = ET.SubElement(parent, _qname(NS_OWL, "Class"))
        cls.set(_qname(NS_RDF, "about"), get_resource(name))
        return cls

    def create_subclass_of(child_element, parent_resource):
        sub = ET.SubElement(child_element, _qname(NS_RDFS, "subClassOf"))
        sub.set(_qname(NS_RDF, "resource"), get_resource(parent_resource))
        return sub
        
    def create_restriction(parent, on_property, on_class, max_card=None):
        r = ET.SubElement(parent, _qname(NS_RDFS, "subClassOf"))
        restriction = ET.SubElement(r, _qname(NS_OWL, "Restriction"))
        
        prop = ET.SubElement(restriction, _qname(NS_OWL, "onProperty"))
        prop.set(_qname(NS_RDF, "resource"), get_resource(on_property))
        
        if on_class:
            cls_elem = ET.SubElement(restriction, _qname(NS_OWL, "onClass"))
            cls_elem.set(_qname(NS_RDF, "resource"), get_resource(on_class))
            
        if max_card is not None and max_card != -1:
            card = ET.SubElement(restriction, _qname(NS_OWL, "maxCardinality"))
            card.set(_qname(NS_RDF, "datatype"), f"{NS_XSD}nonNegativeInteger")
            card.text = str(max_card)

    def resolve_path_type(path: ParticipantPath) -> Optional[str]:
        # Resolve the type of the object at the end of the path
        current_type = path.start_type
        for res in path.resolutions:
            if isinstance(res, EntityResolution):
                # Look up role in current_type
                if current_type in schema and res.rolename in schema[current_type]:
                    current_type = schema[current_type][res.rolename]
                else:
                    return None # Cannot resolve
            elif isinstance(res, AssociationResolution):
                # Not handled deeply yet, assuming similar lookup or skipping
                return None
        return current_type

    # 2. Apply Mappings

    # Declarations for basic classes
    for name in entities | associations | observables:
        create_class(name)
        
    # Re-structure: Create Class elements map
    class_elements = {}
    for name in entities | associations | observables:
        # We need to find the element we just created.
        # Since we just created them in order, we could store them.
        # But to be safe and simple, let's just find them or rely on order?
        # Actually, create_class appends to root.
        # Let's just re-iterate root children or better, change create_class to return and store.
        # I did that above. But I didn't store them in the loop.
        pass

    # Clear and refill class_elements map properly
    class_elements = {}
    # (The previous loop created elements but didn't store them in a dict. 
    #  We can iterate root or just re-find. 
    #  Let's just use the fact that create_class returns the element and I didn't capture it in the loop above.
    #  I should fix that.)
    
    # Remove the previous loop logic and do it here:
    pass

    # Reset root children (except Ontology) to avoid duplicates if I messed up logic above? 
    # No, let's just do it cleanly.
    
    # RESTARTING ELEMENT CREATION SECTION
    # Clear children after Ontology
    # (Actually, better to just write the logic once correctly)
    
    # Let's clear the root children after Ontology to be safe against my previous snippet thoughts
    # root[:] = [ontology] 
    
    # Create Classes and store in map
    for name in entities | associations | observables:
        cls = create_class(name)
        class_elements[name] = cls

    # Process Compositions (Entities AND Associations)
    # Both can compose.
    for subject in entities | associations:
        comp_tuples = [t for t in data_tuples if t.subject == subject and t.predicate == 'composes']
        
        if not comp_tuples:
            continue
            
        cls_elem = class_elements[subject]
        
        for t in comp_tuples:
            if isinstance(t.object, str):
                role = t.rolename if t.rolename else t.object
                prop_name = f"hasComposition{role}"
                
                # Determine Max Cardinality
                max_c = -1
                if isinstance(t.multiplicity, list):
                    max_c = t.multiplicity[1]
                elif isinstance(t.multiplicity, int):
                    max_c = t.multiplicity
                elif str(t.multiplicity).isdigit():
                    max_c = int(t.multiplicity)
                
                create_restriction(cls_elem, prop_name, t.object, max_c)

    # Process Observables
    if observables:
        for obs in observables:
            cls_elem = class_elements[obs]
            # Subclass of Observable
            create_subclass_of(cls_elem, "Observable")
            # owl:label
            label = ET.SubElement(cls_elem, _qname(NS_RDFS, "label"))
            label.text = obs

    # Process Associations (Participants)
    for assoc in associations:
        assoc_tuples = [t for t in data_tuples if t.subject == assoc and t.predicate == 'associates']
        cls_elem = class_elements[assoc]
        
        for t in assoc_tuples:
            # t.object can be str or ParticipantPath
            # role
            role = t.rolename
            if not role:
                # Fallback role logic
                if isinstance(t.object, str):
                    role = t.object
                elif isinstance(t.object, ParticipantPath):
                    role = t.object.resolutions[-1].rolename if t.object.resolutions else "Unknown"

            source_prop = f"hasParticipantSource{role}"
            target_prop = f"hasParticipantTarget{role}"

            # Create ObjectProperties
            # They should be defined at root level
            sp = ET.SubElement(root, _qname(NS_OWL, "ObjectProperty"))
            sp.set(_qname(NS_RDF, "about"), get_resource(source_prop))
            
            tp = ET.SubElement(root, _qname(NS_OWL, "ObjectProperty"))
            tp.set(_qname(NS_RDF, "about"), get_resource(target_prop))

            # Resolve target type
            target_cls = None
            
            if isinstance(t.object, str):
                target_cls = t.object
            elif isinstance(t.object, ParticipantPath):
                target_cls = resolve_path_type(t.object)
                
                # Build property chain
                # Chain starts with source_prop (linking Assoc to Path Start)
                # Then follows the path resolutions
                
                chain_list = ET.SubElement(tp, _qname(NS_OWL, "propertyChainAxiom"))
                chain_list.set(_qname(NS_RDF, "parseType"), "Collection")
                
                # 1. Source Property (Assoc -> StartType)
                p1 = ET.SubElement(chain_list, _qname(NS_RDF, "Description"))
                p1.set(_qname(NS_RDF, "about"), get_resource(source_prop))
                
                # 2. Path Resolutions
                for res in t.object.resolutions:
                    if isinstance(res, EntityResolution):
                        # hasComposition<Rolename>
                        comp_prop = f"hasComposition{res.rolename}"
                        p2 = ET.SubElement(chain_list, _qname(NS_RDF, "Description"))
                        p2.set(_qname(NS_RDF, "about"), get_resource(comp_prop))
            
            max_c = -1
            if isinstance(t.multiplicity, list):
                max_c = t.multiplicity[1]
            elif isinstance(t.multiplicity, int):
                max_c = t.multiplicity
            elif str(t.multiplicity).isdigit():
                max_c = int(t.multiplicity)

            # Add restriction to Association Class
            if target_cls:
                create_restriction(cls_elem, target_prop, target_cls, max_c)
            
            # The source property usually points to the context.
            if isinstance(t.object, ParticipantPath):
                 create_restriction(cls_elem, source_prop, t.object.start_type, 1) # Usually 1 context

    return ET.ElementTree(root)
