import xml.etree.ElementTree as ET
from typing import List, Union
from tuple import UddlTuple
from query_parser import QueryStatement


# Namespaces
NS_RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
NS_RDFS = "http://www.w3.org/2000/01/rdf-schema#"
NS_OWL = "http://www.w3.org/2002/07/owl#"
NS_XSD = "http://www.w3.org/2001/XMLSchema#"
NS_XML = "http://www.w3.org/XML/1998/namespace"
NS_DEFAULT = "http://example.org/uddl#"


def _qname(ns, tag):
    return f"{{{ns}}}{tag}"


def tuple2owl(tuples: List[Union[UddlTuple, QueryStatement]], namespace: str = NS_DEFAULT) -> ET.ElementTree:
    # Filter out queries
    data_tuples = [t for t in tuples if isinstance(t, UddlTuple)]

    # 1. Analyze tuples to identify Entities, Associations, Observables
    subjects = set()
    objects = set()
    association_subjects = set()

    for t in data_tuples:
        subjects.add(t.subject)
        
        if t.predicate == 'associates':
            association_subjects.add(t.subject)
            
        if isinstance(t.object, str):
            objects.add(t.object)

    # Classification Rules:
    associations = association_subjects
    entities = subjects - associations
    observables = objects - subjects
    
    valid_subjects = entities | associations | observables

    # 2. Build Inheritance Map (needed for rolename resolution)
    parent_map = {} # child -> set(parents)
    for t in data_tuples:
        if t.predicate == 'specializes' and t.subject in valid_subjects and isinstance(t.object, str):
            if t.subject not in parent_map:
                parent_map[t.subject] = set()
            parent_map[t.subject].add(t.object)

    # 3. Collect all composition subjects and ranges for the single composes property
    composition_subjects = set()
    composition_ranges = set()

    for t in data_tuples:
        if t.predicate == 'composes' and t.subject in valid_subjects and isinstance(t.object, str):
            composition_subjects.add(t.subject)
            composition_ranges.add(t.object)

    # 4. Build OWL
    ET.register_namespace("rdf", NS_RDF)
    ET.register_namespace("rdfs", NS_RDFS)
    ET.register_namespace("owl", NS_OWL)
    ET.register_namespace("xsd", NS_XSD)
    ET.register_namespace("", namespace)
    ET.register_namespace("xml", NS_XML)
    root = ET.Element(_qname(NS_RDF, "RDF"))
    root.set(_qname(NS_XML, "base"), namespace)
    
    # Ontology declaration
    ontology = ET.SubElement(root, _qname(NS_OWL, "Ontology"))
    ontology.set(_qname(NS_RDF, "about"), namespace)

    def get_resource(name):
        if name.startswith("http"):
            return name
        
        base_ns = namespace
        if base_ns.endswith("#"):
            return f"{base_ns}{name}"
        else:
            return f"{base_ns}#{name}"

    def create_section_header(title):
        root.append(ET.Comment(f"\n    ///////////////////////////////////////////////////////////////////////////////////////\n    //\n    // {title}\n    //\n    ///////////////////////////////////////////////////////////////////////////////////////\n     "))

    def create_iri_comment(name):
        root.append(ET.Comment(f" {get_resource(name)} "))

    def create_class(name):
        create_iri_comment(name)
        cls = ET.Element(_qname(NS_OWL, "Class"))
        cls.set(_qname(NS_RDF, "about"), get_resource(name))
        root.append(cls)
        return cls

    def create_subclass_of(child_element, parent_resource):
        sub = ET.SubElement(child_element, _qname(NS_RDFS, "subClassOf"))
        sub.set(_qname(NS_RDF, "resource"), get_resource(parent_resource))
        return sub

    def create_restriction(parent_class_element, on_property, on_class, min_card=None, max_card=None):
        # If min and max are equal and both are set, use qualifiedCardinality
        if min_card is not None and max_card is not None and min_card == max_card:
            r = ET.SubElement(parent_class_element, _qname(NS_RDFS, "subClassOf"))
            restriction = ET.SubElement(r, _qname(NS_OWL, "Restriction"))
            
            prop = ET.SubElement(restriction, _qname(NS_OWL, "onProperty"))
            prop.set(_qname(NS_RDF, "resource"), get_resource(on_property))
            
            cls = ET.SubElement(restriction, _qname(NS_OWL, "onClass"))
            cls.set(_qname(NS_RDF, "resource"), get_resource(on_class))
            
            card = ET.SubElement(restriction, _qname(NS_OWL, "qualifiedCardinality"))
            card.set(_qname(NS_RDF, "datatype"), f"{NS_XSD}nonNegativeInteger")
            card.text = str(min_card)
        else:
            # Otherwise, create separate restrictions for min and/or max
            if min_card is not None:
                r = ET.SubElement(parent_class_element, _qname(NS_RDFS, "subClassOf"))
                restriction = ET.SubElement(r, _qname(NS_OWL, "Restriction"))
                
                prop = ET.SubElement(restriction, _qname(NS_OWL, "onProperty"))
                prop.set(_qname(NS_RDF, "resource"), get_resource(on_property))
                
                cls = ET.SubElement(restriction, _qname(NS_OWL, "onClass"))
                cls.set(_qname(NS_RDF, "resource"), get_resource(on_class))
                
                card = ET.SubElement(restriction, _qname(NS_OWL, "minQualifiedCardinality"))
                card.set(_qname(NS_RDF, "datatype"), f"{NS_XSD}nonNegativeInteger")
                card.text = str(min_card)

            if max_card is not None:
                r = ET.SubElement(parent_class_element, _qname(NS_RDFS, "subClassOf"))
                restriction = ET.SubElement(r, _qname(NS_OWL, "Restriction"))
                
                prop = ET.SubElement(restriction, _qname(NS_OWL, "onProperty"))
                prop.set(_qname(NS_RDF, "resource"), get_resource(on_property))
                
                cls = ET.SubElement(restriction, _qname(NS_OWL, "onClass"))
                cls.set(_qname(NS_RDF, "resource"), get_resource(on_class))
                
                card = ET.SubElement(restriction, _qname(NS_OWL, "maxQualifiedCardinality"))
                card.set(_qname(NS_RDF, "datatype"), f"{NS_XSD}nonNegativeInteger")
                card.text = str(max_card)

    # --- Object Properties ---
    if composition_subjects or composition_ranges:
        create_section_header("Object Properties")
        
        # Create a single composes property
        create_iri_comment("composes")
        op = ET.Element(_qname(NS_OWL, "ObjectProperty"))
        op.set(_qname(NS_RDF, "about"), get_resource("composes"))
        root.append(op)
        
        # Domain: Entity (things that compose are entities or associations, which are entities)
        if entities or associations:
            r_domain = ET.SubElement(op, _qname(NS_RDFS, "domain"))
            r_domain.set(_qname(NS_RDF, "resource"), get_resource("Entity"))
        
        # Range: union of all ranges that can be composed
        if len(composition_ranges) == 1:
            r_range = ET.SubElement(op, _qname(NS_RDFS, "range"))
            r_range.set(_qname(NS_RDF, "resource"), get_resource(list(composition_ranges)[0]))
        elif len(composition_ranges) > 1:
            r_range = ET.SubElement(op, _qname(NS_RDFS, "range"))
            cls_elem = ET.SubElement(r_range, _qname(NS_OWL, "Class"))
            union = ET.SubElement(cls_elem, _qname(NS_OWL, "unionOf"))
            union.set(_qname(NS_RDF, "parseType"), "Collection")
            for rng in sorted(composition_ranges):
                cls_ref = ET.SubElement(union, _qname(NS_OWL, "Class"))
                cls_ref.set(_qname(NS_RDF, "about"), get_resource(rng))


    # --- Classes ---
    if entities or associations or observables:
        create_section_header("Classes")
    
    # Store class elements for later modification
    class_elements = {}

    # Create Entity base class if we have entities or associations
    if entities or associations:
        class_elements["Entity"] = create_class("Entity")

    # Helper to check if a class has Entity in its inheritance chain
    def has_entity_in_chain(class_name):
        """Check if Entity is in the inheritance chain of a class.
        This checks if any ancestor is an Entity or Association (which will be subclasses of Entity),
        or if Entity itself is in the chain."""
        if class_name == "Entity":
            return True
        if class_name not in parent_map:
            return False
        
        # Check all ancestors
        queue = list(parent_map[class_name])
        visited = set(queue)
        
        while queue:
            ancestor = queue.pop(0)
            # If ancestor is Entity itself, or if ancestor is an entity/association (which will be subclass of Entity)
            if ancestor == "Entity" or ancestor in entities or ancestor in associations:
                return True
            if ancestor in parent_map:
                for p in parent_map[ancestor]:
                    if p not in visited:
                        visited.add(p)
                        queue.append(p)
        return False

    # Entities
    for name in sorted(entities):
        class_elements[name] = create_class(name)
        # Only make Entity subclass if Entity exists and it's not already in the inheritance chain
        if (entities or associations) and not has_entity_in_chain(name):
            create_subclass_of(class_elements[name], "Entity")
        
    # Associations
    for name in sorted(associations):
        class_elements[name] = create_class(name)
        # Only make Entity subclass if Entity exists and it's not already in the inheritance chain
        if (entities or associations) and not has_entity_in_chain(name):
            create_subclass_of(class_elements[name], "Entity")
        
    # Observables
    if observables:
        create_class("Observable")
        for name in sorted(observables):
            class_elements[name] = create_class(name)
            create_subclass_of(class_elements[name], "Observable")

        # Disjoint Observables
        if len(observables) > 1:
            disjoint = ET.Element(_qname(NS_OWL, "AllDisjointClasses"))
            root.append(disjoint)
            
            members = ET.SubElement(disjoint, _qname(NS_OWL, "members"))
            members.set(_qname(NS_RDF, "parseType"), "Collection")
            
            for name in sorted(observables):
                cls_ref = ET.SubElement(members, _qname(NS_OWL, "Class"))
                cls_ref.set(_qname(NS_RDF, "about"), get_resource(name))

    # Process Specializations (Inheritance)
    for child, parents in parent_map.items():
        if child in class_elements:
             for parent in parents:
                 if isinstance(parent, str):
                    create_subclass_of(class_elements[child], parent)

    # Helper to check if composition is inherited
    # Pre-calculate direct compositions for quick lookup
    direct_compositions = {} # subject -> set(target_types)
    for t in data_tuples:
        if t.predicate == 'composes' and isinstance(t.object, str):
             if t.subject not in direct_compositions:
                 direct_compositions[t.subject] = set()
             direct_compositions[t.subject].add(t.object)

    def is_composition_inherited(subject, target):
        if subject not in parent_map:
            return False
        
        # Check all ancestors
        queue = list(parent_map[subject])
        visited = set(queue)
        
        while queue:
            ancestor = queue.pop(0)
            if ancestor in direct_compositions and target in direct_compositions[ancestor]:
                return True
            
            if ancestor in parent_map:
                for p in parent_map[ancestor]:
                    if p not in visited:
                        visited.add(p)
                        queue.append(p)
        return False

    # Apply Restrictions (modifies existing class elements)
    for t in data_tuples:
        if t.predicate == 'composes' and t.subject in class_elements and isinstance(t.object, str):
             # Skip if inherited
             if is_composition_inherited(t.subject, t.object):
                 continue

             # Use the single composes property for all restrictions
             # The owl:onClass restriction will specify what is being composed
             prop_name = "composes"
             
             min_c = None
             max_c = None
             
             if t.multiplicity is None:
                 pass 
             elif isinstance(t.multiplicity, list) and len(t.multiplicity) >= 2:
                 min_c = t.multiplicity[0]
                 max_c = t.multiplicity[1]
             elif isinstance(t.multiplicity, int):
                 max_c = t.multiplicity
                 if min_c is None: min_c = t.multiplicity
             
             if min_c is None and max_c is None:
                 min_c = 1
                 max_c = 1

             if str(min_c) == '*': min_c = 0
             if str(max_c) == '*': max_c = None 
             
             create_restriction(class_elements[t.subject], prop_name, t.object, min_c, max_c)

    # --- Individuals ---
    # create_section_header("Individuals")

    return ET.ElementTree(root)
