import xml.etree.ElementTree as ET
from typing import List, Union
from tuple import UddlTuple
from query_parser import QueryStatement
from participant_path_parser import ParticipantPath, EntityResolution, AssociationResolution


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
    
    # Separate instance tuples (individuals) from class-defining tuples
    individual_tuples = [t for t in data_tuples if t.predicate == 'instance']
    class_tuples = [t for t in data_tuples if t.predicate != 'instance']

    # 1. Analyze tuples to identify Entities, Associations, Observables
    # (exclude typeOf tuples which define individuals, not classes)
    subjects = set()
    objects = set()
    association_subjects = set()

    for t in class_tuples:
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
    for t in class_tuples:
        if t.predicate == 'specializes' and t.subject in valid_subjects and isinstance(t.object, str):
            if t.subject not in parent_map:
                parent_map[t.subject] = set()
            parent_map[t.subject].add(t.object)

    # 3. Collect all composition subjects and ranges for the single composes property
    composition_subjects = set()
    composition_ranges = set()

    for t in class_tuples:
        if t.predicate == 'composes' and t.subject in valid_subjects and isinstance(t.object, str):
            composition_subjects.add(t.subject)
            composition_ranges.add(t.object)

    # Helper function to resolve participant path to final type
    def resolve_participant_path_to_type(path: ParticipantPath) -> str:
        """Resolve a participant path to its final type."""
        if not path.resolutions:
            return path.start_type
        
        last = path.resolutions[-1]
        if isinstance(last, AssociationResolution):
            return last.association_name
        
        # If EntityResolution has a target_type, use it
        if isinstance(last, EntityResolution) and last.target_type:
            return last.target_type
        
        # Get the parent type by resolving up to the second-to-last resolution
        if len(path.resolutions) == 1:
            source_type = path.start_type
        else:
            # Recursively get the type of the parent path
            parent_path = ParticipantPath(path.start_type, path.resolutions[:-1])
            source_type = resolve_participant_path_to_type(parent_path)
        
        # Look up the rolename in the model (check both composes and associates)
        for t in class_tuples:
            if t.subject == source_type and t.rolename == last.rolename:
                if isinstance(t.object, str):
                    return t.object.split('.')[0] if '.' in t.object else t.object
                elif isinstance(t.object, ParticipantPath):
                    return resolve_participant_path_to_type(t.object)
        
        # Fallback to rolename
        return last.rolename

    # 4. Collect all associates relationships and their final types
    association_subjects_with_associates = set()
    association_ranges = set()
    # Collect all rolenames used in associates relationships (both participant paths and direct)
    rolenames_in_paths = set()

    for t in class_tuples:
        if t.predicate == 'associates' and t.subject in associations:
            association_subjects_with_associates.add(t.subject)
            # Collect the rolename (used for both participant paths and direct relationships)
            if t.rolename:
                rolenames_in_paths.add(t.rolename)
            
            # Resolve the object to its final type
            if isinstance(t.object, ParticipantPath):
                final_type = resolve_participant_path_to_type(t.object)
                association_ranges.add(final_type)
                # Collect rolenames from the path if it has resolutions
                if t.object.resolutions:
                    for res in t.object.resolutions:
                        rolenames_in_paths.add(res.rolename)
            elif isinstance(t.object, str):
                association_ranges.add(t.object)

    # 5. Build OWL
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
    if composition_subjects or composition_ranges or association_subjects_with_associates or association_ranges:
        create_section_header("Object Properties")
        
        # Create a single composes property
        if composition_subjects or composition_ranges:
            create_iri_comment("composes")
            op = ET.Element(_qname(NS_OWL, "ObjectProperty"))
            op.set(_qname(NS_RDF, "about"), get_resource("composes"))
            root.append(op)
            
            # Domain: Entity (things that compose are entities or associations, which are entities)
            if entities or associations:
                r_domain = ET.SubElement(op, _qname(NS_RDFS, "domain"))
                r_domain.set(_qname(NS_RDF, "resource"), get_resource("Entity"))
            
            # Range: union of Entity and Observable (generic - can compose any Entity or Observable)
            # Since we're creating composes property, we must have entities/associations, so Entity always exists
            r_range = ET.SubElement(op, _qname(NS_RDFS, "range"))
            cls_elem = ET.SubElement(r_range, _qname(NS_OWL, "Class"))
            union = ET.SubElement(cls_elem, _qname(NS_OWL, "unionOf"))
            union.set(_qname(NS_RDF, "parseType"), "Collection")
            
            # Add Entity to the union (includes all entities and associations)
            # Entity always exists if we have entities/associations (which we do if we're creating this property)
            entity_ref = ET.SubElement(union, _qname(NS_OWL, "Class"))
            entity_ref.set(_qname(NS_RDF, "about"), get_resource("Entity"))
            
            # Add Observable to the union if observables exist
            if observables:
                observable_ref = ET.SubElement(union, _qname(NS_OWL, "Class"))
                observable_ref.set(_qname(NS_RDF, "about"), get_resource("Observable"))

            # Create isComposedBy inverse property
            create_iri_comment("isComposedBy")
            op_inv = ET.Element(_qname(NS_OWL, "ObjectProperty"))
            op_inv.set(_qname(NS_RDF, "about"), get_resource("isComposedBy"))
            root.append(op_inv)
            
            inverse_of = ET.SubElement(op_inv, _qname(NS_OWL, "inverseOf"))
            inverse_of.set(_qname(NS_RDF, "resource"), get_resource("composes"))
        
        # Create ObjectProperties for rolenames used in participant paths
        if rolenames_in_paths:
            for rolename in sorted(rolenames_in_paths):
                create_iri_comment(rolename)
                op_rolename = ET.Element(_qname(NS_OWL, "ObjectProperty"))
                op_rolename.set(_qname(NS_RDF, "about"), get_resource(rolename))
                root.append(op_rolename)
        
        # Create a single associates property
        if association_subjects_with_associates or association_ranges:
            create_iri_comment("associates")
            op_assoc = ET.Element(_qname(NS_OWL, "ObjectProperty"))
            op_assoc.set(_qname(NS_RDF, "about"), get_resource("associates"))
            root.append(op_assoc)
            
            # Domain: Association (things that associate are associations)
            if associations:
                r_domain = ET.SubElement(op_assoc, _qname(NS_RDFS, "domain"))
                r_domain.set(_qname(NS_RDF, "resource"), get_resource("Association"))
            
            # Range: union of Entity and Observable (generic - can associate any Entity or Observable)
            # Since we're creating associates property, we must have associations, so Entity always exists
            r_range = ET.SubElement(op_assoc, _qname(NS_RDFS, "range"))
            cls_elem = ET.SubElement(r_range, _qname(NS_OWL, "Class"))
            union = ET.SubElement(cls_elem, _qname(NS_OWL, "unionOf"))
            union.set(_qname(NS_RDF, "parseType"), "Collection")
            
            # Add Entity to the union (includes all entities and associations)
            # Entity always exists if we have associations (which we do if we're creating this property)
            entity_ref = ET.SubElement(union, _qname(NS_OWL, "Class"))
            entity_ref.set(_qname(NS_RDF, "about"), get_resource("Entity"))
            
            # Add Observable to the union if observables exist
            if observables:
                observable_ref = ET.SubElement(union, _qname(NS_OWL, "Class"))
                observable_ref.set(_qname(NS_RDF, "about"), get_resource("Observable"))
        
        # Create isAssociatedWith (inverse of associates) aka isParticipantIn
        create_iri_comment("isParticipantIn")
        op_part = ET.Element(_qname(NS_OWL, "ObjectProperty"))
        op_part.set(_qname(NS_RDF, "about"), get_resource("isParticipantIn"))
        root.append(op_part)
        
        inverse_of = ET.SubElement(op_part, _qname(NS_OWL, "inverseOf"))
        inverse_of.set(_qname(NS_RDF, "resource"), get_resource("associates"))

        # Create hasContextRoot
        create_iri_comment("hasContextRoot")
        op_root = ET.Element(_qname(NS_OWL, "ObjectProperty"))
        op_root.set(_qname(NS_RDF, "about"), get_resource("hasContextRoot"))
        root.append(op_root)

        # Create hasContext chain
        create_iri_comment("hasContext")
        op_ctx = ET.Element(_qname(NS_OWL, "ObjectProperty"))
        op_ctx.set(_qname(NS_RDF, "about"), get_resource("hasContext"))
        root.append(op_ctx)

        # Create hasObservable property for Observations
        if individual_tuples:
            create_iri_comment("hasObservable")
            op_has_obs = ET.Element(_qname(NS_OWL, "ObjectProperty"))
            op_has_obs.set(_qname(NS_RDF, "about"), get_resource("hasObservable"))
            root.append(op_has_obs)
            
            # Domain: Observation
            r_domain = ET.SubElement(op_has_obs, _qname(NS_RDFS, "domain"))
            r_domain.set(_qname(NS_RDF, "resource"), get_resource("Observation"))
            
            # Range: Observable
            if observables:
                r_range = ET.SubElement(op_has_obs, _qname(NS_RDFS, "range"))
                r_range.set(_qname(NS_RDF, "resource"), get_resource("Observable"))

    # --- Classes ---
    if entities or associations or observables:
        create_section_header("Classes")
    
    # Store class elements for later modification
    class_elements = {}

    # Create Entity base class if we have entities or associations
    if entities or associations:
        class_elements["Entity"] = create_class("Entity")

    # Create Association base class if we have associations
    if associations:
        class_elements["Association"] = create_class("Association")
        # Association is a subclass of Entity
        create_subclass_of(class_elements["Association"], "Entity")

    # Helper to check if a class has Entity or Association in its inheritance chain
    def has_entity_in_chain(class_name):
        """Check if Entity or Association is in the inheritance chain of a class.
        This checks if any ancestor is an Entity, Association, or if Entity/Association itself is in the chain."""
        if class_name == "Entity" or class_name == "Association":
            return True
        if class_name not in parent_map:
            return False
        
        # Check all ancestors
        queue = list(parent_map[class_name])
        visited = set(queue)
        
        while queue:
            ancestor = queue.pop(0)
            # If ancestor is Entity or Association itself, or if ancestor is an entity/association
            if ancestor == "Entity" or ancestor == "Association" or ancestor in entities or ancestor in associations:
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
        # Make Association subclass (not Entity directly, since Association inherits from Entity)
        # Only if Association exists and it's not already in the inheritance chain
        if associations and not has_entity_in_chain(name):
            create_subclass_of(class_elements[name], "Association")
        
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
    
    # Create Observation class if we have individuals (individuals are always based on Observables)
    if individual_tuples:
        create_class("Observation")

    # Process Specializations (Inheritance)
    for child, parents in parent_map.items():
        if child in class_elements:
             for parent in parents:
                 if isinstance(parent, str):
                    create_subclass_of(class_elements[child], parent)

    # Helper to check if composition is inherited
    # Pre-calculate direct compositions for quick lookup
    direct_compositions = {} # subject -> set(target_types)
    for t in class_tuples:
        if t.predicate == 'composes' and isinstance(t.object, str):
             if t.subject not in direct_compositions:
                 direct_compositions[t.subject] = set()
             direct_compositions[t.subject].add(t.object)

    # Helper to check if association is inherited
    # Pre-calculate direct associations for quick lookup
    direct_associations = {} # subject -> set(target_types)
    for t in class_tuples:
        if t.predicate == 'associates' and t.subject in associations:
            if isinstance(t.object, ParticipantPath):
                final_type = resolve_participant_path_to_type(t.object)
            elif isinstance(t.object, str):
                final_type = t.object
            else:
                continue
            if t.subject not in direct_associations:
                direct_associations[t.subject] = set()
            direct_associations[t.subject].add(final_type)

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

    def is_association_inherited(subject, target):
        if subject not in parent_map:
            return False
        
        # Check all ancestors
        queue = list(parent_map[subject])
        visited = set(queue)
        
        while queue:
            ancestor = queue.pop(0)
            if ancestor in direct_associations and target in direct_associations[ancestor]:
                return True
            
            if ancestor in parent_map:
                for p in parent_map[ancestor]:
                    if p not in visited:
                        visited.add(p)
                        queue.append(p)
        return False

    # Apply Restrictions (modifies existing class elements)
    for t in class_tuples:
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

    # Apply Restrictions for Associates (modifies existing class elements)
    for t in class_tuples:
        if t.predicate == 'associates' and t.subject in class_elements and t.subject in associations:
            # Resolve the object to its final type
            if isinstance(t.object, ParticipantPath):
                final_type = resolve_participant_path_to_type(t.object)
            elif isinstance(t.object, str):
                final_type = t.object
            else:
                continue
            
            # Skip if inherited
            if is_association_inherited(t.subject, final_type):
                continue

            # Use the associates property
            # The owl:onClass restriction will specify what is being associated
            prop_name = "associates"
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
            
            create_restriction(class_elements[t.subject], prop_name, final_type, min_c, max_c)

    # --- Individuals ---
    # Process 'instance' tuples: (class_name, instance, participant_path, individual_name)
    # For simple cases without paths, the object may be empty or a string
    individual_tuples_filtered = [t for t in individual_tuples if t.predicate == 'instance']
    
    if individual_tuples_filtered:
        create_section_header("Individuals")
        
        # Track property names created for individual paths to avoid duplicates
        individual_property_names = set()
        property_superproperties = {} # Map prop_name -> super_property_name
        
        # Group individuals by their type for better organization
        individuals_by_type = {}
        individual_paths = {}  # Map individual_name -> ParticipantPath (if it has a path)
        individual_classes = {}  # Map individual_name -> class_name
        
        for t in individual_tuples_filtered:
            # Format: (class_name, instance, participant_path, individual_name)
            class_name = t.subject
            individual_name = t.rolename  # Individual name is in rolename
            
            individual_classes[individual_name] = class_name
            
            if class_name not in individuals_by_type:
                individuals_by_type[class_name] = []
            individuals_by_type[class_name].append(individual_name)
            
            # Store the participant path if it exists (already parsed in parse_tuple.py)
            if isinstance(t.object, ParticipantPath):
                individual_paths[individual_name] = t.object
            # If object is empty string, there's no path for this individual
        
        # Create individuals grouped by type
        for class_name in sorted(individuals_by_type.keys()):
            for individual_name in sorted(individuals_by_type[class_name]):
                create_iri_comment(individual_name)
                individual = ET.Element(_qname(NS_OWL, "NamedIndividual"))
                individual.set(_qname(NS_RDF, "about"), get_resource(individual_name))
                root.append(individual)
                
                # Add rdf:type pointing to Observation (individuals are always Observation instances)
                rdf_type = ET.SubElement(individual, _qname(NS_RDF, "type"))
                rdf_type.set(_qname(NS_RDF, "resource"), get_resource("Observation"))

                # Add hasObservable pointing to the Observable type
                has_obs = ET.SubElement(individual, f"{{{namespace}}}hasObservable")
                has_obs.set(_qname(NS_RDF, "resource"), get_resource(class_name))
                
                # If this individual has a participant path, create property chain
                if individual_name in individual_paths:
                    path = individual_paths[individual_name]
                    
                    # Build property chain for each hop in the path
                    property_chain = []
                    current_type = path.start_type
                    
                    for resolution in path.resolutions:
                        target_to_assert = None
                        if isinstance(resolution, AssociationResolution):
                            # Association resolution: "has<Entity/Association name>Participant_<rolename>"
                            # The source is current_type, target is the association
                            prop_name = f"has{current_type}Participant_{resolution.rolename}"
                            property_chain.append(prop_name)
                            individual_property_names.add(prop_name)
                            property_superproperties[prop_name] = "isParticipantIn"
                            
                            # Move to the association type
                            current_type = resolution.association_name
                            target_to_assert = current_type
                        elif isinstance(resolution, EntityResolution):
                            # Entity resolution: "has<Entity/Association name>Composition_<rolename>"
                            # The source is current_type
                            prop_name = f"has{current_type}Composition_{resolution.rolename}"
                            property_chain.append(prop_name)
                            individual_property_names.add(prop_name)
                            
                            # Update current_type by looking up the rolename in the model
                            # Find what type the rolename points to
                            target_type = None
                            predicate = "associates" # Default fallback
                            
                            for t in class_tuples:
                                if t.subject == current_type and t.rolename == resolution.rolename:
                                    predicate = t.predicate
                                    if isinstance(t.object, str):
                                        target_type = t.object.split('.')[0] if '.' in t.object else t.object
                                        break
                                    elif isinstance(t.object, ParticipantPath):
                                        target_type = resolve_participant_path_to_type(t.object)
                                        break
                            
                            if predicate == 'composes':
                                property_superproperties[prop_name] = "composes"
                            else:
                                property_superproperties[prop_name] = "associates"

                            if target_type:
                                current_type = target_type
                                target_to_assert = target_type
                            # If we can't resolve, keep current_type (may be incomplete but won't break)
                        
                        # Add assertion to individual for this path step
                        if target_to_assert:
                            prop_tag = f"{{{namespace}}}{prop_name}"
                            step_elem = ET.SubElement(individual, prop_tag)
                            step_elem.set(_qname(NS_RDF, "resource"), get_resource(target_to_assert))
                    
                    # Register the start type property (needed for the general hasContext chain)
                    if property_chain:
                        # Create a property for the start_type to include in the chain
                        start_type_prop_name = f"has{path.start_type}"
                        individual_property_names.add(start_type_prop_name)
                        property_superproperties[start_type_prop_name] = "hasContextRoot"
                        
                        # Add assertion to individual: <hasStartType rdf:resource="StartType"/>
                        prop_tag = f"{{{namespace}}}{start_type_prop_name}"
                        start_prop_elem = ET.SubElement(individual, prop_tag)
                        start_prop_elem.set(_qname(NS_RDF, "resource"), get_resource(path.start_type))
        
        # Create all the individual hop properties that were referenced
        if individual_property_names:
            for prop_name in sorted(individual_property_names):
                create_iri_comment(prop_name)
                prop = ET.Element(_qname(NS_OWL, "ObjectProperty"))
                prop.set(_qname(NS_RDF, "about"), get_resource(prop_name))
                root.append(prop)
                
                # Add subPropertyOf if we have a mapping
                if prop_name in property_superproperties:
                    sub = ET.SubElement(prop, _qname(NS_RDFS, "subPropertyOf"))
                    sub.set(_qname(NS_RDF, "resource"), get_resource(property_superproperties[prop_name]))

    return ET.ElementTree(root)
