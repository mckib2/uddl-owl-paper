import xml.etree.ElementTree as ET
from typing import List, Union, Optional, Dict
import html
import pathlib
import sys

from tuple import UddlTuple
from query_parser import get_ast, QueryStatement
from participant_path_parser import ParticipantPath, EntityResolution, AssociationResolution


def uddl2tuple(uddl_doc: ET.ElementTree) -> List[Union[UddlTuple, QueryStatement]]:
    """
    Parse a UDDL .face XML file and convert it to a list of UddlTuples and QueryStatements.
    
    Only processes platform:Entity, platform:Association, and platform:Query elements.
    For compositions, follows the realizes chain through logical layer to find conceptual Observables.
    """
    root = uddl_doc.getroot()
    tuples = []
    
    # Register namespaces for easier searching
    namespaces = {
        'xmi': 'http://www.omg.org/XMI',
        'platform': 'http://www.opengroup.us/datamodel/platform/1.0',
        'logical': 'http://www.opengroup.us/datamodel/logical/1.0',
        'conceptual': 'http://www.opengroup.us/datamodel/conceptual/1.0',
    }
    
    # Build a map of xmi:id to elements for quick lookup
    id_map: Dict[str, ET.Element] = {}
    
    def build_id_map(elem: ET.Element):
        """Recursively build a map of xmi:id to elements."""
        elem_id = elem.get('{http://www.omg.org/XMI}id')
        if elem_id:
            id_map[elem_id] = elem
        for child in elem:
            build_id_map(child)
    
    build_id_map(root)
    
    def find_element_by_id(elem_id: str) -> Optional[ET.Element]:
        """Find an element by its xmi:id."""
        return id_map.get(elem_id)
    
    def follow_realizes_chain(elem: ET.Element, target_type: str = "conceptual:Observable") -> Optional[ET.Element]:
        """
        Follow the realizes chain from platform/logical layer to conceptual layer.
        Returns the element at the conceptual level.
        """
        if elem is None:
            return None
        
        # Check if this element is the target type
        xmi_type = elem.get('{http://www.omg.org/XMI}type', '')
        if target_type in xmi_type:
            return elem
        
        # Follow realizes chain
        realizes = elem.get('realizes')
        if realizes:
            realized_elem = find_element_by_id(realizes)
            if realized_elem is not None:
                return follow_realizes_chain(realized_elem, target_type)
        
        return None
    
    def get_conceptual_entity_or_association(platform_elem: ET.Element) -> Optional[ET.Element]:
        """
        Follow the realizes chain from a platform Entity/Association through logical to conceptual.
        Returns the conceptual Entity or Association.
        """
        # First go to logical level
        realizes = platform_elem.get('realizes')
        if not realizes:
            return None
        
        logical_elem = find_element_by_id(realizes)
        if logical_elem is None:
            return None
        
        # Then go to conceptual level
        logical_realizes = logical_elem.get('realizes')
        if not logical_realizes:
            return None
        
        conceptual_elem = find_element_by_id(logical_realizes)
        if conceptual_elem is None:
            return None
        
        # Verify it's a conceptual Entity or Association
        xmi_type = conceptual_elem.get('{http://www.omg.org/XMI}type', '')
        if 'conceptual:Entity' in xmi_type or 'conceptual:Association' in xmi_type:
            return conceptual_elem
        
        return None
    
    def get_observable_name(composition_elem: ET.Element) -> Optional[str]:
        """
        Follow the realizes chain from a composition to find the Observable name.
        """
        # First, get the type (which might be a platform type)
        type_id = composition_elem.get('type')
        if not type_id:
            return None
        
        type_elem = find_element_by_id(type_id)
        if type_elem is None:
            return None
        
        # Follow realizes chain to conceptual Observable
        observable_elem = follow_realizes_chain(type_elem, "conceptual:Observable")
        if observable_elem is not None:
            return observable_elem.get('name')
        
        return None
    
    def get_entity_name_by_id(elem_id: str) -> Optional[str]:
        """Get the name of an entity by its xmi:id."""
        elem = find_element_by_id(elem_id)
        if elem:
            return elem.get('name')
        return None
    
    def parse_participant_path(participant_elem: ET.Element, association_name: str) -> Optional[ParticipantPath]:
        """
        Parse a participant path from a participant element (can be conceptual, logical, or platform).
        Returns a ParticipantPath if the participant has a path, None otherwise.
        
        Paths can have:
        - A ParticipantPathNode with a projectedParticipant (association participant)
        - Nested CharacteristicPathNode(s) for entity compositions
        """
        # Look for ParticipantPathNode first (for association paths)
        # Tags are not namespaced, so search for "path" with xmi:type
        path_node = None
        for path_elem in participant_elem.findall('.//path'):
            path_type = path_elem.get('{http://www.omg.org/XMI}type', '')
            if 'ParticipantPathNode' in path_type:
                path_node = path_elem
                break
        
        resolutions = []
        
        if path_node is not None:
            # Get the projected participant (which is another participant in an association)
            projected_participant_id = path_node.get('projectedParticipant')
            if projected_participant_id:
                projected_participant = find_element_by_id(projected_participant_id)
                if projected_participant is not None:
                    # Get the association this participant belongs to
                    parent_assoc = projected_participant.find('..')
                    while parent_assoc is not None:
                        xmi_type = parent_assoc.get('{http://www.omg.org/XMI}type', '')
                        if xmi_type and 'Association' in xmi_type:
                            projected_assoc_name = parent_assoc.get('name')
                            if projected_assoc_name:
                                rolename = projected_participant.get('rolename')
                                if rolename:
                                    resolutions.append(AssociationResolution(
                                        rolename=rolename,
                                        association_name=projected_assoc_name
                                    ))
                            break
                        parent_assoc = parent_assoc.find('..')
            
            # Look for nested CharacteristicPathNode(s) (tag is "node")
            for node_elem in path_node.findall('.//node'):
                node_type = node_elem.get('{http://www.omg.org/XMI}type', '')
                if 'CharacteristicPathNode' in node_type:
                    projected_char_id = node_elem.get('projectedCharacteristic')
                    if projected_char_id:
                        projected_char = find_element_by_id(projected_char_id)
                        if projected_char is not None:
                            rolename = projected_char.get('rolename')
                            if rolename:
                                resolutions.append(EntityResolution(rolename=rolename))
        else:
            # Check for direct CharacteristicPathNode (no ParticipantPathNode wrapper)
            # Look for "path" with CharacteristicPathNode type
            for path_elem in participant_elem.findall('.//path'):
                path_type = path_elem.get('{http://www.omg.org/XMI}type', '')
                if 'CharacteristicPathNode' in path_type:
                    projected_char_id = path_elem.get('projectedCharacteristic')
                    if projected_char_id:
                        projected_char = find_element_by_id(projected_char_id)
                        if projected_char is not None:
                            rolename = projected_char.get('rolename')
                            if rolename:
                                resolutions.append(EntityResolution(rolename=rolename))
                    break
        
        # If no path found, return None
        if not resolutions:
            return None
        
        # Get the type of the participant (the starting entity)
        # For conceptual participants, the type points to a conceptual entity
        participant_type_id = participant_elem.get('type')
        if not participant_type_id:
            return None
        
        # Resolve the type to get the entity name
        # The type might point to a conceptual entity directly
        type_elem = find_element_by_id(participant_type_id)
        if type_elem is None:
            return None
        
        # Get the name from the conceptual entity
        start_type = type_elem.get('name')
        if start_type is None:
            return None
        
        return ParticipantPath(start_type=start_type, resolutions=resolutions)
    
    # Track processed conceptual entities/associations to avoid duplicates
    processed_conceptual_ids = set()
    
    def process_conceptual_entity(conceptual_entity: ET.Element, entity_name: str):
        """Process a conceptual entity: specializes, compositions."""
        entity_id = conceptual_entity.get('{http://www.omg.org/XMI}id')
        if entity_id in processed_conceptual_ids:
            return  # Already processed
        processed_conceptual_ids.add(entity_id)
        
        # Process specializes relationship (if present)
        specializes_id = conceptual_entity.get('specializes')
        if specializes_id:
            parent_entity = find_element_by_id(specializes_id)
            if parent_entity is not None:
                parent_entity_name = parent_entity.get('name')
                if parent_entity_name:
                    tuples.append(UddlTuple(
                        subject=entity_name,
                        predicate='specializes',
                        object=parent_entity_name,
                        rolename="",
                        multiplicity=None
                    ))
        
        # Process compositions from conceptual level
        for composition in conceptual_entity.findall('.//composition'):
            comp_type = composition.get('{http://www.omg.org/XMI}type', '')
            if 'conceptual:Composition' not in comp_type:
                continue
            rolename = composition.get('rolename')
            if not rolename:
                continue
            
            observable_name = get_observable_name(composition)
            if observable_name:
                lower_bound = composition.get('lowerBound', '1')
                upper_bound = composition.get('upperBound', '1')
                
                try:
                    lower = int(lower_bound)
                except ValueError:
                    lower = -1 if upper_bound == '*' else 1
                
                try:
                    upper = int(upper_bound) if upper_bound != '*' else -1
                except ValueError:
                    upper = -1
                
                multiplicity = [lower, upper]
                tuples.append(UddlTuple(
                    subject=entity_name,
                    predicate='composes',
                    object=observable_name,
                    rolename=rolename,
                    multiplicity=multiplicity
                ))
    
    def process_conceptual_association(conceptual_association: ET.Element, association_name: str):
        """Process a conceptual association: compositions and participants."""
        assoc_id = conceptual_association.get('{http://www.omg.org/XMI}id')
        if assoc_id in processed_conceptual_ids:
            return  # Already processed
        processed_conceptual_ids.add(assoc_id)
        
        # Process compositions from conceptual level
        for composition in conceptual_association.findall('.//composition'):
            comp_type = composition.get('{http://www.omg.org/XMI}type', '')
            if 'conceptual:Composition' not in comp_type:
                continue
            rolename = composition.get('rolename')
            if not rolename:
                continue
            
            observable_name = get_observable_name(composition)
            if observable_name:
                lower_bound = composition.get('lowerBound', '1')
                upper_bound = composition.get('upperBound', '1')
                
                try:
                    lower = int(lower_bound)
                except ValueError:
                    lower = -1 if upper_bound == '*' else 1
                
                try:
                    upper = int(upper_bound) if upper_bound != '*' else -1
                except ValueError:
                    upper = -1
                
                multiplicity = [lower, upper]
                tuples.append(UddlTuple(
                    subject=association_name,
                    predicate='composes',
                    object=observable_name,
                    rolename=rolename,
                    multiplicity=multiplicity
                ))
        
        # Process participants from conceptual level
        for participant in conceptual_association.findall('.//participant'):
            part_type = participant.get('{http://www.omg.org/XMI}type', '')
            if 'conceptual:Participant' not in part_type:
                continue
            rolename = participant.get('rolename')
            if not rolename:
                continue
            
            participant_type_id = participant.get('type')
            if not participant_type_id:
                continue
            
            # Get participant type name (resolve to conceptual entity)
            type_elem = find_element_by_id(participant_type_id)
            if type_elem is None:
                continue
            participant_type_name = type_elem.get('name')
            if not participant_type_name:
                continue
            
            # Get bounds for multiplicity
            lower_bound = participant.get('lowerBound', '1')
            upper_bound = participant.get('upperBound', '1')
            source_lower_bound = participant.get('sourceLowerBound', '0')
            source_upper_bound = participant.get('sourceUpperBound', '-1')
            
            try:
                lower = int(lower_bound)
            except ValueError:
                lower = -1
            
            try:
                upper = int(upper_bound) if upper_bound != '*' else -1
            except ValueError:
                upper = -1
            
            try:
                source_lower = int(source_lower_bound)
            except ValueError:
                source_lower = -1
            
            try:
                source_upper = int(source_upper_bound) if source_upper_bound != '*' else -1
            except ValueError:
                source_upper = -1
            
            multiplicity = [lower, upper, source_lower, source_upper]
            
            # Check if participant has a path
            path = parse_participant_path(participant, association_name)
            
            if path:
                tuples.append(UddlTuple(
                    subject=association_name,
                    predicate='associates',
                    object=path,
                    rolename=rolename,
                    multiplicity=multiplicity
                ))
            else:
                tuples.append(UddlTuple(
                    subject=association_name,
                    predicate='associates',
                    object=participant_type_name,
                    rolename=rolename,
                    multiplicity=multiplicity
                ))
    
    # First, process all conceptual:Entity and conceptual:Association elements directly
    for elem in root.iter():
        xmi_type = elem.get('{http://www.omg.org/XMI}type', '')
        
        # Process conceptual:Entity directly
        if xmi_type and xmi_type.startswith('conceptual:Entity'):
            entity_name = elem.get('name')
            if entity_name:
                process_conceptual_entity(elem, entity_name)
        
        # Process conceptual:Association directly
        elif xmi_type and xmi_type.startswith('conceptual:Association'):
            association_name = elem.get('name')
            if association_name:
                process_conceptual_association(elem, association_name)
    
    # Then process platform:Entity, platform:Association, and platform:Query elements
    # (these will follow realizes chain, but we'll skip if already processed)
    for elem in root.iter():
        xmi_type = elem.get('{http://www.omg.org/XMI}type', '')
        
        # Process platform:Entity
        if xmi_type and xmi_type.startswith('platform:Entity'):
            entity_name = elem.get('name')
            if not entity_name:
                continue
            
            # Follow realizes chain to conceptual Entity
            conceptual_entity = get_conceptual_entity_or_association(elem)
            if conceptual_entity is None:
                continue
            
            # Process using the same function (will skip if already processed)
            process_conceptual_entity(conceptual_entity, entity_name)
        
        # Process platform:Association
        elif xmi_type and xmi_type.startswith('platform:Association'):
            association_name = elem.get('name')
            if not association_name:
                continue
            
            # Follow realizes chain to conceptual Association
            conceptual_association = get_conceptual_entity_or_association(elem)
            if conceptual_association is None:
                continue
            
            # Process using the same function (will skip if already processed)
            process_conceptual_association(conceptual_association, association_name)
        
        # Process platform:Query
        elif xmi_type and xmi_type.startswith('platform:Query'):
            specification = elem.get('specification')
            if specification:
                # Decode HTML entities (e.g., &#10; for newline)
                query_text = html.unescape(specification)
                try:
                    query_ast = get_ast(query_text)
                    if query_ast:
                        tuples.append(query_ast)
                except Exception as e:
                    print(f"Warning: Failed to parse query '{elem.get('name', 'unknown')}': {e}", file=sys.stderr)
    
    return tuples


def format_tuple_for_output(tuple_obj: Union[UddlTuple, QueryStatement]) -> str:
    """
    Format a UddlTuple or QueryStatement for output to a tuple file.
    Returns the string representation suitable for writing to a file.
    Matches the format expected by parse_tuple.py.
    """
    if isinstance(tuple_obj, QueryStatement):
        # For queries, return the string representation with proper formatting
        # Queries should end with a semicolon and newline
        query_str = str(tuple_obj)
        if not query_str.endswith(';'):
            query_str += ';'
        return query_str + "\n\n"
    elif isinstance(tuple_obj, UddlTuple):
        # Format tuple as: (Subject, Predicate[multiplicity], Object, rolename)
        # Match the format from satellite.txt
        predicate_str = tuple_obj.predicate
        
        # Add multiplicity if present
        if tuple_obj.multiplicity:
            if isinstance(tuple_obj.multiplicity, list):
                if len(tuple_obj.multiplicity) == 4:
                    predicate_str = f"{predicate_str}[{tuple_obj.multiplicity[0]}, {tuple_obj.multiplicity[1]}][{tuple_obj.multiplicity[2]}, {tuple_obj.multiplicity[3]}]"
                elif len(tuple_obj.multiplicity) == 2:
                    predicate_str = f"{predicate_str}[{tuple_obj.multiplicity[0]}, {tuple_obj.multiplicity[1]}]"
        
        # Format object (could be string or ParticipantPath)
        object_str = str(tuple_obj.object) if tuple_obj.object else ""
        
        # Format rolename - include trailing comma even if empty
        rolename_str = tuple_obj.rolename if tuple_obj.rolename else ""
        
        return f"({tuple_obj.subject}, {predicate_str}, {object_str}, {rolename_str})\n"
    else:
        return str(tuple_obj) + "\n"


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Convert a UDDL .face XML file to a tuple file format.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s input.face
  %(prog)s input.face -o output.txt
  %(prog)s input.face --output output.txt
        """
    )
    parser.add_argument(
        "face_file",
        type=pathlib.Path,
        help="Path to the .face XML file to convert"
    )
    parser.add_argument(
        "-o", "--output",
        type=pathlib.Path,
        default=None,
        help="Output file path. If not specified, output is printed to stdout."
    )
    
    args = parser.parse_args()
    
    try:
        # Parse the XML file
        tree = ET.parse(args.face_file)
        
        # Convert to tuples
        tuples = uddl2tuple(tree)
        
        # Prepare output
        output_lines = []
        for tuple_obj in tuples:
            output_lines.append(format_tuple_for_output(tuple_obj))
        
        output_text = "".join(output_lines)
        
        # Write to file or stdout
        if args.output:
            args.output.write_text(output_text)
            print(f"Successfully converted {len(tuples)} tuples/queries to {args.output}", file=sys.stderr)
        else:
            # Print to stdout (without trailing newlines)
            sys.stdout.write(output_text.rstrip("\n"))
            if output_text and not output_text.rstrip("\n"):
                # If all content was newlines, keep one
                sys.stdout.write("\n")
            elif output_text:
                sys.stdout.write("\n")
    
    except FileNotFoundError:
        print(f"Error: File not found: {args.face_file}", file=sys.stderr)
        sys.exit(1)
    except ET.ParseError as e:
        print(f"Error: Failed to parse XML file: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
