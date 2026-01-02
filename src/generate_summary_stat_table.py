import xml.etree.ElementTree as ET
import sys
import pathlib
import argparse
from collections import defaultdict, Counter

def generate_summary_stats(face_file_path):
    try:
        tree = ET.parse(face_file_path)
    except Exception as e:
        print(f"Error parsing file: {e}", file=sys.stderr)
        sys.exit(1)

    root = tree.getroot()
    
    # Namespaces
    ns = {
        'xmi': 'http://www.omg.org/XMI',
        'conceptual': 'http://www.opengroup.us/datamodel/conceptual/1.0',
    }
    
    # ID maps
    # We need a map of ID -> Element to traverse paths and resolve types
    id_map = {}
    for elem in root.iter():
        xmi_id = elem.get(f"{{{ns['xmi']}}}id")
        if xmi_id:
            id_map[xmi_id] = elem

    def get_element_name(elem_id):
        if not elem_id or elem_id not in id_map:
            return "?"
        elem = id_map[elem_id]
        return elem.get('name') or elem.get('rolename') or "?"

    def get_element_by_id(elem_id):
        return id_map.get(elem_id)

    # Counters
    num_entities = 0
    num_associations = 0
    total_compositions = 0
    total_participants = 0
    
    # Graph for centrality
    # Entity -> set of related things (associations it is in)
    entity_connections = defaultdict(set)
    # Association -> set of related things (participants)
    association_connections = defaultdict(set)
    
    # Also track composition counts per element for scoring
    entity_composition_counts = defaultdict(int)
    association_composition_counts = defaultdict(int)
    
    # Store details for display
    # ID -> list of composition strings
    entity_composition_details = defaultdict(list)
    association_composition_details = defaultdict(list)
    # ID -> list of participant strings
    association_participant_details = defaultdict(list)

    # First pass: Find elements
    conceptual_entities = []
    conceptual_associations = []
    
    for elem in root.iter():
        xmi_type = elem.get(f"{{{ns['xmi']}}}type", "")
        
        if 'conceptual:Entity' in xmi_type:
            conceptual_entities.append(elem)
        elif 'conceptual:Association' in xmi_type:
            conceptual_associations.append(elem)

    num_entities = len(conceptual_entities)
    num_associations = len(conceptual_associations)
    
    # Formatting helper for compositions
    def format_composition(rolename, type_name):
        # If rolename is same as type_name (except for first letter case)
        # e.g. identifier (Identifier) -> Identifier
        if rolename and type_name:
            if rolename == type_name or (len(rolename) > 0 and len(type_name) > 0 and rolename[0].lower() + rolename[1:] == type_name[0].lower() + type_name[1:]):
                return type_name
        return f"{rolename} ({type_name})"

    # Process Entities
    for entity in conceptual_entities:
        entity_name = entity.get('name')
        entity_id = entity.get(f"{{{ns['xmi']}}}id")
        
        # Compositions
        compositions = entity.findall(".//composition")
        count_comps = 0
        for comp in compositions:
            comp_type = comp.get(f"{{{ns['xmi']}}}type", "")
            if 'conceptual:Composition' in comp_type:
                count_comps += 1
                
                # Get details
                rolename = comp.get('rolename')
                type_id = comp.get('type')
                type_name = get_element_name(type_id)
                entity_composition_details[entity_name].append(format_composition(rolename, type_name))

        total_compositions += count_comps
        entity_composition_counts[entity_name] = count_comps
        
        # Specializes
        specializes_id = entity.get('specializes')
        if specializes_id:
            parent_name = get_element_name(specializes_id)
            entity_connections[entity_name].add(parent_name)
            if parent_name != "?":
                entity_connections[parent_name].add(entity_name)

    # Process Associations
    for assoc in conceptual_associations:
        assoc_name = assoc.get('name')
        assoc_id = assoc.get(f"{{{ns['xmi']}}}id")
        
        # Compositions
        compositions = assoc.findall(".//composition")
        count_comps = 0
        for comp in compositions:
            comp_type = comp.get(f"{{{ns['xmi']}}}type", "")
            if 'conceptual:Composition' in comp_type:
                count_comps += 1
                
                # Get details
                rolename = comp.get('rolename')
                type_id = comp.get('type')
                type_name = get_element_name(type_id)
                association_composition_details[assoc_name].append(format_composition(rolename, type_name))
        
        total_compositions += count_comps
        association_composition_counts[assoc_name] = count_comps
        
        # Participants
        participants = assoc.findall(".//participant")
        count_parts = 0
        for part in participants:
            part_type = part.get(f"{{{ns['xmi']}}}type", "")
            if 'conceptual:Participant' in part_type:
                count_parts += 1
                
                # Link Association to Entity
                entity_ref_id = part.get('type')
                entity_name = get_element_name(entity_ref_id)
                
                if entity_name != "?":
                    association_connections[assoc_name].add(entity_name)
                    entity_connections[entity_name].add(assoc_name)
                    
                # Get details
                rolename = part.get('rolename')
                
                # Path
                path_str = ""
                # Find path nodes
                # Simplified path extraction
                path_steps = []
                
                # ParticipantPathNode
                for path in part.findall(".//path"):
                    if 'ParticipantPathNode' in path.get(f"{{{ns['xmi']}}}type", ""):
                        proj_part_id = path.get('projectedParticipant')
                        if proj_part_id:
                            path_steps.append(get_element_name(proj_part_id))
                    
                    # CharacteristicPathNode
                    for node in path.findall(".//node"):
                         if 'CharacteristicPathNode' in node.get(f"{{{ns['xmi']}}}type", ""):
                            proj_char_id = node.get('projectedCharacteristic')
                            if proj_char_id:
                                path_steps.append(get_element_name(proj_char_id))

                # Also check for direct CharacteristicPathNode as direct child of path
                for path in part.findall(".//path"):
                    if 'CharacteristicPathNode' in path.get(f"{{{ns['xmi']}}}type", ""):
                        proj_char_id = path.get('projectedCharacteristic')
                        if proj_char_id:
                            path_steps.append(get_element_name(proj_char_id))

                if path_steps:
                    path_str = " $\\to$ " + " $\\to$ ".join(path_steps)
                
                association_participant_details[assoc_name].append(f"{rolename}: {entity_name}{path_str}")
        
        total_participants += count_parts

    # Calculate Centrality Scores
    best_entity = None
    max_entity_score = -1
    
    for entity in conceptual_entities:
        name = entity.get('name')
        score = len(entity_connections[name]) + entity_composition_counts[name]
        if score > max_entity_score:
            max_entity_score = score
            best_entity = name
            
    best_assoc = None
    max_assoc_score = -1
    
    # Find representative observation and assembly associations
    observe_assoc = None
    assembly_assoc = None
    
    # Helper to calculate score for an association name
    def get_assoc_score(name):
        return len(association_connections[name]) + association_composition_counts[name]

    for assoc in conceptual_associations:
        name = assoc.get('name')
        score = get_assoc_score(name)
        
        if score > max_assoc_score:
            max_assoc_score = score
            best_assoc = name
            
        # Check for Observe pattern
        if "_Observe_" in name:
            if observe_assoc is None or score > get_assoc_score(observe_assoc):
                observe_assoc = name
                
        # Check for PartOf pattern
        if "_PartOf_" in name:
             if assembly_assoc is None or score > get_assoc_score(assembly_assoc):
                assembly_assoc = name

    # Formatting helper
    def format_list(items):
        if not items:
            return "None"
        # Join with commas, escape underscores
        return ", ".join(items).replace('_', '\\_')
    
    # Formatting helper for participants (one per line with indentation)
    def format_participants(items):
        if not items:
            return "None"
        # Format each participant on a new line with indentation, escape underscores
        escaped_items = [item.replace('_', '\\_') for item in items]
        if len(escaped_items) == 1:
            return escaped_items[0]
        # First item on same line, rest indented to align with text after "Participants: "
        # We use phantom to match the width of "\textit{Participants}: "
        indent = "\\phantom{\\textit{Participants}: }"
        return escaped_items[0] + "".join(f"\\newline{indent}" + item for item in escaped_items[1:])

    # Prepare strings for best entity/assoc
    best_entity_display = best_entity.replace('_', '\\_') if best_entity else 'N/A'
    entity_compositions_str = format_list(entity_composition_details[best_entity])
    
    best_assoc_display = best_assoc.replace('_', '\\_') if best_assoc else 'N/A'
    assoc_compositions_str = format_list(association_composition_details[best_assoc])
    assoc_participants_str = format_participants(association_participant_details[best_assoc])
    
    # Prepare strings for Observation Pattern
    observe_display = observe_assoc.replace('_', '\\_') if observe_assoc else 'N/A'
    observe_compositions_str = format_list(association_composition_details[observe_assoc]) if observe_assoc else "N/A"
    observe_participants_str = format_participants(association_participant_details[observe_assoc]) if observe_assoc else "N/A"
    
    # Prepare strings for Assembly Pattern
    assembly_display = assembly_assoc.replace('_', '\\_') if assembly_assoc else 'N/A'
    assembly_compositions_str = format_list(association_composition_details[assembly_assoc]) if assembly_assoc else "N/A"
    assembly_participants_str = format_participants(association_participant_details[assembly_assoc]) if assembly_assoc else "N/A"

    # Parse queries to count characteristics
    import re
    from query_parser import UDDLQueryParser

    total_queries = 0
    total_projected_characteristics = 0
    total_entities_in_queries = 0
    total_joins = 0
    total_join_conditions = 0
    
    # Per-query statistics for complexity metrics
    query_details = []

    for elem in root.iter():
        xmi_type = elem.get(f"{{{ns['xmi']}}}type", "")
        # Look for UDDL Query specification elements - might be 'conceptual:Query' or similar
        # Based on typical UDDL, queries are often in 'conceptual:Query' or stored as text in a specification
        # Inspecting the file structure or assuming a standard tag
        # Let's search for 'specification' tags that look like SQL
        
        # Strategy: Look for all 'specification' attributes or child elements
        # Or look for 'platform:Query' elements
        if 'platform:Query' in xmi_type:
             specification = elem.get('specification')
             if specification:
                total_queries += 1
                try:
                    # Clean up query string if needed (e.g. remove XML entities if any, though ElementTree handles basics)
                    parser = UDDLQueryParser(specification)
                    ast = parser.parse()
                    
                    num_projections = len(ast.projections)
                    num_entities = len(ast.from_clause.entities)
                    num_joins = len(ast.from_clause.joins)
                    num_join_conditions = sum(len(join.on) for join in ast.from_clause.joins)
                    
                    total_projected_characteristics += num_projections
                    total_entities_in_queries += num_entities
                    total_joins += num_joins
                    total_join_conditions += num_join_conditions
                    
                    # Store per-query stats for complexity metrics
                    query_details.append({
                        'projections': num_projections,
                        'entities': num_entities,
                        'joins': num_joins,
                        'join_conditions': num_join_conditions
                    })
                except Exception:
                    # If parsing fails, just count the query but not characteristics
                    pass
        
        # Also check for 'query' tags if structure is different
        # Adjust based on actual .face file structure if needed. 
        # Assuming 'conceptual:Query' is the element type based on standard UDDL usage.
    
    # Calculate query complexity metrics
    avg_projections_per_query = 0
    avg_entities_per_query = 0
    avg_joins_per_query = 0
    avg_join_conditions_per_query = 0
    max_projections_in_query = 0
    max_entities_in_query = 0
    max_joins_in_query = 0
    max_join_conditions_in_query = 0
    
    if total_queries > 0:
        avg_projections_per_query = total_projected_characteristics / total_queries
        avg_entities_per_query = total_entities_in_queries / total_queries
        avg_joins_per_query = total_joins / total_queries
        avg_join_conditions_per_query = total_join_conditions / total_queries
        
        for q_detail in query_details:
            if q_detail['projections'] > max_projections_in_query:
                max_projections_in_query = q_detail['projections']
            if q_detail['entities'] > max_entities_in_query:
                max_entities_in_query = q_detail['entities']
            if q_detail['joins'] > max_joins_in_query:
                max_joins_in_query = q_detail['joins']
            if q_detail['join_conditions'] > max_join_conditions_in_query:
                max_join_conditions_in_query = q_detail['join_conditions']

    # Generate LaTeX Table
    # Using tabularx and matching template style (gray header, no vertical lines)
    script_name = pathlib.Path(__file__).name
    latex_table = f"""
% Generated by {script_name}
% Arguments: face_file={face_file_path}
\\begingroup
    \\centering
    \\begin{{table*}}[t]
    \\renewcommand{{\\arraystretch}}{{1.5}}
    \\begin{{tabularx}}{{\\textwidth}}{{@{{}} >{{\\sffamily\\raggedright\\arraybackslash}}p{{0.35\\textwidth}} >{{\\sffamily\\raggedright\\arraybackslash}}X @{{}}}}
        \\rowcolor{{tableheader}}
        \\headingfont\\bfseries UDDL Data Model Statistics & \\headingfont\\bfseries Model Examples \\\\
        \\addlinespace[4pt]
        % Left Column: Statistics
        \\begin{{tabular}}[t]{{@{{}}l r@{{}}}}
            Number of Entities: & {num_entities} \\\\
            Number of Associations: & {num_associations} \\\\
            Total Compositions: & {total_compositions} \\\\
            Total Participants: & {total_participants} \\\\
            Total Queries: & {total_queries} \\\\
            Total Projected Characteristics: & {total_projected_characteristics} \\\\
            Avg Projections/Query: & {avg_projections_per_query:.1f} \\\\
            Avg Entities/Query: & {avg_entities_per_query:.1f} \\\\
            Avg JOINs/Query: & {avg_joins_per_query:.1f} \\\\
            Avg Join Conditions/Query: & {avg_join_conditions_per_query:.1f} \\\\
            Max Projections: & {max_projections_in_query} \\\\
            Max Join Conditions: & {max_join_conditions_in_query}
        \\end{{tabular}}
        &
        % Right Column: Examples
        \\textbf{{Most Central Entity}}: {best_entity_display} (Score: {max_entity_score}) \\newline
        \\textit{{Compositions}}: {entity_compositions_str} \\par\\vspace{{6pt}}
        
        \\textbf{{Most Central Association}}: {best_assoc_display} (Score: {max_assoc_score}) \\newline
        \\textit{{Compositions}}: {assoc_compositions_str} \\newline
        \\textit{{Participants}}: {assoc_participants_str} \\par\\vspace{{6pt}}
        
        \\textbf{{Observation Pattern}}: {observe_display} \\newline
        \\textit{{Compositions}}: {observe_compositions_str} \\newline
        \\textit{{Participants}}: {observe_participants_str} \\par\\vspace{{6pt}}
        
        \\textbf{{Assembly Pattern}}: {assembly_display} \\newline
        \\textit{{Compositions}}: {assembly_compositions_str} \\newline
        \\textit{{Participants}}: {assembly_participants_str}
    \\end{{tabularx}}
    \\caption{{Summary statistics of the UDDL Conceptual Data Model (CDM). The table presents the total counts of core modeling elements: Entities, Associations, Compositions, and Participants, as well as Query statistics including complexity metrics (averages and maximums for projections, entities, JOINs, and join conditions per query). It identifies the most central elements based on their structural connectivity and property density. Additionally, it highlights representative associations for the Observation pattern (\\textit{{Observe}}) and the Assembly pattern (\\textit{{PartOf}}) with their respective compositions and participants.}}
    \\label{{tab:uddl_summary}}
    \\end{{table*}}
\\endgroup
"""
    print(latex_table)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate a LaTeX summary table from a UDDL .face file."
    )
    
    # Check if the default example file exists
    default_face_file = pathlib.Path(__file__).parent / "examples" / "incose_uddl2owl.face"
    
    parser.add_argument(
        "face_file",
        nargs="?",  # Optional positional argument
        default=str(default_face_file) if default_face_file.exists() else None,
        help="Path to the .face XML file. Defaults to example file if not provided."
    )
    
    args = parser.parse_args()
    
    if args.face_file:
        generate_summary_stats(args.face_file)
    else:
        parser.print_help()
        print("\nError: No input file provided and default example not found.", file=sys.stderr)
        sys.exit(1)
