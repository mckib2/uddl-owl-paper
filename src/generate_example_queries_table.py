#!/usr/bin/env python3
"""
Generate a table of progressively complex UDDL queries and their SPARQL equivalents.

This script reads a UDDL model file and generates a series of queries starting from
a simple SELECT on an entity, then progressively adding JOINs through associations
and compositions to demonstrate increasing complexity.
"""

import xml.etree.ElementTree as ET
import sys
import pathlib
import argparse
from typing import List, Dict, Set, Tuple, Optional
from collections import defaultdict

from uddl2tuple import uddl2tuple
from tuple import UddlTuple
from query_parser import QueryStatement, Entity, Join, Equivalence, Reference, ProjectedCharacteristic, FromClause, get_ast
from query_path_conversion import query2path, load_model
from sparql_conversion import generate_sparql
from participant_path_parser import ParticipantPath, EntityResolution, AssociationResolution


def get_entity_attributes(model: List[UddlTuple], entity_name: str) -> List[str]:
    """Get all attribute rolenames for an entity (compositions that are Observables)."""
    attributes = []
    for t in model:
        if t.subject == entity_name and t.predicate == 'composes':
            # Check if object is an Observable (not an Entity)
            # Observables are typically not subjects of any tuple
            object_name = str(t.object) if isinstance(t.object, str) else None
            if object_name:
                # Check if it's an Observable (not a subject of any composes/associates)
                is_observable = True
                for t2 in model:
                    if t2.subject == object_name:
                        is_observable = False
                        break
                if is_observable:
                    attributes.append(t.rolename)
    return sorted(attributes)


def get_entity_compositions(model: List[UddlTuple], entity_name: str) -> List[Tuple[str, str]]:
    """
    Get compositions that are Entities (not Observables).
    Returns list of (rolename, target_entity_name) tuples.
    """
    compositions = []
    for t in model:
        if t.subject == entity_name and t.predicate == 'composes':
            object_name = str(t.object) if isinstance(t.object, str) else None
            if object_name:
                # Check if it's an Entity (is a subject of some tuple)
                is_entity = False
                for t2 in model:
                    if t2.subject == object_name:
                        is_entity = True
                        break
                if is_entity:
                    compositions.append((t.rolename, object_name))
    return compositions


def get_associations_for_entity(model: List[UddlTuple], entity_name: str) -> List[Tuple[str, str, str, Optional[ParticipantPath]]]:
    """
    Get all associations where the entity is a participant.
    Returns list of (association_name, participant_rolename, target_entity_or_path, optional_path) tuples.
    For PartOf associations, returns (assoc_name, 'assembly', target_entity, None) or (assoc_name, 'part', source_entity, None).
    """
    associations = []
    # Find associations where entity is a participant
    for t in model:
        if t.predicate == 'associates':
            assoc_name = t.subject
            rolename = t.rolename
            
            if isinstance(t.object, str):
                # Direct entity reference
                if t.object == entity_name:
                    # This association has our entity as a participant
                    # Find the other participant(s) in this association
                    for t2 in model:
                        if t2.subject == assoc_name and t2.predicate == 'associates' and t2.rolename != rolename:
                            target = str(t2.object) if isinstance(t2.object, str) else None
                            if target:
                                associations.append((assoc_name, rolename, target, None))
            elif isinstance(t.object, ParticipantPath):
                path = t.object
                if path.start_type == entity_name:
                    # Path starts from our entity
                    target_type = resolve_path_target_type(path, model)
                    if target_type:
                        associations.append((assoc_name, rolename, target_type, path))
    
    return associations


def resolve_path_target_type(path: ParticipantPath, model: List[UddlTuple]) -> Optional[str]:
    """Resolve the final target type of a participant path."""
    if not path.resolutions:
        return path.start_type
    
    last_res = path.resolutions[-1]
    if isinstance(last_res, AssociationResolution):
        return last_res.association_name
    
    # For EntityResolution, we need to look up in the model
    if len(path.resolutions) == 1:
        source_type = path.start_type
    else:
        # Recursively resolve parent path
        parent_path = ParticipantPath(path.start_type, path.resolutions[:-1])
        source_type = resolve_path_target_type(parent_path, model)
    
    if source_type:
        for t in model:
            if t.subject == source_type and t.rolename == last_res.rolename:
                if isinstance(t.object, str):
                    return t.object.split('.')[0] if '.' in t.object else t.object
                elif isinstance(t.object, ParticipantPath):
                    return resolve_path_target_type(t.object, model)
    
    return None


def generate_simple_query(entity_name: str, num_attributes: int = 3) -> QueryStatement:
    """Generate a simple SELECT query on an entity with some attributes."""
    # Use a placeholder - we'll need to get actual attributes from the model
    # For now, generate a query structure
    projections = []
    # We'll add actual attributes when we have the model
    # For now, create a query that selects some characteristics
    for i in range(min(num_attributes, 3)):
        projections.append(ProjectedCharacteristic(Reference(entity=None, characteristic=f"attr_{i}")))
    
    from_clause = FromClause(entities=[Entity(name=entity_name, alias=f"{entity_name.lower()}_from")], joins=[])
    return QueryStatement(projections=projections, from_clause=from_clause)


def generate_join_query(base_entity: str, join_target: str, join_rolename: str, 
                        base_alias: str, target_alias: str, 
                        is_association: bool = False) -> Join:
    """Generate a JOIN clause."""
    if is_association:
        # For associations, join on the association rolename
        # Format: JOIN AssociationName alias ON alias.rolename = base_alias
        equivalences = [Equivalence(
            left=Reference(entity=target_alias, characteristic=join_rolename),
            right=Reference(entity=base_alias, characteristic=None)
        )]
    else:
        # For compositions, join on the composition rolename
        # Format: JOIN TargetEntity alias ON base_alias.rolename = alias
        equivalences = [Equivalence(
            left=Reference(entity=base_alias, characteristic=join_rolename),
            right=Reference(entity=target_alias, characteristic=None)
        )]
    
    return Join(target=Entity(name=join_target, alias=target_alias), on=equivalences)


def escape_latex(text: str) -> str:
    """Escape special LaTeX characters."""
    # Order matters: escape backslash first, then braces, then other characters
    # This prevents issues like {} becoming \textbackslash{}{}
    text = text.replace('\\', '\\textbackslash{}')
    text = text.replace('{', '\\{')
    text = text.replace('}', '\\}')
    text = text.replace('_', '\\_')
    text = text.replace('&', '\\&')
    text = text.replace('%', '\\%')
    text = text.replace('$', '\\$')
    text = text.replace('#', '\\#')
    text = text.replace('^', '\\textasciicircum{}')
    text = text.replace('~', '\\textasciitilde{}')
    return text


def format_query_for_latex(query: str, max_lines: int = 8, use_same_size: bool = True) -> str:
    """Format a query string for LaTeX display.
    
    Args:
        query: The query string to format
        max_lines: Maximum number of lines to display
        use_same_size: If True, use consistent font size (footnotesize) for all queries
    """
    # Escape LaTeX special characters
    query_escaped = escape_latex(query)
    # Split into lines and limit length
    lines = [line.strip() for line in query_escaped.split('\n') if line.strip()]
    if len(lines) > max_lines:
        # Truncate and add ellipsis
        lines = lines[:max_lines-1] + ['...']
    
    # Simple approach: format each line with \texttt and join with line breaks
    # Use a minipage to contain the multi-line content
    formatted_lines = []
    for line in lines:
        if line:
            formatted_lines.append(f"\\texttt{{{line}}}")
    
    # Join with \\ for line breaks
    formatted = ' \\\\ '.join(formatted_lines)
    
    # Use consistent font size for all queries if requested
    if use_same_size:
        font_cmd = "\\scriptsize"  # Smaller than footnotesize
    else:
        # Determine font size based on length
        if len(query) > 300:
            font_cmd = "\\tiny"
        elif len(query) > 150:
            font_cmd = "\\scriptsize"
        else:
            font_cmd = "\\footnotesize"
    
    # Use minipage with proper width to avoid brace issues
    if font_cmd:
        return f"\\begin{{minipage}}{{\\linewidth}}\\raggedright{font_cmd} {formatted} \\end{{minipage}}"
    else:
        return f"\\begin{{minipage}}{{\\linewidth}}\\raggedright {formatted} \\end{{minipage}}"


def generate_progressive_queries(model: List[UddlTuple], entity_name: str, max_depth: int = 4) -> List[Tuple[QueryStatement, str]]:
    """
    Generate progressively complex queries starting from the entity.
    Returns list of (QueryStatement, description) tuples.
    """
    queries = []
    
    # Get entity attributes
    attributes = get_entity_attributes(model, entity_name)
    if not attributes:
        # If no attributes found, use common placeholders
        attributes = ['identifier', 'name']
    
    # Query 1: Simple SELECT from entity (no aliases)
    projections = [ProjectedCharacteristic(Reference(entity=None, characteristic=attr)) 
                   for attr in attributes[:2]]  # Limit to 2 attributes
    from_clause = FromClause(entities=[Entity(name=entity_name, alias=None)], joins=[])
    query1 = QueryStatement(projections=projections, from_clause=from_clause)
    queries.append((query1, f"Simple selection of {entity_name} attributes"))
    
    # Query 2: JOIN through a PartOf association (most common pattern)
    # Find PartOf associations where entity is the assembly
    partof_associations = []
    for t in model:
        if t.predicate == 'associates' and '_PartOf_' in t.subject:
            assoc_name = t.subject
            # Check if this is a PartOf where our entity is the assembly
            for t2 in model:
                if t2.subject == assoc_name and t2.predicate == 'associates':
                    if t2.rolename == 'assembly' and isinstance(t2.object, str) and t2.object == entity_name:
                        # Find the 'part' participant
                        for t3 in model:
                            if t3.subject == assoc_name and t3.predicate == 'associates' and t3.rolename == 'part':
                                part_entity = str(t3.object) if isinstance(t3.object, str) else None
                                if part_entity:
                                    partof_associations.append((assoc_name, part_entity))
                                    break
                        break
    
    if partof_associations:
        assoc_name, part_entity = partof_associations[0]
        
        # Get attributes from the part entity
        part_attributes = get_entity_attributes(model, part_entity)
        if part_attributes:
            part_attr = part_attributes[0]
        else:
            part_attr = 'identifier'
        
        # Add part entity attribute to projections (use entity name instead of alias)
        projections2 = projections + [ProjectedCharacteristic(
            Reference(entity=part_entity, characteristic=part_attr)
        )]
        
        # Join pattern: JOIN Association ON association.assembly = base
        #                JOIN PartEntity ON association.part = part
        # No aliases - use entity names directly
        join1 = Join(
            target=Entity(name=assoc_name, alias=None),
            on=[Equivalence(
                left=Reference(entity=assoc_name, characteristic='assembly'),
                right=Reference(entity=entity_name, characteristic=None)
            )]
        )
        join2 = Join(
            target=Entity(name=part_entity, alias=None),
            on=[Equivalence(
                left=Reference(entity=assoc_name, characteristic='part'),
                right=Reference(entity=part_entity, characteristic=None)
            )]
        )
        
        from_clause2 = FromClause(entities=[Entity(name=entity_name, alias=None)], 
                                 joins=[join1, join2])
        query2 = QueryStatement(projections=projections2, from_clause=from_clause2)
        queries.append((query2, f"Join through PartOf association to {part_entity}"))
        
        # Query 3: Add another PartOf join (nested composition)
        # Look for PartOf associations where the first part entity is the assembly
        query3_generated = False
        for t in model:
            if query3_generated:
                break
            if t.predicate == 'associates' and '_PartOf_' in t.subject:
                assoc_name2 = t.subject
                if assoc_name2 == assoc_name:
                    continue  # Skip the one we already used
                # Check if this PartOf has our part_entity as assembly
                for t2 in model:
                    if t2.subject == assoc_name2 and t2.predicate == 'associates' and t2.rolename == 'assembly':
                        if isinstance(t2.object, str) and t2.object == part_entity:
                            # Find the part entity
                            for t3 in model:
                                if t3.subject == assoc_name2 and t3.predicate == 'associates' and t3.rolename == 'part':
                                    part_entity2 = str(t3.object) if isinstance(t3.object, str) else None
                                    if part_entity2:
                                        part2_attributes = get_entity_attributes(model, part_entity2)
                                        if part2_attributes:
                                            part2_attr = part2_attributes[0]
                                        else:
                                            part2_attr = 'identifier'
                                        
                                        projections3 = projections2 + [ProjectedCharacteristic(
                                            Reference(entity=part_entity2, characteristic=part2_attr)
                                        )]
                                        
                                        # No aliases - use entity names directly
                                        join3 = Join(
                                            target=Entity(name=assoc_name2, alias=None),
                                            on=[Equivalence(
                                                left=Reference(entity=assoc_name2, characteristic='assembly'),
                                                right=Reference(entity=part_entity, characteristic=None)
                                            )]
                                        )
                                        join4 = Join(
                                            target=Entity(name=part_entity2, alias=None),
                                            on=[Equivalence(
                                                left=Reference(entity=assoc_name2, characteristic='part'),
                                                right=Reference(entity=part_entity2, characteristic=None)
                                            )]
                                        )
                                        
                                        from_clause3 = FromClause(entities=[Entity(name=entity_name, alias=None)],
                                                                 joins=[join1, join2, join3, join4])
                                        query3 = QueryStatement(projections=projections3, from_clause=from_clause3)
                                        queries.append((query3, f"Nested PartOf join to {part_entity2}"))
                                        query3_generated = True
                                        break
                            if query3_generated:
                                break
                if query3_generated:
                    break
        
        # Query 4: Add an Observe association
        # Strategy: Find a sensor that's part of the satellite, then join through its Observe association
        # First, find a sensor entity that's part of the satellite
        sensor_entity = None
        sensor_partof_assoc = None
        for t in model:
            if t.predicate == 'associates' and '_PartOf_' in t.subject:
                assoc_name = t.subject
                # Check if this PartOf has satellite as assembly and part is a sensor
                for t2 in model:
                    if t2.subject == assoc_name and t2.predicate == 'associates' and t2.rolename == 'assembly':
                        if isinstance(t2.object, str) and t2.object == entity_name:
                            # Find the part entity
                            for t3 in model:
                                if t3.subject == assoc_name and t3.predicate == 'associates' and t3.rolename == 'part':
                                    potential_sensor = str(t3.object) if isinstance(t3.object, str) else None
                                    # Look for sensor entities (TemperatureSensor, PositionSensor, etc.)
                                    if potential_sensor and ('Sensor' in potential_sensor):
                                        sensor_entity = potential_sensor
                                        sensor_partof_assoc = assoc_name
                                        break
                            if sensor_entity:
                                break
                if sensor_entity:
                    break
        
        if sensor_entity and sensor_partof_assoc:
            # Now find an Observe association where this sensor is the observer
            for t in model:
                if t.predicate == 'associates' and '_Observe_' in t.subject:
                    obs_assoc_name = t.subject
                    # Check if observer is our sensor
                    for t2 in model:
                        if t2.subject == obs_assoc_name and t2.predicate == 'associates' and t2.rolename == 'observer':
                            if isinstance(t2.object, str) and t2.object == sensor_entity:
                                # Found an Observe association with our sensor as observer
                                # Get the observed entity
                                for t3 in model:
                                    if t3.subject == obs_assoc_name and t3.predicate == 'associates' and t3.rolename == 'observed':
                                        observed_entity = str(t3.object) if isinstance(t3.object, str) else None
                                        if observed_entity:
                                            # Get attributes
                                            obs_attributes = get_entity_attributes(model, obs_assoc_name)
                                            if obs_attributes:
                                                obs_attr = obs_attributes[0]
                                            else:
                                                obs_attr = 'identifier'
                                            
                                            projections4 = projections2 + [ProjectedCharacteristic(
                                                Reference(entity=obs_assoc_name, characteristic=obs_attr)
                                            )]
                                            
                                            # Join sensor PartOf (no aliases)
                                            join_sensor_assoc = Join(
                                                target=Entity(name=sensor_partof_assoc, alias=None),
                                                on=[Equivalence(
                                                    left=Reference(entity=sensor_partof_assoc, characteristic='assembly'),
                                                    right=Reference(entity=entity_name, characteristic=None)
                                                )]
                                            )
                                            join_sensor = Join(
                                                target=Entity(name=sensor_entity, alias=None),
                                                on=[Equivalence(
                                                    left=Reference(entity=sensor_partof_assoc, characteristic='part'),
                                                    right=Reference(entity=sensor_entity, characteristic=None)
                                                )]
                                            )
                                            join_obs = Join(
                                                target=Entity(name=obs_assoc_name, alias=None),
                                                on=[Equivalence(
                                                    left=Reference(entity=obs_assoc_name, characteristic='observer'),
                                                    right=Reference(entity=sensor_entity, characteristic=None)
                                                )]
                                            )
                                            
                                            from_clause4 = FromClause(entities=[Entity(name=entity_name, alias=None)],
                                                                     joins=[join1, join2, join_sensor_assoc, join_sensor, join_obs])
                                            query4 = QueryStatement(projections=projections4, from_clause=from_clause4)
                                            queries.append((query4, f"Join through sensor and Observe association {obs_assoc_name}"))
                                            break
                                if len(queries) >= 4:
                                    break
                            if len(queries) >= 4:
                                break
                    if len(queries) >= 4:
                        break
                if len(queries) >= 4:
                    break
    
    return queries


def main():
    parser = argparse.ArgumentParser(
        description="Generate a table of progressively complex UDDL queries and SPARQL equivalents."
    )
    
    default_face_file = pathlib.Path(__file__).parent / "examples" / "incose_uddl2owl.face"
    
    parser.add_argument(
        "face_file",
        nargs="?",
        default=str(default_face_file) if default_face_file.exists() else None,
        help="Path to the .face XML file. Defaults to example file if not provided."
    )
    
    parser.add_argument(
        "--entity",
        default="Satellite",
        help="Entity name to generate queries for (default: Satellite)"
    )
    
    parser.add_argument(
        "--max-depth",
        type=int,
        default=4,
        help="Maximum depth of query complexity (default: 4)"
    )
    
    args = parser.parse_args()
    
    if not args.face_file:
        parser.print_help()
        print("\nError: No input file provided and default example not found.", file=sys.stderr)
        sys.exit(1)
    
    # Load model
    try:
        tree = ET.parse(args.face_file)
        tuples = uddl2tuple(tree)
        model = [t for t in tuples if isinstance(t, UddlTuple)]
    except Exception as e:
        print(f"Error loading model: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Generate queries
    queries = generate_progressive_queries(model, args.entity, args.max_depth)
    
    if not queries:
        print(f"Error: Could not generate queries for entity '{args.entity}'", file=sys.stderr)
        sys.exit(1)
    
    # Convert to SPARQL and generate table
    table_rows = []
    
    for i, (query_ast, description) in enumerate(queries, 1):
        try:
            # Convert to SPARQL
            alias_map, projected_paths = query2path(query_ast=query_ast, model=model)
            sparql = generate_sparql(alias_map=alias_map, projected_paths=projected_paths, model=model)
            
            # Remove PREFIX line from SPARQL query
            sparql_lines = sparql.split('\n')
            sparql_cleaned = '\n'.join([line for line in sparql_lines if not line.strip().startswith('PREFIX')])
            # Remove empty line after PREFIX if present
            if sparql_cleaned.startswith('\n'):
                sparql_cleaned = sparql_cleaned.lstrip('\n')
            
            # Format queries for LaTeX
            # Use pretty_print for better formatting
            # Use same font size for both UDDL and SPARQL queries
            uddl_str = query_ast.pretty_print()
            uddl_latex = format_query_for_latex(uddl_str, use_same_size=True)
            sparql_latex = format_query_for_latex(sparql_cleaned, use_same_size=True)
            
            table_rows.append((i, description, uddl_latex, sparql_latex))
        except Exception as e:
            print(f"Warning: Failed to convert query {i} to SPARQL: {e}", file=sys.stderr)
            # Still add the UDDL query even if SPARQL conversion fails
            uddl_str = str(query_ast)
            uddl_latex = format_query_for_latex(uddl_str, use_same_size=True)
            table_rows.append((i, description, uddl_latex, "\\textit{Conversion failed}"))
    
    # Generate LaTeX table
    script_name = pathlib.Path(__file__).name
    latex_table = f"""
% Generated by {script_name}
% Arguments: face_file={args.face_file}, entity={args.entity}, max_depth={args.max_depth}
\\begingroup
    \\centering
    \\begin{{table*}}[t]
    \\renewcommand{{\\arraystretch}}{{1.2}}
    \\footnotesize
    \\begin{{tabularx}}{{\\textwidth}}{{@{{}} >{{\\raggedright\\arraybackslash}}p{{0.15\\textwidth}} >{{\\raggedright\\arraybackslash}}X >{{\\raggedright\\arraybackslash}}X @{{}}}}
        \\rowcolor{{tableheader}}
        \\headingfont\\bfseries Description & \\headingfont\\bfseries UDDL Query & \\headingfont\\bfseries SPARQL Query \\\\
        \\addlinespace[4pt]
"""
    
    for i, desc, uddl, sparql in table_rows:
        # Format queries - they're already formatted with \texttt in format_query_for_latex
        # Use same font size for both query columns
        latex_table += f"        {escape_latex(desc)} & {uddl} & {sparql} \\\\\n"
        if i < len(table_rows):
            latex_table += "        \\addlinespace[2pt]\n"
    
    latex_table += f"""    \\end{{tabularx}}
    \\caption{{Progressive complexity examples of UDDL queries for \\texttt{{{escape_latex(args.entity)}}} and their SPARQL translations. The queries demonstrate increasing complexity from simple attribute selection to multi-join queries traversing associations and compositions. SPARQL queries omit the \\texttt{{PREFIX}} declaration for brevity.}}
    \\label{{tab:example_queries}}
    \\end{{table*}}
\\endgroup
"""
    
    print(latex_table)


if __name__ == "__main__":
    main()
