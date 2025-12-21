from typing import List, Union
from collections import defaultdict
import pathlib
import xml.etree.ElementTree as ET

from tuple import UddlTuple
from parse_tuple import parse_tuple
from uddl2tuple import uddl2tuple

from participant_path_parser import EntityResolution, AssociationResolution, ParticipantPath
from query_parser import (
    QueryStatement, FromClause, Entity, Join, Equivalence, Reference,
    ProjectedCharacteristic, AllCharacteristics, EntityWildcard, get_ast
)


def query2path(query_ast: QueryStatement) -> List[ParticipantPath]:
    """
    Transforms a query AST into a list of ParticipantPath objects, 
    one for each projected characteristic.
    """
    from_clause = query_ast.from_clause
    entities_list = from_clause.entities
    if not entities_list:
        return []

    # root_info: (entity_name, alias)
    root_entity = entities_list[0]
    start_type = root_entity.name
    root_ref = root_entity.alias or root_entity.name

    # Map entity aliases/names to the list of Resolutions required to reach them
    # path_map[alias] = [Resolution, ...]
    path_map = {root_ref: []}
    
    # Trace JOINs to build resolution prefixes for all joined entities
    for join in from_clause.joins:
        target = join.target
        target_name = target.name
        target_alias = target.alias or target_name
        
        # Determine which side of the 'ON' criteria belongs to an already resolved entity
        rolename = None
        source_ref = None
        for cond in join.on:
            left, right = cond.left, cond.right
            # We look for a rolename on an entity we already know (the source)
            if left.entity in path_map:
                source_ref = left.entity
                rolename = left.characteristic
            elif right.entity in path_map:
                source_ref = right.entity
                rolename = right.characteristic
        
        if source_ref is not None and rolename:
            # If target name differs from rolename, it is an AssociationResolution
            if target_name != rolename:
                res = AssociationResolution(rolename, target_name)
            else:
                res = EntityResolution(rolename)
            
            path_map[target_alias] = path_map[source_ref] + [res]

    # Generate a ParticipantPath for every projected characteristic
    results = []
    for proj in query_ast.projections:
        if isinstance(proj, ProjectedCharacteristic):
            ref = proj.reference
            entity_ref = ref.entity or root_ref # Default to root if no entity prefix
            
            # resolutions = steps to reach entity + final characteristic
            resolutions = path_map.get(entity_ref, []).copy()
            resolutions.append(EntityResolution(ref.characteristic))
            
            results.append(ParticipantPath(start_type, resolutions))
            
        elif isinstance(proj, AllCharacteristics):
            # Wildcards represent a path ending in the root or a joined entity
            # Note: This logic seems to assume * applies to root. 
            # If strictly recreating previous logic:
            results.append(ParticipantPath(start_type, path_map[root_ref]))
        
        # EntityWildcard handling was missing in original code, ignoring here too.

    return results


def find_reference_observable(entity_name: str, tuples: List[UddlTuple]) -> str:
    """
    Finds the observable characteristic used for reference for a given entity name from the model tuples.
    Assumes a well-formed model where the reference observable is the first composed observable.
    """
    if not tuples:
        raise ValueError("Model is required to find reference observable")
    
    # Identify Entities (Subjects) to distinguish Observables
    entities = {t.subject for t in tuples if isinstance(t, UddlTuple)}
    
    for t in tuples:
        if isinstance(t, UddlTuple) and t.subject == entity_name and t.predicate == 'composes':
            # Check if object is an Observable (not an Entity)
            if isinstance(t.object, str) and t.object not in entities:
                 return t.rolename

    raise ValueError(f"No observable characteristic found for entity {entity_name}")


def find_association_role(association_name: str, participant_type: str, model: List[UddlTuple]) -> str:
    """
    Finds the rolename (characteristic) on the Association entity that refers to the participant_type.
    """
    if not model:
        return "id" # Fallback, though likely wrong for association
        
    for t in model:
        if isinstance(t, UddlTuple) and t.subject == association_name and t.predicate == 'associates':
            # Check if object matches participant type
            # Object could be string or ParticipantPath.
            # If Path, start_type or resolution end type must match.
            
            match = False
            if isinstance(t.object, str):
                match = (t.object == participant_type)
            elif isinstance(t.object, ParticipantPath):
                # If path, we assume the path points to the participant
                # e.g. (Assoc, associates, A.prop) -> refers to A? Or Prop?
                # Usually (Assoc, associates, A)
                if t.object.start_type == participant_type:
                    match = True
                    
            if match:
                return t.rolename
                
    # Fallback: construct default rolename from participant type (camelCase)
    return participant_type[0].lower() + participant_type[1:]



def find_property_type(entity_name: str, property_name: str, tuples: List[UddlTuple]) -> str:
    """
    Finds the type (object) of a property (rolename) for a given entity.
    Returns the property_name itself if not found (fallback).
    """
    if not tuples:
        return property_name
        
    for t in tuples:
        if isinstance(t, UddlTuple) and t.subject == entity_name:
             # Check rolename
             if t.rolename == property_name:
                 # Found it. Return the object (type).
                 # Note: object could be ParticipantPath, but usually Entity name for compositions.
                 return str(t.object)
                 
    return property_name


def path2query(paths: List[ParticipantPath], model: List[UddlTuple] = None) -> Union[QueryStatement, List[QueryStatement]]:
    """
    Transforms a list of ParticipantPath objects into one or more query ASTs.
    Paths starting with the same entity type are merged into a single query.
    """
    # Group paths by their start_type
    groups = defaultdict(list)
    for p in paths:
        groups[p.start_type].append(p)

    queries = []
    for start_type, path_list in groups.items():
        projections = []
        joins_registry = {} # key: path_tuple, value: Join object
        
        for p in path_list:
            # The last resolution is the projected characteristic
            if not p.resolutions:
                projections.append(AllCharacteristics())
                continue
                
            leaf_res = p.resolutions[-1]
            intermediate_res = p.resolutions[:-1]
            
            # Track current entity/alias as we build the join chain
            current_alias = start_type
            current_type = start_type
            
            # Process intermediate resolutions into JOINs
            for i, res in enumerate(intermediate_res):
                # Unique key for this specific navigation step to reuse JOINs
                step_key = tuple(p.resolutions[:i+1])
                
                if step_key not in joins_registry:
                    if isinstance(res, AssociationResolution):
                         target_type = res.association_name
                         target_alias = res.association_name 
                         
                         # For Association Resolution:
                         # JOIN Target (Assoc) ON Source.role = Target.ParticipantRole
                         # Source.role comes from res.rolename
                         # Target.ParticipantRole must be found in the model
                         right_char = find_association_role(target_type, current_type, model)
                         
                    else:
                         # EntityResolution
                         # Look up the type of the property
                         target_type = find_property_type(current_type, res.rolename, model)
                         target_alias = res.rolename
                         
                         # For Entity Resolution:
                         # JOIN Target (Entity) ON Source.role = Target.Identifier
                         right_char = find_reference_observable(target_type, model)

                    joins_registry[step_key] = Join(
                        target=Entity(name=target_type, alias=target_alias),
                        on=[Equivalence(
                            left=Reference(entity=current_alias, characteristic=res.rolename),
                            right=Reference(entity=target_alias, characteristic=right_char)
                        )]
                    )
                
                # Update current pointers for next step
                join_obj = joins_registry[step_key]
                current_alias = join_obj.target.alias or join_obj.target.name
                current_type = join_obj.target.name

            # Add the projection
            projections.append(
                ProjectedCharacteristic(
                    reference=Reference(entity=current_alias, characteristic=leaf_res.rolename),
                    alias=None
                )
            )

        # Assemble Query AST
        query_ast = QueryStatement(
            projections=projections,
            from_clause=FromClause(
                entities=[Entity(name=start_type, alias=None)],
                joins=list(joins_registry.values())
            ),
            qualifier=None
        )
        queries.append(query_ast)

    return queries[0] if len(queries) == 1 else queries


if __name__ == "__main__":
    import argparse
    import sys

    parser = argparse.ArgumentParser(description="Convert between UDDL queries and Participant Paths.")
    parser.add_argument("input", nargs="*", help="UDDL query string OR list of Participant path strings")
    parser.add_argument("--model", help="Path to UDDL model file (.face) or tuple file (.txt)")

    args = parser.parse_args()

    # Load model if provided
    model_tuples = []
    if args.model:
        model_path = pathlib.Path(args.model)
        if not model_path.exists():
             print(f"Error: Model file not found: {model_path}", file=sys.stderr)
             sys.exit(1)
        
        # Check extension
        if model_path.suffix.lower() == '.face' or model_path.suffix.lower() == '.xml':
             try:
                 tree = ET.parse(model_path)
                 loaded = uddl2tuple(tree)
                 if loaded:
                      model_tuples = [t for t in loaded if isinstance(t, UddlTuple)]
             except Exception as e:
                 print(f"Error parsing UDDL model: {e}", file=sys.stderr)
                 sys.exit(1)
        else:
             # Assume tuple file
             try:
                 loaded = parse_tuple(model_path)
                 if loaded:
                      model_tuples = [t for t in loaded if isinstance(t, UddlTuple)]
             except Exception as e:
                 print(f"Error parsing tuple model: {e}", file=sys.stderr)
                 sys.exit(1)

    # Heuristic detection of input type
    is_query = False
    input_data = args.input
    
    if input_data:
        # Check if the first argument looks like the start of a query
        first_arg = input_data[0].strip().upper()
        if first_arg == "SELECT" or first_arg.startswith("SELECT "):
            is_query = True

        if is_query:
            # Join all arguments to form the query string
            query_str = " ".join(input_data)
            try:
                ast = get_ast(query_str)
                paths = query2path(ast)
                for p in paths:
                    print(str(p))
            except Exception as e:
                print(f"Error processing query: {e}", file=sys.stderr)
                sys.exit(1)
        else:
            # Treat as list of paths
            try:
                parsed_paths = []
                for p_str in input_data:
                    parsed_paths.append(ParticipantPath.parse(p_str))
                
                queries = path2query(parsed_paths, model=model_tuples)
                
                def print_query(q):
                    print("Query:")
                    print(q)
                
                if isinstance(queries, list):
                    for q in queries:
                        print_query(q)
                else:
                    print_query(queries)
                    
            except Exception as e:
                print(f"Error processing paths: {e}", file=sys.stderr)
                sys.exit(1)

    else:
        # Default behavior if no arguments provided
        # Simulate a query: SELECT A.x, B.y FROM A JOIN B ON A.link = B
        # Recreate the sample_ast using classes
        
        sample_ast = QueryStatement(
            projections=[
                ProjectedCharacteristic(reference=Reference(entity="A", characteristic="x")),
                ProjectedCharacteristic(reference=Reference(entity="B", characteristic="y"))
            ],
            from_clause=FromClause(
                entities=[Entity(name="A", alias="A")],
                joins=[
                    Join(
                        target=Entity(name="B", alias="B"),
                        on=[Equivalence(
                            left=Reference(entity="A", characteristic="link"),
                            right=Reference(entity="B", characteristic="id")
                        )]
                    )
                ]
            ),
            qualifier=None
        )

        paths = query2path(sample_ast)
        print("Query to Paths:")
        for p in paths:
            print(f"  {p}")

        reconstructed_query = path2query(paths, model=model_tuples)
        if isinstance(reconstructed_query, list):
            reconstructed_query = reconstructed_query[0]
            
        print("\nPaths back to Query (Projections):")
        for proj in reconstructed_query.projections:
            if isinstance(proj, ProjectedCharacteristic):
                print(f"  {proj.reference}")
            else:
                print(f"  {proj}")
