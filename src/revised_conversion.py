from typing import List, Union, Dict, Tuple
from collections import OrderedDict, defaultdict
import argparse
import sys
import pathlib
import xml.etree.ElementTree as ET

from tuple import UddlTuple
from parse_tuple import parse_tuple
from uddl2tuple import uddl2tuple
from participant_path_parser import EntityResolution, AssociationResolution, ParticipantPath
from query_parser import (
    QueryStatement, FromClause, Entity, Join, Equivalence, Reference,
    ProjectedCharacteristic, AllCharacteristics, get_ast
)


def query2path(query_ast: QueryStatement, model: List[UddlTuple] = None) -> List[ParticipantPath]:
    """
    Maps a UDDL Query (SELECT, FROM, JOIN) to a list of ParticipantPaths.
    """
    from_clause = query_ast.from_clause
    if not from_clause.entities:
        return []

    # The first entity in the FROM clause is the root (start_type)
    root_entity = from_clause.entities[0]
    start_type = root_entity.name
    root_alias = root_entity.alias or root_entity.name

    # path_map stores the sequence of Resolutions to reach each entity alias
    # Key: Alias, Value: List of Resolutions
    path_map: Dict[str, List[Union[EntityResolution, AssociationResolution]]] = {root_alias: []}

    # Process joins to map out the graph of aliases
    for join in from_clause.joins:
        target_alias = join.target.alias or join.target.name
        target_type = join.target.name
        
        found_res = None
        source_alias = None
        
        for cond in join.on:
            left, right = cond.left, cond.right
            # Helper to resolve alias from reference
            def resolve_alias(ref):
                if ref is None:
                    return None
                if ref.entity:
                    return ref.entity
                # If entity is None, characteristic might be the alias (implicit ID)
                return ref.characteristic

            left_alias = resolve_alias(left)

            right_alias = resolve_alias(right)
            
            # Check if operands refer to the Entity ID (alias itself)
            def is_id_ref(ref):
                if ref is None: return False
                return ref.entity is None or ref.characteristic == 'id'

            left_is_id = is_id_ref(left)
            right_is_id = is_id_ref(right)
            
            # Handle unary join conditions (implicit join on previous entity)
            if right is None:
                if left_alias in path_map:
                    # Forward navigation: Source.role -> Target
                    found_res = EntityResolution(left.characteristic)
                    source_alias = left_alias
                    break
                # If left.entity refers to a known alias (not the target), treat as Source.role -> Target
                if left.entity in path_map: 
                     found_res = EntityResolution(left.characteristic)
                     source_alias = left.entity
                     break
                
                continue

            # Forward Navigation: Source.role = Target.id
            if left_alias in path_map and right_alias == target_alias and right_is_id:
                found_res = EntityResolution(left.characteristic)
                source_alias = left_alias
                break
            if right_alias in path_map and left_alias == target_alias and left_is_id:
                found_res = EntityResolution(right.characteristic)
                source_alias = right_alias
                break
                
            # Backward Navigation: Target.role = Source.id
            if left_alias == target_alias and right_alias in path_map and right_is_id:
                found_res = AssociationResolution(left.characteristic, target_type)
                source_alias = right_alias
                break
            if right_alias == target_alias and left_alias in path_map and left_is_id:
                found_res = AssociationResolution(right.characteristic, target_type)
                source_alias = left_alias
                break


        if found_res and source_alias:
            path_map[target_alias] = path_map[source_alias] + [found_res]



    # Generate paths for each projected characteristic (the SELECT clause)
    results = []

    projections = list(query_ast.projections)
    if projections and isinstance(projections[0], AllCharacteristics):
        if model:
            projections = []
            
            # Identify known Entities (Subjects in the model)
            # This allows us to distinguish between Composition of an Entity vs an Observable
            known_entities = {t.subject for t in model if isinstance(t, UddlTuple)}

            # Look up all compositions in the start_type
            for t in model:
                if isinstance(t, UddlTuple) and t.subject == start_type and t.predicate == 'composes':
                    # Only include if the target object is NOT an entity (i.e. it is an Observable)
                    # Note: t.object is the type name of the composed element.
                    target_type = str(t.object)
                    if target_type not in known_entities:
                        # Add projection for the composed rolename
                        projections.append(ProjectedCharacteristic(
                            Reference(entity=root_alias, characteristic=t.rolename)
                        ))
        else:
             # Fallback if no model: path to root entity itself
             results.append(ParticipantPath(start_type, path_map[root_alias]))
             projections = [] # Skip the loop below

    for proj in projections:
        if isinstance(proj, ProjectedCharacteristic):
            entity_ref = proj.reference.entity or root_alias
            attr = proj.reference.characteristic
            
            # Retrieve the steps to reach the entity, then append the final attribute
            resolutions = path_map.get(entity_ref, []).copy()
            resolutions.append(EntityResolution(attr)) 
            results.append(ParticipantPath(start_type, resolutions))
            
    return results


def path2query(paths: List[ParticipantPath], model: List[UddlTuple] = None) -> List[QueryStatement]:
    """
    Maps a list of ParticipantPaths back to a list of QueryStatements.
    Paths are grouped by their start_type (root entity).
    """
    print(f"path2query: {paths}")
    if not paths:
        return []

    # Group paths by their root entity (start_type)
    grouped_paths = defaultdict(list)
    for p in paths:
        grouped_paths[p.start_type].append(p)

    queries = []

    for start_type, path_list in grouped_paths.items():
        root_alias = start_type
        # join_registry avoids duplicate JOINs for the same path segments
        # Key: Tuple of resolutions (path prefix), Value: Alias name
        join_registry: Dict[Tuple, str] = {(): root_alias}
        joins: List[Join] = []
        projections = []
        alias_counter = 1

        for p in path_list:
            current_alias = root_alias
            
            # Process all resolutions except the terminal attribute
            for i in range(len(p.resolutions) - 1):
                res = p.resolutions[i]
                path_prefix = tuple(p.resolutions[:i+1])
                
                if path_prefix not in join_registry:
                    new_alias = f"t{alias_counter}"
                    alias_counter += 1
                    
                    if isinstance(res, EntityResolution):
                        # Forward: The source has a pointer to the target
                        # JOIN Target AS tX ON current_alias.rolename = tX.id
                        target_type = _resolve_target_type(model, current_alias, res.rolename, start_type, joins)
                        
                        # When joining on ID (identity), we use Reference(alias, None) which renders as just "alias"
                        # The Equivalence.right side handles this by checking if right is None or right.characteristic is None
                        # But Equivalence.__str__ handles printing.
                        cond = Equivalence(Reference(current_alias, res.rolename), Reference(new_alias, None))

                        joins.append(Join(Entity(target_type, new_alias), [cond]))
                    else:
                        # Backward: The target (Association) has a pointer to the source
                        cond = Equivalence(Reference(new_alias, res.rolename), Reference(current_alias, None))
                        joins.append(Join(Entity(res.association_name, new_alias), [cond]))

                    
                    join_registry[path_prefix] = new_alias
                
                current_alias = join_registry[path_prefix]

            # The final resolution is the attribute to be SELECTed
            leaf_res = p.resolutions[-1]
            projections.append(ProjectedCharacteristic(Reference(current_alias, leaf_res.rolename)))

        queries.append(QueryStatement(
            projections=projections,
            from_clause=FromClause([Entity(start_type)], joins)
        ))

    return queries


def _resolve_target_type(model: List[UddlTuple], source_alias: str, rolename: str, root_type: str, joins: List[Join]) -> str:
    """Helper to determine the entity type of a forward navigation step."""
    if not model:
        return rolename # Fallback to rolename if no model metadata exists
    
    # 1. Determine the actual Type of the source_alias
    source_type = root_type
    for j in joins:
        if j.target.alias == source_alias:
            source_type = j.target.name
            break
            
    # 2. Find the tuple in the model where this type has this rolename
    for t in model:
        if t.subject == source_type and t.rolename == rolename:
            obj = str(t.object)
            if '.' in obj:
                 # If object is a path (e.g. Louver_PartOf_Frame.part), it typically refers to a participant 
                 # in another association. To navigate through it, we land on the Association entity (the root).
                 # So we return the first segment.
                 segments = obj.split('.')
                 return segments[0]
                 
            return obj
            
    return rolename


if __name__ == "__main__":
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
                paths = query2path(ast, model=model_tuples)
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

        paths = query2path(sample_ast, model=model_tuples)
        print("Query to Paths:")
        for p in paths:
            print(f"  {p}")

        reconstructed_query = path2query(paths, model=model_tuples)
        if isinstance(reconstructed_query, list):
            reconstructed_query = reconstructed_query[0]
            
        print("\nPaths back to Query (Projections):")
        if reconstructed_query:
            for proj in reconstructed_query.projections:
                if isinstance(proj, ProjectedCharacteristic):
                    print(f"  {proj.reference}")
                else:
                    print(f"  {proj}")

