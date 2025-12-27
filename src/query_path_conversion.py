"""
The alias map is a dictionary-based data structure used during the conversion process to track the
relationship between UDDL query aliases and their corresponding navigational paths. It serves as a
middle layer that translates the relational structure of a SQL-like query into the navigational
structure of ParticipantPaths.

In the query2path function, the map is defined with the following structure:
      Key: The Alias Name (e.g., a_from, b_in_A, or a generated name like t1) defined in the FROM or
           JOIN clauses.
    Value: A List of ParticipantPath objects that represent every possible way to reach that
           specific entity instance starting from the root entity.

The alias map is primarily used to solve two problems in the translation process:
- Enforcing "AND" Semantics: In a standard tree-based path, each branch is independent. However,
  in a "diamond join" where an entity (like D) is joined on two different criteria
  (D.b = B AND D.c = C), the alias map groups both paths (A.b.d and A.c.d) under the single key D.
  This ensures that during reconstruction, the logic knows these paths must converge on the same
  instance rather than creating two separate entities.
- Identity Resolution: According to the UDDL v1.1 grammar, many joins occur on the identity of an
  entity rather than a named characteristic. The alias map allows the parser to identify when a
  reference (like JOIN B ON A.b = B) is pointing to the entity's identity (the alias) rather than
  an observable attribute.
- Join Ordering: During the "Path to Query" conversion, the alias map is sorted by path depth.
  This ensures that JOIN statements are reconstructed in the correct logical order, beginning at
  the root entity and moving outward through the graph.
- Characteristic Projection: Because UDDL queries should only select characteristics (observables)
  and not the entities themselves, the alias map acts as a lookup table. When the query says
  SELECT d.attr, the system uses the map to find the navigational path to alias d and then appends
  the .attr resolution to create the final terminal path.
"""

from typing import List, Union, Dict, Tuple
from collections import defaultdict
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
    ProjectedCharacteristic, AllCharacteristics, EntityWildcard, get_ast
)


def get_model_attributes(model: List[UddlTuple], type_name: str) -> List[str]:
    """
    Finds all attribute rolenames for a given entity type from the model.
    Attributes are identified by the 'composes' predicate.
    """
    attributes = set()
    
    for t in model:
        if t.subject == type_name and t.predicate == 'composes':
            attributes.add(t.rolename)
            
    return sorted(list(attributes))


def query2path(query_ast: QueryStatement, model: List[UddlTuple] = None) -> Tuple[Dict[str, List[ParticipantPath]], List[ParticipantPath]]:
    """
    Maps a UDDL Query to (alias_map, projected_paths).
    """
    from_clause = query_ast.from_clause
    if not from_clause.entities:
        return {}, []

    root_entity = from_clause.entities[0]
    start_type = root_entity.name
    root_alias = root_entity.alias or root_entity.name

    alias_map: Dict[str, List[ParticipantPath]] = {root_alias: [ParticipantPath(start_type, [])]}
    alias_types = {root_alias: start_type}

    # 1. Build the JOIN graph (alias_map)
    for join in from_clause.joins:
        target_alias = join.target.alias or join.target.name
        target_type = join.target.name
        alias_types[target_alias] = target_type
        
        if target_alias not in alias_map:
            alias_map[target_alias] = []

        for cond in join.on:
            left, right = cond.left, cond.right
            
            def is_identity(ref):
                return ref is not None and (ref.characteristic is None or ref.characteristic == "")

            left_alias = left.entity or root_alias
            right_alias = right.entity if right else None
            
            found_res = None
            source_alias = None

            if right is None: # Unary JOIN
                source_alias, found_res = left_alias, EntityResolution(left.characteristic, target_type)
            elif left_alias in alias_map and right_alias == target_alias and is_identity(right):
                source_alias, found_res = left_alias, EntityResolution(left.characteristic, target_type)
            elif right_alias in alias_map and left_alias == target_alias and is_identity(left):
                source_alias, found_res = right_alias, EntityResolution(right.characteristic, target_type)
            elif left_alias == target_alias and right_alias in alias_map and is_identity(right):
                source_alias, found_res = right_alias, AssociationResolution(left.characteristic, target_type)
            elif right_alias == target_alias and left_alias in alias_map and is_identity(left):
                source_alias, found_res = left_alias, AssociationResolution(right.characteristic, target_type)

            if found_res and source_alias in alias_map:
                for parent_path in alias_map[source_alias]:
                    new_path = ParticipantPath(start_type, list(parent_path.resolutions) + [found_res])
                    if str(new_path) not in [str(p) for p in alias_map[target_alias]]:
                        alias_map[target_alias].append(new_path)

    # 2. Build projected characteristic paths
    projected_paths = []
    for proj in query_ast.projections:
        if isinstance(proj, ProjectedCharacteristic):
            entity_ref = proj.reference.entity or root_alias
            attr = proj.reference.characteristic
            if attr and entity_ref in alias_map:
                for base in alias_map[entity_ref]:
                    projected_paths.append(ParticipantPath(start_type, list(base.resolutions) + [EntityResolution(attr)]))
        elif isinstance(proj, (AllCharacteristics, EntityWildcard)) and model:
            target_alias = proj.entity if isinstance(proj, EntityWildcard) else root_alias
            
            if target_alias in alias_map and target_alias in alias_types:
                type_name = alias_types[target_alias]
                attributes = get_model_attributes(model, type_name)
                for attr in attributes:
                    for base in alias_map[target_alias]:
                        projected_paths.append(ParticipantPath(start_type, list(base.resolutions) + [EntityResolution(attr)]))
    
    return alias_map, projected_paths


def path2query(alias_map: Dict[str, List[ParticipantPath]], projected_paths: List[ParticipantPath], model: List[UddlTuple] = None) -> List[QueryStatement]:
    """
    Reconstructs QueryStatements from a mapping of aliases and terminal characteristic paths.
    """
    if not alias_map: return []

    root_alias = next(k for k, v in alias_map.items() if any(len(p.resolutions) == 0 for p in v))
    start_type = alias_map[root_alias][0].start_type
    
    joins = []
    alias_type_tracker = {root_alias: start_type}
    sorted_aliases = sorted(alias_map.keys(), key=lambda k: min(len(p.resolutions) for p in alias_map[k]))
    
    for alias in sorted_aliases:
        if alias == root_alias: continue
        target_type = _resolve_target_type(alias_map[alias], model, alias_type_tracker, root_alias)
        alias_type_tracker[alias] = target_type
        
        equivalences = []
        for p in alias_map[alias]:
            prefix = p.resolutions[:-1]
            try:
                source_alias = next(k for k, v in alias_map.items() if any(list(p2.resolutions) == list(prefix) for p2 in v))
                res = p.resolutions[-1]
                if isinstance(res, EntityResolution):
                    equivalences.append(Equivalence(Reference(source_alias, res.rolename), Reference(alias, None)))
                else:
                    equivalences.append(Equivalence(Reference(alias, res.rolename), Reference(source_alias, None)))
            except StopIteration: continue
        joins.append(Join(Entity(target_type, alias), equivalences))

    # Reconstruct projections (Ensuring only characteristics are selected)
    projections = []
    for p in projected_paths:
        prefix = p.resolutions[:-1]
        try:
            source_alias = next(k for k, v in alias_map.items() if any(list(p2.resolutions) == list(prefix) for p2 in v))
            projections.append(ProjectedCharacteristic(Reference(source_alias, p.resolutions[-1].rolename)))
        except StopIteration: continue

    return [QueryStatement(projections, FromClause([Entity(start_type, root_alias)], joins))]


def _resolve_target_type(paths: List[ParticipantPath], model: List[UddlTuple], type_tracker: Dict[str, str], root_alias: str) -> str:
    """Uses model lookups to find the object type of a rolename."""
    last_res = paths[0].resolutions[-1]
    if isinstance(last_res, AssociationResolution):
        return last_res.association_name
    
    if isinstance(last_res, EntityResolution) and last_res.target_type:
        return last_res.target_type

    if not model:
        return last_res.rolename

    # Find parent alias and its type
    prefix = paths[0].resolutions[:-1]
    source_type = None
    for alias, known_type in type_tracker.items():
        # This is a bit expensive but precise: find which alias this path came from
        if not prefix and alias == root_alias:
            source_type = known_type
            break
    
    if source_type:
        for t in model:
            if t.subject == source_type and t.rolename == last_res.rolename:
                obj = str(t.object)
                return obj.split('.')[0] if '.' in obj else obj
            
    return last_res.rolename


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="UDDL Query/Path Conversion")
    parser.add_argument("input", nargs="*", help="Query or Paths")
    parser.add_argument("--model", help="Path to .face or tuple file")
    parser.add_argument("--alias-map", action="append", help="Define alias mapping (e.g. 'alias=path1,path2')")

    args = parser.parse_args()

    model_tuples = []
    if args.model:
        p = pathlib.Path(args.model)
        if p.suffix.lower() in ['.face', '.xml']:
            model_tuples = [t for t in uddl2tuple(ET.parse(p)) if isinstance(t, UddlTuple)]
        else:
            model_tuples = [t for t in parse_tuple(p) if isinstance(t, UddlTuple)]

    if args.input or args.alias_map:
        if args.input and args.input[0].upper().startswith("SELECT"):
            alias_map, projected_paths = query2path(query_ast=get_ast(" ".join(args.input)), model=model_tuples)
            print("\nAlias Map:")
            for alias, paths in alias_map.items():
                print(f"  {alias}: {[str(p) for p in paths]}")
            
            print("\nProjected Paths:")
            for p in projected_paths:
                print(f"  {str(p)}")
        else:
            # Assumes multiple paths correspond to one aliased entity
            projected_paths = [ParticipantPath.parse(p) for p in args.input]
            
            if args.alias_map:
                alias_map = {}
                for mapping in args.alias_map:
                    if '=' in mapping:
                        alias, paths_str = mapping.split('=', 1)
                        paths_list = [ParticipantPath.parse(p.strip()) for p in paths_str.split(',')]
                        alias_map[alias.strip()] = paths_list
            else:
                alias_map = {"target": projected_paths}

            queries = path2query(alias_map=alias_map, projected_paths=projected_paths, model=model_tuples)
            print("\nReconstructed:")
            for q in queries:
                print(q)
    else:
        # Example: Diamond Join
        q = "SELECT d FROM A JOIN B ON A.b JOIN C ON A.c JOIN D ON D.b_ref = B AND D.c_ref = C"
        print("Demo Query:")
        print(q)
        print()
        alias_map, projected_paths = query2path(query_ast=get_ast(q))
        print("\nAlias Map:")
        for alias, paths in alias_map.items():
            print(f"  {alias}: {[str(p) for p in paths]}")
            
        print("\nProjected Paths:")
        for p in projected_paths:
            print(f"  {str(p)}")
        print()
        print("Reconstructed:")
        for q in path2query(alias_map=alias_map, projected_paths=projected_paths):
            print(q)
        print()
