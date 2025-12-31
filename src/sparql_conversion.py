import argparse
from typing import List, Dict

from query_path_conversion import query2path, load_model, PathUnion, ProjectedPath, _get_alias_type
from query_parser import get_ast
from participant_path_parser import ParticipantPath, EntityResolution, AssociationResolution
from tuple2owl import NS_DEFAULT
from tuple import UddlTuple


def _resolve_participant_path_to_type(path: ParticipantPath, model: List[UddlTuple] = None) -> str:
    """
    Resolve a participant path to its final type.
    Similar to resolve_participant_path_to_type in tuple2owl.py.
    """
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
        source_type = _resolve_participant_path_to_type(parent_path, model)
    
    # Look up the rolename in the model
    if model:
        for t in model:
            if t.subject == source_type and t.rolename == last.rolename:
                if isinstance(t.object, str):
                    return t.object.split('.')[0] if '.' in t.object else t.object
                elif isinstance(t.object, ParticipantPath):
                    return _resolve_participant_path_to_type(t.object, model)
    
    # Fallback to rolename
    return last.rolename


def _generate_property_name_for_path_step(current_type: str, resolution, model: List[UddlTuple] = None) -> str:
    """
    Generate the property name for a single step in a ParticipantPath.
    This matches the naming convention used in tuple2owl.py for individual properties.
    
    Returns: (property_name, target_type)
    """
    if isinstance(resolution, AssociationResolution):
        # Association resolution: "has<Entity/Association name>Participant_<rolename>"
        prop_name = f"has{current_type}Participant_{resolution.rolename}"
        target_type = resolution.association_name
        return prop_name, target_type
    elif isinstance(resolution, EntityResolution):
        # Entity resolution: "has<Entity/Association name>Composition_<rolename>"
        prop_name = f"has{current_type}Composition_{resolution.rolename}"
        
        # Find target type by looking up in model
        target_type = None
        if model:
            for t in model:
                if t.subject == current_type and t.rolename == resolution.rolename:
                    if isinstance(t.object, str):
                        target_type = t.object.split('.')[0] if '.' in t.object else t.object
                        break
                    elif isinstance(t.object, ParticipantPath):
                        # Resolve the path to get the final type
                        target_type = _resolve_participant_path_to_type(t.object, model)
                        break
        
        if not target_type:
            # If model is provided but lookup failed, do not generate property (matches tuple2owl)
            if model:
                return None, None
            # Fallback: use rolename if model is missing
            target_type = resolution.rolename
        
        return prop_name, target_type
    
    return None, None


def _get_observable_type_from_path(path: ParticipantPath, model: List[UddlTuple] = None) -> str:
    """
    Get the Observable type for a path that ends in a characteristic.
    """
    if not path.resolutions:
        return None
    
    last_res = path.resolutions[-1]
    if not isinstance(last_res, EntityResolution):
        return None
    
    characteristic_name = last_res.rolename
    
    # Get the source type for the characteristic
    if len(path.resolutions) == 1:
        source_type = path.start_type
    else:
        parent_path = ParticipantPath(path.start_type, path.resolutions[:-1])
        source_type = _get_alias_type(parent_path, model)
    
    # Look up the observable type
    if model:
        for t in model:
            if t.subject == source_type and t.predicate == 'composes' and t.rolename == characteristic_name:
                if isinstance(t.object, str):
                    return t.object.split('.')[-1] if '.' in t.object else t.object
    
    # Fallback: assume the characteristic name is the observable type
    return characteristic_name


def generate_sparql(alias_map: Dict[str, List[ParticipantPath]], 
                    projected_paths: List[ProjectedPath], 
                    model: List[UddlTuple] = None,
                    namespace: str = NS_DEFAULT) -> str:
    """
    Translates an Alias Map and Projected Paths into a SPARQL SELECT query.
    Handles both ParticipantPath and PathUnion in projections to support 
    ambiguous references and multi-path navigation.
    """
    where_clauses = []
    select_vars = set()
    
    # Track which triples we've already added to avoid duplicates in the graph
    seen_triples = set()

    # 1. Resolve Types for all Aliases
    # We use the _get_alias_type helper from query_path_conversion
    alias_to_type = {}
    for alias, paths in alias_map.items():
        # Any path in the list for this alias will resolve to the same type
        # We use the first path as representative
        if paths:
            alias_to_type[alias] = _get_alias_type(paths[0], model)
        else:
            alias_to_type[alias] = "Unknown"

    # 2. (Skipped) Inject rdf:type triples for all aliases
    # We are selecting Observation individuals that represent paths, not querying a populated graph of instances.
    # So we don't need to match ?alias a :Type.
    # # 2. Inject rdf:type triples for all aliases
    # # This ensures ?alias is linked to :ActualTypeName (e.g., ?t1 a :TemperatureSensor)
    # for alias, type_name in alias_to_type.items():
    #     type_triple = f"  ?{alias} a :{type_name} ."
    #     where_clauses.append(type_triple)

    # 3. (Skipped) Process the Alias Map to build the graph structure (JOINs)
    # The graph structure is implicitly handled by the property chains of the Observation individuals.
    # We don't need to generate structural triples like ?source :rel ?target.
    # # 3. Process the Alias Map to build the graph structure (JOINs)
    # # We sort by path length to ensure the root triples appear first
    # sorted_aliases = sorted(alias_map.keys(), key=lambda k: min(len(p.resolutions) for p in alias_map[k]))
    
    # for alias in sorted_aliases:
    #     paths = alias_map[alias]
    #     for p in paths:
    #         if not p.resolutions:
    #             continue  # Skip root entity itself
            
    #         # Find the parent alias for this specific path segment
    #         prefix_res = p.resolutions[:-1]
    #         try:
    #             source_alias = next(k for k, v in alias_map.items() 
    #                               if any(list(p2.resolutions) == list(prefix_res) for p2 in v))
    #         except StopIteration:
    #             continue

    #         leaf_res = p.resolutions[-1]
    #         predicate = f":{leaf_res.rolename}"
            
    #         if isinstance(leaf_res, EntityResolution):
    #             # Forward navigation: Subject -> Predicate -> Object
    #             triple = f"  ?{source_alias} {predicate} ?{alias} ."
    #         else:
    #             # Backward navigation: Object -> Predicate -> Subject
    #             triple = f"  ?{alias} {predicate} ?{source_alias} ."
            
    #         if triple not in seen_triples:
    #             where_clauses.append(triple)
    #             seen_triples.add(triple)

    # 4. Process Projected Paths (SELECT clause) - Select Observation individuals
    for i, proj in enumerate(projected_paths):
        if isinstance(proj, ParticipantPath):
            # Standard single-path projection
            p = proj
            if not p.resolutions:
                continue
            
            # Get the observable type for this path
            observable_type = _get_observable_type_from_path(p, model)
            if not observable_type:
                continue
            
            # Generate variable name for the Observation individual
            char_name = p.resolutions[-1].rolename if p.resolutions else "unknown"
            obs_var_name = f"obs_{i}_{char_name}"
            select_vars.add(f"?{obs_var_name}")
            
            # Match the Observation individual
            # 1. It's an Observation
            where_clauses.append(f"  ?{obs_var_name} a :Observation .")
            
            # 2. It has the correct Observable type
            where_clauses.append(f"  ?{obs_var_name} :hasObservable :{observable_type} .")
            
            # 3. Match the property chain that corresponds to the path
            # Properties on individuals point to type classes, not instances
            # Start with the start type property
            start_type_prop = f"has{p.start_type}"
            where_clauses.append(f"  ?{obs_var_name} :{start_type_prop} :{p.start_type} .")
            
            # Follow the path step by step
            # Skip the last resolution (the characteristic) - it's represented by hasObservable
            current_type = p.start_type
            
            for j, resolution in enumerate(p.resolutions[:-1]):  # Exclude last resolution (characteristic)
                prop_name, target_type = _generate_property_name_for_path_step(current_type, resolution, model)
                if not prop_name or not target_type:
                    break
                
                # Add the property assertion (points to type class)
                where_clauses.append(f"  ?{obs_var_name} :{prop_name} :{target_type} .")
                
                # Update current_type for next iteration
                current_type = target_type
                
        elif isinstance(proj, PathUnion):
            # For a PathUnion, we need to match any of the possible paths
            # Get observable type (should be the same for all paths in the union)
            if not proj.paths:
                continue
            
            observable_type = _get_observable_type_from_path(proj.paths[0], model)
            if not observable_type:
                continue
            
            char_name = proj.paths[0].resolutions[-1].rolename if proj.paths[0].resolutions else "unknown"
            obs_var_name = f"obs_{i}_{char_name}"
            select_vars.add(f"?{obs_var_name}")
            
            # Match the Observation individual
            where_clauses.append(f"  ?{obs_var_name} a :Observation .")
            where_clauses.append(f"  ?{obs_var_name} :hasObservable :{observable_type} .")
            
            # Create UNION branches for each possible path
            union_branches = []
            for path_idx, p in enumerate(proj.paths):
                if not p.resolutions:
                    continue
                
                # Build the property chain matching for this path
                branch_clauses = []
                
                # Start type property (points to type class)
                start_type_prop = f"has{p.start_type}"
                branch_clauses.append(f"    ?{obs_var_name} :{start_type_prop} :{p.start_type} .")
                
                # Follow the path
                # Skip the last resolution (the characteristic) - it's represented by hasObservable
                current_type = p.start_type
                for j, resolution in enumerate(p.resolutions[:-1]):  # Exclude last resolution (characteristic)
                    prop_name, target_type = _generate_property_name_for_path_step(current_type, resolution, model)
                    if not prop_name or not target_type:
                        break
                    
                    # Properties point to type classes, not instances
                    branch_clauses.append(f"    ?{obs_var_name} :{prop_name} :{target_type} .")
                    
                    current_type = target_type
                
                if branch_clauses:
                    branch = "{\n" + "\n".join(branch_clauses) + "\n  }"
                    union_branches.append(branch)
            
            if len(union_branches) == 1:
                # Single branch, no UNION needed
                # Extract the clauses from the branch (remove outer braces)
                branch_content = union_branches[0][2:-2]  # Remove "{\n" and "\n  }"
                for clause in branch_content.strip().split('\n'):
                    if clause.strip():
                        where_clauses.append(clause.strip())
            elif len(union_branches) > 1:
                # Multiple branches, use UNION
                union_pattern = "  " + " UNION\n  ".join(union_branches)
                where_clauses.append(union_pattern)

    # 5. Assemble Query
    sparql = f"PREFIX : <{namespace}>\n\n"
    sparql += f"SELECT {' '.join(sorted(list(select_vars)))}\n"
    sparql += "WHERE {\n"
    sparql += "\n".join(where_clauses)
    sparql += "\n}"
    
    return sparql


if __name__ == "__main__":    
    parser = argparse.ArgumentParser(description="Convert UDDL Query to SPARQL")
    parser.add_argument("query", help="The UDDL Query string")
    parser.add_argument("--model", help="Path to .face or tuple file")
    parser.add_argument("--ns", default=NS_DEFAULT, help=f"Namespace prefix (Default is {NS_DEFAULT})")
    args = parser.parse_args()

    # Load model if provided
    model_tuples = []
    if args.model:
        model_tuples = load_model(model_path=args.model)

    # 1. Parse UDDL to AST
    ast = get_ast(query_string=args.query)

    # 2. Convert AST to Alias Map and flattened Paths
    alias_map, projected_paths = query2path(query_ast=ast, model=model_tuples)

    # 3. Generate SPARQL
    sparql_output = generate_sparql(alias_map=alias_map, projected_paths=projected_paths, model=model_tuples, namespace=args.ns)

    print(sparql_output)
