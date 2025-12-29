import argparse
from typing import List, Dict

from query_path_conversion import query2path, load_model, PathUnion, ProjectedPath, _get_alias_type
from query_parser import get_ast
from participant_path_parser import ParticipantPath, EntityResolution
from tuple2owl import NS_DEFAULT
from tuple import UddlTuple


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

    # 2. Inject rdf:type triples for all aliases
    # This ensures ?alias is linked to :ActualTypeName (e.g., ?t1 a :TemperatureSensor)
    for alias, type_name in alias_to_type.items():
        type_triple = f"  ?{alias} a :{type_name} ."
        where_clauses.append(type_triple)

    # 3. Process the Alias Map to build the graph structure (JOINs)
    # We sort by path length to ensure the root triples appear first
    sorted_aliases = sorted(alias_map.keys(), key=lambda k: min(len(p.resolutions) for p in alias_map[k]))
    
    for alias in sorted_aliases:
        paths = alias_map[alias]
        for p in paths:
            if not p.resolutions:
                continue  # Skip root entity itself
            
            # Find the parent alias for this specific path segment
            prefix_res = p.resolutions[:-1]
            try:
                source_alias = next(k for k, v in alias_map.items() 
                                  if any(list(p2.resolutions) == list(prefix_res) for p2 in v))
            except StopIteration:
                continue

            leaf_res = p.resolutions[-1]
            predicate = f":{leaf_res.rolename}"
            
            if isinstance(leaf_res, EntityResolution):
                # Forward navigation: Subject -> Predicate -> Object
                triple = f"  ?{source_alias} {predicate} ?{alias} ."
            else:
                # Backward navigation: Object -> Predicate -> Subject
                triple = f"  ?{alias} {predicate} ?{source_alias} ."
            
            if triple not in seen_triples:
                where_clauses.append(triple)
                seen_triples.add(triple)

    # 4. Process Projected Paths (SELECT clause)
    for i, proj in enumerate(projected_paths):
        if isinstance(proj, ParticipantPath):
            # Standard single-path projection
            p = proj
            prefix_res = p.resolutions[:-1]
            try:
                source_alias = next(k for k, v in alias_map.items() 
                                  if any(list(p2.resolutions) == list(prefix_res) for p2 in v))
                
                char_name = p.resolutions[-1].rolename
                var_name = f"{source_alias}_{char_name}"
                select_vars.add(f"?{var_name}")
                
                triple = f"  ?{source_alias} :{char_name} ?{var_name} ."
                if triple not in seen_triples:
                    where_clauses.append(triple)
                    seen_triples.add(triple)
            except StopIteration:
                continue
                
        elif isinstance(proj, PathUnion):
            # For a PathUnion, we bind all possible paths to a single output variable
            char_name = proj.paths[0].resolutions[-1].rolename
            # Generate a unique variable name for this projection to avoid collisions
            var_name = f"result_{i}_{char_name}"
            select_vars.add(f"?{var_name}")
            
            union_branches = []
            for p in proj.paths:
                prefix_res = p.resolutions[:-1]
                try:
                    source_alias = next(k for k, v in alias_map.items() 
                                      if any(list(p2.resolutions) == list(prefix_res) for p2 in v))
                    # Use double curly braces for literal braces in the f-string
                    branch = f"{{ ?{source_alias} :{char_name} ?{var_name} }}"
                    if branch not in union_branches:
                        union_branches.append(branch)
                except StopIteration:
                    continue
            
            if len(union_branches) == 1:
                # If deduplication results in only one unique alias, no UNION is needed
                triple = f"  {union_branches[0][2:-2]} ."
                if triple not in seen_triples:
                    where_clauses.append(triple)
                    seen_triples.add(triple)
            elif len(union_branches) > 1:
                # Construct the UNION block for alternative entity sources
                union_pattern = "  " + " UNION ".join(union_branches) + " ."
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
