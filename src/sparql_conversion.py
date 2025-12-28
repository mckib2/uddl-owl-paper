import argparse
import pathlib
import xml.etree.ElementTree as ET
from typing import List, Dict

from query_path_conversion import query2path, UddlTuple, load_model
from query_parser import get_ast
from participant_path_parser import ParticipantPath, EntityResolution, AssociationResolution


def generate_sparql(alias_map: Dict[str, List[ParticipantPath]], 
                    projected_paths: List[ParticipantPath], 
                    namespace: str = "http://skayl.com/model/") -> str:
    """
    Translates an Alias Map and Projected Paths into a SPARQL SELECT query.
    """
    where_clauses = []
    select_vars = set()
    
    # Track which triples we've already added to avoid duplicates in AND joins
    seen_triples = set()

    # 1. Process the Alias Map to build the graph structure (JOINs)
    # We sort by path length to ensure the root triples appear first (conventional)
    sorted_aliases = sorted(alias_map.keys(), key=lambda k: min(len(p.resolutions) for p in alias_map[k]))
    
    for alias in sorted_aliases:
        paths = alias_map[alias]
        for p in paths:
            if not p.resolutions:
                continue  # Skip the root entity itself
            
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
                # Forward: Source -> role -> Target
                triple = f"  ?{source_alias} {predicate} ?{alias} ."
            else:
                # Backward: Target -> role -> Source
                triple = f"  ?{alias} {predicate} ?{source_alias} ."
            
            if triple not in seen_triples:
                where_clauses.append(triple)
                seen_triples.add(triple)

    # 2. Process Projected Paths (SELECT clause)
    # We treat the characteristic as the object of a triple
    for i, p in enumerate(projected_paths):
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

    # 3. Assemble Query
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
    parser.add_argument("--ns", default="http://skayl.com/model/", help="Namespace prefix")

    args = parser.parse_args()

    # Load model if provided
    model_tuples = load_model(model_path=args.model)

    # 1. Parse UDDL to AST
    ast = get_ast(args.query)

    # 2. Convert AST to Alias Map and flattened Paths
    amap, ppaths = query2path(ast, model_tuples)

    # 3. Generate SPARQL
    sparql_output = generate_sparql(amap, ppaths, namespace=args.ns)

    print(sparql_output)
