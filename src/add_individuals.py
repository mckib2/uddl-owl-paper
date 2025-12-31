"""
Generate individual tuples from UDDL queries.

For each query, creates Observation individuals for each selected characteristic,
with paths that match the query's JOIN structure so that SPARQL queries can find them.
"""
import argparse
import pathlib
import sys
from typing import List, Union, Set
from collections import defaultdict

from tuple import UddlTuple
from parse_tuple import parse_tuple
from query_parser import QueryStatement, get_ast
from query_path_conversion import query2path, load_model, ProjectedPath, PathUnion, _get_alias_type
from participant_path_parser import ParticipantPath, EntityResolution, AssociationResolution


def get_observable_type_from_characteristic(
    source_type: str,
    characteristic_name: str,
    model: List[UddlTuple]
) -> str:
    """
    Look up the observable type for a characteristic (rolename) on a given entity type.
    Returns the observable type name, or the characteristic_name if not found.
    """
    for t in model:
        if t.subject == source_type and t.predicate == 'composes' and t.rolename == characteristic_name:
            if isinstance(t.object, str):
                # Return the observable type (strip namespace if present)
                return t.object.split('.')[-1] if '.' in t.object else t.object
    # Fallback: assume the characteristic name is the observable type
    return characteristic_name


def create_individuals_from_query(
    query: QueryStatement,
    model: List[UddlTuple],
    individual_counter: defaultdict
) -> List[UddlTuple]:
    """
    Create individual tuples for a single query.
    
    Args:
        query: The QueryStatement to process
        model: The model tuples for type resolution
        individual_counter: Counter dict for generating unique individual names
    
    Returns:
        List of individual tuples (instance tuples)
    """
    individual_tuples = []
    
    # Convert query to paths
    alias_map, projected_paths = query2path(query_ast=query, model=model)
    
    if not projected_paths:
        return individual_tuples
    
    # Get root entity type
    root_entity = query.from_clause.entities[0]
    root_type = root_entity.name
    
    # Process each projected path
    for proj in projected_paths:
        # Handle both single paths and path unions
        paths_to_process = proj.paths if isinstance(proj, PathUnion) else [proj]
        
        for path in paths_to_process:
            if not path.resolutions:
                continue
            
            # Get the characteristic name (last resolution)
            last_res = path.resolutions[-1]
            if not isinstance(last_res, EntityResolution):
                # Skip if last resolution is not an entity resolution (shouldn't happen for characteristics)
                continue
            
            characteristic_name = last_res.rolename
            
            # Get the source type for the characteristic (the entity that composes it)
            if len(path.resolutions) == 1:
                source_type = path.start_type
            else:
                # Get the type of the entity that has this characteristic
                parent_path = ParticipantPath(path.start_type, path.resolutions[:-1])
                source_type = _get_alias_type(parent_path, model)
            
            # Look up the observable type
            observable_type = get_observable_type_from_characteristic(
                source_type, characteristic_name, model
            )
            
            # Generate unique individual name
            # Use characteristic name as base, with counter for uniqueness
            base_name = characteristic_name
            counter = individual_counter[base_name]
            individual_counter[base_name] += 1
            individual_name = f"{base_name}_{counter}" if counter > 0 else base_name
            
            # Create the individual tuple
            # Format: (ObservableType, instance, ParticipantPath, individual_name)
            individual_tuple = UddlTuple(
                subject=observable_type,
                predicate='instance',
                object=path,  # The full path from root to characteristic
                rolename=individual_name,
                multiplicity=None
            )
            individual_tuples.append(individual_tuple)
    
    return individual_tuples


def format_tuple_for_output(tuple_obj: Union[UddlTuple, QueryStatement]) -> str:
    """
    Format a UddlTuple or QueryStatement for output to a tuple file.
    Returns the string representation suitable for writing to a file.
    Matches the format expected by parse_tuple.py.
    """
    if isinstance(tuple_obj, QueryStatement):
        # For queries, use pretty_print for better formatting
        query_str = tuple_obj.pretty_print()
        if not query_str.rstrip().endswith(';'):
            query_str = query_str.rstrip() + ';'
        return query_str + "\n\n"
    elif isinstance(tuple_obj, UddlTuple):
        # Format tuple as: (Subject, Predicate[multiplicity], Object, rolename)
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


def add_individuals(
    tuples: List[Union[UddlTuple, QueryStatement]],
    model: List[UddlTuple] = None
) -> List[UddlTuple]:
    """
    Generate individual tuples from queries in the input.
    
    Args:
        tuples: List of tuples and query statements
        model: Optional model tuples for type resolution (if None, extracted from tuples)
    
    Returns:
        List of individual tuples (instance tuples)
    """
    # Separate queries from data tuples
    queries = [t for t in tuples if isinstance(t, QueryStatement)]
    data_tuples = [t for t in tuples if isinstance(t, UddlTuple)]
    
    # Use provided model or extract from data tuples
    if model is None:
        model = data_tuples
    
    # Counter for generating unique individual names
    individual_counter = defaultdict(int)
    
    # Process each query
    all_individuals = []
    for query in queries:
        individuals = create_individuals_from_query(query, model, individual_counter)
        all_individuals.extend(individuals)
    
    return all_individuals


def main():
    parser = argparse.ArgumentParser(
        description="Generate individual tuples from UDDL queries in a tuple file."
    )
    parser.add_argument(
        "tuple_file",
        type=pathlib.Path,
        help="Path to the tuple file (can contain queries)"
    )
    parser.add_argument(
        "-o", "--output",
        type=pathlib.Path,
        help="Output file path (if not provided, prints to stdout)"
    )
    parser.add_argument(
        "--model",
        type=pathlib.Path,
        help="Optional model file for type resolution (if different from tuple_file)"
    )
    parser.add_argument(
        "--all", "-a",
        action="store_true",
        help="Output all tuples (data, queries, and individuals) together instead of just individuals"
    )
    
    args = parser.parse_args()
    
    # Load tuples and queries
    try:
        tuples = parse_tuple(args.tuple_file)
    except FileNotFoundError:
        print(f"Error: File not found: {args.tuple_file}", file=__import__('sys').stderr)
        return 1
    except Exception as e:
        print(f"Error parsing file: {e}", file=__import__('sys').stderr)
        return 1
    
    # Load model if provided separately
    model = None
    if args.model:
        try:
            model = load_model(str(args.model))
        except Exception as e:
            print(f"Warning: Failed to load model file: {e}", file=__import__('sys').stderr)
    
    # Generate individuals
    try:
        individual_tuples = add_individuals(tuples, model)
    except Exception as e:
        print(f"Error generating individuals: {e}", file=__import__('sys').stderr)
        import traceback
        traceback.print_exc()
        return 1
    
    # Format output
    if args.all:
        # Output all tuples: data tuples, queries, and individuals
        output_lines = []
        
        # Add all original tuples (data and queries)
        for t in tuples:
            output_lines.append(format_tuple_for_output(t))
        
        # Add generated individuals
        if individual_tuples:
            # Add a blank line before individuals if there are any
            if output_lines and not output_lines[-1].strip().endswith('\n\n'):
                output_lines.append("\n")
            for t in individual_tuples:
                output_lines.append(format_tuple_for_output(t))
        
        output_text = "".join(output_lines).rstrip() + "\n" if output_lines else ""
    else:
        # Output only individuals (original behavior)
        output_lines = []
        for t in individual_tuples:
            # Format: (Subject, predicate, object, rolename)
            # For instance tuples, object is a ParticipantPath
            path_str = str(t.object) if isinstance(t.object, ParticipantPath) else str(t.object)
            output_lines.append(f"({t.subject}, {t.predicate}, {path_str}, {t.rolename})")
        
        output_text = "\n".join(output_lines)
    
    # Write output
    if args.output:
        try:
            with open(args.output, 'w') as f:
                f.write(output_text)
        except Exception as e:
            print(f"Error writing output file: {e}", file=__import__('sys').stderr)
            return 1
    else:
        # Print to stdout (without extra trailing newline if it's already there)
        sys.stdout.write(output_text.rstrip("\n"))
        if output_text.strip():
            sys.stdout.write("\n")
    
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())

