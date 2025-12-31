import xml.etree.ElementTree as ET
import pathlib
import sys

from uddl2tuple import uddl2tuple
from tuple2owl import tuple2owl, NS_DEFAULT
from parse_tuple import parse_tuple
from tuple import UddlTuple
from add_individuals import add_individuals
from query_parser import QueryStatement
from query_path_conversion import query2path
from sparql_conversion import generate_sparql


def uddl2owl(uddl_file: pathlib.Path, output_file: pathlib.Path = None, generate_individuals: bool = False, output_sparql: bool = False) -> None:
    tuples = None
    
    # Try parsing as XML first
    try:
        with open(uddl_file, 'r') as file:
            uddl_doc = ET.parse(file)
        tuples = uddl2tuple(uddl_doc=uddl_doc)
    except ET.ParseError:
        # XML parsing failed, try parsing as tuple file
        try:
            tuples = parse_tuple(tuple_file=uddl_file)
        except Exception as e:
            print(f"Error parsing UDDL file {uddl_file}: {e}")
            print("Error: Failed to convert UDDL to tuples.", file=sys.stderr)
            return

    if tuples is None:
        print("Error: Failed to convert UDDL to tuples.", file=sys.stderr)
        return

    # Generate individuals from queries if requested
    if generate_individuals:
        try:
            # Separate data tuples from queries for model
            data_tuples = [t for t in tuples if isinstance(t, UddlTuple)]
            # Generate individuals from queries
            individual_tuples = add_individuals(tuples, model=data_tuples)
            # Append generated individuals to the tuples list
            tuples.extend(individual_tuples)
        except Exception as e:
            print(f"Warning: Failed to generate individuals: {e}", file=sys.stderr)
            # Continue without individuals if generation fails

    # Output SPARQL queries if requested
    if output_sparql:
        # Separate data tuples from queries for model
        data_tuples = [t for t in tuples if isinstance(t, UddlTuple)]
        # Find all QueryStatement objects
        queries = [t for t in tuples if isinstance(t, QueryStatement)]
        
        if queries:
            for i, query in enumerate(queries, 1):
                try:
                    # Convert QueryStatement to alias_map and projected_paths
                    alias_map, projected_paths = query2path(query_ast=query, model=data_tuples)
                    # Generate SPARQL
                    sparql_output = generate_sparql(
                        alias_map=alias_map,
                        projected_paths=projected_paths,
                        model=data_tuples,
                        namespace=NS_DEFAULT
                    )
                    # Output with separator
                    if len(queries) > 1:
                        print(f"# SPARQL Query {i}/{len(queries)}", file=sys.stdout)
                    print(sparql_output, file=sys.stdout)
                    if i < len(queries):
                        print("", file=sys.stdout)  # Blank line between queries
                except Exception as e:
                    print(f"Warning: Failed to convert query {i} to SPARQL: {e}", file=sys.stderr)
        else:
            print("No queries found in the UDDL file.", file=sys.stderr)

    owl_doc = tuple2owl(tuples=tuples)
    if owl_doc is None:
        print("Error: Failed to convert UDDL to OWL.", file=sys.stderr)
        return

    tree = ET.ElementTree(owl_doc.getroot())
    ET.indent(tree, space="\t", level=0)
    xml_string = ET.tostring(tree.getroot(), encoding='utf-8', xml_declaration=True).decode('utf-8')

    if output_file is not None:
        if not output_file.parent.exists():
            output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, 'w') as file:
            file.write(xml_string)
    else:
        print(xml_string)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Convert a UDDL file to OWL.")
    parser.add_argument("uddl_file", type=pathlib.Path, help="Path to the UDDL file to convert")
    parser.add_argument("-o", "--output", type=pathlib.Path, help="Path to the output OWL file")
    parser.add_argument(
        "--add-individuals", "-a",
        action="store_true",
        help="Generate individuals from queries in the UDDL file before converting to OWL"
    )
    parser.add_argument(
        "--sparql", "-s",
        action="store_true",
        help="Output converted SPARQL queries to stdout for each querystatement found in the UDDL file"
    )
    args = parser.parse_args()
    uddl2owl(uddl_file=args.uddl_file, output_file=args.output, generate_individuals=args.add_individuals, output_sparql=args.sparql)

