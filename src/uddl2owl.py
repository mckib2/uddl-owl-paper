import xml.etree.ElementTree as ET
import pathlib
import sys

from uddl2tuple import uddl2tuple
from tuple2owl import tuple2owl
from parse_tuple import parse_tuple


def uddl2owl(uddl_file: pathlib.Path, output_file: pathlib.Path = None) -> None:
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

    owl_doc = tuple2owl(tuples=tuples)
    if owl_doc is None:
        print("Error: Failed to convert UDDL to OWL.", file=sys.stderr)
        return

    tree = ET.ElementTree(owl_doc.getroot())
    ET.indent(tree, space="\t", level=0)
    xml_string = ET.tostring(tree.getroot(), encoding='utf-8').decode('utf-8')

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
    args = parser.parse_args()
    uddl2owl(uddl_file=args.uddl_file, output_file=args.output)

