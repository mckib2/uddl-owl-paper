import sys
import argparse
import pathlib
import xml.etree.ElementTree as ET
from typing import Dict, List, Set, Tuple

def get_local_name(uri: str) -> str:
    """Extract local name from URI (after # or /)."""
    if not uri:
        return ""
    if '#' in uri:
        return uri.split('#')[-1]
    return uri.split('/')[-1]

def parse_owl(file_path: pathlib.Path) -> Tuple[Dict, Dict]:
    """
    Parse OWL XML file to extract Classes and ObjectProperties.
    Returns (classes, properties) dictionaries.
    """
    try:
        tree = ET.parse(file_path)
    except ET.ParseError as e:
        raise ValueError(f"Failed to parse XML: {e}")
        
    root = tree.getroot()
    
    # Namespaces
    ns = {
        'owl': 'http://www.w3.org/2002/07/owl#',
        'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
        'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
    }
    
    classes = {} # id -> {subClassOf: [], disjointWith: []}
    properties = {} # id -> {domain: "", range: "", inverse: ""}
    
    # Find Classes
    for cls in root.findall('.//owl:Class', ns):
        about = cls.get(f"{{{ns['rdf']}}}about")
        if not about: continue
        
        cls_id = get_local_name(about)
        classes[cls_id] = {'subClassOf': [], 'disjointWith': []}
        
        for sub in cls.findall('./rdfs:subClassOf', ns):
            res = sub.get(f"{{{ns['rdf']}}}resource")
            if res:
                classes[cls_id]['subClassOf'].append(get_local_name(res))
                
        for dis in cls.findall('./owl:disjointWith', ns):
            res = dis.get(f"{{{ns['rdf']}}}resource")
            if res:
                classes[cls_id]['disjointWith'].append(get_local_name(res))

    # Find Object Properties
    for prop in root.findall('.//owl:ObjectProperty', ns):
        about = prop.get(f"{{{ns['rdf']}}}about")
        if not about: continue
        
        prop_id = get_local_name(about)
        properties[prop_id] = {'domain': None, 'range': None, 'inverse': None}
        
        # Domain
        domain_elem = prop.find('./rdfs:domain', ns)
        if domain_elem is not None:
             res = domain_elem.get(f"{{{ns['rdf']}}}resource")
             if res: properties[prop_id]['domain'] = get_local_name(res)

        # Range
        range_elem = prop.find('./rdfs:range', ns)
        if range_elem is not None:
             res = range_elem.get(f"{{{ns['rdf']}}}resource")
             if res: properties[prop_id]['range'] = get_local_name(res)

        # InverseOf
        inv_elem = prop.find('./owl:inverseOf', ns)
        if inv_elem is not None:
             res = inv_elem.get(f"{{{ns['rdf']}}}resource")
             if res: properties[prop_id]['inverse'] = get_local_name(res)

    return classes, properties

def generate_mermaid(classes: Dict, properties: Dict) -> str:
    """Generate Mermaid graph definition from parsed OWL data."""
    lines = ["graph LR"]
    lines.append("    classDef entity fill:#ffffff,stroke:#000000,stroke-width:1px,color:#000000;")
    
    # Nodes (Classes)
    for cls_id in sorted(classes.keys()):
        lines.append(f"    {cls_id}[{cls_id}]:::entity")
        
    # Inheritance
    for cls_id, data in classes.items():
        for parent in data['subClassOf']:
            if parent in classes:
                 lines.append(f"    {cls_id} -->|subClassOf| {parent}")
                 
    # Disjoints
    disjoint_pairs = set()
    for cls_id, data in classes.items():
        for dis in data['disjointWith']:
            if dis in classes:
                # Store pair sorted to avoid duplicates (A-B vs B-A)
                pair = tuple(sorted((cls_id, dis)))
                if pair not in disjoint_pairs:
                    disjoint_pairs.add(pair)
                    lines.append(f"    {cls_id} -.-|disjointWith| {dis}")

    # Properties
    processed_props = set()
    
    # Sort properties for deterministic output
    for prop_id in sorted(properties.keys()):
        if prop_id in processed_props:
            continue
            
        data = properties[prop_id]
        domain = data.get('domain')
        range_ = data.get('range')
        inverse = data.get('inverse')
        
        # Skip if incomplete property definition
        if not domain or not range_:
            continue
            
        label = prop_id
        if inverse:
            processed_props.add(inverse)
            label = f"{prop_id}/{inverse}"
            
        processed_props.add(prop_id)
        
        # Only add edge if both domain and range are known classes
        if domain in classes and range_ in classes:
            lines.append(f"    {domain} -->|{label}| {range_}")
            
    return "\n".join(lines)

def main():
    parser = argparse.ArgumentParser(description="Generate Mermaid diagram from OWL XML file.")
    parser.add_argument("owl_file", type=pathlib.Path, help="Input OWL XML file")
    parser.add_argument("--output", "-o", type=pathlib.Path, help="Output .mmd file (default: stdout)")
    
    args = parser.parse_args()
    
    if not args.owl_file.exists():
        print(f"Error: File not found: {args.owl_file}", file=sys.stderr)
        sys.exit(1)
    
    try:
        classes, properties = parse_owl(args.owl_file)
        mermaid_code = generate_mermaid(classes, properties)
        
        if args.output:
            with open(args.output, 'w') as f:
                f.write(mermaid_code)
        else:
            print(mermaid_code)
            
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()

