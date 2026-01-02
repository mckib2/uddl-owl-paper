import xml.etree.ElementTree as ET
import sys
import pathlib
import argparse
import subprocess
import tempfile
import re
from collections import defaultdict
from typing import Dict, List, Tuple
from io import StringIO

# Import the uddl2owl function
from uddl2owl import uddl2owl


def get_local_name(uri: str) -> str:
    """Extract local name from URI (after # or /)."""
    if not uri:
        return ""
    if '#' in uri:
        return uri.split('#')[-1]
    return uri.split('/')[-1]


def parse_owl_statistics(owl_file_path: pathlib.Path) -> Dict:
    """
    Parse OWL XML file to extract comprehensive statistics.
    Returns a dictionary with counts and details.
    """
    try:
        tree = ET.parse(owl_file_path)
    except ET.ParseError as e:
        raise ValueError(f"Failed to parse OWL XML: {e}")
        
    root = tree.getroot()
    
    # Namespaces
    ns = {
        'owl': 'http://www.w3.org/2002/07/owl#',
        'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
        'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
    }
    
    stats = {
        'classes': 0,
        'object_properties': 0,
        'datatype_properties': 0,
        'individuals': 0,
        'subclass_relations': 0,
        'property_domains': 0,
        'property_ranges': 0,
        'inverse_properties': 0,
        'disjoint_classes': 0,
        'class_details': {},  # class_name -> {subClassOf: [], disjointWith: []}
        'property_details': {},  # prop_name -> {domain: "", range: "", inverse: ""}
    }
    
    # Find Classes
    for cls in root.findall('.//owl:Class', ns):
        about = cls.get(f"{{{ns['rdf']}}}about")
        if not about:
            continue
        
        cls_id = get_local_name(about)
        stats['classes'] += 1
        stats['class_details'][cls_id] = {'subClassOf': [], 'disjointWith': []}
        
        for sub in cls.findall('./rdfs:subClassOf', ns):
            res = sub.get(f"{{{ns['rdf']}}}resource")
            if res:
                parent_name = get_local_name(res)
                stats['class_details'][cls_id]['subClassOf'].append(parent_name)
                stats['subclass_relations'] += 1
                
        for dis in cls.findall('./owl:disjointWith', ns):
            res = dis.get(f"{{{ns['rdf']}}}resource")
            if res:
                dis_name = get_local_name(res)
                stats['class_details'][cls_id]['disjointWith'].append(dis_name)
                stats['disjoint_classes'] += 1
    
    # Find Object Properties
    for prop in root.findall('.//owl:ObjectProperty', ns):
        about = prop.get(f"{{{ns['rdf']}}}about")
        if not about:
            continue
        
        prop_id = get_local_name(about)
        stats['object_properties'] += 1
        stats['property_details'][prop_id] = {'domain': None, 'range': None, 'inverse': None}
        
        # Domain
        domain_elem = prop.find('./rdfs:domain', ns)
        if domain_elem is not None:
            res = domain_elem.get(f"{{{ns['rdf']}}}resource")
            if res:
                stats['property_details'][prop_id]['domain'] = get_local_name(res)
                stats['property_domains'] += 1
        
        # Range
        range_elem = prop.find('./rdfs:range', ns)
        if range_elem is not None:
            res = range_elem.get(f"{{{ns['rdf']}}}resource")
            if res:
                stats['property_details'][prop_id]['range'] = get_local_name(res)
                stats['property_ranges'] += 1
        
        # InverseOf
        inv_elem = prop.find('./owl:inverseOf', ns)
        if inv_elem is not None:
            res = inv_elem.get(f"{{{ns['rdf']}}}resource")
            if res:
                stats['property_details'][prop_id]['inverse'] = get_local_name(res)
                stats['inverse_properties'] += 1
    
    # Find Datatype Properties
    for prop in root.findall('.//owl:DatatypeProperty', ns):
        about = prop.get(f"{{{ns['rdf']}}}about")
        if not about:
            continue
        
        prop_id = get_local_name(about)
        stats['datatype_properties'] += 1
        stats['property_details'][prop_id] = {'domain': None, 'range': None, 'inverse': None}
        
        # Domain
        domain_elem = prop.find('./rdfs:domain', ns)
        if domain_elem is not None:
            res = domain_elem.get(f"{{{ns['rdf']}}}resource")
            if res:
                stats['property_details'][prop_id]['domain'] = get_local_name(res)
                stats['property_domains'] += 1
        
        # Range
        range_elem = prop.find('./rdfs:range', ns)
        if range_elem is not None:
            res = range_elem.get(f"{{{ns['rdf']}}}resource")
            if res:
                stats['property_details'][prop_id]['range'] = get_local_name(res)
                stats['property_ranges'] += 1
    
    # Find Individuals (NamedIndividual)
    for ind in root.findall('.//owl:NamedIndividual', ns):
        about = ind.get(f"{{{ns['rdf']}}}about")
        if about:
            stats['individuals'] += 1
    
    # Also check for individuals defined with rdf:type
    for elem in root.findall('.//*'):
        type_elem = elem.find('./rdf:type', ns)
        if type_elem is not None:
            type_res = type_elem.get(f"{{{ns['rdf']}}}resource")
            if type_res and 'NamedIndividual' in type_res:
                about = elem.get(f"{{{ns['rdf']}}}about")
                if about and get_local_name(about) not in stats['class_details']:
                    # This is likely an individual (already counted above, but double-check)
                    pass
    
    # Find AllDisjointClasses (disjoint class sets)
    for all_disjoint in root.findall('.//owl:AllDisjointClasses', ns):
        # Find the members collection
        members_elem = all_disjoint.find('./owl:members', ns)
        if members_elem is not None:
            # Count all owl:Class elements within the members collection
            member_classes = members_elem.findall('.//owl:Class', ns)
            num_members = len(member_classes)
            
            if num_members > 1:
                # For n classes that are all pairwise disjoint, there are n*(n-1)/2 relationships
                # But we'll count it as one disjoint set with n members
                # Add the pairwise relationships to the count
                pairwise_relationships = num_members * (num_members - 1) // 2
                stats['disjoint_classes'] += pairwise_relationships
                
                # Also update class_details for each member
                for member_class in member_classes:
                    about = member_class.get(f"{{{ns['rdf']}}}about")
                    if about:
                        cls_id = get_local_name(about)
                        # Ensure this class is in class_details
                        if cls_id not in stats['class_details']:
                            stats['class_details'][cls_id] = {'subClassOf': [], 'disjointWith': []}
                        # Add all other members to disjointWith (avoid duplicates)
                        for other_member in member_classes:
                            other_about = other_member.get(f"{{{ns['rdf']}}}about")
                            if other_about and other_about != about:
                                other_id = get_local_name(other_about)
                                if other_id not in stats['class_details'][cls_id]['disjointWith']:
                                    stats['class_details'][cls_id]['disjointWith'].append(other_id)
    
    return stats


def parse_sparql_statistics(sparql_text: str) -> Dict:
    """
    Parse SPARQL queries to extract statistics.
    Returns a dictionary with counts and details.
    """
    stats = {
        'total_queries': 0,
        'total_select_variables': 0,
        'total_where_triples': 0,
        'query_details': [],  # List of dicts with per-query stats
    }
    
    # Split queries by "# SPARQL Query" markers or blank lines
    # First, try to split by "# SPARQL Query" markers
    query_blocks = re.split(r'# SPARQL Query \d+/\d+\s*\n', sparql_text)
    
    # If no markers found, try to split by multiple blank lines
    if len(query_blocks) == 1:
        query_blocks = re.split(r'\n\s*\n\s*\n', sparql_text)
    
    # If still one block, treat entire text as one query
    if len(query_blocks) == 1 and sparql_text.strip():
        query_blocks = [sparql_text]
    
    for query_text in query_blocks:
        query_text = query_text.strip()
        if not query_text:
            continue
        
        query_stats = {
            'select_variables': 0,
            'where_triples': 0,
        }
        
        # Count SELECT variables
        select_match = re.search(r'SELECT\s+(?:DISTINCT\s+)?(.*?)\s+WHERE', query_text, re.IGNORECASE | re.DOTALL)
        if select_match:
            select_vars = select_match.group(1).strip()
            # Count variables (words starting with ? or *)
            vars_list = re.findall(r'[?*]\w+', select_vars)
            query_stats['select_variables'] = len(vars_list)
            stats['total_select_variables'] += len(vars_list)
        
        # Count WHERE triples (basic triple patterns)
        # Look for patterns like ?var property ?var . or ?var property "literal" .
        where_match = re.search(r'WHERE\s*\{(.*?)\}', query_text, re.IGNORECASE | re.DOTALL)
        if where_match:
            where_clause = where_match.group(1)
            # Count triple patterns by looking for patterns ending with periods
            # A triple is: subject predicate object .
            # Subjects can be: ?var, :Class, <uri>
            # Predicates can be: :property, a, <uri>
            # Objects can be: ?var, :Class, "literal", <uri>
            # We'll count lines that have a period at the end (after optional semicolons)
            # Remove OPTIONAL blocks first to avoid double counting
            where_clause_no_optional = re.sub(r'OPTIONAL\s*\{[^}]*\}', '', where_clause, flags=re.IGNORECASE | re.DOTALL)
            # Count triple patterns - look for subject predicate object ending with period
            # Pattern: (subject) whitespace (predicate) whitespace (object) whitespace* period
            triples = re.findall(r'(?:[?<:][^\s{}]+|"[^"]*")\s+(?:[?<:][^\s{}]+|a)\s+(?:[?<:][^\s{}]+|"[^"]*")\s*\.', where_clause_no_optional)
            query_stats['where_triples'] = len(triples)
            stats['total_where_triples'] += len(triples)
        
        stats['query_details'].append(query_stats)
        stats['total_queries'] += 1
    
    return stats


def generate_ontology_summary_stats(face_file_path: pathlib.Path):
    """
    Generate ontology and SPARQL statistics from a UDDL .face file.
    """
    # Create temporary file for OWL output
    with tempfile.NamedTemporaryFile(mode='w', suffix='.owl', delete=False) as tmp_owl:
        tmp_owl_path = pathlib.Path(tmp_owl.name)
    
    try:
        # Capture stdout to get SPARQL queries
        old_stdout = sys.stdout
        sys.stdout = StringIO()
        
        try:
            # Generate OWL with individuals and SPARQL output
            uddl2owl(
                uddl_file=face_file_path,
                output_file=tmp_owl_path,
                generate_individuals=True,
                output_sparql=True
            )
        finally:
            sparql_output = sys.stdout.getvalue()
            sys.stdout = old_stdout
        
        # Parse OWL statistics
        owl_stats = parse_owl_statistics(tmp_owl_path)
        
        # Parse SPARQL statistics
        sparql_stats = parse_sparql_statistics(sparql_output)
        
    finally:
        # Clean up temporary file
        try:
            tmp_owl_path.unlink()
        except:
            pass
    
    # Find most used property (by domain/range usage)
    most_used_property = None
    max_property_usage = -1
    
    for prop_name, prop_details in owl_stats['property_details'].items():
        usage = 0
        if prop_details['domain']:
            usage += 1
        if prop_details['range']:
            usage += 1
        if prop_details['inverse']:
            usage += 1
        
        if usage > max_property_usage:
            max_property_usage = usage
            most_used_property = prop_name
    
    # Find most complex query stats (for display)
    most_complex_query_stats = None
    if sparql_stats['query_details']:
        # Find query with most variables
        max_vars = -1
        for q_detail in sparql_stats['query_details']:
            if q_detail['select_variables'] > max_vars:
                max_vars = q_detail['select_variables']
                most_complex_query_stats = q_detail
    
    # Formatting helpers
    def escape_latex(text):
        if not text:
            return "N/A"
        return str(text).replace('_', '\\_').replace('#', '\\#').replace('$', '\\$').replace('%', '\\%')
    
    def format_list(items):
        if not items:
            return "None"
        escaped = [escape_latex(item) for item in items]
        return ", ".join(escaped[:5])  # Limit to 5 items
        if len(escaped) > 5:
            return ", ".join(escaped[:5]) + ", ..."
        return ", ".join(escaped)
    
    # Prepare display strings
    most_used_prop_display = escape_latex(most_used_property) if most_used_property else "N/A"
    
    # Get domain/range info for most used property
    domain_range_info = "N/A"
    if most_used_property and most_used_property in owl_stats['property_details']:
        prop_details = owl_stats['property_details'][most_used_property]
        parts = []
        if prop_details['domain']:
            parts.append(f"Domain: {escape_latex(prop_details['domain'])}")
        if prop_details['range']:
            parts.append(f"Range: {escape_latex(prop_details['range'])}")
        if prop_details['inverse']:
            parts.append(f"Inverse: {escape_latex(prop_details['inverse'])}")
        domain_range_info = "; ".join(parts) if parts else "N/A"
    
    # Calculate additional query statistics
    avg_variables_per_query = 0
    avg_triples_per_query = 0
    max_variables_in_query = 0
    max_triples_in_query = 0
    
    if sparql_stats['total_queries'] > 0:
        avg_variables_per_query = sparql_stats['total_select_variables'] / sparql_stats['total_queries']
        avg_triples_per_query = sparql_stats['total_where_triples'] / sparql_stats['total_queries']
        
        for q_detail in sparql_stats['query_details']:
            if q_detail['select_variables'] > max_variables_in_query:
                max_variables_in_query = q_detail['select_variables']
            if q_detail['where_triples'] > max_triples_in_query:
                max_triples_in_query = q_detail['where_triples']
    
    # Generate LaTeX Table
    script_name = pathlib.Path(__file__).name
    latex_table = f"""
% Generated by {script_name}
% Arguments: face_file={face_file_path}
\\begingroup
    \\centering
    \\begin{{table*}}[t]
    \\renewcommand{{\\arraystretch}}{{1.5}}
    \\begin{{tabularx}}{{\\textwidth}}{{@{{}} >{{\\sffamily\\raggedright\\arraybackslash}}p{{0.35\\textwidth}} >{{\\sffamily\\raggedright\\arraybackslash}}X @{{}}}}
        \\rowcolor{{tableheader}}
        \\headingfont\\bfseries Ontology Statistics & \\headingfont\\bfseries SPARQL Query Statistics \\\\
        \\addlinespace[4pt]
        % Left Column: Ontology Statistics
        \\begin{{tabular}}[t]{{@{{}}l r@{{}}}}
            Classes: & {owl_stats['classes']} \\\\
            Object Properties: & {owl_stats['object_properties']} \\\\
            Individuals: & {owl_stats['individuals']} \\\\
            Subclass Relations: & {owl_stats['subclass_relations']} \\\\
            Property Domains: & {owl_stats['property_domains']} \\\\
            Property Ranges: & {owl_stats['property_ranges']} \\\\
            Inverse Properties: & {owl_stats['inverse_properties']} \\\\
            Disjoint Classes: & {owl_stats['disjoint_classes']}
        \\end{{tabular}}
        &
        % Right Column: SPARQL Statistics
        \\begin{{tabular}}[t]{{@{{}}l r@{{}}}}
            Total Queries: & {sparql_stats['total_queries']} \\\\
            Total SELECT Variables: & {sparql_stats['total_select_variables']} \\\\
            Total WHERE Triples: & {sparql_stats['total_where_triples']}
        \\end{{tabular}} \\par\\vspace{{6pt}}
        
        \\textbf{{Most Used Property}}: {most_used_prop_display} \\newline
        \\textit{{Details}}: {domain_range_info} \\par\\vspace{{6pt}}
        
        \\textbf{{Query Complexity}}: \\newline
        \\textit{{Avg Variables/Query}}: {avg_variables_per_query:.1f} \\newline
        \\textit{{Avg Triples/Query}}: {avg_triples_per_query:.1f} \\newline
        \\textit{{Max Variables}}: {max_variables_in_query} \\newline
        \\textit{{Max Triples}}: {max_triples_in_query}
    \\end{{tabularx}}
    \\caption{{Summary statistics of the generated OWL ontology and SPARQL queries from the UDDL Conceptual Data Model. The table presents counts of ontology elements (classes, properties, individuals, and relationships) and SPARQL query characteristics (variables and triples). It identifies the most used property and provides query complexity metrics.}}
    \\label{{tab:ontology_sparql_summary}}
    \\end{{table*}}
\\endgroup
"""
    print(latex_table)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate a LaTeX summary table from ontology and SPARQL queries generated from a UDDL .face file."
    )
    
    # Check if the default example file exists
    default_face_file = pathlib.Path(__file__).parent / "examples" / "incose_uddl2owl.face"
    
    parser.add_argument(
        "face_file",
        nargs="?",  # Optional positional argument
        default=str(default_face_file) if default_face_file.exists() else None,
        help="Path to the .face XML file. Defaults to example file if not provided."
    )
    
    args = parser.parse_args()
    
    if args.face_file:
        generate_ontology_summary_stats(pathlib.Path(args.face_file))
    else:
        parser.print_help()
        print("\nError: No input file provided and default example not found.", file=sys.stderr)
        sys.exit(1)
