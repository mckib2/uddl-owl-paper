import sys
import argparse
import pathlib
from typing import List, Union, Set, Dict, Optional

# Assuming these are in the same directory (src/)
from parse_tuple import parse_tuple
from tuple import UddlTuple
from participant_path_parser import ParticipantPath, EntityResolution, AssociationResolution

def resolve_participant_path_to_type(path: ParticipantPath, class_tuples: List[UddlTuple], parent_map: Dict[str, Set[str]]) -> str:
    """Resolve a participant path to its final type."""
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
        source_type = resolve_participant_path_to_type(parent_path, class_tuples, parent_map)
    
    # Look up the rolename in the model (check both composes and associates)
    # Check source_type and its ancestors
    queue = [source_type]
    visited = {source_type}
    
    while queue:
        current = queue.pop(0)
        
        for t in class_tuples:
            if t.subject == current and t.rolename == last.rolename:
                if isinstance(t.object, str):
                    # Handle cases where object is "Type" or "Type.property"
                    return t.object.split('.')[0] if '.' in t.object else t.object
                elif isinstance(t.object, ParticipantPath):
                    return resolve_participant_path_to_type(t.object, class_tuples, parent_map)
        
        # Add parents to queue
        if current in parent_map:
            for p in parent_map[current]:
                if p not in visited:
                    visited.add(p)
                    queue.append(p)
    
    # Fallback to rolename if not found
    return last.rolename

def generate_mermaid(tuples: List[UddlTuple]) -> str:
    # Separate instance tuples (individuals) from class-defining tuples
    # User request: "We don't need individuals, so you can leave out instances."
    class_tuples = [t for t in tuples if t.predicate != 'instance']
    
    subjects = set()
    objects = set()
    association_subjects = set()

    for t in class_tuples:
        subjects.add(t.subject)
        if t.predicate == 'associates':
            association_subjects.add(t.subject)
        if isinstance(t.object, str):
            objects.add(t.object)

    associations = association_subjects
    entities = subjects - associations
    observables = objects - subjects
    
    # Build Inheritance Map
    parent_map = {} # child -> set(parents)
    for t in class_tuples:
        if t.predicate == 'specializes' and isinstance(t.object, str):
            if t.subject not in parent_map:
                parent_map[t.subject] = set()
            parent_map[t.subject].add(t.object)

    # Prepare mermaid lines
    lines = ["graph LR"]
    
    # Add class definitions for styling
    lines.append("    classDef observable fill:#ffffff,stroke:#000000,stroke-width:1px,color:#000000;")
    lines.append("    classDef entity fill:#ffffff,stroke:#000000,stroke-width:2px,color:#000000;")
    
    # Identify compositions
    # We want to distinguish between Entity/Association vs Observable in compositions
    # Observable members become nodes inside the Entity/Association subgraph
    # Entity members become external nodes connected by an edge
    
    # Map: container -> list of (type, name, is_observable)
    composition_map = {}
    
    for t in class_tuples:
        if t.predicate == 'composes':
            container = t.subject
            type_name = str(t.object)
            name = t.rolename
            
            is_observable = type_name in observables
            
            if container not in composition_map:
                composition_map[container] = []
            
            composition_map[container].append({
                'type': type_name,
                'name': name,
                'is_observable': is_observable
            })

    # Generate Subgraphs for Entities and Associations
    # We iterate over all subjects (Entities and Associations)
    all_containers = entities | associations
    
    # Track which nodes we've defined to avoid redundancy if possible, 
    # though in Mermaid graph multiple defs are okay.
    
    for container in sorted(all_containers):
        # User request: "Just reference the name of the subgraph... Don't create a hidden node"
        # We will use the container name as the Subgraph ID.
        # Edges will reference this container name as the node.
        # Implicitly, Mermaid will treat this as a node with ID = container.
        # This node will likely render inside the subgraph or AS the subgraph node.
        
        lines.append(f"    subgraph {container}")
        lines.append(f"    direction TB")
        
        # No explicit node creation here.
        # Mermaid will auto-create a node if referenced in edges.
        
        # Add composed Observables as nodes inside the subgraph
        if container in composition_map:
            for member in composition_map[container]:
                if member['is_observable']:
                    member_type = member['type']
                    member_name = member['name']
                    # Create a unique ID for this member node
                    member_id = f"{container}_{member_name}"
                    # Label: "Type name" or just "Type"
                    label = f"{member_type} {member_name}" if member_name else member_type
                    lines.append(f"    {member_id}[{label}]:::observable") # Rectangular shape for observables
        
        lines.append("    end")
        lines.append(f"    style {container} fill:#ffffff,stroke:#000000,stroke-width:2px,color:#000000")
        
    # Generate Edges
    
    # 1. Inheritance
    for child, parents in parent_map.items():
        for parent in parents:
            # Use standard arrow with label
            # Use container name directly as node ID
            lines.append(f"    {child} -->|specializes| {parent}")

    # 2. Entity Compositions (composed Entities)
    # "If there is a composed Entity, then we should draw a 'composes' edge"
    for container in composition_map:
        for member in composition_map[container]:
            if not member['is_observable']:
                # It's an Entity or Association
                target_type = member['type']
                member_name = member['name']
                # Edge label: "composes (name)"
                label = f"composes {member_name}" if member_name else "composes"
                lines.append(f"    {container} -->|{label}| {target_type}")

    # 3. Associations
    for t in class_tuples:
        if t.predicate == 'associates':
            subject = t.subject
            
            # Resolve target
            target_type = None
            path_str = ""
            
            if isinstance(t.object, ParticipantPath):
                target_type = t.object.start_type
                # Replace start type with rolename in path string
                resolutions_str = "".join(str(res) for res in t.object.resolutions)
                path_str = f"{t.rolename}{resolutions_str}"
            elif isinstance(t.object, str):
                if '.' in t.object:
                    target_type = t.object.split('.')[0]
                    # Attempt to replace start type with rolename
                    parts = t.object.split('.', 1)
                    path_str = f"{t.rolename}.{parts[1]}"
                elif '->' in t.object:
                    target_type = t.object.split('->')[0]
                    path_str = str(t.object)
                else:
                    target_type = t.object
                    path_str = t.rolename
            
            if target_type:
                # Add edge
                # Label is the path string
                lines.append(f"    {subject} -->|{path_str}| {target_type}")
                
    return "\n".join(lines)

def main():
    parser = argparse.ArgumentParser(description="Generate Mermaid diagram from tuple file.")
    parser.add_argument("tuple_file", type=pathlib.Path, help="Input tuple file")
    parser.add_argument("--output", "-o", type=pathlib.Path, help="Output .mmd file (default: stdout)")
    
    args = parser.parse_args()
    
    try:
        tuples = parse_tuple(args.tuple_file)
        # Filter strictly for UddlTuple, ignore queries
        uddl_tuples = [t for t in tuples if isinstance(t, UddlTuple)]
        
        mermaid_code = generate_mermaid(uddl_tuples)
        
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
