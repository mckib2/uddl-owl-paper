from typing import List, Union
import pathlib

import re

from tuple import UddlTuple
from query_parser import get_ast as get_query_ast, QueryStatement
from participant_path_parser import ParticipantPath


def parse_tuple(tuple_file: pathlib.Path) -> List[Union[UddlTuple, QueryStatement]]:
    tuples = []
    with open(tuple_file, 'r') as file:
        query_buffer = []
        in_query = False
        query_start_line = 0
        
        for line_num, line in enumerate(file, 1):
            line_stripped = line.strip()
            
            # Skip empty lines (unless we're in a query)
            if not line_stripped:
                if in_query:
                    query_buffer.append(line)
                continue
            
            # Check if we're starting a new query
            if line_stripped.startswith('SELECT'):
                in_query = True
                query_start_line = line_num
                query_buffer = [line]
                # Check if query ends on same line
                if ';' in line:
                    query_text = ''.join(query_buffer).strip()
                    query = get_query_ast(query_string=query_text)
                    if query:
                        tuples.append(query)
                    else:
                        print(f"Warning: Failed to parse query at line {query_start_line}")
                    query_buffer = []
                    in_query = False
                continue
            
            # If we're in a query, accumulate lines
            if in_query:
                query_buffer.append(line)
                # Check if query ends with semicolon
                if ';' in line:
                    query_text = ''.join(query_buffer).strip()
                    query = get_query_ast(query_string=query_text)
                    if query:
                        tuples.append(query)
                    else:
                        print(f"Warning: Failed to parse query starting at line {query_start_line}")
                    query_buffer = []
                    in_query = False
                continue
            
            # Parse tuple: (Subject, Predicate[Multiplicity], Object,) or (Subject, Predicate, Object, Rolename)
            if line_stripped.startswith('(') and line_stripped.endswith(')'):
                # Remove parentheses
                content = line_stripped[1:-1]

                parts = []
                current_part = []
                bracket_depth = 0
                
                for char in content:
                    if char == '[':
                        bracket_depth += 1
                        current_part.append(char)
                    elif char == ']':
                        bracket_depth -= 1
                        current_part.append(char)
                    elif char == ',' and bracket_depth == 0:
                        parts.append(''.join(current_part).strip())
                        current_part = []
                    else:
                        current_part.append(char)
                
                if current_part:
                    parts.append(''.join(current_part).strip())
                
                # Remove empty strings from trailing commas (often the last part is empty if line ends with ,)
                parts = [part for part in parts if part]
                
                if len(parts) >= 3:
                    subject = parts[0]
                    predicate_raw = parts[1]
                    object_ = parts[2]
                    rolename = parts[3] if len(parts) > 3 else ""
                    
                    # Parse predicate and multiplicity
                    multiplicity = None
                    predicate = predicate_raw
                    
                    mult_match = re.match(r'(\w+)\[(.*)\]', predicate_raw)
                    if mult_match:
                        predicate = mult_match.group(1)
                        mult_str = mult_match.group(2)
                        
                        if ',' in mult_str:
                            # associates[s, t]
                            s, t = [x.strip() for x in mult_str.split(',')]
                            multiplicity = [int(s), int(t)]
                        else:
                            # composes[n]
                            multiplicity = int(mult_str)
                    else:
                        # Defaults
                        if predicate == 'composes':
                            multiplicity = 1
                        elif predicate == 'associates':
                            multiplicity = [-1, -1]
                    
                    if predicate == 'associates':
                        # Parse object as ParticipantPath
                        try:
                            object_ = ParticipantPath.parse(object_)
                            # print(f"Parsed ParticipantPath: {object_.__repr__()}")
                        except ValueError as e:
                             print(f"Warning: Failed to parse ParticipantPath in 'associates' tuple at line {line_num}: {e}")
                             pass

                    if not rolename and isinstance(object_, str):
                        # Use the last part after a dot, if present
                        object_name = object_.split('.')[-1]
                        rolename = object_name[0].lower() + object_name[1:]
                    elif not rolename and isinstance(object_, ParticipantPath):
                         if object_.resolutions:
                              rolename = object_.resolutions[-1].rolename
                         else:
                              rolename = object_.start_type
                         
                         rolename = rolename[0].lower() + rolename[1:]

                    
                    # Ensure rolename is not overwritten if it was parsed as None but object_ exists
                    # (Wait, logic above handles if not rolename)
                    
                    tuples.append(UddlTuple(subject=subject, predicate=predicate, object=object_, rolename=rolename, multiplicity=multiplicity))
                else:
                     print(f"Warning: Malformed tuple at line {line_num}: {line_stripped}")
            else:
                 print(f"Warning: Unrecognized line format at line {line_num}: {line_stripped}")
    
    return tuples


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Parse a UDDL tuple file.")
    parser.add_argument("tuple_file", type=pathlib.Path, help="Path to the tuple file to parse")
    args = parser.parse_args()

    try:
        parsed_items = parse_tuple(args.tuple_file)
        for item in parsed_items:
            print(item)
    except FileNotFoundError:
        print(f"Error: File not found: {args.tuple_file}")
    except Exception as e:
        print(f"Error parsing file: {e}")
