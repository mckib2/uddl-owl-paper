import re
from typing import List, Union
from dataclasses import dataclass


@dataclass
class EntityResolution:
    rolename: str

    def __str__(self) -> str:
        return f".{self.rolename}"


@dataclass
class AssociationResolution:
    rolename: str
    association_name: str

    def __str__(self) -> str:
        return f"->{self.rolename}[{self.association_name}]"


Resolution = Union[EntityResolution, AssociationResolution]


class ParticipantPath:
    def __init__(self, start_type: str, resolutions: List[Resolution]):
        self.start_type = start_type
        self.resolutions = resolutions

    @staticmethod
    def parse(text: str) -> 'ParticipantPath':
        resolutions = []
        pos = 0
        n = len(text)
        
        # Regex definitions according to grammar
        # identifier = ( alphabetic character | "_" ), { alphabetic character | digit | "_" }
        identifier_pattern = r'[a-zA-Z_][a-zA-Z0-9_]*'
        
        # Parse start_type
        start_type_match = re.match(f'^({identifier_pattern})', text)
        if not start_type_match:
             raise ValueError("Input string must start with a valid identifier (start_type).")
        
        start_type = start_type_match.group(1)
        pos = start_type_match.end()
        
        # compiled regexes for performance
        # Entity resolution: "." rolename
        entity_re = re.compile(rf'\.({identifier_pattern})')
        
        # Association resolution: "->" rolename "[" association_name "]"
        assoc_re = re.compile(rf'->({identifier_pattern})\[({identifier_pattern})\]')
        
        while pos < n:
            if text.startswith('->', pos):
                match = assoc_re.match(text, pos)
                if not match:
                    raise ValueError(f"Invalid association resolution starting at index {pos}. Expected ->rolename[assoc_name]")
                
                rolename = match.group(1)
                assoc_name = match.group(2)
                resolutions.append(AssociationResolution(rolename, assoc_name))
                pos = match.end()
                
            elif text.startswith('.', pos):
                match = entity_re.match(text, pos)
                if not match:
                    raise ValueError(f"Invalid entity resolution starting at index {pos}. Expected .rolename")
                
                rolename = match.group(1)
                resolutions.append(EntityResolution(rolename))
                pos = match.end()
                
            else:
                raise ValueError(f"Unexpected character at index {pos}: '{text[pos]}'. Expected '.' or '->'")
        
        if not resolutions:
             # If no resolutions are found, it's valid if we just have a start_type
             # However, we must check if there were leftover characters that were invalid
             if pos < n:
                  raise ValueError(f"Unexpected character at index {pos}: '{text[pos]}'. Expected '.' or '->' or end of string.")

        return ParticipantPath(start_type, resolutions)

    def __str__(self) -> str:
        return f"{self.start_type}" + "".join(str(res) for res in self.resolutions)

    def __repr__(self) -> str:
        return f"ParticipantPath(start_type='{self.start_type}', resolutions={self.resolutions})"
