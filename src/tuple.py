from typing import Union
from participant_path_parser import ParticipantPath


class UddlTuple:
    def __init__(self, subject: str, predicate: str, object: Union[str, ParticipantPath], rolename: str, multiplicity=None):
        self.subject = subject
        self.predicate = predicate
        self.object = object
        self.rolename = rolename
        self.multiplicity = multiplicity

    def __str__(self) -> str:
        if isinstance(self.multiplicity, list):
            if len(self.multiplicity) == 4:
                 return f"{self.subject} {self.predicate}[{self.multiplicity[0]}, {self.multiplicity[1]}][{self.multiplicity[2]}, {self.multiplicity[3]}] {self.object} as {self.rolename}"
            elif len(self.multiplicity) == 2:
                return f"{self.subject} {self.predicate}[{self.multiplicity[0]}, {self.multiplicity[1]}] {self.object} as {self.rolename}"
            else:
                return f"{self.subject} {self.predicate}{self.multiplicity} {self.object} as {self.rolename}"
        else:
            return f"{self.subject} {self.predicate} {self.multiplicity} {self.object} {self.rolename}"

    def __repr__(self) -> str:
        return f"UddlTuple(subject={self.subject}, predicate={self.predicate}, object={self.object}, rolename={self.rolename}, multiplicity={self.multiplicity})"
