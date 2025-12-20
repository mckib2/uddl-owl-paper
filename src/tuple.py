from typing import List, Union
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
            return f"{self.subject} {self.predicate}[{self.multiplicity[0]}, {self.multiplicity[1]}] {self.object} as {self.rolename}"
        else:
            return f"{self.subject} {self.predicate} {self.multiplicity} {self.object} {self.rolename}"

    def __repr__(self) -> str:
        return f"UddlTuple(subject={self.subject}, predicate={self.predicate}, object={self.object}, rolename={self.rolename}, multiplicity={self.multiplicity})"


class SelectClause:
    def __init__(self, columns: List[str]):
        self.columns = columns

    def __str__(self) -> str:
        return f"{self.columns}"

    def __repr__(self) -> str:
        return f"SelectClause(columns={self.columns})"


class FromClause:
    def __init__(self, tables: List[str]):
        self.tables = tables

    def __str__(self) -> str:
        return f"{self.tables}"

    def __repr__(self) -> str:
        return f"FromClause(tables={self.tables})"


class JoinClause:
    def __init__(self, table: str, condition: str):
        self.table = table
        self.condition = condition

    def __str__(self) -> str:
        return f"{self.table} {self.condition}"

    def __repr__(self) -> str:
        return f"JoinClause(table={self.table}, condition={self.condition})"


class Query:
    def __init__(self, select: SelectClause, from_: FromClause, joins: List[JoinClause]):
        self.select = select
        self.from_ = from_
        self.joins = joins

    def __str__(self) -> str:
        return f"{self.select} {self.from_} {self.joins}"

    def __repr__(self) -> str:
        return f"Query(select={self.select}, from_={self.from_}, joins={self.joins})"
