import re
from dataclasses import dataclass
from typing import List, Optional, Union


@dataclass(frozen=True)
class Reference:
    entity: Optional[str]
    characteristic: Optional[str]  # None indicates an Entity Identity reference

    def __str__(self) -> str:
        if self.entity and self.characteristic:
             return f"{self.entity}.{self.characteristic}"
        return self.entity or self.characteristic or ""


@dataclass(frozen=True)
class ProjectedCharacteristic:
    reference: Reference
    alias: Optional[str] = None

    def __str__(self) -> str:
        return f"{self.reference}{f' AS {self.alias}' if self.alias else ''}"


@dataclass(frozen=True)
class AllCharacteristics:
    def __str__(self) -> str:
        return "*"


@dataclass(frozen=True)
class EntityWildcard:
    entity: str

    def __str__(self) -> str:
        return f"{self.entity}.*"


Projection = Union[ProjectedCharacteristic, AllCharacteristics, EntityWildcard]


@dataclass(frozen=True)
class Entity:
    name: str
    alias: Optional[str] = None

    def __str__(self) -> str:
        if self.alias and self.alias != self.name:
            return f"{self.name} AS {self.alias}"
        return self.name


@dataclass(frozen=True)
class Equivalence:
    left: Reference
    right: Optional[Reference] = None

    def __str__(self) -> str:
        if self.right:
            return f"{self.left} = {self.right}"
        return str(self.left)


@dataclass(frozen=True)
class Join:
    target: Entity
    on: List[Equivalence]

    def __str__(self) -> str:
        return f"JOIN {self.target} ON {' AND '.join(str(e) for e in self.on)}"


@dataclass(frozen=True)
class FromClause:
    entities: List[Entity]
    joins: List[Join]

    def __str__(self) -> str:
        res = f"FROM {', '.join(str(e) for e in self.entities)}"
        if self.joins:
            res += f" {' '.join(str(j) for j in self.joins)}"
        return res


@dataclass(frozen=True)
class QueryStatement:
    projections: List[Projection]
    from_clause: FromClause
    qualifier: Optional[str] = None

    def __str__(self) -> str:
        q = f" {self.qualifier}" if self.qualifier else ""
        return f"SELECT{q} {', '.join(str(p) for p in self.projections)} {self.from_clause}"

    def pretty_print(self, indent: int = 0) -> str:
        """Pretty-prints the query with readable indentation."""
        indent_str = " " * indent
        lines = []
        
        # SELECT clause
        select_parts = ["SELECT"]
        if self.qualifier:
            select_parts.append(self.qualifier)
        select_base = " ".join(select_parts)
        
        # Projections with proper indentation
        if len(self.projections) == 1:
            lines.append(f"{select_base} {str(self.projections[0])}")
        else:
            # Multi-line projections
            for i, proj in enumerate(self.projections):
                if i == 0:
                    lines.append(f"{select_base} {proj},")
                else:
                    prefix = " " * (len(select_base) + 1)
                    suffix = "," if i < len(self.projections) - 1 else ""
                    lines.append(f"{prefix}{proj}{suffix}")
        
        # FROM clause
        from_line = f"FROM {', '.join(str(e) for e in self.from_clause.entities)}"
        lines.append(from_line)
        
        # JOIN clauses with indentation
        for join in self.from_clause.joins:
            join_line = f"JOIN {join.target}"
            lines.append(join_line)
            
            # ON clause with conditions
            if join.on:
                for i, eq in enumerate(join.on):
                    if i == 0:
                        lines.append(f"    ON {eq}")
                    else:
                        lines.append(f"    AND {eq}")
        
        return "\n".join(f"{indent_str}{line}" for line in lines)


class UDDLQueryParser:
    def __init__(self, query_text):
        self.tokens = self._tokenize(query_text)
        self.pos = 0

    def _tokenize(self, text):
        token_specification = [
            ('SELECT',   r'\bSELECT\b'),
            ('FROM',     r'\bFROM\b'),
            ('JOIN',     r'\bJOIN\b'),
            ('ON',       r'\bON\b'),
            ('AND',      r'\bAND\b'),
            ('AS',       r'\bAS\b'),
            ('ALL',      r'\bALL\b'),
            ('ID',       r'[a-zA-Z_][a-zA-Z0-9_]*'),
            ('ASTERISK', r'\*'),
            ('PERIOD',   r'\.'),
            ('COMMA',    r','),
            ('EQUALS',   r'='),
            ('SKIP',     r'[ \t\n]+'),
        ]
        tok_regex = '|'.join('(?P<%s>%s)' % pair for pair in token_specification)
        tokens = []
        for mo in re.finditer(tok_regex, text, re.IGNORECASE):
            kind = mo.lastgroup
            if kind != 'SKIP':
                tokens.append((kind, mo.group()))
        return tokens

    def _peek(self, offset=0):
        idx = self.pos + offset
        return self.tokens[idx] if idx < len(self.tokens) else (None, None)

    def _consume(self, expected_kind=None):
        kind, value = self._peek()
        if expected_kind and kind != expected_kind:
            raise SyntaxError(f"Expected {expected_kind}, got {kind}")
        self.pos += 1
        return value

    def parse(self) -> QueryStatement:
        self._consume('SELECT')
        qualifier = self._consume('ALL') if self._peek()[0] == 'ALL' else None
        projected = self.parse_projected_list()
        from_clause = self.parse_from_clause()
        return QueryStatement(qualifier=qualifier, projections=projected, from_clause=from_clause)

    def parse_projected_list(self) -> List[Projection]:
        if self._peek()[0] == 'ASTERISK':
            self._consume('ASTERISK')
            return [AllCharacteristics()]
        projections = []
        while True:
            projections.append(self.parse_projected_expression())
            if self._peek()[0] == 'COMMA':
                self._consume('COMMA')
            else:
                break
        return projections

    def parse_projected_expression(self) -> Projection:
        first_id = self._consume('ID')
        if self._peek()[0] == 'PERIOD':
            self._consume('PERIOD')
            if self._peek()[0] == 'ASTERISK':
                self._consume('ASTERISK')
                return EntityWildcard(entity=first_id)
            char_name = self._consume('ID')
            ref = Reference(entity=first_id, characteristic=char_name)
        else:
            # In SELECT, a standalone ID is a characteristic of the root entity
            ref = Reference(entity=None, characteristic=first_id)

        alias = None
        if self._peek()[0] == 'AS':
            self._consume('AS')
            alias = self._consume('ID')
        elif self._peek()[0] == 'ID' and self._peek()[0] not in ('FROM', 'JOIN'):
            alias = self._consume('ID')
        return ProjectedCharacteristic(reference=ref, alias=alias)

    def parse_from_clause(self) -> FromClause:
        self._consume('FROM')
        entities = [self.parse_selected_entity()]
        joins = []
        while self._peek()[0] == 'JOIN':
            joins.append(self.parse_join_expression())
        return FromClause(entities=entities, joins=joins)

    def parse_selected_entity(self) -> Entity:
        name = self._consume('ID')
        alias = None
        if self._peek()[0] == 'AS':
            self._consume('AS')
            alias = self._consume('ID')
        elif self._peek()[0] == 'ID' and self._peek()[0] not in ('JOIN', 'SELECT', 'FROM'):
            alias = self._consume('ID')
        return Entity(name=name, alias=alias)

    def parse_join_expression(self) -> Join:
        self._consume('JOIN')
        entity = self.parse_selected_entity()
        self._consume('ON')
        return Join(target=entity, on=self.parse_join_criteria())

    def parse_join_criteria(self) -> List[Equivalence]:
        conditions = []
        while True:
            conditions.append(self.parse_equivalence_expression())
            if self._peek()[0] == 'AND':
                self._consume('AND')
            else:
                break
        return conditions

    def parse_equivalence_expression(self) -> Equivalence:
        left = self.parse_operand()
        right = None
        if self._peek()[0] == 'EQUALS':
            self._consume('EQUALS')
            right = self.parse_operand()
        return Equivalence(left=left, right=right)

    def parse_operand(self) -> Reference:
        first = self._consume('ID')
        if self._peek()[0] == 'PERIOD':
            self._consume('PERIOD')
            return Reference(entity=first, characteristic=self._consume('ID'))
        # In JOIN, a standalone ID is an Entity Alias (Identity)
        return Reference(entity=first, characteristic=None)


def get_ast(query_string):
    return UDDLQueryParser(query_string).parse()


if __name__ == "__main__":
    import argparse
    import sys

    parser = argparse.ArgumentParser(description="Parse and pretty-print a UDDL query.")
    parser.add_argument("query", help="The UDDL query string to parse")

    args = parser.parse_args()

    try:
        ast = get_ast(args.query)
        print(ast.pretty_print())
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
