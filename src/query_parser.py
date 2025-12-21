import re
from dataclasses import dataclass
from typing import List, Optional, Union


@dataclass(frozen=True)
class Reference:
    entity: Optional[str]
    characteristic: str


@dataclass(frozen=True)
class ProjectedCharacteristic:
    reference: Reference
    alias: Optional[str] = None


@dataclass(frozen=True)
class AllCharacteristics:
    pass


@dataclass(frozen=True)
class EntityWildcard:
    entity: str


Projection = Union[ProjectedCharacteristic, AllCharacteristics, EntityWildcard]


@dataclass(frozen=True)
class Entity:
    name: str
    alias: Optional[str] = None


@dataclass(frozen=True)
class Equivalence:
    left: Reference
    right: Reference


@dataclass(frozen=True)
class Join:
    target: Entity
    on: List[Equivalence]


@dataclass(frozen=True)
class FromClause:
    entities: List[Entity]
    joins: List[Join]


@dataclass(frozen=True)
class QueryStatement:
    projections: List[Projection]
    from_clause: FromClause
    qualifier: Optional[str] = None


class UDDLQueryParser:
    def __init__(self, query_text):
        self.tokens = self._tokenize(query_text)
        self.pos = 0

    def _tokenize(self, text):
        # Token definitions from the UDDL specification pages 68--69
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

    def _peek(self):
        return self.tokens[self.pos] if self.pos < len(self.tokens) else (None, None)

    def _consume(self, expected_kind=None):
        kind, value = self._peek()
        if expected_kind and kind != expected_kind:
            raise SyntaxError(f"Expected {expected_kind}, got {kind}")
        self.pos += 1
        return value

    def parse(self) -> QueryStatement:
        """Entry point for parsing the query_specification"""
        return self.parse_query_statement()

    def parse_query_statement(self) -> QueryStatement:
        """Parses SELECT [ALL] projected_list from_clause"""
        self._consume('SELECT')
        
        # Optional ALL qualifier
        qualifier = None
        if self._peek()[0] == 'ALL':
            qualifier = self._consume('ALL')

        projected = self.parse_projected_list()
        from_clause = self.parse_from_clause()
        
        return QueryStatement(
            qualifier=qualifier,
            projections=projected,
            from_clause=from_clause
        )

    def parse_projected_list(self) -> List[Projection]:
        """Parses the projected_characteristic_list"""
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
        """Parses projected characteristic expressions and wildcards"""
        first_id = self._consume('ID')
        
        if self._peek()[0] == 'PERIOD':
            self._consume('PERIOD')
            if self._peek()[0] == 'ASTERISK':
                self._consume('ASTERISK')
                return EntityWildcard(entity=first_id)
            else:
                char_name = self._consume('ID')
                ref = Reference(entity=first_id, characteristic=char_name)
        else:
            ref = Reference(entity=None, characteristic=first_id)

        # Optional AS alias
        alias = None
        if self._peek()[0] == 'AS':
            self._consume('AS')
            alias = self._consume('ID')
        elif self._peek()[0] == 'ID': # AS is optional
            alias = self._consume('ID')
            
        return ProjectedCharacteristic(reference=ref, alias=alias)

    def parse_from_clause(self) -> FromClause:
        """Parses the FROM clause and entity expressions"""
        self._consume('FROM')
        
        # Initial selected entity
        entities = [self.parse_selected_entity()]
        
        # Handle JOINs
        joins = []
        while self._peek()[0] == 'JOIN':
            joins.append(self.parse_join_expression())
            
        return FromClause(entities=entities, joins=joins)

    def parse_selected_entity(self) -> Entity:
        """Parses an entity type and its optional alias"""
        entity_type = self._consume('ID')
        alias = None
        if self._peek()[0] == 'AS':
            self._consume('AS')
            alias = self._consume('ID')
        return Entity(name=entity_type, alias=alias)

    def parse_join_expression(self) -> Join:
        """Parses JOIN join_entity ON criteria"""
        self._consume('JOIN')
        entity = self.parse_selected_entity()
        self._consume('ON')
        criteria = self.parse_join_criteria()
        return Join(target=entity, on=criteria)

    def parse_join_criteria(self) -> List[Equivalence]:
        """Parses one or more equivalence expressions"""
        conditions = []
        while True:
            conditions.append(self.parse_equivalence_expression())
            if self._peek()[0] == 'AND':
                self._consume('AND')
            else:
                break
        return conditions

    def parse_equivalence_expression(self) -> Equivalence:
        """Parses characteristic equivalence (e.g., A.id = B.ref)"""
        left = self.parse_char_ref()
        self._consume('EQUALS')
        right = self.parse_char_ref()
        return Equivalence(left=left, right=right)

    def parse_char_ref(self) -> Reference:
        """Parses [entity.]characteristic reference"""
        first = self._consume('ID')
        if self._peek()[0] == 'PERIOD':
            self._consume('PERIOD')
            second = self._consume('ID')
            return Reference(entity=first, characteristic=second)
        return Reference(entity=None, characteristic=first)


def get_ast(query_string):
    parser = UDDLQueryParser(query_string)
    return parser.parse()


if __name__ == "__main__":
    query = "SELECT EntityA.attr1 AS a, attr2 FROM EntityA JOIN EntityB ON EntityA.id = EntityB.ref_id"
    ast = get_ast(query)
    print(ast)
