import re


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
            ('ID',       r'[a-zA-Z][a-zA-Z0-9]*'),
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

    def parse(self):
        """Entry point for parsing the query_specification"""
        return self.parse_query_statement()

    def parse_query_statement(self):
        """Parses SELECT [ALL] projected_list from_clause"""
        self._consume('SELECT')
        
        # Optional ALL qualifier
        qualifier = None
        if self._peek()[0] == 'ALL':
            qualifier = self._consume('ALL')

        projected = self.parse_projected_list()
        from_clause = self.parse_from_clause()
        
        return {
            "type": "QueryStatement",
            "qualifier": qualifier,
            "projections": projected,
            "from": from_clause
        }

    def parse_projected_list(self):
        """Parses the projected_characteristic_list"""
        if self._peek()[0] == 'ASTERISK':
            self._consume('ASTERISK')
            return [{"type": "AllCharacteristics", "value": "*"}]
        
        projections = []
        while True:
            projections.append(self.parse_projected_expression())
            if self._peek()[0] == 'COMMA':
                self._consume('COMMA')
            else:
                break
        return projections

    def parse_projected_expression(self):
        """Parses projected characteristic expressions and wildcards"""
        first_id = self._consume('ID')
        
        if self._peek()[0] == 'PERIOD':
            self._consume('PERIOD')
            if self._peek()[0] == 'ASTERISK':
                self._consume('ASTERISK')
                return {"type": "EntityWildcard", "entity": first_id}
            else:
                char_name = self._consume('ID')
                ref = {"entity": first_id, "characteristic": char_name}
        else:
            ref = {"entity": None, "characteristic": first_id}

        # Optional AS alias
        alias = None
        if self._peek()[0] == 'AS':
            self._consume('AS')
            alias = self._consume('ID')
        elif self._peek()[0] == 'ID': # AS is optional
            alias = self._consume('ID')
            
        return {
            "type": "ProjectedCharacteristic",
            "reference": ref,
            "alias": alias
        }

    def parse_from_clause(self):
        """Parses the FROM clause and entity expressions"""
        self._consume('FROM')
        
        # Initial selected entity
        entities = [self.parse_selected_entity()]
        
        # Handle JOINs
        joins = []
        while self._peek()[0] == 'JOIN':
            joins.append(self.parse_join_expression())
            
        return {"entities": entities, "joins": joins}

    def parse_selected_entity(self):
        """Parses an entity type and its optional alias"""
        entity_type = self._consume('ID')
        alias = None
        if self._peek()[0] == 'AS':
            self._consume('AS')
            alias = self._consume('ID')
        return {"type": "Entity", "name": entity_type, "alias": alias}

    def parse_join_expression(self):
        """Parses JOIN join_entity ON criteria"""
        self._consume('JOIN')
        entity = self.parse_selected_entity()
        self._consume('ON')
        criteria = self.parse_join_criteria()
        return {"target": entity, "on": criteria}

    def parse_join_criteria(self):
        """Parses one or more equivalence expressions"""
        conditions = []
        while True:
            conditions.append(self.parse_equivalence_expression())
            if self._peek()[0] == 'AND':
                self._consume('AND')
            else:
                break
        return conditions

    def parse_equivalence_expression(self):
        """Parses characteristic equivalence (e.g., A.id = B.ref)"""
        left = self.parse_char_ref()
        self._consume('EQUALS')
        right = self.parse_char_ref()
        return {"left": left, "right": right}

    def parse_char_ref(self):
        """Parses [entity.]characteristic reference"""
        first = self._consume('ID')
        if self._peek()[0] == 'PERIOD':
            self._consume('PERIOD')
            second = self._consume('ID')
            return {"entity": first, "characteristic": second}
        return {"entity": None, "characteristic": first}


def get_ast(query_string):
    parser = UDDLQueryParser(query_string)
    return parser.parse()


if __name__ == "__main__":
    query = "SELECT EntityA.attr1 AS a, attr2 FROM EntityA JOIN EntityB ON EntityA.id = EntityB.ref_id"
    ast = get_ast(query)
    import json
    print(json.dumps(ast, indent=2))
