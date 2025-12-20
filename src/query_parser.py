import re
from typing import Optional

from tuple import Query, SelectClause, FromClause, JoinClause


def parse_query(query_text: str) -> Optional[Query]:
    """
    Parse a query string that starts with SELECT and ends with a semicolon.
    
    Args:
        query_text: The query string (may be multi-line)
    
    Returns:
        Query object if parsing succeeds, None otherwise
    """
    # Remove trailing semicolon and whitespace
    query_text = query_text.rstrip(';').strip()
    
    # Parse SELECT clause
    select_match = re.match(r'SELECT\s+(.+)', query_text, re.IGNORECASE | re.DOTALL)
    if not select_match:
        return None
    
    select_part = select_match.group(1).strip()
    # Find where SELECT clause ends (before FROM)
    from_pos = re.search(r'\bFROM\b', select_part, re.IGNORECASE)
    if from_pos:
        select_part = select_part[:from_pos.start()].strip()
    
    if select_part == '*':
        columns = ['*']
    else:
        columns = [col.strip() for col in select_part.split(',')]
    select_clause = SelectClause(columns)
    
    # Parse FROM clause
    from_match = re.search(r'FROM\s+(\w+)', query_text, re.IGNORECASE)
    if not from_match:
        return None
    
    from_table = from_match.group(1).strip()
    from_clause = FromClause([from_table])
    
    # Parse JOIN ON clause (optional)
    joins = []
    join_match = re.search(r'JOIN\s+ON\s+([\w.]+)', query_text, re.IGNORECASE)
    if join_match:
        join_condition = join_match.group(1).strip()
        # Extract table and condition (e.g., "Divorce.husband" -> table="Divorce", condition="husband")
        if '.' in join_condition:
            table, condition = join_condition.split('.', 1)
            joins.append(JoinClause(table, condition))
        else:
            joins.append(JoinClause("", join_condition))
    
    return Query(select_clause, from_clause, joins)

