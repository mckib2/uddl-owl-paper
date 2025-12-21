import xml.etree.ElementTree as ET
from typing import List, Union

from tuple import UddlTuple
from query_parser import QueryStatement


def uddl2tuple(uddl_doc: ET.ElementTree) -> List[Union[UddlTuple, QueryStatement]]:
    pass
