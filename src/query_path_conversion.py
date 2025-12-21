from typing import List, Union
from collections import defaultdict

from participant_path_parser import EntityResolution, AssociationResolution, ParticipantPath
from revised_query_parser import (
    QueryStatement, FromClause, Entity, Join, Equivalence, Reference,
    ProjectedCharacteristic, AllCharacteristics, EntityWildcard
)


def query2path(query_ast: QueryStatement) -> List[ParticipantPath]:
    """
    Transforms a query AST into a list of ParticipantPath objects, 
    one for each projected characteristic.
    """
    from_clause = query_ast.from_clause
    entities_list = from_clause.entities
    if not entities_list:
        return []

    # root_info: (entity_name, alias)
    root_entity = entities_list[0]
    start_type = root_entity.name
    root_ref = root_entity.alias or root_entity.name

    # Map entity aliases/names to the list of Resolutions required to reach them
    # path_map[alias] = [Resolution, ...]
    path_map = {root_ref: []}
    
    # Trace JOINs to build resolution prefixes for all joined entities
    for join in from_clause.joins:
        target = join.target
        target_name = target.name
        target_alias = target.alias or target_name
        
        # Determine which side of the 'ON' criteria belongs to an already resolved entity
        rolename = None
        source_ref = None
        for cond in join.on:
            left, right = cond.left, cond.right
            # We look for a rolename on an entity we already know (the source)
            if left.entity in path_map:
                source_ref = left.entity
                rolename = left.characteristic
            elif right.entity in path_map:
                source_ref = right.entity
                rolename = right.characteristic
        
        if source_ref is not None and rolename:
            # If target name differs from rolename, it is an AssociationResolution
            if target_name != rolename:
                res = AssociationResolution(rolename, target_name)
            else:
                res = EntityResolution(rolename)
            
            path_map[target_alias] = path_map[source_ref] + [res]

    # Generate a ParticipantPath for every projected characteristic
    results = []
    for proj in query_ast.projections:
        if isinstance(proj, ProjectedCharacteristic):
            ref = proj.reference
            entity_ref = ref.entity or root_ref # Default to root if no entity prefix
            
            # resolutions = steps to reach entity + final characteristic
            resolutions = path_map.get(entity_ref, []).copy()
            resolutions.append(EntityResolution(ref.characteristic))
            
            results.append(ParticipantPath(start_type, resolutions))
            
        elif isinstance(proj, AllCharacteristics):
            # Wildcards represent a path ending in the root or a joined entity
            # Note: This logic seems to assume * applies to root. 
            # If strictly recreating previous logic:
            results.append(ParticipantPath(start_type, path_map[root_ref]))
        
        # EntityWildcard handling was missing in original code, ignoring here too.

    return results


def path2query(paths: List[ParticipantPath]) -> Union[QueryStatement, List[QueryStatement]]:
    """
    Transforms a list of ParticipantPath objects into one or more query ASTs.
    Paths starting with the same entity type are merged into a single query.
    """
    # Group paths by their start_type
    groups = defaultdict(list)
    for p in paths:
        groups[p.start_type].append(p)

    queries = []
    for start_type, path_list in groups.items():
        projections = []
        joins_registry = {} # key: path_tuple, value: Join object
        
        for p in path_list:
            # The last resolution is the projected characteristic
            if not p.resolutions:
                projections.append(AllCharacteristics())
                continue
                
            leaf_res = p.resolutions[-1]
            intermediate_res = p.resolutions[:-1]
            
            # Track current entity/alias as we build the join chain
            current_entity_ref = start_type
            
            # Process intermediate resolutions into JOINs
            for i, res in enumerate(intermediate_res):
                # Unique key for this specific navigation step to reuse JOINs
                step_key = tuple(p.resolutions[:i+1])
                
                if step_key not in joins_registry:
                    target_name = res.association_name if isinstance(res, AssociationResolution) else res.rolename
                    # Generate a unique alias if needed (omitted here for simplicity, using target_name)
                    joins_registry[step_key] = Join(
                        target=Entity(name=target_name, alias=None),
                        on=[Equivalence(
                            left=Reference(entity=current_entity_ref, characteristic=res.rolename),
                            right=Reference(entity=target_name, characteristic="id") # Simplified ID match
                        )]
                    )
                
                # Update current_entity_ref for next step. 
                # Since we didn't alias target (alias=None), we use its name.
                current_entity_ref = joins_registry[step_key].target.name

            # Add the projection
            projections.append(
                ProjectedCharacteristic(
                    reference=Reference(entity=current_entity_ref, characteristic=leaf_res.rolename),
                    alias=None
                )
            )

        # Assemble Query AST
        query_ast = QueryStatement(
            projections=projections,
            from_clause=FromClause(
                entities=[Entity(name=start_type, alias=None)],
                joins=list(joins_registry.values())
            ),
            qualifier=None
        )
        queries.append(query_ast)

    return queries[0] if len(queries) == 1 else queries


# Example usage to verify logic
if __name__ == "__main__":
    # Simulate a query: SELECT A.x, B.y FROM A JOIN B ON A.link = B
    # Recreate the sample_ast using classes
    
    sample_ast = QueryStatement(
        projections=[
            ProjectedCharacteristic(reference=Reference(entity="A", characteristic="x")),
            ProjectedCharacteristic(reference=Reference(entity="B", characteristic="y"))
        ],
        from_clause=FromClause(
            entities=[Entity(name="A", alias="A")],
            joins=[
                Join(
                    target=Entity(name="B", alias="B"),
                    on=[Equivalence(
                        left=Reference(entity="A", characteristic="link"),
                        right=Reference(entity="B", characteristic="id")
                    )]
                )
            ]
        ),
        qualifier=None
    )

    paths = query2path(sample_ast)
    print("Query to Paths:")
    for p in paths:
        print(f"  {p}")

    reconstructed_query = path2query(paths)
    if isinstance(reconstructed_query, list):
        reconstructed_query = reconstructed_query[0]
        
    print("\nPaths back to Query (Projections):")
    for proj in reconstructed_query.projections:
        if isinstance(proj, ProjectedCharacteristic):
            print(f"  {proj.reference}")
        else:
            print(f"  {proj}")
