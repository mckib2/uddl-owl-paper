import pathlib

import pytest

from query_path_conversion import load_model, _reconstruct_alias_map, path2query, ParticipantPath, get_ast


@pytest.fixture(scope="module")
def gordon_model():
    return load_model(pathlib.Path(__file__).parent.parent / "examples/gordon.txt")


def normalize_query(q_str):
    # Simple normalization by removing extra whitespace
    return " ".join(q_str.split())


def check_query_equivalence(generated_ast, expected_ast):
    # Compare Projections
    gen_proj = sorted([str(p) for p in generated_ast.projections])
    exp_proj = sorted([str(p) for p in expected_ast.projections])
    assert gen_proj == exp_proj, f"Projections mismatch: {gen_proj} != {exp_proj}"
    
    # Compare From Entities
    gen_ents = sorted([str(e) for e in generated_ast.from_clause.entities])
    exp_ents = sorted([str(e) for e in expected_ast.from_clause.entities])
    assert gen_ents == exp_ents, f"From Entities mismatch: {gen_ents} != {exp_ents}"
    
    # Helper for normalizing conditions
    def normalize_condition(cond, target_alias):
        left_str = str(cond.left)
        if cond.right:
            right_str = str(cond.right)
        else:
            # Implicit identity match with target
            right_str = target_alias
            
        # Sort operands to handle commutativity A=B vs B=A
        return " = ".join(sorted([left_str, right_str]))

    # Compare Joins - check order first
    gen_join_list = generated_ast.from_clause.joins
    exp_join_list = expected_ast.from_clause.joins
    
    assert len(gen_join_list) == len(exp_join_list), \
        f"Join count mismatch: {len(gen_join_list)} != {len(exp_join_list)}"
    
    # Check join order by comparing target entities
    gen_targets = [str(j.target) for j in gen_join_list]
    exp_targets = [str(j.target) for j in exp_join_list]
    assert gen_targets == exp_targets, \
        f"Join order mismatch:\nGenerated: {gen_targets}\nExpected:  {exp_targets}"
    
    # Compare join conditions for each join (by position)
    for i, (gen_join, exp_join) in enumerate(zip(gen_join_list, exp_join_list)):
        target_alias = gen_join.target.alias if gen_join.target.alias else gen_join.target.name
        gen_cond = sorted([normalize_condition(c, target_alias) for c in gen_join.on])
        exp_cond = sorted([normalize_condition(c, target_alias) for c in exp_join.on])
        assert gen_cond == exp_cond, \
            f"Conditions mismatch for join {i+1} ({str(gen_join.target)}):\nGenerated: {gen_cond}\nExpected:  {exp_cond}"


def test_slide_2(gordon_model):
    # path: AirSystem.navSystem->observer[Observe].observed.pos
    # query: SELECT AirFrame.pos
    #        FROM AirSystem
    #        JOIN NavigationSystem ON AirSystem.navSystem
    #        JOIN Observe ON Observe.observer = NavigationSystem
    #        JOIN AirFrame ON Observe.observed
    
    path_str = "AirSystem.navSystem->observer[Observe].observed.pos"
    paths = [ParticipantPath.parse(path_str)]
    
    alias_map = _reconstruct_alias_map(gordon_model, paths)
    queries = path2query(alias_map, paths, gordon_model)
    assert len(queries) == 1
    generated_q = queries[0]
    
    # Expected components
    # Just asserting the structure is present
    q_str = str(generated_q)
    assert "SELECT AirFrame.pos" in q_str
    assert "FROM AirSystem" in q_str
    assert "JOIN NavigationSystem" in q_str
    assert "JOIN Observe" in q_str
    assert "JOIN AirFrame" in q_str

    expected_q_str = """
    SELECT AirFrame.pos
    FROM AirSystem
    JOIN NavigationSystem ON AirSystem.navSystem
    JOIN Observe ON Observe.observer = NavigationSystem
    JOIN AirFrame ON Observe.observed
    """
    check_query_equivalence(generated_q, get_ast(expected_q_str))


def test_slide_3(gordon_model):
    # path: AirSystem.navSystem->observer[Observe].observed.pos
    # alias map: see the AND in the query
    # query: ... JOIN AirFrame ON Observe.observed AND AirSystem.airFrame
    
    p1 = ParticipantPath.parse("AirSystem.navSystem->observer[Observe].observed.pos")
    p2 = ParticipantPath.parse("AirSystem.airFrame")
    
    # Construct partial map for diamond join
    # AirFrame is the target of both:
    # 1. ...->observed
    # 2. AirSystem.airFrame
    
    # Extract prefix of p1 that leads to AirFrame
    # p1 resolutions: .navSystem, ->observer[Observe], .observed, .pos
    # AirFrame is at index 3 (after .observed)
    p1_prefix = ParticipantPath(p1.start_type, p1.resolutions[:-1]) # up to .observed
    
    partial = {"AirFrame": [p1_prefix, p2]}
    
    alias_map = _reconstruct_alias_map(gordon_model, [p1], partial_map=partial)
    queries = path2query(alias_map, [p1], gordon_model)
    assert len(queries) == 1
    generated_q = queries[0]
    
    q_str = str(generated_q)
    # Check for AND join
    # Should see "JOIN AirFrame ON ... AND ..."
    assert "JOIN AirFrame" in q_str
    assert " AND " in q_str.split("JOIN AirFrame ON ")[1].split("JOIN")[0] # Check logic in ON clause
    
    # Check constraints
    assert "Observe.observed" in q_str
    assert "AirSystem.airFrame" in q_str

    expected_q_str = """
    SELECT AirFrame.pos
    FROM AirSystem
    JOIN NavigationSystem ON AirSystem.navSystem
    JOIN Observe ON Observe.observer = NavigationSystem
    JOIN AirFrame ON Observe.observed = AirFrame AND AirSystem.airFrame = AirFrame
    """
    check_query_equivalence(generated_q, get_ast(expected_q_str))


def test_slide_4(gordon_model):
    # path:
    #   AirSystem.navSystem->observer[Observe].observed.pos
    #   AirSystem.navSystem->observer[Observe].observer.mode
    # alias map: see the AND in the query
    # query: SELECT AirFrame.pos, NavigationSystem.mode ...
    # AND Join on AirFrame (same as Slide 3)
    
    p1 = ParticipantPath.parse("AirSystem.navSystem->observer[Observe].observed.pos")
    p2 = ParticipantPath.parse("AirSystem.navSystem->observer[Observe].observer.mode")

    # Paths for alias "NavigationSystem":
    nav_p1 = ParticipantPath.parse("AirSystem.navSystem")
    nav_p2 = ParticipantPath("AirSystem", p2.resolutions[:-1]) # up to .observer
    
    # Paths for alias "AirFrame":
    af_p1 = ParticipantPath("AirSystem", p1.resolutions[:-1]) # up to .observed
    af_p2 = ParticipantPath.parse("AirSystem.airFrame")
    
    partial = {
        "AirFrame": [af_p1, af_p2],
        "NavigationSystem": [nav_p1, nav_p2]
    }
    
    paths = [p1, p2]
    alias_map = _reconstruct_alias_map(gordon_model, paths, partial_map=partial)
    queries = path2query(alias_map, paths, gordon_model)
    assert len(queries) == 1
    generated_q = queries[0]
    
    q_str = str(generated_q)
    assert "SELECT AirFrame.pos, NavigationSystem.mode" in q_str or "SELECT NavigationSystem.mode, AirFrame.pos" in q_str
    assert "JOIN AirFrame ON" in q_str
    assert "AND" in q_str.split("JOIN AirFrame")[1]

    expected_q_str = """
    SELECT AirFrame.pos, NavigationSystem.mode
    FROM AirSystem
    JOIN NavigationSystem ON AirSystem.navSystem AND Observe.observer = NavigationSystem
    JOIN Observe ON Observe.observer = NavigationSystem
    JOIN AirFrame ON Observe.observed = AirFrame AND AirSystem.airFrame = AirFrame
    """
    check_query_equivalence(generated_q, get_ast(expected_q_str))


def test_slide_5(gordon_model):
    # path:
    #   AirSystem.navSystem->observer[Observe].observed.pos
    #   AirSystem.navSystem->observer[Observe].observer.mode
    #   AirSystem.navSystem->observer[Observe].validity
    # alias map: see the AND in the query
    # query: SELECT AirFrame.pos, NavigationSystem.mode, Observe.validity
    
    p1 = ParticipantPath.parse("AirSystem.navSystem->observer[Observe].observed.pos")
    p2 = ParticipantPath.parse("AirSystem.navSystem->observer[Observe].observer.mode")
    p3 = ParticipantPath.parse("AirSystem.navSystem->observer[Observe].validity")
    
    # Explicit aliases from Slide 4 logic
    nav_p1 = ParticipantPath.parse("AirSystem.navSystem")
    nav_p2 = ParticipantPath("AirSystem", p2.resolutions[:-1])
    
    af_p1 = ParticipantPath("AirSystem", p1.resolutions[:-1])
    af_p2 = ParticipantPath.parse("AirSystem.airFrame")
    
    partial = {
        "AirFrame": [af_p1, af_p2],
        "NavigationSystem": [nav_p1, nav_p2]
    }
    
    paths = [p1, p2, p3]
    alias_map = _reconstruct_alias_map(gordon_model, paths, partial_map=partial)
    queries = path2query(alias_map, paths, gordon_model)
    assert len(queries) == 1
    generated_q = queries[0]
    
    q_str = str(generated_q)
    # Check projections
    assert "Observe.validity" in q_str

    expected_q_str = """
    SELECT AirFrame.pos, NavigationSystem.mode, Observe.validity
    FROM AirSystem
    JOIN NavigationSystem ON AirSystem.navSystem AND Observe.observer = NavigationSystem
    JOIN Observe ON Observe.observer = NavigationSystem
    JOIN AirFrame ON Observe.observed = AirFrame AND AirSystem.airFrame = AirFrame
    """
    check_query_equivalence(generated_q, get_ast(expected_q_str))


def test_slide_7(gordon_model):
    # path:
    #   AirSystem.controlSystem->controller[Control].controlled.pos
    #   AirSystem.controlSystem->controller[Control].controller.mode
    #   AirSystem.controlSystem->controller[Control].validity
    # alias map: see the AND in the query
    # query: SELECT AirFrame.pos, ControlSystem.mode, Control.validity
    
    p1 = ParticipantPath.parse("AirSystem.controlSystem->controller[Control].controlled.pos")
    p2 = ParticipantPath.parse("AirSystem.controlSystem->controller[Control].controller.mode")
    p3 = ParticipantPath.parse("AirSystem.controlSystem->controller[Control].validity")
    
    # Paths for alias "ControlSystem":
    cs_p1 = ParticipantPath.parse("AirSystem.controlSystem")
    cs_p2 = ParticipantPath("AirSystem", p2.resolutions[:-1]) # up to .controller
    
    # Paths for alias "AirFrame":
    af_p1 = ParticipantPath("AirSystem", p1.resolutions[:-1]) # up to .controlled
    af_p2 = ParticipantPath.parse("AirSystem.airFrame")
    
    partial = {
        "AirFrame": [af_p1, af_p2],
        "ControlSystem": [cs_p1, cs_p2]
    }
    
    paths = [p1, p2, p3]
    alias_map = _reconstruct_alias_map(gordon_model, paths, partial_map=partial)
    queries = path2query(alias_map, paths, gordon_model)
    assert len(queries) == 1
    generated_q = queries[0]
    
    q_str = str(generated_q)
    # Check projections
    assert "Control.validity" in q_str
    assert "JOIN AirFrame ON" in q_str
    assert "AND" in q_str.split("JOIN AirFrame")[1]

    expected_q_str = """
    SELECT AirFrame.pos, ControlSystem.mode, Control.validity
    FROM AirSystem
    JOIN ControlSystem ON AirSystem.controlSystem AND Control.controller = ControlSystem
    JOIN Control ON Control.controller = ControlSystem
    JOIN AirFrame ON Control.controlled = AirFrame AND AirSystem.airFrame = AirFrame
    """
    check_query_equivalence(generated_q, get_ast(expected_q_str))
