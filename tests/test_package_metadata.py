import ast
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _setup_runtime_requirements() -> list[str]:
    tree = ast.parse((ROOT / "setup.py").read_text(encoding="utf-8"))
    return next(
        ast.literal_eval(node.value)
        for node in tree.body
        if isinstance(node, ast.Assign)
        and len(node.targets) == 1
        and isinstance(node.targets[0], ast.Name)
        and node.targets[0].id == "install_requires"
    )


def test_authoritative_runtime_and_direct_dependency_ranges():
    metadata = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    project = metadata["project"]
    requirements = project["dependencies"]

    assert project["requires-python"] == ">=3.11"
    assert {
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "Programming Language :: Python :: 3.14",
    }.issubset(project["classifiers"])
    assert "pyzmq>=25.1.2,<28" in project["dependencies"]
    assert "parquery>=2.0.6,<3" in project["dependencies"]
    assert all(any(operator in requirement for operator in (">", "<", "~")) for requirement in requirements)
    assert all("==" not in requirement for requirement in requirements)


def test_legacy_setup_metadata_matches_pep621_runtime_dependencies():
    metadata = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))

    assert set(_setup_runtime_requirements()) == set(metadata["project"]["dependencies"])
