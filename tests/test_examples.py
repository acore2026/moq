import importlib.util
import sys
from pathlib import Path


EXAMPLES = [
    "basic_example.py",
    "fetch_example.py",
    "integration_example.py",
    "external_usage_example.py",
    "publisher_example.py",
    "reconnection_example.py",
    "relay_example.py",
    "subscriber_example.py",
]


def test_example_modules_import_cleanly():
    examples_dir = Path(__file__).resolve().parents[1] / "examples"
    examples_path = str(examples_dir)
    if examples_path not in sys.path:
        sys.path.insert(0, examples_path)

    for filename in EXAMPLES:
        path = examples_dir / filename
        spec = importlib.util.spec_from_file_location(path.stem, path)
        module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(module)
