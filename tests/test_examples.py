from importlib import import_module
from pathlib import Path


EXAMPLES = [
    "basic_example.py",
    "fetch_example.py",
    "publisher_example.py",
    "reconnection_example.py",
    "relay_example.py",
    "subscriber_example.py",
]


def test_example_modules_import_cleanly():
    for filename in EXAMPLES:
        module_name = f"examples.{Path(filename).stem}"
        import_module(module_name)
