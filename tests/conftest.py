"""Load the checkout using the same package name as its installed wheel."""

import importlib.util
import sys
from pathlib import Path

source = Path(__file__).resolve().parents[1] / "src"
spec = importlib.util.spec_from_file_location(
    "streaming_tts", source / "__init__.py", submodule_search_locations=[str(source)]
)
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)
