import json
import pytest
import sys

sys.path.insert(0, '/Users/gonzo/Desktop/Capstone\ Project/data_generators/Dump_data/')

from ...data_generators.Dump_data.json_generator import JsonFactory

factory = JsonFactory()
factory.generate_jsonlines(n_lines=10)


