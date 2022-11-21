import os
import sys


def setup_tests():
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
