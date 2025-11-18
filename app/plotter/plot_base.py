import sys
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from core.fetcher import Fetcher

class Plotter:
    def __init__(self, host_mothership='34.250.225.205'):
        self.fetcher = Fetcher(host_mothership)
        self.md_storage = None
        self.plots = dict()