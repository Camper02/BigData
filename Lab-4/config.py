import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data')
DB_PATH = os.path.join(BASE_DIR, 'search_engine.db')

# Параметры PageRank
DAMPING_FACTOR = 0.85
MAX_ITERATIONS = 100
TOLERANCE = 1e-6