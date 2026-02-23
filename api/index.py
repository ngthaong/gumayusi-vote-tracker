import sys
import os

# Add parent directory to path so we can import server
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from server import app

# Vercel expects the WSGI app to be named 'app' or 'handler'
# Flask app is already WSGI-compatible

