#!/usr/bin/env python3
"""
Standalone script to run the dashboard locally without Docker
"""
import subprocess
import sys
from pathlib import Path

def main():
    dashboard_path = Path(__file__).parent / "dashboard" / "app.py"
    
    if not dashboard_path.exists():
        print(f"Dashboard app not found at {dashboard_path}")
        sys.exit(1)
    
    print("Starting CitiBike Dashboard...")
    print("Dashboard will be available at: http://localhost:8501")
    print("Press Ctrl+C to stop")
    
    try:
        subprocess.run([
            "streamlit", "run", str(dashboard_path),
            "--server.port=8501",
            "--server.address=localhost"
        ])
    except KeyboardInterrupt:
        print("\nDashboard stopped.")
    except FileNotFoundError:
        print("Error: Streamlit not found. Install with: pip install streamlit plotly pandas")
        sys.exit(1)

if __name__ == "__main__":
    main()