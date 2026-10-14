"""Compatibility entry point for the browser control room.

The project no longer requires Streamlit.  Existing launch scripts that still
refer to ``app/dashboard.py`` are forwarded to the standard-library server.
"""

from app.web_server import main


if __name__ == "__main__":
    main()
