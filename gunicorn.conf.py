import os

bind = f"0.0.0.0:{os.environ.get('PORT', '5050')}"
workers = 1
