#!/bin/bash
python3 load_data.py --force
gunicorn app:app --bind 0.0.0.0:$PORT