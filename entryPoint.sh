#!/bin/bash
python Pipeline.py &
python Ingestion.py &
wait
