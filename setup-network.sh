#!/bin/bash
# Run once before starting any branch (01-18)
docker network create kafka-lab 2>/dev/null || echo "Network 'kafka-lab' already exists"