#!/bin/bash

# Check script for hands-on-iceberg-compression
# Runs linting and build to verify code quality

set -e  # Exit on any error

echo "🔍 Running TypeScript type check..."
yarn lint

echo "🔧 Running ESLint..."
yarn lint:fix

echo "🏗️  Building project..."
yarn build

echo "✅ All checks passed!"
