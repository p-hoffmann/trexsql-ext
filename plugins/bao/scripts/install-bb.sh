#!/bin/bash
# Install Babashka for nbb dependency management.

echo "Installing Babashka..."

curl -sLO https://raw.githubusercontent.com/babashka/babashka/master/install
chmod +x install
./install --dir /usr/local/bin
rm install

echo "Babashka installed successfully!"
echo "You can now run: npm start"
