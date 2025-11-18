#!/usr/bin/env bash
set -e

echo "=== Updating system ==="
sudo apt update -y
sudo apt upgrade -y

echo "=== Installing Python3 & Pip ==="
sudo apt install -y python3 python3-pip

echo "=== Installing Docker ==="
sudo apt install -y ca-certificates curl gnupg lsb-release

# Add Docker’s official GPG key
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

# Add repo
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt update -y

# Install Docker Engine + compose plugin
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

echo "=== Adding user to docker group ==="
sudo usermod -aG docker $USER

echo "=== Starting remote QuestDB ==="
docker compose up -d

echo "IMPORTANT: Log out & log back in so Docker group updates take effect."
echo "Verify ILP TCP port 9009 is open and reachable before running ingestion."
