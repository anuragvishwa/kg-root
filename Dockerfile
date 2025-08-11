FROM python:3.10-slim

WORKDIR /app

# ── OS + Node ─────────────────────────────────────────────
RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends curl build-essential && \
    curl -fsSL https://deb.nodesource.com/setup_18.x | bash - && \
    apt-get install -y nodejs && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# ── Node deps ─────────────────────────────────────────────
COPY package*.json tsconfig.json ./
COPY connectors ./connectors
RUN npm install --silent

# ── Python deps ───────────────────────────────────────────
# Install base deps first
COPY requirements.base.txt .
RUN pip install --no-cache-dir -r requirements.base.txt

# Install PyTorch + PyG deps next
COPY requirements.torch.txt .
RUN pip install --no-cache-dir -r requirements.torch.txt


# ── Copy rest of code ─────────────────────────────────────
COPY . .

CMD ["bash"]
