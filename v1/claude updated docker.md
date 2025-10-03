The `make` command isn't available on your system. Here are alternative ways to run the Docker pipeline without `make`:

## **Alternative Commands (Without Make)**

### **1. Build the Docker Image**
```bash
# Instead of: make build
docker build -t excel-parquet-pipeline .

# Build without cache if needed
docker build --no-cache -t excel-parquet-pipeline .
```

### **2. One-Time Processing**
```bash
# Instead of: make process
docker-compose run --rm excel-parquet-pipeline run --clean-orphaned

# Or with custom arguments
docker-compose run --rm excel-parquet-pipeline run --log-level DEBUG --pattern "*.xls"
```

### **3. Run Continuous Service**
```bash
# Instead of: make run
docker-compose up -d

# View logs
docker-compose logs -f
```

### **4. Other Common Operations**
```bash
# Instead of: make logs
docker-compose logs -f excel-parquet-pipeline

# Instead of: make shell
docker-compose exec excel-parquet-pipeline /bin/bash

# Instead of: make stop
docker-compose down

# Instead of: make status
docker-compose ps

# Instead of: make clean
docker-compose down -v
docker system prune -f
```

## **Complete Workflow Without Make**

```bash
# 1. Build the image
docker build -t excel-parquet-pipeline .

# 2. Run one-time processing
docker-compose run --rm excel-parquet-pipeline run --clean-orphaned

# 3. Or run as continuous service
docker-compose up -d

# 4. View logs
docker-compose logs -f excel-parquet-pipeline

# 5. Check status
docker-compose ps

# 6. Stop services
docker-compose down
```

## **Direct Docker Run (Without Docker Compose)**

If you prefer to run directly with Docker:

```bash
# Create directories
mkdir -p data/{input,output} logs state

# Build image
docker build -t excel-parquet-pipeline .

# Run one-time processing
docker run --rm \
  -v $(pwd)/data/input:/app/data/input \
  -v $(pwd)/data/output:/app/data/output \
  -v $(pwd)/logs:/app/logs \
  -v $(pwd)/state:/app/state \
  -e LOG_LEVEL=INFO \
  excel-parquet-pipeline run --clean-orphaned

# Run continuous service
docker run -d \
  --name excel-parquet-pipeline \
  -v $(pwd)/data/input:/app/data/input \
  -v $(pwd)/data/output:/app/data/output \
  -v $(pwd)/logs:/app/logs \
  -v $(pwd)/state:/app/state \
  -e LOG_LEVEL=INFO \
  -e CRON_SCHEDULE="0 */6 * * *" \
  excel-parquet-pipeline

# View logs
docker logs -f excel-parquet-pipeline

# Stop container
docker stop excel-parquet-pipeline
docker rm excel-parquet-pipeline
```

## **Install Make (Optional)**

If you want to use the Makefile commands:

**On Ubuntu/Debian:**
```bash
sudo apt update
sudo apt install make
```

**On CentOS/RHEL:**
```bash
sudo yum install make
# Or on newer versions:
sudo dnf install make
```

**On macOS:**
```bash
# Install Xcode Command Line Tools
xcode-select --install

# Or with Homebrew
brew install make
```

**On Windows:**
- Install through Windows Subsystem for Linux (WSL)
- Or use Git Bash
- Or install through Chocolatey: `choco install make`

## **Quick Start Without Make**

```bash
# 1. Setup
mkdir excel-pipeline && cd excel-pipeline
mkdir -p data/{input,output} logs state

# 2. Place your Excel files in data/input/
cp /path/to/your/excel/files/*.xlsx data/input/

# 3. Build and run
docker build -t excel-parquet-pipeline .
docker-compose up -d

# 4. Check results
ls -la data/output/
docker-compose logs excel-parquet-pipeline
```

The docker-compose commands are the most straightforward alternative to the Makefile commands!