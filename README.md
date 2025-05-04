# Crop Health Monitoring System

## Installation

1. Install Python dependencies:
```bash
pip install -r requirements.txt
```

2. Start Docker containers:
```bash
docker-compose up -d
```

## Data Setup

### Download Training Data
Download Agriculture-Vision 2021 Raw dataset from AWS:
```bash
aws s3 cp s3://intelinair-data-releases/agriculture-vision/cvpr_challenge_2021/raw/10 raw/10 --no-sign-request --recursive
```

### Initialize Database
1. Update the raw data directory path in `init_db.py`
2. Run the database initialization script:
```bash
python init_db.py
```

## Running the Application

### Start the Data Pipeline
1. Start the consumer:
```bash
python consumer.py
```

2. Start the producer:
```bash
python producer.py
```

### Launch Web Interface
Navigate to the web application directory and start the development server:
```bash
cd web-stream-uavs
npm run dev
```

## Project Structure
- `consumer.py` - Data processing service
- `producer.py` - Data ingestion service
- `init_db.py` - Database initialization script
- `web-stream-uavs/` - Web interface application
