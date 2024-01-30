# DataStreaming-Prefect

This repository contains the code for a data streaming application that utilizes Prefect for workflow orchestration. The project is structured to stream data from an API to Kafka (using `Ingestion.py`) and then from Kafka to MongoDB (using `Pipeline.py`).

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites

Before running the project, ensure you have the following installed:
- Python 3.8 or higher
- Prefect
- Kafka
- MongoDB
- Dependencies listed in `requirements.txt`

### Installing

Clone the repository to your local machine:

```bash
git clone https://github.com/your-username/DataStreaming-Prefect.git
cd DataStreaming-Prefect
```

Installing Requirements 

```bash
pip install -r requirements.txt
```

### Running the Application
To run the application, use the provided shell script `entryPoint.sh`. This script starts both the `Ingestion.py` and `Pipeline.py` scripts in the background.

Run the script with the following command:

```bash
chmod +x ./entryPoint.sh
./entryPoint.sh
```

To - DO

- Containerize the application using docker
- Deploy the pipeline
- Create analytics dashboard from the streamed data