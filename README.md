# Magi
## BI Data Orchestration & ETL Tool

## Overview

Magi is an enterprise-grade ETL (Extract, Transform, Load) framework designed to streamline data integration processes. It supports automated data ingestion, transformation, and visualization through a Dash-powered web interface, scheduled task management via Celery/Redbeat, and alerting capability via Slack API.

## Features

- **Automated ETL Pipelines** – Extract data from multiple sources, transform it efficiently, and load it into the desired destination.
- **Dash Web Interface** – Interactive dashboards for monitoring ETL jobs and visualizing data insights.
- **Modular Design** – Easily extendable with custom transformations and connectors.
- **Enterprise-Ready** – Includes support for authentication, scheduling, and cloud integrations.

## Installation

### Prerequisites

Ensure you have the following installed:

- Python 3.8+
- Virtual Environment (`venv` or `conda`)
- Required dependencies from `requirements.txt`

### Setup

1. Clone the repository:
   ```sh
   git clone https://github.com/your-username/magi-etl.git
   cd magi_etl
   ```
2. Create and activate a virtual environment:
   ```sh
   python -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`
   ```
3. Install dependencies:
   ```sh
   pip install -r requirements.txt
   ```

## Usage


### Running the Dash Web Interface

See details in app.py to launch the web interface at `http://127.0.0.1:8050/`.

## Project Structure

```
├── magi_etl/
│   ├── biutils.py           # Business Intelligence utilities
│   ├── app.py               # Main ETL application
│   ├── dash_app/
│   │   ├── dash_app.py      # Dash-based web dashboard
│   │   ├── assets/
│   │   │   ├── data/
│   │   │   │   ├── country_map.csv  # Sample data asset
```
