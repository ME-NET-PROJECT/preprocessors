# Data Pre-Processing

## Overview
This project provides Python scripts for processing air quality and atmospheric data from different sources, including AURN monitoring stations, Sentinel-5P satellite data, and CAMS model data. The scripts extract, process, and store data in structured CSV files for further analysis.

## Features
- Extracts air quality data from HTML tables.
- Processes Sentinel-5P satellite data and interpolates grid data for stations.
- Reads and processes CAMS model datasets.
- Supports multiple geographical regions (e.g., UK, Ghana).
- Saves processed datasets in structured CSV files.

## Installation
### Prerequisites
Ensure you have the following dependencies installed:

```bash
pip install pandas numpy xarray pathlib
```

## Usage
### Processing AURN Air Quality Data
This script reads HTML tables from the AURN dataset, extracts monitoring station data, and generates structured CSV files.

```python
from data_processing import DataProcessing

data_processor = DataProcessing("NO2", "data.html")
data_processor.save_csv_dataset()
```

### Processing CAMS Model Data
This script processes CAMS model NetCDF (`.nc`) files and interpolates them for monitoring station locations.

```python
data_processor = DataProcessing(data_folder="./data/cams", band_name="UK", model_level="single_level")
data_processor.read_cams_station_data_multiple()
```

### Processing Sentinel-5P Satellite Data
This script reads Sentinel-5P satellite `.npy` files and extracts air quality data for specific monitoring stations.

```python
data_processor = DataProcessing("UK", (-7.5, 49.9, 1.6, 58.6), "NO2_S")
data_processor.read_sentinel5p_station_data_multiple()
```

## Project Structure
```
project_root/
│── data_processing.py
│── data/
│   ├── aurn/
│   │   ├── raw_data/
│   │   ├── processed_data/
│   ├── sentinel5p/
│   ├── cams/
│── utils/
│── README.md
```

## License
This project is licensed under the MIT License - see the LICENSE file for details.

