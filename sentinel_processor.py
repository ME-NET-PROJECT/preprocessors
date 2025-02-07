import os
from pathlib import Path
import pandas as pd
import numpy as np
import xarray as xr
from datetime import datetime
from utils import CoordinateConverter


class DataProcessing:
    def __init__(self, bbox_name, bbox_coords, band_name, processing_folder="raw_data", resolution=1000,
                 data_folder="./data/sentinel5p"):
        self.bbox_name = bbox_name
        self.bbox_coords = bbox_coords
        self.resolution = resolution
        self.band_name = band_name
        self.processing_folder = processing_folder
        self.data_folder = data_folder
        self.raw_folder = Path(self.data_folder) / Path(self.bbox_name) / Path(self.processing_folder) / Path(
            self.band_name)
        self.ms_df = self.get_monitoring_stations()
        self.convertor = CoordinateConverter()

    def get_monitoring_stations(self, stations_csv='data/aurn/monitoring_stations.csv'):
        ms_df = pd.read_csv(stations_csv)
        return ms_df

    def extract_start_date(self, file_path):
        # Extract date from filename
        filename = os.path.basename(file_path)
        date_str = filename.split('_')[0]
        date = datetime.strptime(date_str, '%Y%m%d')
        return date

    def xarray_from_numpy(self, file_path):
        # Read the stored numpy array
        numpy_array = np.load(file_path)

        lon_min, lat_min, lon_max, lat_max = self.bbox_coords
        nrows, ncols = numpy_array.shape

        # lat_res = (lat_max - lat_min) / nrows
        # lon_res = (lon_max - lon_min) / ncols

        latitudes = np.linspace(lat_min, lat_max, nrows)
        longitudes = np.linspace(lon_min, lon_max, ncols)

        eastings = np.zeros((nrows, ncols))
        northings = np.zeros((nrows, ncols))

        for i in range(nrows):
            for j in range(ncols):
                eastings[i, j], northings[i, j] = self.convertor.wgs84_to_osgb36_coordinates(longitudes[j], latitudes[i])

        projection_x_coordinate = np.array(eastings)
        projection_y_coordinate = np.array(northings)

        date = self.extract_start_date(file_path)

        dataset = xr.Dataset(
            {
                "data": (("time", "latitude", "longitude"), np.expand_dims(numpy_array, axis=0))
            },
            coords={
                "longitude": longitudes,
                "latitude": latitudes,
                "projection_x_coordinate": (("latitude", "longitude"), projection_x_coordinate),
                "projection_y_coordinate": (("latitude", "longitude"), projection_y_coordinate),
                self.band_name: (("latitude", "longitude"), numpy_array),
                "time": [date],
            }
        )

        return dataset

    def read_sentinel5p_station_data(self, file_path):
        print(file_path)
        dataset = self.xarray_from_numpy(file_path)

        sentinel5p_station_df = []
        for index, row in self.ms_df.iterrows():
            print(f"Processing {file_path}: #{index}")
            site_name = row["SiteName"]
            longitude = row['Longitude']
            site_number = row['SiteNumber']
            latitude = row['Latitude']

            nearest_data = dataset.sel(latitude=latitude, longitude=longitude, method='nearest')
            #print(nearest_data)
            nancount = nearest_data[self.band_name].isnull().sum().item()
            print(nancount)
            data = nearest_data[['time', self.band_name]].to_dataframe().reset_index()
            data['time'] = data['time'].dt.date
            data = data[['time', self.band_name]]
            data.columns = ['Time', self.band_name]
            data['SiteNumber'] = site_number
            data['SiteName'] = site_name
            data['Longitude'] = longitude
            data['Latitude'] = latitude

            sentinel5p_station_df.append(data)

        sentinel5p_station_df = pd.concat(sentinel5p_station_df, ignore_index=True)

        return sentinel5p_station_df

    def read_sentinel5p_station_data_multiple(self):
        print(self.raw_folder)
        npy_files = list(self.raw_folder.glob("*.npy"))
        output_folder = Path(self.data_folder) / Path(self.bbox_name) / "processed_data" / Path(self.band_name)

        os.makedirs(output_folder, exist_ok=True)  # Create output folder if it doesn't exist
        for file_path in npy_files:
            sentinel5p_station_df = self.read_sentinel5p_station_data(file_path)
            output_csv = output_folder / (self.band_name + ".csv")  # Output CSV filename
            if output_csv.exists():
                sentinel5p_station_df.to_csv(output_csv, mode='a', header=False, index=False)
            else:
                sentinel5p_station_df.to_csv(output_csv, index=False)  # Save DataFrame to CSV


if __name__ == '__main__':
    # Define the resolution
    resolution = 1000

    # Store bounding box coordinates in a dictionary
    bbox_dict = {
        "UK": (-7.57216793459, 49.959999905, 1.68153079591, 58.6350001085),  # UK
        "Ghana": (-3.24437008301, 4.71046214438, 1.0601216976, 11.0983409693)  # Ghana
    }
    band_names = ["CH4_S", "CO_S", "HCHO_S", "NO2_S"]

    # Loop through the keys (band names) of bbox_dict
    for bbox_name, bbox_coords in bbox_dict.items():
        # Create data_processor with the band_name and bbox_coords
        for band_name in band_names:
            data_processor = DataProcessing(
                bbox_name,  # bbox_name
                bbox_coords,  # bbox_coords for this band_name
                band_name  # band_name again (or it could be another argument if needed)
            )

            # Process the data
            data_processor.read_sentinel5p_station_data_multiple()
