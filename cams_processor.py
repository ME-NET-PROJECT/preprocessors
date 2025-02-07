import datetime
from pathlib import Path
import os
import pandas as pd
import numpy as np
import xarray as xr


class DataProcessing:
    def __init__(self, stations_csv='data/aurn/monitoring_stations.csv', data_folder="./data/sentinel5p", band_name="UK",
                 upsample=40, model_level="single_level"):
        self.stations_csv = stations_csv
        self.data_folder = data_folder
        self.band_name = band_name
        self.upsample = upsample
        self.model_level = model_level
        self.ms_df = self.get_monitoring_stations()

    def get_monitoring_stations(self):
        return pd.read_csv(self.stations_csv)

    def read_cams_station_data(self, file_path, output_folder):
        dataset = xr.open_dataset(file_path)

        # Check if 'forecast_reference_time' exists and if any dimension has size 1
        if 'forecast_reference_time' in dataset and any(size == 1 for size in dataset.sizes.values()):
            dataset = dataset.squeeze()  # Remove all dimensions with size 1

        # Generate new longitude and latitude values for upsampling
        new_lon = np.linspace(dataset.longitude.min(), dataset.longitude.max(), len(dataset.longitude) * self.upsample)
        new_lat = np.linspace(dataset.latitude.min(), dataset.latitude.max(), len(dataset.latitude) * self.upsample)

        # Loop through each variable in the dataset
        for var_name in dataset.data_vars:

            # Perform the interpolation for the current variable
            dataset_up = dataset[var_name].interp(longitude=new_lon, latitude=new_lat, method='nearest')

            output_csv = output_folder / Path(str(var_name) + ".csv")

            for index, row in self.ms_df.iterrows():
                print(f"Processing {file_path} - {var_name}: #{index}")

                site_number = row["SiteNumber"]
                site_name = row["SiteName"]
                longitude = row['Longitude']
                latitude = row['Latitude']

                # Find the nearest data for the station coordinates
                nearest_data = dataset_up.sel(latitude=latitude, longitude=longitude, method='nearest')

                # Extract the time coordinate and the variable's data
                time_data = dataset.get('time', dataset.get('forecast_reference_time'))
                variable_data = nearest_data.values

                # Create a dataframe
                data = pd.DataFrame({
                    'Time': time_data,
                    var_name: variable_data
                })

                # Convert the data to a dataframe
                data['Time'] = pd.to_datetime(data['Time']).dt.date
                data['SiteNumber'] = site_number
                data['SiteName'] = site_name
                data['Longitude'] = longitude
                data['Latitude'] = latitude

                # Write the data to a CSV file
                if output_csv.exists():
                    data.to_csv(output_csv, mode='a', header=False, index=False)
                else:
                    data.to_csv(output_csv, index=False)

    def read_cams_station_data_multiple(self):
        cams_process_folder = Path(self.data_folder) / Path(self.band_name) / Path("raw_data") / Path(self.model_level)
        nc_files = cams_process_folder.glob("*.nc")
        output_folder = Path(self.data_folder) / Path(self.band_name) / Path("processed_data")
        os.makedirs(output_folder, exist_ok=True)

        for file_path in nc_files:
            self.read_cams_station_data(file_path, output_folder)


# Example usage
if __name__ == '__main__':
    band_names = ["UK", "Ghana"]
    variables = ["multi_level", "single_level"]
    data_folder = "./data/cams"
    for band_name in band_names:
        for variable in variables:
            dp = DataProcessing(data_folder=data_folder, model_level=variable, band_name=band_name, upsample=1)
            dp.read_cams_station_data_multiple()


