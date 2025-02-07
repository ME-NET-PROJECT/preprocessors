import pandas as pd
from pathlib import Path
import os


class DataProcessing:
    def __init__(self, band_name, tables_html_url, processing_folder="raw_data", data_folder="./data/aurn"):
        self.band_name = band_name
        self.processing_folder = processing_folder
        self.tables_html_url = tables_html_url
        self.data_folder = data_folder

        self.html_file = Path(self.data_folder) / Path(self.processing_folder) / f"{self.band_name}" / Path(tables_html_url)
        self.tables_df = pd.read_html(self.html_file)

        self.lookup_df = self.get_monitoring_stations()
        self.global_df = pd.DataFrame()

    def get_monitoring_stations(self, stations_csv='data/aurn/monitoring_stations.csv'):
        ms_df = pd.read_csv(stations_csv)
        return ms_df

    def lookup_coordinates(self, site_name):
        row = self.lookup_df[self.lookup_df['SiteName'] == site_name]
        if not row.empty:
            return row.iloc[0]['Latitude'], row.iloc[0]['Longitude'], row.iloc[0]['Northing'], row.iloc[0]['Easting'], row.iloc[0]['SiteNumber']
        return None, None, None, None, None

    @staticmethod
    def remove_brackets(site_name):
        return site_name.split('(')[0].strip()

    def fetch_table_data(self):
        collected_data = []

        for i in range(len(self.tables_df)):
            print("Processing Table #", i)
            for j in range(1, len(self.tables_df[i].columns), 2):
                site_name = self.remove_brackets(self.tables_df[i].columns[j])
                latitude, longitude, northing, easting, site_number = self.lookup_coordinates(site_name)
                if longitude is not None:
                    temp_df = self.tables_df[i].iloc[1:, [0, j]].copy()
                    temp_df.columns = ['Time', self.band_name]
                    temp_df['SiteNumber'] = site_number
                    temp_df['SiteName'] = site_name
                    temp_df['Longitude'] = longitude
                    temp_df['Latitude'] = latitude
                    collected_data.append(temp_df)

        self.global_df = pd.concat(collected_data, ignore_index=True)
        self.global_df['Time'] = pd.to_datetime(self.global_df['Time'], format='%d/%m/%Y')
        self.global_df['SiteNumber'] = self.global_df['SiteNumber'].astype(int)
        self.global_df['SiteName'] = self.global_df['SiteName'].astype(str)
        self.global_df['Longitude'] = self.global_df['Longitude'].astype(float)
        self.global_df['Latitude'] = self.global_df['Latitude'].astype(float)
        self.global_df[self.band_name] = pd.to_numeric(self.global_df[self.band_name], errors='coerce')

    def save_csv_dataset(self):
        self.fetch_table_data()

        output_folder = Path(self.data_folder) / self.processing_folder / f"{self.band_name}" / "processed_data"
        output_folder = Path(self.data_folder) / Path("processed_data") / Path(self.band_name)
        os.makedirs(output_folder, exist_ok=True)  # Create output folder if it doesn't exist
        output_csv = output_folder / (self.band_name + ".csv")  # Output CSV filename
        if output_csv.exists():
            self.global_df.to_csv(output_csv, mode='a', header=False, index=False)
        else:
            self.global_df.to_csv(output_csv, index=False)  # Save DataFrame to CSV


if __name__ == '__main__':
    # Get the list of all items in the folder where the raw data (HTML) is stored.
    # The folder names are the band names, prefix with aurn
    raw_folder = "data/aurn/raw_data"
    bands = os.listdir(raw_folder)

    for band_name in bands:
        print("Processing: ", band_name)
        html_folder = f"{raw_folder}/{band_name}"
        html_items = os.listdir(html_folder)
        for html in html_items:
            data_processor = DataProcessing(band_name, html)
            data_processor.save_csv_dataset()
