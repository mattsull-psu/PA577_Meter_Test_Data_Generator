import uuid

from profiler_model import *
from generator_model import *
import os
import csv
import pandas as pd
from csv import DictWriter

def initialize_csv_file(fields, filepath):
    if not os.path.exists(filepath):
        with open(filepath, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, delimiter=",", fieldnames=fields)
            writer.writeheader()


class CsvProfilePersistor(Persistor):
    # this is the default. You can either change this code, or overwrite the default variable at runtime
    fields = ['id', 'name', 'facility']
    filepath = 'Data\\Profiles.csv'

    def __init__(self):
        initialize_csv_file(fields=self.fields, filepath=self.filepath)

    def persist(self, profiles:[Profile]):
        rows = []
        df = pd.read_csv(self.filepath, names=self.fields, index_col=['id'], header=0)
        for profile in profiles:
            row_count = sum(df.index == profile.id)
            if row_count == 1:
                df.loc[profile.id, 'name'] = profile.name
                df.loc[profile.id, 'facility'] = profile.facility
                print(f"Updated profile id {profile.id}")
            elif row_count == 0:
                df.loc[profile.id] = [profile.name, profile.facility]
                print(f"Created new profile id {profile.id}")
            elif row_count > 1:
                raise f"Multiple rows found when saving profile id {profile.id}"
            else:
                print('Something went wrong while getting a count of profiles')
        df.to_csv(self.filepath)

    def load_all(self) -> [object]:
       profiles = []

       df = pd.read_csv(self.filepath,header=0)
       for i in range(len(df)):
           profile = Profile()
           profile.id = df.loc[i,'id']
           profile.facility = df.loc[i,'facility']
           profile.name = df.loc[i,'name']
           profiles.append(profile)
       return profiles

    def load(self, profile_id:str)-> Profile:
        profile = Profile()
        profile_id = str(profile_id)

        df = pd.read_csv(self.filepath, header=0)
        df = df[df['id'] == str(profile_id)]
        if len(df) > 0:
            profile.id = profile_id
            profile.facility = df.iloc[0]['facility']
            profile.name = df.iloc[0]['name']
        return profile
class CsvFacilityPersistor(Persistor):
    # this is the default. You can either change this code, or overwrite the default variable at runtime
    fields = ['id', 'name', 'postal_code', 'customer_type', 'building_type']
    filepath = 'Data\\Facilities.csv'

    def __init__(self):
        initialize_csv_file(filepath=self.filepath, fields=self.fields)

    def persist(self, facilities:[Facility]):
        rows = []
        df = pd.read_csv(self.filepath, names=self.fields, index_col=['id'], header=0)
        for facility in facilities:
            row_count = sum(df.index == facility.id)
            if row_count == 1:
                df.loc[facility.id, 'name'] = facility.name
                df.loc[facility.id, 'postal_code'] = facility.postal_code
                df.loc[facility.id, 'customer_type'] = facility.customer_type
                df.loc[facility.id, 'building_type'] = facility.building_type
                print(f"Updated facility id {facility.id}")
            elif row_count == 0:
                df.loc[facility.id] = [facility.name, facility.postal_code, facility.customer_type,
                                       facility.building_type]
                print(f"Created new facility id {facility.id}")
            elif row_count > 1:
                raise (PersistorException(f"Multiple rows found when saving facility id {facility.id}"))
            else:
                raise (
                    PersistorException("Something went wrong when getting the row count while saving a facility"))
        df.to_csv(self.filepath)

    def load_all(self) -> [object]:
        df = pd.read_csv(self.filepath, header=0)
        facilities = [Facility]
        for i in range(len(df)):
            this_facility = Facility()
            this_facility.id = df.loc[i, 'id']
            this_facility.name = df.loc[i, 'name']
            this_facility.building_type = df.loc[i, 'building_type']
            this_facility.customer_type = df.loc[i, 'customer_type']
            # add the points
            facilities.append(this_facility)
        return facilities

    def load(self, facilities:[Facility]) -> [object]:

        df = pd.read_csv(self.filepath, header=0)
        for i in range(len(df)):
            this_facility = Facility()
            this_facility.id = df.loc[i, 'id']
            this_facility.name = df.loc[i, 'name']
            this_facility.building_type = df.loc[i, 'building_type']
            this_facility.customer_type = df.loc[i, 'customer_type']
            facilities.append(this_facility)
        return facilities


class CsvProfileIntervalPersistor(Persistor):
    # this is the default. You can either change this code, or overwrite the default variable at runtime
    fields = ['id', 'profile', 'week', 'hour', 'quarter_hour', 'interval_day_type','upper_bound','lower_bound']
    filepath = 'Data\\ProfileIntervals.csv'

    def __init__(self):
        initialize_csv_file(filepath=self.filepath, fields=self.fields)

    def save_to_file(self, intervals:[ProfileInterval], filename):
        file_path = f'Data\\ProfileIntervals\\{filename}.csv'
        initialize_csv_file(fields=self.fields, filepath=file_path)
        df = pd.DataFrame([t.__dict__ for t in intervals])
        df.to_csv(file_path)

    def load(self,profile_id)-> [object]:
        file_path = f'Data\\ProfileIntervals\\{profile_id}.csv'
        df =  pd.read_csv(file_path, index_col=0, header=0)
        return df.to_dict('records')

    def persist(self, intervals:[ProfileInterval], filename=None):
        if filename == None:
            filename = 'ProfileIntervals'
        self.save_to_file(intervals, filename= filename)


class CsvMeterPersistor(Persistor):
    fields = ['id', 'frequency_in_minutes','name']
    filepath = 'Data\\Meters.csv'

    def __init__(self):
        initialize_csv_file(filepath=self.filepath, fields= self.fields)

    def persist(self, meters:[Meter]):
        df = pd.read_csv(self.filepath, names=self.fields, index_col=['id'], header=0)
        for meter in meters:
            row_count = sum(df.index == meter.id)
            if row_count == 1:
                df.loc[meter.id, 'frequency_in_minutes'] = meter.frequency_in_minutes
                df.loc[meter.id, 'name'] = meter.name
                print(f"Updated meter id {meter.id}")
            elif row_count == 0:
                df.loc[meter.id] = [
                    meter.frequency_in_minutes,
                    meter.name
                ]
                print(f"Created new meter id {meter.id}")
            elif row_count > 1:
                raise f"Multiple rows found when saving meter id {meter.id}"
            else:
                raise "Something went wrong when getting the row count while saving a meter"
        df.to_csv(self.filepath)

    def load(self, meter_id=None) -> [object]:
        meters = []
        df = pd.read_csv(self.filepath, header=0)
        if meter_id is not None:
            df = df[df['id'] == meter_id]
        for i in range(len(df)):
            this_meter = Meter()
            this_meter.id = df.loc[i, 'id']
            this_meter.frequency_in_minutes = df.loc[i, 'frequency_in_minutes']
            this_meter.name = df.loc[i, 'name']
            meters.append(this_meter)
        return meters
class CsvMeterDataPersistor(MeterDataPersistor):
    fields = ['id', 'meter_id', 'meter_name', 'date_time', 'value', 'interval_day_type', 'upper_bound', 'lower_bound']
    filepath = 'Data\\Meter\\MeterDataIntervals.csv'

    def __init__(self):
        initialize_csv_file(filepath=self.filepath, fields=self.fields)

    def persist(self,point):
        filepath = f'Data\\Meter\\{point.meter_id}.csv'
        with open(filepath,'a') as f_object:
            dw = DictWriter(f_object, fieldnames=['date_time','meter_id','meter_name','value'])
            dw.writerow(point)
            f_object.close()

    def bulk_save(self, points):
        for point in points:
            # This is not performant
            self.persist(point)

    def stream(self,point:GeneratedDataPoint):
        # change this with a new implementation
        self.standard_output_stream(point)
    def standard_output_stream(self,point:GeneratedDataPoint):
        print(f'meter:{point.meter_name}\tdate:{point.date_time}\tvalue:{point.value}')
        #self.persist(point)


class CsvRawDataLoader:

    def load(self,filename:str) -> [object]:
        fields = ['DATE_TIME','UNIT_OF_MEASURE','VALUE','INTERVAL_IN_MINUTES']
        raw_data_intervals = []
        df = pd.read_csv(filename, header=0)
        for i in range(len(df)):
            raw_data_interval = RawDataInterval()
            raw_data_interval.date_time = datetime.datetime.strptime(df.loc[i,'DATE_TIME'], "%Y-%m-%d %H:%M:%S")
            raw_data_interval.unit_of_measure = UnitOfMeasure[df.loc[i, 'UNIT_OF_MEASURE']]
            date_parts = DatePartExtractor.extract(df.loc[i,'DATE_TIME'])
            raw_data_interval.hour = date_parts.hour
            raw_data_interval.week = date_parts.week
            raw_data_interval.quarter_hour = date_parts.quarter_hour
            raw_data_interval.day_of_week = date_parts.day_of_week
            raw_data_interval.interval_day_type = date_parts.interval_day_type
            # the enum enforces that only 1,5,or 15 are valid intervals
            # will cause a syntax error if this is violated
            data_interval_length = int(df.loc[i, 'INTERVAL_IN_MINUTES'])
            # split into minutes
            raw_value = df.loc[i, 'VALUE'] / data_interval_length
            # standardize to kw
            if raw_data_interval.unit_of_measure == UnitOfMeasure.kWh:
                standardized_value = UnitOfMeasureConverter.kwh_to_kw(raw_value, 1)
            else:
                standardized_value = raw_value
            raw_data_interval.value = standardized_value
            raw_data_intervals.append(raw_data_interval)
        return raw_data_intervals

class PersistorException(Exception):
    def __init__(self, value):
        self.value = value

    # __str__ is to print() the value
    def __str__(self):
        return (repr(self.value))

