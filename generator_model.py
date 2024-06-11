import time
import profiler_model
import uuid
from abc import ABC, abstractmethod
import datetime
from profiler_model import Profile, ProfileInterval, DatePartExtractor, ExtractedDateParts
import pandas as pd
import random


#interfaces
class Conditioner:
    @staticmethod
    def apply(value) -> bool:
        if value:
            return False
    @staticmethod
    def transform(value) -> float:
        return -.01

class EmptyCondition(Conditioner):
    @staticmethod
    def apply(value) -> bool:
        return True

    @staticmethod
    def transform(value) -> float:
        return value

class MeterDataPersistor:
    def persist(self,profile_point):
        return False

    def bulk_save(self, points):
        return False

    def stream(self,point):
        return False



class Controller:
    meters = []
    persistor = MeterDataPersistor()
    def say_and_sleep(self):
        second = datetime.datetime.now().second
        print(f'Sleeping for {60 - second} seconds')
        while second != 0:
            time.sleep(1)
            second = datetime.datetime.now().second
        # sleep an extra second
        time.sleep(1)

    def generate(self,start:datetime,end:datetime):
        data_points = []
        for meter in self.meters:
            current_time = start
            while current_time <= end:
                meter_data_point = meter.get_data_point()
                data_points.append(meter_data_point)
                time_change = datetime.timedelta(minutes=meter.frequency_in_minutes)
                current_time = current_time + time_change
        self.persistor.bulk_save(data_points)

    def stream(self) -> bool:
        current_time = datetime.datetime.now().replace(second=0)
        print('Starting stream')
        try:
            while True:
                self.say_and_sleep()
                for meter in self.meters:
                    if datetime.datetime.now().minute % meter.frequency_in_minutes ==0:
                        self.persistor.stream(meter.get_data_point(datetime.datetime.now().replace(second=0).strftime("%Y-%m-%d %H:%M:%S"),meter.baseline))
        except KeyboardInterrupt:
            print('Stopping stream')


class GeneratedDataPoint:
    meter_id = None
    meter_name = None
    date_time = datetime.datetime.now().replace(second=0)
    value = 0.0

class Meter:
    id = uuid.uuid1()
    frequency_in_minutes = 0
    name = None
    profile = None
    profile_intervals = None
    conditioners = [EmptyCondition()]
    baseline = 1000


    def __int__(self, profile:Profile=None, profile_intervals:[ProfileInterval]=None):
        if profile is None or profile_intervals is None:
            print('Profile and profile intervals must be provided to constructor')
            raise ValueError
        else:
            self.profile = profile
            self.profile_intervals = profile_intervals

    def get_data_point(self,date_time,baseline) -> GeneratedDataPoint:

        date_parts = DatePartExtractor.extract(date_time)
        df = pd.DataFrame.from_dict(self.profile_intervals)
        this_interval = df.loc[
            (df['week'] == date_parts.week)
            & (df['interval_day_type'] == date_parts.interval_day_type)
            & (df['hour']==date_parts.hour)
            & (df['quarter_hour']==date_parts.quarter_hour)
        ]
        upper_bound = 1.2
        lower_bound = .8
        if len(this_interval) >= 1:
            # always use the most recent profiled interval
            upper_bound = this_interval['upper_bound'].iloc[0]
            lower_bound = this_interval['lower_bound'].iloc[0]
        value = round((random.uniform(upper_bound,lower_bound) * baseline),2)
        for condition in self.conditioners:
            if condition.apply(value):
                value = condition.transform(value)
        data_point = GeneratedDataPoint()
        data_point.meter_id = self.id
        data_point.meter_name = self.name
        data_point.date_time = date_time
        data_point.value = value
        return data_point













