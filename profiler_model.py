import datetime
from enum import Enum, IntEnum
import uuid
import statistics
import pandas as pd


#domain model
#enums
class CustomerType(Enum):
    Unknown = 0
    Residential = 1
    Commercial = 2
    Industrial = 3

class UnitOfMeasure(Enum):
    Unknown = 0
    kWh = 1
    kW = 2


class IntervalMinutes(Enum):
    Unknown = 0
    One = 1
    Five = 2
    Fifteen = 15
class IntervalDayType(IntEnum):
    Unknown = 0
    Weekday = 1
    Weekend = 2


class BuildingType(Enum):
    Unknown = 0
    Standalone = 1
    Multitenant = 2


#classes
class Facility:
    id = uuid.uuid1()
    name = None
    postal_code = None
    customer_type = CustomerType.Unknown
    building_type = BuildingType.Unknown


class Profile:
    id = uuid.uuid1()
    name = None
    facility = None
    points = []


class ProfileInterval:
    id = uuid.uuid1()
    week = 0
    hour = 0
    quarter_hour = 0
    interval_day_type = IntervalDayType.Unknown
    profile = None
    upper_bound = 0
    lower_bound = 0





# profiler
# interfaces



class Persistor:

    def persist(self, object):
        return False

    def load(self, object) -> [object]:
        return object()



class ProfileRule:
    def check(self,value) -> bool:
        return False

class ZeroValueProfileRule(ProfileRule):
    def check(self,value):
        if value == 0:
            return False
        else:
            return True



class RawDataInterval:
    date_time = datetime.datetime.now()
    unit_of_measure = UnitOfMeasure.Unknown
    value = 0.0
    hour = 0
    quarter_hour = 0
    interval_day_type = IntervalDayType.Unknown
    day_of_week = 0
    week = 0

# utility methods
class ExtractedDateParts:
    # this is just for use by the date part extractor
    week = 0
    hour = 0
    quarter_hour = 0
    interval_day_type = IntervalDayType.Unknown
    day_of_week = 0

class UnitOfMeasureConverter:
    @staticmethod
    def kwh_to_kw(kwh:float,interval_in_minutes:int) -> float:
        interval_in_hours = interval_in_minutes / 60
        kw = float(kwh / interval_in_hours)
        return kw

    @staticmethod
    def kw_to_kwh(kw: float, interval_in_minutes: int) -> float:
        interval_in_hours = interval_in_minutes / 60
        kwh = float(kw * interval_in_hours)
        return kwh

class DatePartExtractor:
    @staticmethod
    def extract(date_time:str) -> ExtractedDateParts:
        date_obj = datetime.datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S")
        extracted_date_parts = ExtractedDateParts()
        extracted_date_parts.week = date_obj.isocalendar()[1]
        extracted_date_parts.hour = date_obj.hour
        extracted_date_parts.quarter_hour = DatePartExtractor.quarter_hour(date_obj)
        extracted_date_parts.interval_day_type = DatePartExtractor.weekend_or_weekday(date_obj)
        extracted_date_parts.day_of_week = date_obj.isoweekday()
        return extracted_date_parts
    @staticmethod
    def weekend_or_weekday(date_obj:datetime.datetime) -> IntervalDayType:
        day_of_week = date_obj.isoweekday()
        interval_type = IntervalDayType.Unknown
        if 0 < day_of_week < 6:
            interval_type = IntervalDayType.Weekday
        elif day_of_week == 6 or day_of_week == 7:
            interval_type = IntervalDayType.Weekend
        return interval_type
    @staticmethod
    def quarter_hour(date_obj:datetime.datetime) -> int:
        quarter_hour = 0
        minute = date_obj.minute
        if minute < 15:
            quarter_hour = 1
        elif minute < 30:
            quarter_hour = 2
        elif minute < 45:
            quarter_hour = 3
        elif minute < 60:
            quarter_hour = 4
        else:
            # something weird happened
            quarter_hour = 5
        return quarter_hour

class Profiler:
    @staticmethod
    def get_baseline(raw_data:[RawDataInterval]) -> float:
        raw_value_sum = 0.0
        for raw_data_interval in raw_data:
            raw_value_sum += float(raw_data_interval.value)
        baseline = raw_value_sum / len(raw_data)
        return baseline

    @staticmethod
    def generate(rules:[ProfileRule], raw_intervals, profile=Profile()) -> [ProfileInterval]:
        empty_profile_intervals = Profiler.get_one_year_of_empty_profile_intervals()
        data_frame = pd.DataFrame([o.__dict__ for o in raw_intervals])
        baseline = Profiler.get_baseline(raw_intervals)
        return_profile_intervals = []
        print(f'Generating {len(empty_profile_intervals)} profile intervals:')
        for profile_interval in empty_profile_intervals:
            if profile_interval.interval_day_type != 'IntervalDayType.Unknown':
                raw_data_rows = data_frame[
                    (data_frame['week']==profile_interval.week)
                    & (data_frame['hour']==profile_interval.hour)
                    & (data_frame['quarter_hour']==profile_interval.quarter_hour)
                    & (data_frame['interval_day_type']==profile_interval.interval_day_type)
                ]
                raw_values = raw_data_rows['value'].tolist()
                if(len(raw_values)>0):
                    for raw_value in raw_values:
                        for rule in rules:
                            if not rule.check(raw_value):
                                raw_values.remove(raw_value)
                    mean = sum(raw_values) / len(raw_values)
                    std_dev = statistics.stdev(raw_values)
                    profile_interval.upper_bound = (mean + (std_dev/2))/baseline
                    profile_interval.lower_bound = (mean - (std_dev/2))/baseline
                    #print(f"appending profile - {profile_interval.week}:{int(profile_interval.interval_day_type)}:{profile_interval.hour}:{profile_interval.quarter_hour} - Values - U: {profile_interval.upper_bound} L:{profile_interval.upper_bound}")
                    profile_interval.profile = profile.id
                    return_profile_intervals.append(profile_interval)
                else:
                    print(f'No matches for profile point:')
                    print(f'Week: {profile_interval.week}')
                    print(f'Hour: {profile_interval.hour}')
                    print(f'Quarter Hour: {profile_interval.quarter_hour}')
                    print(f'Day type: {profile_interval.interval_day_type}')
            else:
                print('excluding interval with an invalid day type')
        return return_profile_intervals


    @staticmethod
    def get_one_year_of_empty_profile_intervals() -> [ProfileInterval]:
        profile_intervals = []
        for w in range(1,53):
            for h in range(0,24):
                for q in range(1,5):
                    weekend_profile_interval = ProfileInterval()
                    weekend_profile_interval.interval_day_type = IntervalDayType.Weekend
                    weekend_profile_interval.week = w
                    weekend_profile_interval.hour = h
                    weekend_profile_interval.quarter_hour = q
                    weekday_profile_interval = ProfileInterval()
                    weekday_profile_interval.interval_day_type = IntervalDayType.Weekday
                    weekday_profile_interval.week = w
                    weekday_profile_interval.hour = h
                    weekday_profile_interval.quarter_hour = q
                    profile_intervals.append(weekend_profile_interval)
                    profile_intervals.append(weekday_profile_interval)
        return profile_intervals


