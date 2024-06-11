import datetime

from csv_persistor import *
from profiler_model import *
from generator_model import *



if __name__ == '__main__':

    print('In main')
    print(f'This script demonstrates the functionality of the meter data modules')
    facility = Facility()
    print('First we create a facility that we are modeling')
    facility.name = "Acme Apartments"
    facility.postal_code = '99999'
    facility.building_type = BuildingType.Multitenant
    facility.customer_type = CustomerType.Residential
    print(f"The facility has been created, and the id is{facility.id}")
    print('Next we will create two profiles for the facility. The first is for the meter in the common areas. The second is the meter for a residence')
    profile_1 = Profile()
    profile_1.name = "Apartment building common area"
    profile_1.facility = str(facility.id)
    print(f'Common area profile is {profile_1.id}')
    profile_2 = Profile()
    profile_2.name = 'Apartment building residence'
    print(f'Residence profile is {profile_2.id}')

    facility_id = str(facility.id)
    profile_1_id = str(profile_1.id)
    profile_2_id = str(profile_2.id)

    facility_persistor = CsvFacilityPersistor()
    profile_persistor = CsvProfilePersistor()
    point_persistor = CsvProfileIntervalPersistor()

    facility_persistor.persist([facility])
    profile_persistor.persist([profile_1, profile_2])
    print('The facility and profiles have been saved')
    print(f'Now loading raw data. The time is{datetime.datetime.now()}')
    raw_data = CsvRawDataLoader().load('Data\\raw_data_example_1.csv')
    print (f'{datetime.datetime.now()} - {len(raw_data)} raw data rows were retrieved')
    print('Adding a rule which discards any zero values while profiling')
    profile_rule = [ZeroValueProfileRule()]
    print(f'Generating a profile using the raw data - start time {datetime.datetime.now()}')
    profile_points = Profiler.generate(rules=profile_rule, raw_intervals=raw_data, profile=profile_1)
    print(f'{datetime.datetime.now()} - {len(profile_points)} profile points were generated')
    print(f'Saving the profile points for profile {profile_1.id}')
    point_persistor.persist(profile_points, profile_1.id)
    print(f'Loading previously created set of profile points')
    profile_points = point_persistor.load('2f6467f1-26ae-11ef-960c-f8e4e3db329b')
    print('Now generating data using meters and controller')
    meters = CsvMeterPersistor().load()
    print(f'{len(meters)} meters have been loaded')
    cmdp = CsvMeterDataPersistor()
    condition = EmptyCondition()
    # There are meters already generated with definitions for baseline value and reporting interval
    for meter in meters:
        print(f'Assigning the new profile to meter {meter.id}')
        meter.conditioners = [condition]
        meter.profile_intervals = profile_points
    print('Creating a new controller')
    controller = Controller()
    print('Specifying the CsvMeterDataPersistor type object as the persistor')
    controller.persistor = cmdp
    controller.meters = meters
    print('Starting streams. Use CTRL-C or the stop button to stop streaming')
    controller.stream()
    print('Done')

def print_hi(name):
    print(f'Hi, {name}')

def demonstrate():
    print(f'This script demonstrates the functionality of the meter data modules')
    facility = Facility()
    facility.name = "Acme Apartments"
    facility.postal_code = '99999'
    facility.building_type = BuildingType.Multitenant
    facility.customer_type = CustomerType.Residential
    print(f"The facility id is{facility.id}")
    profile_1 = Profile()
    profile_1.name = "Apartment building common area"
    profile_1.facility = facility.id
    print(f'Common area profile is {profile_1.id}')
    profile_2 = Profile()
    profile_2.name = 'Apartment building residence'
    print(f'Residence profile is {profile_2.id}')
    CsvFacilityPersistor().persist([facility])
    CsvProfilePersistor().persist([profile_1, profile_2])

    print('Now loading raw data')
    raw_data_1 = CsvRawDataLoader().load('Data\\raw_data_example_1.csv')
    raw_data_2 = CsvRawDataLoader().load('Data\\raw_data_example_2.csv')
    print('Done loading')
    point_persistor = CsvProfileIntervalPersistor()
    profile_rule = [ZeroValueProfileRule()]
    print('Generating first profile')
    profile_points_1 = Profiler.generate(rules=profile_rule, raw_intervals=raw_data_1, profile=profile_1)
    print('Generating second profile')
    profile_points_2 = Profiler.generate(rules=profile_rule, raw_intervals=raw_data_2, profile=profile_2)

    print('Saving first set of profile points')
    point_persistor.persist(profile_points_1, profile_1.id)
    print('Saving second set of profile points')
    point_persistor.persist(profile_points_2, profile_1.id)


def old_code():


    meter = Meter()
    meter.name = "Test meter"
    meter.frequency_in_minutes = 1
    meters.append(meter)
    meter_2 = Meter()
    meter_2.name = "Test meter 2"
    meter_2.frequency_in_minutes = 5
    meter_2.id = meter.id
    meters.append(meter_2)
    csv_meter_persistor = CsvMeterPersistor()
    csv_meter_persistor.persist(meters)
    facility_persistor = CsvFacilityPersistor()
    facilities = facility_persistor.load_all()
    print(f"{len(facilities)} loaded")
    facility = [facilities[0]]
    this_facility = facility_persistor.load(facility)
    print('hello')
    profile_persistor = CsvProfilePersistor()
    point_persistor = CsvProfileIntervalPersistor()
    raw_data = CsvRawDataLoader().load('Data\\raw_data_example_1.csv')
    profile_rule = [ZeroValueProfileRule()]
    profiles = profile_persistor.load_all()
    profile = profiles[0]
    profile_points = Profiler.generate(rules=profile_rule, raw_intervals=raw_data, profile=profile)
    point_persistor.persist(profile_points, profile.id)


    profile_persistor = CsvProfilePersistor()
    profiles = profile_persistor.load_all()
    profile = profiles[0]
    profile_intervals = CsvProfileIntervalPersistor().load('20e4269f-1d4d-11ef-9e0b-f8e4e3db3298')


    profile_id = '20e4269f-1d4d-11ef-9e0b-f8e4e3db3298'
    profile_persistor = CsvProfilePersistor()
    profile=profile_persistor.load(profile_id)
    print(profile.name)
    profile_intervals = CsvProfileIntervalPersistor().load(profile_id)
    print(f'{len(profile_intervals)} profiles loaded')
    profile_id = '20e4269f-1d4d-11ef-9e0b-f8e4e3db3298'
    profile_intervals = CsvProfileIntervalPersistor().load(profile_id)
    meters = CsvMeterPersistor().load()
    cmdp = CsvMeterDataPersistor()
    condition = EmptyCondition
    for meter in meters:
        meter.conditioners = [condition]
        meter.profile_intervals = profile_intervals
    controller = Controller()
    controller.persistor = cmdp
    controller.meters=meters
    controller.stream()

