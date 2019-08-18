import sqlalchemy
#from sqlalchemy import create_engine
from sqlalchemy import Table
from sqlalchemy import MetaData
import pandas as pd
import requests
import io

class WeatherForcastWrapper:

    #dwh
    db_engine = sqlalchemy.create_engine('postgresql://postgres:2Lq90U9Kc6K438A@spark-home-test.c6wjrol75jy8.eu-central-1.rds.amazonaws.com:5432/postgres')
    db_conn = db_engine.connect()

    #weather forecast history
    history_table_name = 'stg_fc_history'
    history_files_base_url = 'https://www.metoffice.gov.uk/pub/data/weather/uk/climate/stationdata/'

    #weather data - current & forecast
    api_base_url = 'http://api.openweathermap.org/data/2.5/{weather_data_type}'
    app_id = 'e749eb638659a70d6cd190ec65dc5f62'

    #db tables
    forecast_stg_table = 'stg_fc'
    current_weather_stg_table = 'stg_current'

    weather_data_types = [
                {'data_type_name':'Current Weather','api_call_weather_type':'weather','staging_table': current_weather_stg_table},
                {'data_type_name': 'Weather Forecast', 'api_call_weather_type': 'forecast','staging_table': forecast_stg_table},
            ]

    stations = [
        {'station_name':'Aberporth,uk','station_history_file':'aberporthdata'},
        {'station_name': 'Armagh,uk', 'station_history_file': 'armaghdata'},
        {'station_name': 'Durham,uk', 'station_history_file': 'durhamdata'},
        {'station_name': 'Bradford,uk', 'station_history_file': 'bradforddata'},
        {'station_name': 'Camborne,uk', 'station_history_file': 'cambornedata'},
    ]

    def find_header_line_in_file(self,file,header_text):
        i=0
        for line in file:
            i+=1
            if header_text in line:
                return i

    def load_history_to_staging(self):
        for station in self.stations:
            data_file_url = self.history_files_base_url + station['station_history_file'] + '.txt'
            r = requests.get(data_file_url)
            data = r.content


            #find the file's header
            f = io.StringIO(data.decode('utf-8'))
            header_line = self.find_header_line_in_file(f,' yyyy')


            #create data frame based on the file
            df = pd.read_csv(io.StringIO(data.decode('utf-8')),
                             delim_whitespace=True,
                             engine='python',
                             usecols=[0,1,2,3,4,5,6],
                             skiprows=header_line+1,
                             names=['year','month','tmax_degc','tmin_degc','af_days','rain_mm','sun_hours'],
                             dtype={'year':'str','month':'str','tmax_degc':'str','tmin_degc':'str','af_days':'str','rain_mm':'str','sun_hours':'str'},
                             comment='#',
                             error_bad_lines=False)
            df.insert(0,column='station_name',value=station['station_name'])
            print(df.head(3))
            df.to_sql(self.history_table_name,self.db_conn,if_exists='append',index=False)


    def ingest_weather_data(self):

        for station in self.stations:
            for weather_data_type in self.weather_data_types:
                #sqlalchemy table object
                metadata = sqlalchemy.MetaData()
                stg_table = sqlalchemy.Table(weather_data_type['staging_table'], metadata, autoload=True,
                                                 autoload_with=self.db_engine)

                #get data from the api
                weather_data_json = self.get_weather_data_from_api(station['station_name'],weather_data_type['api_call_weather_type'])

                #truncate the staging table
                self.db_engine.execute(sqlalchemy.text("truncate table {stg_table}".format(stg_table=weather_data_type['staging_table'])))

                #insert new data to staging
                insert_stmt = sqlalchemy.insert(stg_table,values={'weather_data':weather_data_json})

                self.db_conn.execute(insert_stmt)

        self.db_conn.close()


    def get_weather_data_from_api(self,station_name,weather_data_type):

        payload = {'q': station_name, 'APPID': self.app_id, 'units': 'metric'}
        r = requests.get(self.api_base_url.format(weather_data_type=weather_data_type), params=payload)

        return r.json()

wfl = WeatherForcastWrapper()
#wfl.load_history_to_staging()
wfl.ingest_weather_data()

