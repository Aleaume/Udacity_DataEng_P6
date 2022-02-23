import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dl.cfg')

ARN      = config.get("IAM_ROLE", "ARN")

# DROP TABLES

airport_table_drop = "DROP table IF EXISTS airport"
cities_table_drop = "DROP table IF EXISTS cities"
population_table_drop = "DROP table IF EXISTS population"
race_table_drop = "DROP table IF EXISTS race"
immigration_table_drop = "DROP table IF EXISTS immigration"
immigration_port_table_drop = "DROP table IF EXISTS immigration_port"
weather_table_drop = "DROP table IF EXISTS weather"

# CREATE TABLES

airport_table_create= ("""CREATE TABLE IF NOT EXISTS airport(\
                                cityState varchar,
                                name varchar,
                                local_code varchar,
                                type varchar,
                                coordinates varchar
    )

""")

cities_table_create = ("""CREATE TABLE IF NOT EXISTS cities(\
                    cityState varchar,\
                    state_code varchar,\
                    city varchar,\
                    state_name varchar);

""")

population_table_create = ("""CREATE TABLE IF NOT EXISTS population(\
                        cityState varchar, \
                        median_age float, \
                        male_population int,\
                        female_population int,\
                        total_population int,\
                        number_veterans int,\
                        foreign_born int,\
                        avg_household_size float);
""")

race_table_create = ("""CREATE TABLE IF NOT EXISTS race(\
                    cityState varchar,\
                    race varchar,\
                    count int);
""")

immigration_table_create = ("""CREATE TABLE IF NOT EXISTS immigration(\
                    entry_port varchar,\
                    year int,\
                    month int,\
                    arrdate timestamp,\
                    depdate timestamp,\
                    mode float,\
                    addr varchar,\
                    birthyear int,\
                    gender varchar,\
                    occupation varchar,\
                    airline varchar,\
                    flightno varchar,\
                    entdepa varchar,\
                    entdepd varchar,\
                    entdepu varchar,\
                    matflag varchar,\
                    visatype varchar);
""")

immigration_port_table_create = ("""CREATE TABLE IF NOT EXISTS immigration_port(\
                        cityState varchar,\
                        city varchar,\
                        state_code varchar,\
                        country varchar,\
                        immigration_code varchar);
""")

weather_table_create = ("""CREATE TABLE IF NOT EXISTS weather (\
                    city varchar,\
                    avgTemp float,\
                    year int,\
                    month int);
""")

# FILLING TABLES

airport_copy = ("""
copy airport from 's3://udacityexercisealeaume/P6/output/airports/airports.csv' 
credentials 'aws_iam_role={}'
region 'us-east-1'
IGNOREHEADER 1
CSV;
""").format(ARN)

cities_copy = ("""
copy cities from 's3://udacityexercisealeaume/P6/output/demographics/cities.csv' 
credentials 'aws_iam_role={}'
region 'us-east-1'
IGNOREHEADER 1
CSV;
""").format(ARN)

population_copy = ("""
copy population from 's3://udacityexercisealeaume/P6/output/demographics/population.csv' 
credentials 'aws_iam_role={}'
region 'us-east-1'
IGNOREHEADER 1
CSV;
""").format(ARN)

race_copy = ("""
copy race from 's3://udacityexercisealeaume/P6/output/demographics/races.csv' 
credentials 'aws_iam_role={}'
region 'us-east-1'
IGNOREHEADER 1
CSV;
""").format(ARN)

immigration_port_copy = ("""
copy immigration_port from 's3://udacityexercisealeaume/P6/output/immigration_port/immigration_port.csv' 
credentials 'aws_iam_role={}'
region 'us-east-1'
IGNOREHEADER 1
CSV;
""").format(ARN)

weather_copy = ("""
copy weather from 's3://udacityexercisealeaume/P6/output/weather/weather.csv' 
credentials 'aws_iam_role={}'
region 'us-east-1'
IGNOREHEADER 1
CSV;
""").format(ARN)

immigration_copy = ("""
copy immigration(entry_port,year,month,arrdate,depdate,mode,addr,birthyear,gender,occupation,airline,flightno,entdepa,entdepd,entdepu,matflag,visatype)
from 's3://udacityexercisealeaume/P6/output/immigration/' 
credentials 'aws_iam_role={}'
format parquet SERIALIZETOJSON;
""").format(ARN)



sql_count_airport = "SELECT COUNT(*) FROM airport;"
sql_count_cities = "SELECT COUNT(*) FROM cities"
sql_count_population = "SELECT COUNT(*) FROM population;"
sql_count_race = "SELECT COUNT(*) FROM race;"
sql_count_immigration_port = "SELECT COUNT(*) FROM immigration_port;"
sql_count_weather = "SELECT COUNT(*) FROM weather;"
sql_count_immigration = "SELECT COUNT(*) FROM immigration;"

#SOURCE / HELP : for timestamp conversion, https://stackoverflow.com/questions/39815425/how-to-convert-epoch-to-datetime-redshift + discussions in udacity forum 

# QUERY LISTS

create_table_queries = [airport_table_create,cities_table_create,population_table_create,race_table_create,immigration_table_create,immigration_port_table_create,weather_table_create]
drop_table_queries = [airport_table_drop,cities_table_drop,population_table_drop,race_table_drop,immigration_table_drop,immigration_port_table_drop,weather_table_drop]
copy_table_queries = [airport_copy,cities_copy,population_copy,race_copy,immigration_port_copy,weather_copy,immigration_copy]
sql_counts = [sql_count_airport, sql_count_cities, sql_count_population, sql_count_race, sql_count_immigration_port, sql_count_weather, sql_count_immigration]