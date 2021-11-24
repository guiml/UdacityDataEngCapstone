from pyspark.sql.types import IntegerType
from pyspark.sql import functions as F
from pyspark.sql.functions import col, upper, split, when
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession

'''
Initiate the spark session, 
calling the SAS package for reading
BDAT files
'''

spark = SparkSession.builder.\
        config("spark.jars.packages","saurfang:spark-sas7bdat:3.0.0-s_2.12")\
        .enableHiveSupport().getOrCreate()

'''
Define the connection variables
for reading the input files
And read them from the S3 buckets
'''

paths3 = 's3://BUCKETNAME/'
i94sas = '18-83510-I94-Data-2016/i94_jan16_sub.sas7bdat'
portcode = 'port_codes.csv'
countrycode = 'country_codes.csv'
citiestemp = 'GlobalLandTemperaturesByCity.csv'
citiesdem = 'us-cities-demographics.json'

portcode_df = spark.read.format("csv").option("header","true").load(paths3+portcode)
countrycoder_df = spark.read.format("csv").option("header","true").load(paths3+countrycode)
countrycodec_df = spark.read.format("csv").option("header","true").load(paths3+countrycode)
i94_df = spark.read.format('com.github.saurfang.sas.spark').load(paths3+i94sas)
citiestemp_df = spark.read.format('csv').option('header','true').load(paths3+citiestemp)
citiesdem_df = spark.read.format('json').option('header','true').load(paths3+citiesdem)

i94_df = i94_df.limit(1000)

'''
Create dataframe for mapping
the country codes in the original
I94 dataset.
Two datasets created to avoid error 
of matching same column twice
'''

countrycodec_df = countrycodec_df.withColumnRenamed("COUNTRY","cit_country")
countrycoder_df = countrycoder_df.withColumnRenamed("COUNTRY","res_country")


'''
Creation of dataset for the Fact Table Arrivals
Define the usefull columns for the 
dataframe.
Map city names to port codes.
'''

Arrivals_df = i94_df[['cicid','i94port','airline','fltno','depdate','arrdate','entdepd','entdepa']]
Arrivals_df = Arrivals_df.join(portcode_df,Arrivals_df.i94port == portcode_df.CODE,"inner")
Arrivals_df = Arrivals_df.withColumn('cityname', split(Arrivals_df['city'], ',').getItem(0))

'''
Creation of Dim Table Immigrants
Mapping of country codes from the
original I94 dataset
'''

Immigrants_df = i94_df[['cicid','i94cit','i94res','i94visa','visapost','dtaddto','visatype']]
Immigrants_df = Immigrants_df.join(countrycodec_df,Immigrants_df.i94cit == countrycodec_df.CODE,"inner")
Immigrants_df = Immigrants_df.join(countrycoder_df,Immigrants_df.i94res == countrycoder_df.CODE,"inner")
Immigrants_df = Immigrants_df.drop('_c0','CODE')

'''
Creation of Dim Table Personal Demographics
'''

PersonalDem_df = i94_df[['cicid','biryear','gender']]

'''
Creation of Dim Table Cities Demographics
Filtering only United States since the 
orientation of this project
is to analyze the Arrivals/Departures
in US cities
'''

citiestemp_df = citiestemp_df.filter(citiestemp_df.Country =='United States')
citiestemp_df = citiestemp_df[['AverageTemperature','City']]
citiestemp_df = citiestemp_df.withColumn('AverageTemperature', citiestemp_df['AverageTemperature'].cast(IntegerType()))

'''
Creation of Dim Table Cities Temperatures
Transformation of multiple records per city
into single record per city to facilitate
analysis
'''

Cities_Temp_min_df = citiestemp_df.groupBy('City').min('AverageTemperature')
Cities_Temp_min_df = Cities_Temp_min_df.withColumnRenamed('City','CityMin')
Cities_Temp_max_df = citiestemp_df.groupBy('City').max('AverageTemperature')
Cities_Temp_max_df = Cities_Temp_max_df.withColumnRenamed('City','CityMax')
Cities_Temp_min_df = Cities_Temp_min_df.withColumnRenamed('min(AverageTemperature)','MinTemperature')
Cities_Temp_max_df = Cities_Temp_max_df.withColumnRenamed('max(AverageTemperature)','MaxTemperature')
Cities_Temp_avg_df = citiestemp_df.groupBy('City').avg('AverageTemperature')
Cities_Temp_avg_df = Cities_Temp_avg_df.withColumnRenamed('avg(AverageTemperature)','AvgTemperature')
citiestemp_df = Cities_Temp_min_df.join(Cities_Temp_max_df,Cities_Temp_min_df.CityMin == Cities_Temp_max_df.CityMax,'inner')
citiestemp_df = citiestemp_df.join(Cities_Temp_avg_df,citiestemp_df.CityMin == Cities_Temp_avg_df.City,'inner')
citiestemp_df = citiestemp_df.drop('CityMin','CityMax')

'''
Creation of Dim Table Cities Demographics
Data Quality Correction:
Exploding enclosed JSON structure
'''

citiesdem_df = citiesdem_df.select('fields.*')
citiesdem_df = citiesdem_df.withColumn('city', upper(col('city')))

'''
Data Quality Correction:
Correction of city names for facilitating
analysis when consulting 2 cities in the 
top20 list of cities with most appearances
that would not result in successfull mapping 
'''

Arrivals_df = Arrivals_df.withColumn('cityname', when(Arrivals_df.cityname == 'NEWARK/TETERBORO','NEWARK') \
        .when(Arrivals_df.cityname == 'WASHINGTON DC','WASHINGTON') \
        .when(Arrivals_df.cityname == 'ST PAUL','SAINT PAUL') \
        .otherwise(Arrivals_df.cityname))



'''
Store Arrivals/Departures informations in the
arrivals table in Redshift
'''

print('##### ')
print('##### ')
print('  Arrivals    ')
print('##### ')
print('##### ')

Arrivals_df = Arrivals_df[['cicid','i94port','airline','depdate','arrdate','city','cityname']]
Arrivals_df.write.format('jdbc').options(
        url='jdbc:redshift://REDSHIFT_INSTANCE:5439/i94',
        driver='com.amazon.redshift.jdbc42.Driver',
        dbtable='arrivals',
        user='USER',
        password='PASSWORD').mode('overwrite').save()

'''
Store Immigration informations in the
Immigration table in Redshift
'''

print('##### ')
print('##### ')
print('  Immigrants    ')
print('##### ')
print('##### ')

Immigrants_df = Immigrants_df[['cicid','i94visa','dtaddto','visatype','cit_country','res_country']]
Immigrants_df.write.format('jdbc').options(
        url='jdbc:redshift://REDSHIFT_INSTANCE:5439/i94',
        driver='com.amazon.redshift.jdbc42.Driver',
        dbtable='immigrants',
        user='USER',
        password='PASSWORD').mode('overwrite').save()

'''
Store Personal Demographics informations in the
Personal Demographics table in Redshift
'''

print('##### ')
print('##### ')
print('  PersonalDem    ')
print('##### ')
print('##### ')

PersonalDem_df.write.format('jdbc').options(
        url='jdbc:redshift://REDSHIFT_INSTANCE:5439/i94',
        driver='com.amazon.redshift.jdbc42.Driver',
        dbtable='personal_demographics',
        user='USER',
        password='PASSWORD').mode('overwrite').save()

'''
Store Cities Temperatures informations in the
Personal Cities Temperatures table in Redshift
'''

print('##### ')
print('##### ')
print('  CitiesTemp    ')
print('##### ')
print('##### ')

citiestemp_df.write.format('jdbc').options(
        url='jdbc:redshift://REDSHIFT_INSTANCE:5439/i94',
        driver='com.amazon.redshift.jdbc42.Driver',
        dbtable='cities_temperature',
        user='USER',
        password='PASSWORD').mode('overwrite').save()

'''
Store Cities Demographics informations in the
Personal Cities Demographics table in Redshift
'''

print('##### ')
print('##### ')
print('  CitiesDem    ')
print('##### ')
print('##### ')

citiesdem_df.write.format('jdbc').options(
        url='jdbc:redshift://REDSHIFT_INSTANCE:5439/i94',
        driver='com.amazon.redshift.jdbc42.Driver',
        dbtable='cities_demographics',
        user='USER',
        password='PASSWORD').mode('overwrite').save()