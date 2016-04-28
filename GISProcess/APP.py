import os
import sys
from operator import add

# read shapefile
import shapefile

# pngcanvas 1.0.3: A minimalist library to render PNG images using pure Python.

# Path for spark source folder
os.environ['SPARK_HOME'] = "/Users/dustinchen/Documents/APP/spark-1.6.1-bin-hadoop2.6"

# Path for pyspark and py4j
sys.path.append("/Users/dustinchen/Documents/APP/spark-1.6.1-bin-hadoop2.6/python")
sys.path.append("/Users/dustinchen/Documents/APP/spark-1.6.1-bin-hadoop2.6/python/lib/py4j-0.9-src.zip")

try:
    from pyspark import SparkConf, SparkContext, SQLContext
    from pyspark.sql.functions import regexp_extract
    from pyspark.sql import Row

except ImportError as e:
    print ("Can not import Spark Modules", e)

if __name__ == "__main__":
    conf = SparkConf().setAppName("GISAPP").setMaster("local")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    nyc_shapefile = shapefile.Reader("/Users/dustinchen/Documents/APP/Resources/NY_counties_clip/NY_counties_clip.shp")
    """
       0 ('deletionflag', 'c', 1, 0)
       1 ['objectid', 'n', 9, 0]
       2 ['statefp', 'c', 2, 0]
       3 ['countyfp', 'c', 3, 0]
       4 ['countyns', 'c', 8, 0]
       5 ['geoid', 'c', 5, 0]
       6 ['name', 'c', 100, 0]
        ['namelsad', 'c', 100, 0]
        ['lsad', 'c', 2, 0]
        ['classfp', 'c', 2, 0]
        ['mtfcc', 'c', 5, 0]
        ['csafp', 'c', 3, 0]
        ['cbsafp', 'c', 5, 0]
        ['metdivfp', 'c', 5, 0]
        ['funcstat', 'c', 1, 0]
        ['aland', 'f', 19, 11]
        ['awater', 'f', 19, 11]
        ['intptlat', 'c', 11, 0]
        ['intptlon', 'c', 12, 0]
        ['shape_leng', 'f', 19, 11]
        ['shape_area', 'f', 19, 11]
    """

    # Input the trip data
    tripsData = sqlContext.read.parquet("/Users/dustinchen/Documents/APP/Resources/Green_parquet/green_2015-01.parquet")
    # tripData.printShema()
    # tripsData.show(10)
    """
        root
        |-- lpep_pickup_datetime: string (nullable = true)
        |-- Pickup_longitude: double (nullable = true)
        |-- Pickup_latitude: double (nullable = true)
        |-- Dropoff_longitude: double (nullable = true)
        |-- Dropoff_latitude: double (nullable = true)
        |-- Passenger_count: integer (nullable = true)
        |-- Total_amount: double (nullable = true)
        |-- Trip_type: integer (nullable = true)
    """
    tripsData.registerTempTable("tripsData")

    # The trips whose pickup time is between 2am -- 6am
    GIS_Points = sqlContext.sql("select lpep_pickup_datetime, Pickup_longitude, Pickup_latitude from tripsData " +
                                "where lpep_pickup_datetime like '%2015-__-__ 02:%' "
                                "or lpep_pickup_datetime like '%2015-__-__ 03:%' "
                                "or lpep_pickup_datetime like '%2015-__-__ 04:%' "
                                "or lpep_pickup_datetime like '%2015-__-__ 05:%' "
                                ).cache()
    sqlContext.dropTempTable("tripsData")

    # This is a list
    shapeRecs = nyc_shapefile.shapeRecords()

    def point_in_poly(x, y, poly):

        n = len(poly)
        inside = False

        p1x, p1y = poly[0]
        for i in range(n + 1):
            p2x, p2y = poly[i % n]
            if y > min(p1y, p2y):
                if y <= max(p1y, p2y):
                    if x <= max(p1x, p2x):
                        if p1y != p2y:
                            xints = (y - p1y) * (p2x - p1x) / (p2y - p1y) + p1x
                        if p1x == p2x or x <= xints:
                            inside = not inside
            p1x, p1y = p2x, p2y

        return inside


    def parse(row):
        #print "Judge point"
        try:
            x = float(row.Pickup_longitude)
            y = float(row.Pickup_latitude)
            print x, y
        except:
            print "Invalid Point"
            return "Invalid Point", 1

        find = False
        for sr in shapeRecs:
            coords = sr.shape.points
            if point_in_poly(x, y, coords):
                #county = sr.record[6]
                find = True
                #print '********Find'
                break
        if find:
            return sr.record[6], 1
        else:
            print "-------Not Find"
            return 'NotFindedPoints', 1

            # trips = sc.textFile("/Users/dustinchen/Documents/APP/Resources/Yellow/yellow_tripdata_2015-01.csv").map(
            #     parse).reduceByKey(add).sortBy(lambda x: x[1], False)
            # trips.saveAsTextFile("/Users/dustinchen/Documents/APP/Resources/Yellow/yellowOutput/yellow__2015-01")


    output = GIS_Points.map(parse).reduceByKey(add).sortBy(lambda x: x[1], False)
    output.collect().saveAsTextFile("out/GIS")

    sc.stop()
