from cottontaildb_client import CottontailDBClient, Type, Literal, column_def
import pandas
from haversine import haversine, Unit

#  This script imports the lsc2021-metadata.csv into Cottontail DB. It creates a new table cineast_distinctlocations.
#  With this, it is possible to obtain names of locations (that the life-logger visited) and the respective GPS data and
#  it therewith serves as autocomplete.
with CottontailDBClient('localhost', 1865) as client:
    # Drop entity
    #  client.drop_entity('cineast', 'cineast_distinctlocations')

    # Define entity columns
    columns = [
        column_def('semantic_name', Type.STRING, primary=True, nullable=False),
        column_def('lat', Type.FLOAT, nullable=False),
        column_def('lon', Type.FLOAT, nullable=False),
    ]
    # Create entity
    client.create_entity('cineast', 'cineast_distinctlocations', columns)

    df = pandas.read_csv('lsc2021-metadata.csv', index_col=False, sep=',', encoding='utf-8')
    columns = ['semantic_name', 'lat', 'lon']

    distinct_semantic_lat = {}
    distinct_semantic_lon = {}
    name_occurences = {}

    for index, row in df.iterrows():
        latitude = row['lat']
        longitude = row['lon']
        semantic_name = row['semantic_name']

        if isinstance(latitude, float) and isinstance(longitude, float) and isinstance(semantic_name, str):
            if semantic_name in name_occurences:
                isClose = False
                same_names = [semantic_name + " (" + str(num) + ")" for num in range(name_occurences[semantic_name])]
                for name in same_names:
                    if name in distinct_semantic_lat and name in distinct_semantic_lon:
                        point_in_dict = (distinct_semantic_lat[name], distinct_semantic_lon[name])
                        if haversine((latitude, longitude), point_in_dict, unit=Unit.METERS) <= 3000:  # 3km
                            isClose = True
                            break  # exit inner loop because cluster found for current point
                if not isClose:
                    distinct_semantic_lat[semantic_name + " (" + str(name_occurences[semantic_name]) + ")"] = latitude
                    distinct_semantic_lon[semantic_name + " (" + str(name_occurences[semantic_name]) + ")"] = longitude
                    name_occurences[semantic_name] += 1
            else:
                distinct_semantic_lat[semantic_name + " (" + str(0) + ")"] = latitude  # key not in dict
                distinct_semantic_lon[semantic_name + " (" + str(0) + ")"] = longitude
                name_occurences[semantic_name] = 1


    for key in distinct_semantic_lat.keys():
        values = [[Literal(stringData=key), Literal(floatData=distinct_semantic_lat[key]), Literal(floatData=distinct_semantic_lon[key])]]
        client.insert_batch('cineast', 'cineast_distinctlocations', columns, values)
        #  print(key + " " + str(distinct_semantic_lat[key]) + " " + str(distinct_semantic_lon[key]))

    print("length of list = " + str(len(distinct_semantic_lat.keys())))


