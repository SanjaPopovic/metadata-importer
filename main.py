from cottontaildb_client import CottontailDBClient, Type, Literal, column_def
import pandas

with CottontailDBClient('localhost', 1865) as client:
    # Define entity columns
    columns = [
        column_def('semantic_name', Type.STRING, primary=True, nullable=False),
        column_def('lat', Type.FLOAT, nullable=False),
        column_def('lon', Type.FLOAT, nullable=False)
    ]
    # Create entity
    client.create_entity('cineast', 'cineast_distinctlocations', columns)

    df = pandas.read_csv('lsc2021-metadata.csv', index_col=False, sep=',', encoding='utf-8')
    columns = ['semantic_name', 'lat', 'lon']

    distinct_semantic_lat = {}
    distinct_semantic_lon = {}

    for index, row in df.iterrows():
        latitude = row['lat']
        longitude = row['lon']
        semantic_name = row['semantic_name']

        if ((isinstance(latitude, float) and isinstance(longitude, float) and isinstance(semantic_name, str))):

            if semantic_name in distinct_semantic_lat and semantic_name in distinct_semantic_lon:  # check if semantic name already in dictionary as key!
                distinct_semantic_lat[semantic_name].append(latitude)  # key in dict: get list and append to list
                distinct_semantic_lon[semantic_name].append(longitude)
            else:
                distinct_semantic_lat.update({semantic_name: [latitude]})  # key not in dict: create new list
                distinct_semantic_lon.update({semantic_name: [longitude]})

    # here: two dictionaries with lists for each key (semanticname). Now I have to take the most frequent element from list and assign finally.

    for key in distinct_semantic_lat.keys():
        distinct_semantic_lat[key] = max(set(distinct_semantic_lat[key]), key=distinct_semantic_lat[key].count)
        distinct_semantic_lon[key] = max(set(distinct_semantic_lon[key]), key=distinct_semantic_lon[key].count)

        values = [[Literal(stringData=key), Literal(floatData=distinct_semantic_lat[key]),
                   Literal(floatData=distinct_semantic_lon[key])]]
        client.insert_batch('cineast', 'cineast_distinctlocations', columns, values)
