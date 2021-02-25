#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Feb  7 20:10:25 2021

@author: simondonike
"""


""" returns server answer for given SQL statement"""
def db_connector(statement):
    import psycopg2
    return_ls = []
    try:
       connection = psycopg2.connect(user="bot_annakirmes",
                                      password="XXXXXXXXXXX",
                                      host="85.214.150.208",
                                      port="5432",
                                      database="annakirmes")
       cursor = connection.cursor()
       sql_Query = statement
    
       cursor.execute(sql_Query)
       #print("Selecting rows from mobile table using cursor.fetchall")
       mobile_records = cursor.fetchall() 
       
       #print("Print each row and it's columns values")
       for row in mobile_records:
           return_ls.append(list(row))
    
    except (Exception, psycopg2.Error) as error :
        print ("Error while fetching data from PostgreSQL: ", error)
    
    finally:
        #closing database connection.
        if(connection):
            cursor.close()
            connection.close()
            #print("PostgreSQL connection is closed")
    
    return return_ls


"""Save geojsons to disc with prefix to indicate data"""
def export_geojsons(geojsons_ls,name_prefix):
    import geojson
    json_ls = []
    for i in geojsons_ls:
        json_ls.append(i[-1])
    counter = 1
    for i in json_ls:
        #geojson.dump(i,input_name+"_geojson_"+str(counter)+".geojson")
        
        with open("geojsons/"+name_prefix+"_json_"+str(counter)+".geojson", 'w') as outfile:
            geojson.dump(i, outfile)
        counter=counter+1


""" Gets Lat.Lon, returns nearest node id from dataabase"""
""" construct sql statement of point w/ lat,lon pased in WGS84/4326"""
def get_nearest_point(lon,lat):
    sql_nearest_point = """SELECT id, 
                          ST_Distance(
                            the_geom,
                            ST_Transform(
                                ST_SetSRID(
                                    ST_MakePoint("""+str(lon)+","+str(lat)+ """),4326),3857)) AS dist
                        FROM footpaths_noded_vertices_pgr
                              ORDER BY dist LIMIT 1;"""
    sql_return = db_connector(sql_nearest_point)
    #since list of list, return only first entry
    #print("closest node id to coordinate: "+ str(sql_return[0][0]) + "\tdistance: "+ str(round(sql_return[0][1],2)) + "m")
    return(sql_return[0][0])
                              

""" receives string of table name, returns geom GeoJSONs for each entry (in 3857, i think)"""
def get_polygons(unit_type):
    if unit_type=="marketeers":
        sql_statement = "SELECT ogc_fid, business, name, ST_AsEWKT(st_transform(geom,4326)) AS geom FROM marketeers;"
    if unit_type=="exit_points":
        sql_statement = "SELECT ogc_fid, name, ST_AsGeoJSON(geom) AS geom FROM exit_points;"
    if unit_type=="utilities":
        sql_statement = "SELECT ogc_fid, type, ST_AsGeoJSON(geom) AS geom FROM utilities;"
    if unit_type=="footpaths":
        sql_statement = "SELECT id, ST_AsGeoJSON(geom) AS geom FROM footpaths;"
    if sql_statement!="":
        return db_connector(sql_statement)




"""reuest database for nearest node from point, list woth GeoJSONs in last place"""
def get_routing(source_node,target_node):
    
 
    #print("source node: ",source_node)
    #print("target node: ",target_node)
    #bringing nodes in right order, from small to big, otherwise routing fails
    if source_node<target_node:
        node_statement = str(source_node)+","+str(target_node)
    if source_node>=target_node:
        node_statement = str(target_node)+","+str(source_node)
    #construct SQL statement
    sql_statement = """SELECT seq, path_seq, node, edge, directions.cost , agg_cost, ST_AsEWKT(ST_TRANSFORM(geom,4326)) as geom
    FROM pgr_dijkstra('SELECT id,source,target ,st_length(geom) as cost FROM footpaths_noded',"""+node_statement+"""
    ,false)as directions JOIN footpaths_noded fn ON directions.node = fn.id;"""
    #return database answer
    
    #print(sql_statement)
    server_answer = db_connector(sql_statement)
    #agg_dist = round(server_answer[-1][-2],2)
    #print("length of route from node to target: "+str(agg_dist))
    #print(server_answer)
    return server_answer


"""receives point and destination type, returns closes node id of type"""
def get_closest_type_node(lon,lat,keyword):
    # check if keyword is valid
    if keyword in ["ride","misc","gastronomy","Toilet","Head Office","First Aid","Kid's Office"]:
        #construct sql statment with keyword
        sql_statement = """SELECT ver_id, ST_Distance(wkb_geometry,ST_Transform(ST_SetSRID(ST_MakePoint(
        """+str(lon)+","+str(lat)+"""),4326),3857)) AS dist FROM vertices_sampled 
        WHERE ut_type = '"""+keyword+"' OR mar_bus= '"+keyword+"' ORDER BY dist LIMIT 1;"
        
        #save server answer
        server_answer = db_connector(sql_statement)
        #extract node, turn into int
        target_node = int(server_answer[0][0])
        return target_node
       

""""Saves all GeoJSONs of all info locally"""
def refresh_GeoJSON_library(): 
    export_geojsons(get_polygons("marketeers"), "marketeers")
    export_geojsons(get_polygons("utilities"), "utilities")
    export_geojsons(get_polygons("footpaths"), "footpaths")
    export_geojsons(get_polygons("exit_points"), "exit_points")


"""Example for plotting"""
def get_geopandas_from_return(sql_return):
    import pandas as pd
    import geopandas
    from shapely import wkt
    ls_1 = []
    for i in sql_return:
        ls_1.append([i[0],i[-1][10:]])
    df_1 =pd.DataFrame(ls_1)
    df_1 = df_1.rename(columns={0: 'ID', 1: 'geometry'})
    df_1['geometry'] = df_1['geometry'].apply(wkt.loads)
    gdf_1 = geopandas.GeoDataFrame(df_1, geometry='geometry')
    return gdf_1



"""PLT plot maps, takes route from server as input and plots map w/ background """
def plot_map(route_server):
    gpd_marketeers = get_geopandas_from_return(db_connector("SELECT ogc_fid, business, name, ST_AsEWKT(st_transform(geom,4326)) AS geom FROM marketeers;"))
    gpd_footpaths = get_geopandas_from_return(db_connector("SELECT id, ST_AsEWKT(st_transform(geom,4326)) AS geom FROM footpaths;"))
    gpd_utilities = get_geopandas_from_return(db_connector("SELECT ogc_fid, type, ST_AsEWKT(st_transform(geom,4326)) AS geom FROM utilities;"))
    gpd_route = get_geopandas_from_return(route_server)
    import matplotlib.pyplot as plt
    fig,ax=plt.subplots()
    fig.set_size_inches(18, 18)
    ax.set_aspect('equal')
    gpd_marketeers.plot(ax=ax,color='green',label="Marketeers")
    gpd_utilities.plot(ax=ax,color='dodgerblue',label="Utilities")
    gpd_footpaths.plot(ax=ax,color='dimgray',linestyle=':',label="Footpaths")
    gpd_route.plot(ax=ax,color='red',linewidth=2.5,label="Route")
    plt.show()

"""Get user Coordinates & Type of Target, returns Coor & target keyword for DB"""
def get_user_input():
    # get lat
    lat = 0.0
    while type(lat) != float or lat<50.0 or lat>51.0:
        lat = input("\nEnter your Coordinates!\nLatitude: ")
        lat = float(lat)
        if type(lat) != float or lat<50.0 or lat>51.0:
            print("Invalid input! Give Latitude as float, in the general area of the carnival grounds!")
    # get lon
    lon = 0.0
    while type(lon) != float or lon<6.0 or lon>7.0:
        lon = input("Longitude: ")
        lon = float(lon)
        if type(lon) != float or lon<6.0 or lon>7.0:
            print("Invalid input! Give Latitude as float, in the general area of the carnival grounds!")
    # get user target
    user_input = 0
    while user_input not in [1,2,3,4,5,6,7,8]:
        user_input = input("\nWhere do you want to go? Enter a number!\nto the nearest....\n\t1 - Gastronomy\n\t2 - Carnival Ride\n\t3 - Entertainment Booths\n\t4 - Misc. Vendor\n\t5 - Toilet\n\t6 - First Aid Station\n\t7 - Kid's Office\n\t8 - Carnival Organizational Office\n\tInput: ")
        user_input = int(user_input)
        if user_input not in [1,2,3,4,5,6,7,8]:
            print("Invalid input, please enter a number!")
    target_dict = {
        1:"gastronomy",
        2:"ride",
        3:"entertainment",
        4:"misc",
        5:"toilet",
        6:"First Aid",
        7:"Kid's Office",
        8:"Head Office"}
    return[lon,lat,target_dict[user_input]]
    
# Start user input and routing
lon, lat, target = get_user_input()
plot_map(get_routing(get_nearest_point(lon,lat),get_closest_type_node(lon,lat,target))) 
