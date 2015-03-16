#!/usr/bin/env python
# Use python 3

"""
Read in a topojson file with bentities, then look up properties (right now just 
the GID) from the bentities table in the GABI database for each polygons, then
add the new properties to the JSON objects for the polygons.

Takes 2 command-line arguments: topoJSON file in, topoJSON file out

Database connection parameters are in the script (shame on me.)
"""

import json
import psycopg2
from sys import argv

# set up database connection
conn = psycopg2.connect(database='antmaps', user='antmaps', host='127.0.0.1', port='5432', password='password')
cur = conn.cursor()

# command-line arguments
infilename = argv[1]
outfilename = argv[2]

# load and parse topojson
fin = open(infilename)
js = json.load(fin)
fin.close()

def lookupProperties(bentityname):
    """
    Given a bentity name, look up properties from the bentities_highres database
    table and return a dict of those properties.
    
    Right now, just return a dict with the GID.
    """

    #import pdb; pdb.set_trace()
    cur.execute('select gid from bentities_highres where bentity = %s;', (bentityname,))
    row = cur.fetchone()
    return {'gid': row[0]}
    

# update properties for each object
for geom in js['objects']['collection']['geometries']:
    properties = geom['properties']
    
    # look up and add new properties
    newproperties = lookupProperties(properties['BENTITY'])
    properties.update(newproperties)
    

# write out new topoJSON file
fout = open(outfilename, 'w')
json.dump(js, fout)
fout.close()

cur.close()
conn.close()
