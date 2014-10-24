SELECT ST_length(ST_MakeLine('POINT(40.22 57.39)'::geometry,
'POINT(48.22 58.39)'::geometry)),
ST_AsLatLonText('POINT(40.22 57.39)'::geometry),
ST_AsLatLonText('POINT(48.22 58.39)'::geometry),
ST_Distance('POINT(40.22 57.39)'::geometry,
'POINT(48.22 58.39)'::geometry)*111,
st_distance_sphere('POINT(40.22 57.39)'::geometry,
'POINT(48.22 58.39)'::geometry)

