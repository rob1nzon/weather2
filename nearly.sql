SELECT id
  FROM agz_.mstations 
  ORDER BY loc <-> 'POINT(69.0738827418327 56.6379402293625)'::geometry
  LIMIT 1;
