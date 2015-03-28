# Kojak

## Possible questions:
1. What topics are Italians expressing in their news mostly?
2. Given one type of news, who would talk about it the most?

## How to create the GeoJSON and topojson files
    cd maps/
    ogr2ogr -f GeoJSON -where "ADM0_A3 IN ('CAN', 'MEX', 'USA')" northamerica_subunits.json ne_10m_admin_0_map_subunits/ne_10m_admin_0_map_subunits.shp
    ogr2ogr -f GeoJSON -where "ISO_A2 IN ('CA', 'MX', 'US') AND SCALERANK < 8" northamerica_places.json ne_10m_populated_places/ne_10m_populated_places.shp
    topojson -o northamerica.json --id-property SU_A3 --properties name=NAME -- northamerica_subunits.json northamerica_places.json

### Columns to keep
0   GLOBALEVENTID
1   SQLDATE
5                       Actor1Code
6                       Actor1Name
7                Actor1CountryCode
8             Actor1KnownGroupCode
9                 Actor1EthnicCode
10             Actor1Religion1Code
11             Actor1Religion2Code
12                 Actor1Type1Code
13                 Actor1Type2Code
14                 Actor1Type3Code
15                      Actor2Code
16                      Actor2Name
17               Actor2CountryCode
18            Actor2KnownGroupCode
19                Actor2EthnicCode
20             Actor2Religion1Code
21             Actor2Religion2Code
22                 Actor2Type1Code
23                 Actor2Type2Code
24                 Actor2Type3Code
26                       EventCode
30                  GoldsteinScale
32                      NumSources
34                         AvgTone
38              Actor1Geo_ADM1Code
41             Actor1Geo_FeatureID
45              Actor2Geo_ADM1Code
48             Actor2Geo_FeatureID
52              ActionGeo_ADM1Code
55             ActionGeo_FeatureID
56                       DATEADDED
57                       SOURCEURL

### Columns to throw away
2                        MonthYear
3                             Year
4                     FractionDate
25                     IsRootEvent
27                   EventBaseCode
28                   EventRootCode
29                       QuadClass
31                     NumMentions
33                     NumArticles
35                  Actor1Geo_Type
36              Actor1Geo_FullName
37           Actor1Geo_CountryCode
39                   Actor1Geo_Lat
40                  Actor1Geo_Long
42                  Actor2Geo_Type
43              Actor2Geo_FullName
44           Actor2Geo_CountryCode
46                   Actor2Geo_Lat
47                  Actor2Geo_Long
49                  ActionGeo_Type
50              ActionGeo_FullName
51           ActionGeo_CountryCode
53                   ActionGeo_Lat
54                  ActionGeo_Long

### Deliverables
Visualization of countries' happiness based on their news


### Questions and Problems to solve:
