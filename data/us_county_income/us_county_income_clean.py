# # Constructing a consistent US County Per Capita Income Dataset 1969-2017
# 
# This notebook builds a cleaned geopackage for use in the inequality chapter of the book.  It does so by carrying out a number of data processing steps to:
# 
# - Generalize the boundaries of the county shapefile to faciltate processing
# - Joining income attributes with geometries
# - Handling the birth and deaths of counties over the time series


import pandas
from zipfile import ZipFile
import glob
from urllib.request import urlopen
from io import BytesIO
import fiona.io
import geopandas as gpd
import pandas
import requests
import topojson as tp
get_ipython().system('pip install topojson')

"""
# Not needed because of git-lfs problems, just use raw file
url = 'https://github.com/gdsbook/data_archive/raw/master/us_county_income/tl_2019_us_county.zip'

response = requests.get(url)
data_bytes = response.content

with fiona.io.ZipMemoryFile(data_bytes) as zip_memory_file:
    with zip_memory_file.open('tl_2019_us_county.shp') as collection:
        gdf = gpd.GeoDataFrame.from_features(collection, crs=collection.crs)
#gdf.to_file('tl_2019_us_county.shp')
"""

with fiona.open('tl_2019_us_county/tl_2019_us_county.shp') as collection:
        gdf = gpd.GeoDataFrame.from_features(collection, crs=collection.crs)

topo = tp.Topology(gdf, prequantize=False)
gdf_simplified = topo.toposimplify(5).to_gdf()
gdf_simplified.to_file('tl_2019_us_county.shp')


"""
Not needed, just use raw file
caincurl = 'https://github.com/gdsbook/data_archive/raw/master/us_county_income/CAINC1.zip'
with urlopen(caincurl) as zipresp:
    with ZipFile(BytesIO(zipresp.read())) as zf:
        zf.extractall()
"""

with ZipFile('CAINC1.zip') as zf:
    zf.extractall()
        
        
csv_files = glob.glob("*.csv")
data = pandas.read_csv('CAINC1__ALL_STATES_1969_2017.csv', encoding='latin-1', 
                      skipfooter=3, engine='python')

virginia = data[data.GeoFIPS.str.contains('\"51')] # select virginia
virginia.to_csv('virginia.csv')


# fips codes source: https://www.nrcs.usda.gov/wps/portal/nrcs/detail/?cid=nrcs143_013696
fips = """Alabama 	AL 	01
Alaska 	AK 	02
Arizona 	AZ 	04
Arkansas 	AR 	05
California 	CA 	06
Colorado 	CO 	08
Connecticut 	CT 	09
Delaware 	DE 	10
Florida 	FL 	12
Georgia 	GA 	13
Hawaii 	HI 	15
Idaho 	ID 	16
Illinois 	IL 	17
Indiana 	IN 	18
Iowa 	IA 	19
Kansas 	KS 	20
Kentucky 	KY 	21
Louisiana 	LA 	22
Maine 	ME 	23
Maryland 	MD 	24
Massachusetts 	MA 	25
Michigan 	MI 	26
Minnesota 	MN 	27
Mississippi 	MS 	28
Missouri 	MO 	29
Montana 	MT 	30
Nebraska 	NE 	31
Nevada 	NV 	32
New Hampshire 	NH 	33
New Jersey 	NJ 	34
New Mexico 	NM 	35
New York 	NY 	36
North Carolina 	NC 	37
North Dakota 	ND 	38
Ohio 	OH 	39
Oklahoma 	OK 	40
Oregon 	OR 	41
Pennsylvania 	PA 	42
Rhode Island 	RI 	44
South Carolina 	SC 	45
South Dakota 	SD 	46
Tennessee 	TN 	47
Texas 	TX 	48
Utah 	UT 	49
Vermont 	VT 	50
Virginia 	VA 	51
Washington 	WA 	53
West Virginia 	WV 	54
Wisconsin 	WI 	55
Wyoming 	WY 	56
American Samoa 	AS 	60
Guam 	GU 	66
Northern Mariana Islands 	MP 	69
Puerto Rico 	PR 	72
Virgin Islands 	VI 	78"""


fips_df = pandas.DataFrame([fip.split("\t") for fip in fips.split("\n")])
fips_df.columns=["State", "Abbreviation", 'FIPS']
omit_fips = [" \""+str(fip) for fip in ['02', 15, 60, 66, 69, 72, 78, 90, 91, 92, 93, 94, 95, 96, 97, 98]]

small = data.copy()
for omit in omit_fips:
    small = small[~small.GeoFIPS.str.contains(omit)]
    
geofips = set(code[:4] for code in small.GeoFIPS.values)

for fip in geofips:
    sdf = small[small.GeoFIPS.str.contains(fip)]
    sdf.to_csv(fip[2:]+".csv")

# We are done as all the csv files have been extracted.

# Read remote shapefile with county geometries

gdf = geopandas.read_file("tl_2019_us_county.shp")

#State Specific Data Frames
csv_files = glob.glob("??.csv")
csv_files.sort()
csv_files.pop(0) # kick out US

mismatch = []
gdfs = []
#for csv_file in csv_files[:1]:
for csv_file in csv_files:

    #print(csv_file)
    csv = pandas.read_csv(csv_file)
    st = csv_file[:2]
    st_gdf = gdf[gdf.STATEFP==st]
    csv = csv.iloc[3:] # kick out the state level records
    nc, kc = csv.shape
    if nc/3 != st_gdf.shape[0]:
        mismatch.append(st)
        print(st)
    else:
        csv['GeoFIPS'] = [fip.strip().replace("\"", "") for fip in csv.GeoFIPS]
        csv['GEOID'] = csv.GeoFIPS.astype(str)
        merged_gdf = st_gdf.merge(csv, on='GEOID')
        gdfs.append(merged_gdf)
        
gdf = pandas.concat(gdfs)

#only get columns of dataframe that are numeric for years 
gdf_columns = gdf.columns.to_list()
years = []
for col in gdf_columns:
    if col.isnumeric() == True:
        years.append(col)
        
dtypes = list(zip(years,[gdf[year].dtype for year in years]))

# ## Issues
# 
# 1. Virginia
# 2. Wisconsin
# 3. Object data types for 1969-2001

gdf.to_file('pcincome0.shp')

# ## Virginia independent cities
# 
# Reference https://en.wikipedia.org/wiki/Independent_city_(United_States)
# 
# > In the United States, an independent city is a city that is not in the territory of any county or counties with exceptions noted below. Of the 41 independent U.S. cities,[1] 38 are in Virginia, whose state constitution makes them a special case. The three independent cities outside Virginia are Baltimore, Maryland; St. Louis, Missouri; and Carson City, Nevada. The U.S. Census Bureau uses counties as its base unit for presentation of statistical information, and treats independent cities as county equivalents for those purposes. The most populous of them is Baltimore, Maryland. 
# 
# 
# ### From BEA Income Data:
# > Virginia combination areas consist of one or two independent cities with 1980 populations of less than 100,000 combined with an adjacent county. The county name appears first, followed by the city name(s). Separate estimates for the jurisdictions making up the combination area are not available. Bedford County, VA includes the independent city of Bedford for all years.
# 
# ### Virginia
# > The Commonwealth of Virginia is divided into 95 counties, along with 38 independent cities that are considered county-equivalents for census purposes. The map in this article, taken from the official United States Census Bureau site, includes Clifton Forge and Bedford as independent cities. This reflected the political reality at the time of the 2000 Census. However, both have since chosen to revert to town status. In Virginia, cities are co-equal levels of government to counties, but towns are part of counties. For some counties, for statistical purposes, the Bureau of Economic Analysis combines any independent cities with the county that it was once part of (before the legislation creating independent cities took place in 1871).
# 
# [Source](https://en.wikipedia.org/wiki/List_of_cities_and_counties_in_Virginia)
# 
# ### Approach
# 
# Dissolve boundaries of independent cities that BEA does not disclose values for with their adjacent county.

csv = pandas.read_csv('51.csv')
csv['GeoFIPS'] = [fip.strip().replace("\"", "") for fip in csv.GeoFIPS]
gdf = geopandas.read_file('tl_2019_us_county.shp')


# gdf = geopandas.read_file("zip:tl_2019_us_county.zip!tl_2019_us_county.shp")
# zipfile = "zip:tl_2019_us_county.zip!tl_2019_us_county.shp"

virginia_gdf = gdf[gdf.STATEFP=="51"]
data = csv

data['GEOID'] = data.GeoFIPS

# ## gdf records missing income values

#data = data[data.LineCode==3]
merged_gdf = virginia_gdf.merge(data, on='GEOID')
merged_gdf.shape[0]/3
matched_names = set(merged_gdf.NAMELSAD)

missing_gdf = virginia_gdf[~virginia_gdf.GEOID.isin(merged_gdf.GEOID)]
missing_names = set(missing_gdf.NAMELSAD)

#names = missing_gdf.NAME
names = missing_gdf.NAMELSAD

virginia_income = data[data.GeoName.str.contains(", VA")]

geonames = pandas.unique(virginia_income.GeoName)


class Combination:
    def __init__(self, label):
        n_commas = label.count(",")
        self.label = label
        self.n_commas = n_commas
        if "Fairfax City" in label:
            label = label.replace('Fairfax City', 'Fairfax')
        if n_commas == 1:
            if "Bedford" in label:
                self.county = 'Bedford'
                self.cities = []
            else:
                words = label.split("+")
                county = words[0].strip()
                self.county = county
                self.cities = [words[-1].split(",")[0].strip()+" city"]
        elif n_commas == 2:
            words = label.split(",")
            self.county = words[0].strip()
            cities = words[1]
            cities = cities.split("+")
            
            self.cities = [city.strip()+" city" for city in cities]
        else:
            print('Bad label: ', label)
        self.county = self.county + " County"
        self.NAMELSAD = self.label
        
combinations_0 = [name for name in geonames if "VA*" in name]
combinations = [Combination(c) for c in combinations_0]

c0 = combinations[0]

matches = {}
for name in names:
    hits = []
    matches[name] = []
    if "city" in name:
        hits = [comb for comb in combinations if name in comb.cities]
    else:
        hits = [comb for comb in combinations if name == comb.county]
    if hits:
        matches[name] = hits[0].label
    

# ## Match combinations to rows of df

for combination in combinations:
    print("label: ",combination.label)
    comb_names = [combination.county]+combination.cities
    print(comb_names)

merged_dfs = []
geoids = []
for combination in combinations:
    places = combination.cities + [combination.county]
    print(places)
    rows = virginia_gdf[virginia_gdf.NAMELSAD.isin(places)]
    
    if len(rows) != len(places):
        print('missed:', places)
    #d_df = rows.dissolve(by='STATEFP')
    rd = rows.dissolve(by='STATEFP').head()
    rd['NAMELSAD']= combination.label
    geoid = virginia_income[virginia_income.GeoName==combination.label].GEOID.values[0]
    rd['GEOID'] = geoid
    merged_dfs.append(rd)
    

dissolved = pandas.concat(merged_dfs)
d_merge = dissolved.merge(virginia_income, on='GEOID')
final_gdf = pandas.concat([merged_gdf, d_merge])
final_gdf['1979'] = final_gdf['1979'].astype(int)
final_gdf.STATEFP='51'
final_gdf[final_gdf.NAME=='Charlottesville']

# ## Reverse Matching
# 
# Which income records do not match a record in the shapefile

virginia_gdf = gdf[gdf.STATEFP=="51"]

#data = data[data.LineCode==3]
merged_df = data.merge(virginia_gdf, on='GEOID')

missing_df = data[~data.GEOID.isin(merged_df.GEOID)]
#~virginia_gdf.GEOID.isin(merged_gdf.GEOID)

pcincome_gdf = geopandas.read_file('pcincome0.shp')
common = set(pcincome_gdf.columns).intersection(final_gdf.columns)

us = pcincome_gdf[common]
va = final_gdf[common]
va.reset_index(inplace=True)
dup = va[va[["NAME", "GEOID", "1969"]].duplicated()]

va = va.drop(dup.index)
usva = pandas.concat([us,va])

years = [str(year) for year in range(1969,2018)]
dtypes = list(zip(years,[usva[year].dtype for year in years]))

usva.to_file('usva.shp')

namedf = usva[['GeoName', 'STATEFP', '1969']]

# ## Wisconsin

csv = pandas.read_csv('55.csv')
csv = csv.replace('(NA)', 0)

menominee = csv[csv.GeoName.str.match('Menominee, WI*')].replace('(NA)', 0)
shawano = csv[csv.GeoName.str.match('Shawano, WI*')].replace('(NA)', 0)
combined  = csv[csv.GeoName.str.contains('includes Menominee')].replace('(NA)', 0)

menominee[years] = menominee[years].astype(int)
shawano[years] = shawano[years].astype(int)
combined[years] = combined[years].astype(int)

cindex = [ combined.columns.values.tolist().index(y) for y in years]

for c,y in zip(cindex,years):
    #print(menominee[y].dtype)
    #print(shawano[y].dtype)
    #print(combined[y].dtype)
    combined.iloc[0,c] = menominee.iloc[0,c] + shawano.iloc[0,c]
    try:
        combined.iloc[2,c] = int(combined.iloc[0,c]*1000/ combined.iloc[1,c])
    except ValueError:
        combined.iloc[2,c] = combined.iloc[0,c]*1000/ combined.iloc[1,c]
    csv.iloc[combined.index[0],c] = combined.iloc[0,c]
    csv.iloc[combined.index[1],c] = combined.iloc[1,c]
    csv.iloc[combined.index[2],c] = combined.iloc[2,c]

csv.fillna(0,inplace=True)

# ## Drop shawano and menomiee from csv

drop_ids = menominee.index.to_list() + shawano.index.to_list()
csv = csv.drop(drop_ids)
csv[years] = csv[years].astype(int)

for year in range(1969,2018):
    print(csv[str(year)].dtype)
    
# ## Dissolve shawano and menominee geometries in gdf
# 
# dissolve
# and assign geofips


gdf = geopandas.read_file("tl_2019_us_county.shp")

wisconsin_gdf = gdf[gdf.STATEFP=="55"]

combined = wisconsin_gdf[wisconsin_gdf.COUNTYFP.isin(['115', '078'])].dissolve(by='STATEFP')
combined['NAME'] = "Shawano+Menominee"
combined['NAMELSAD'] = "Shawano+Menominee Counties"
combined['GEOID'] = '55901'

wisc0 = wisconsin_gdf[~wisconsin_gdf.COUNTYFP.isin(['115', '078'])]
wisc1 = pandas.concat([wisc0, combined])

# ## Merge gdf and csv

csv['GeoFIPS'] = [fip.strip().replace("\"", "") for fip in csv.GeoFIPS]

wisc1.STATEFP='55'
data = csv

data['GEOID'] = data.GeoFIPS

# ## gdf records missing income values

#data = data[data.LineCode==3]
merged_gdf = wisc1.merge(data, on='GEOID')

matched_names = set(merged_gdf.NAMELSAD)
missing_gdf = wisc1[~wisc1.GEOID.isin(merged_gdf.GEOID)]
missing_names = set(missing_gdf.NAMELSAD)
usva = geopandas.read_file('usva.shp')

us_no_wisc = usva[usva.STATEFP!='55']

us = pandas.concat([merged_gdf, us_no_wisc])

for y in range(1969, 2018):
    print(us[str(y)].dtype)


us.to_file('usincome.shp')

# # Other
# 
# ## Issue from BEA Income Data:
# <LI>*&nbsp;Cibola, NM was separated from Valencia in June 1981, but in these estimates, Valencia includes Cibola through the end of 1981.</LI>
# <LI>*&nbsp;La Paz County, AZ was separated from Yuma County on January 1, 1983. The Yuma, AZ MSA contains the area that became La Paz County, AZ through 1982 and excludes it beginning with 1983.</LI>
# 
# <LI>*&nbsp;Broomfield County, CO, was created from parts of Adams, Boulder, Jefferson, and Weld counties effective November 15, 2001. Estimates for Broomfield county begin with 2002.</LI>
# 
# 
# ### Approach
# 
# - combine Cibola NM with Valencia for 1981-2017
# - combine La Paz County AZ with Yuma for 1983-2017
# - combine Broomfield County with Boulder CO all years (<2002 Boulder, >2002 Boulder+Broomfield)
# 
# 

# NM

csv = pandas.read_csv('35.csv')
csv = csv.replace('(NA)', 0)

cibola = csv[csv.GeoName.str.startswith('Cibola')]
valencia = csv[csv.GeoName.str.startswith('Valencia')]

years = [ str(y) for y in range(1969, 2018)]

combined = cibola.copy()
cibola[years] = cibola[years].astype(int)
valencia[years] = valencia[years].astype(int)
combined[years] = combined[years].astype(int)

cindex = [ combined.columns.values.tolist().index(y) for y in years]

for c,y in zip(cindex,years):
    #print(menominee[y].dtype)
    #print(shawano[y].dtype)
    #print(combined[y].dtype)
    
    
    combined.iloc[0,c] = valencia.iloc[0,c] + cibola.iloc[0,c]
    combined.iloc[1,c] = valencia.iloc[1,c] + cibola.iloc[1,c]
    combined.iloc[2,c] = int(combined.iloc[0,c]*1000/ combined.iloc[1,c])
    #csv.iloc[219,c] = combined.iloc[0,c]
    #csv.iloc[220,c] = combined.iloc[1,c]
    #csv.iloc[221,c] = combined.iloc[2,c]


# ## Drop Cibola and Valencia and Update Combined Fields

csv = csv.drop([12, 13, 14, 99, 100, 101])
combined['GeoName'] = 'Cibola+Valencia'
combined['GeoFIPS'] = '"35061"'
combined = combined[csv.columns]
csv = pandas.concat([csv, combined])

gdf = geopandas.read_file("tl_2019_us_county.shp")

nm_gdf = gdf[gdf.STATEFP=="35"]
nm_gdf[nm_gdf.NAME.isin(['Valencia', 'Cibola'])]

combined = nm_gdf[nm_gdf.NAME.isin(['Valencia', 'Cibola'])].dissolve(by='STATEFP')
combined['NAME'] = 'Cibola+Valencia'
combined['NAMELSAD'] = 'Cibola+Valencia Counties'
combined['STATEFP'] = 35
combined['GEOID'] =  '35061' # Valencia


nm0 = nm_gdf[~nm_gdf.COUNTYFP.isin(['006', '061'])]
nm1 = pandas.concat([nm0, combined])

csv['GeoFIPS'] = [fip.strip().replace("\"", "") for fip in csv.GeoFIPS]
data = csv
data['GEOID'] = data.GeoFIPS

merged_gdf = nm1.merge(data, on='GEOID')
us = geopandas.read_file('usincome.shp')
us1 = pandas.concat([us[us.STATEFP!='35'], merged_gdf])


# ##  Arizona
# 
# - combine La Paz County AZ with Yuma for 1983-2017
# 

csv = pandas.read_csv('04.csv')
csv = csv.replace('(NA)', 0)
la_paz = csv.iloc[[21, 22, 23]]
yuma = csv.iloc[[45, 46, 47]]
combined = yuma.copy()
years = [str(y) for y in range(1969, 2018)]
la_paz[years] = la_paz[years].astype(int)
yuma[years] = yuma[years].astype(int)
combined[years] = combined[years].astype(int)

cindex = [ combined.columns.values.tolist().index(y) for y in years]

for c,y in zip(cindex,years):
    #print(menominee[y].dtype)
    #print(shawano[y].dtype)
    #print(combined[y].dtype)
    combined.iloc[0,c] = la_paz.iloc[0,c] + yuma.iloc[0,c]
    combined.iloc[1,c] = la_paz.iloc[1,c] + yuma.iloc[1,c]
    combined.iloc[2,c] = int(combined.iloc[0,c]*1000/ combined.iloc[1,c])

csv = csv.drop([21, 22, 23, 45, 46, 47])
combined['GeoName'] = 'Yuma+La Paz'
combined['GeoFIPS'] = '"04027"'
combined['STATEFP'] = 4
combined = combined[csv.columns]

csv = pandas.concat([csv, combined])

gdf = geopandas.read_file("tl_2019_us_county.shp")
az_gdf = gdf[gdf.STATEFP=='04']

combined = az_gdf[az_gdf.COUNTYFP.isin(['027', '012'])].dissolve(by='STATEFP')
combined['NAME']= "Yuma+La Paz"
combined['NAMELSAD'] = "Yuma+La Paz Counties"

az0 = az_gdf[~az_gdf.COUNTYFP.isin(['027', '012'])]
az1 = pandas.concat([az0, combined])


# ## Merge gdf and csv

csv['GeoFIPS'] = [fip.strip().replace("\"", "") for fip in csv.GeoFIPS]
data['GEOID'] = data.GeoFIPS

merged_gdf = az1.merge(data, on='GEOID')
merged_gdf['STATEFP'] = '04'

us2 = pandas.concat([us1[us1.STATEFP!='04'], merged_gdf])

us2[us2.STATEFP=='04'].shape

# ## Colorado
# 
# - combine Broomfield County with Boulder CO all years (<2002 Boulder, >2002 Boulder+Broomfield)
# 

csv = pandas.read_csv('08.csv')

csv = csv.replace('(NA)', 0)
boulder = csv.iloc[[21, 22, 23]]
broomfield = csv.iloc[[24, 25, 26]]
combined = boulder.copy()
years = [str(y) for y in range(1989, 2018)]
boulder[years] = boulder[years].astype(int)
broomfield[years] = broomfield[years].astype(int)
combined[years] = combined[years].astype(int)


cindex = [ combined.columns.values.tolist().index(y) for y in years]

for c,y in zip(cindex,years):
    #print(menominee[y].dtype)
    #print(shawano[y].dtype)
    #print(combined[y].dtype)
    combined.iloc[0,c] = boulder.iloc[0,c] + broomfield.iloc[0,c]
    combined.iloc[1,c] = boulder.iloc[1,c] + broomfield.iloc[1,c]
    combined.iloc[2,c] = int(combined.iloc[0,c]*1000/ combined.iloc[1,c])
    
csv = csv.drop([21, 22, 23, 24, 25, 26])
years = [str(y) for y in range(1969, 2018)]
csv[years] = csv[years].astype(int)

combined['GeoName'] = 'Boulder+Broomfield'
combined['GeoFIPS'] = '"08013"'
combined = combined[csv.columns]

csv = pandas.concat([csv, combined])

gdf = geopandas.read_file("tl_2019_us_county.shp")

co_gdf = gdf[gdf.STATEFP=='08']

combined = co_gdf[co_gdf.COUNTYFP.isin(['013', '014'])].dissolve(by='STATEFP')
combined['NAME'] = "Boulder+Bloomfield"
combined['NAMELSAD'] = "Boulder+Bloomfield Counties"
combined['GEOID'] = '08013'

co0 = co_gdf[~co_gdf.COUNTYFP.isin(['013', '014'])]
co1 = pandas.concat([co0, combined])

csv['GeoFIPS'] = [fip.strip().replace("\"", "") for fip in csv.GeoFIPS]
data = csv
data['GEOID'] = data.GeoFIPS

merged = co1.merge(data, on='GEOID')
merged['STATEFP'] = '08'
merged[merged.NAME.str.startswith('Bould')]['STATEFP']

us3 = pandas.concat([us2[us2.STATEFP!='08'], merged])
columns = us3.columns.values.tolist()
us4 = us3.drop(columns=['Unnamed: 0', 'Unnamed_ 0', 'IndustryCl'])

for year in range(1969, 2018):
    year = str(year)
    us4[year] = us4[year].astype('int')

us4.to_file('usincome_final.shp')


# ## Shrinking file size
# We are duplicating the shapes three times (once for each attribute)
# 
# Split out the attributes from the geometries, two different dataframes

gdf = gpd.read_file('usincome_final.shp')

uscountyincome = pandas.DataFrame(gdf.drop(columns='geometry'))
uscountyincome.to_csv('uscountyincome.csv')

gdf = gdf[gdf.LineCode==3]

gdf.to_file("uscountypcincome.gpkg", layer='pcincome', driver="GPKG")


