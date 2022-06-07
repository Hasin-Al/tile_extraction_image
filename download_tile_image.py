#importing libraries
import ee
import geemap
from geemap import ml
from datetime import datetime
from osgeo import gdal
import geopandas as gpd
import geetools
from geetools import batch

#initialize and authenticate gee
ee.Initialize()
geemap.ee_initialize()
ee.Authenticate()

#read shape file and upload to gee
germany_shp = 'shape_file/german.shp'
germany= geemap.shp_to_ee(germany_shp)

#code from gee 
def toNatural(img):
    return ee.Image(10.0).pow(img.select('..').divide(10.0)).copyProperties(img, ['system:time_start'])

def toDB(img):
    return ee.Image(img).log10().multiply(10.0).copyProperties(img, ['system:time_start'])

# Remove ugly edges
def maskEdge(img):
    mask = img.select(0).unitScale(-25, 5).multiply(255).toByte().connectedComponents(ee.Kernel.rectangle(1,1), 100)
    return img.updateMask(mask.select(0).abs())

#2. Select the dates and time step
#=============================================
# select dates
start_date = '2018-05-01'
end_date = '2018-08-10'

#Set  the time step
step = 10 # in days (time window for averaging)

#Spatial resolution
res=10
#uri = 'gs://corp_classifications/deu_crop_mask.tif'
#mask = 'deu_crop_mask.tif'#ee.Image.load(uri)
#mask = ee.data.getAsset('projects/manifest-space-321313/assets/mask')
#link of the mask tif file given by riaz bhai
mask = 'projects/manifest-space-321313/assets/mask'

# get the data from S1 (VV pol.)
s1 = ee.ImageCollection('COPERNICUS/S1_GRD').filterMetadata('instrumentMode', 'equals', 'IW')\
  .filter(ee.Filter.eq('transmitterReceiverPolarisation', ['VV', 'VH']))\
  .filterBounds(germany)\
  .filterDate(start_date, end_date)\
  .sort('system:time')

# Remove ugly edges
s1 = s1.map(maskEdge)

# Average are made from natural (non-logarithmic) values
s1 = s1.map(toNatural)

def dateCreate(d):
  return ee.Date(start_date).advance(d, "day")

# Olha Danylo's procedure to create weekly means (adapted)
days = ee.List.sequence(0, ee.Date(end_date).difference(ee.Date(start_date), 'day'), step)\
  .map(dateCreate)

dates = days.slice(0,-1).zip(days.slice(1))

# 3.2 / Temporal compositing
#we have to do arrange our vh and vv order here while selecting vv and vh .
def temporal_composite(range):
  dstamp = ee.Date(ee.List(range).get(0)).format('YYYYMMdd')
  temp_collection = s1.filterDate(ee.List(range).get(0),
  ee.List(range).get(1)).mean().select(['VH'], [ee.String('VH_').cat(dstamp)])
  return temp_collection

def temporal_composite_two(range):
  dstamp = ee.Date(ee.List(range).get(0)).format('YYYYMMdd')
  temp_collection = s1.filterDate(ee.List(range).get(0),
  ee.List(range).get(1)).mean().select(['VV'], [ee.String('VV_').cat(dstamp)])
  return temp_collection
  

s1res_one = dates.map(temporal_composite)
s1res_two = dates.map(temporal_composite_two)
s1res_one = s1res_one.reverse()
s1res_two = s1res_two.reverse()
s1res = s1res_two.cat(s1res_one)

#transform back to DB
s1res=s1res.map(toDB)

# Convert ImageCollection to image stack
def stack(i1, i2):
  return ee.Image(i1).addBands(ee.Image(i2))

#transform the image to float to reduce size
s1stack = s1res.slice(1).iterate(stack, s1res.get(0))

#toImage
s1stack = ee.Image(s1stack)

#transform the image to float to reduce size
s1stack = s1stack.toFloat()
#maskig with given tif file
s1stack = s1stack.mask(mask)
#taking a list of id of our shape file . as ecah id is a tile
seq = ee.List.sequence(1,ee.Number(germany.size()))

#split our map with grid and have a final image collectoion
def split(x):
    fill = germany.filterMetadata('id','equals',x)
    scol = s1stack.clip(fill)
    return scol

spl = seq.map(split)
final_col = ee.ImageCollection.fromImages(spl)

#download all the images of the image collection to drive
tasks = batch.Export.imagecollection.toDrive(final_col, 'gee-germany', scale=10,region=germany.geometry(),maxPixels= 8853653097)