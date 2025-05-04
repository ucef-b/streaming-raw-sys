from pymongo import MongoClient
import os
from PIL import Image
import io
import bson
import gridfs

client = MongoClient('mongodb://root:example@localhost:27017/')

db = client['uav_imagery']
fs = gridfs.GridFS(db)


tiles_directory = '10'

for filename in os.listdir(tiles_directory):
    if filename.endswith('.tif') or filename.endswith('.jpg'):
        
        image_path = os.path.join(tiles_directory, filename)
        image = Image.open(image_path)
        print(image_path)
        
        img_byte_arr = io.BytesIO()
        image.save(img_byte_arr, format=image.format)
        img_bytes = img_byte_arr.getvalue()
        
        file_id = fs.put(
            img_bytes,
            filename=filename,
            metadata={
                'width': image.width,
                'height': image.height,
                'format': image.format
            }
        )
