from kafka import KafkaProducer
import json
import base64
import cv2 
import numpy as np
import os
from pymongo import MongoClient
import rasterio
import gridfs
import io
from PIL import Image
import sys
import rasterio
from rasterio.windows import Window

def generate_patches(file_bytes, tile_size=512):
    """Yield (i, j, patch_array) for each tile window in the raster."""
    with rasterio.io.MemoryFile(file_bytes) as mem:
        with mem.open() as src:
            nrows, ncols = src.height, src.width
            for top in range(0, nrows, tile_size):
                for left in range(0, ncols, tile_size):
                    win = Window(left, top,
                                 min(tile_size, ncols-left),
                                 min(tile_size, nrows-top))
                    data = src.read(window=win)  # shape: (bands, h, w)
                    yield {
                        "x_off": left,
                        "y_off": top,
                        "width": win.width,
                        "height": win.height,
                        "bands": data.shape[0],
                        "data": data  # you’ll compress/encode later
                    }


client = MongoClient('mongodb://root:example@localhost:27017/')
db = client['uav_imagery']
missions = db['missions']
tiles = db['tiles']
fs = gridfs.GridFS(db)

def scale_to_uint8(array):
    """Scales numpy array to uint8 for display, handling different dtypes."""
    if array.dtype == np.uint8:
        return array 
    
    min_val = np.min(array)
    max_val = np.max(array)

    if max_val == min_val: 
        
        return np.zeros(array.shape, dtype=np.uint8) + (128 if min_val != 0 else 0)
    
    scaled_array = 255 * (array - min_val) / (max_val - min_val)

    cv2.imwrite("image.jpg", scaled_array)
    return scaled_array.astype(np.uint8)

def stream_data_as_encoded_image():
    
    for grid_file in fs.find():
        try:
            file_data = grid_file.read()
            filename = grid_file.filename
            metadata = grid_file.metadata or {} 
            print(f"Processing file: {filename}")

            encoded_image_data_uri = None
            image_metadata = {} 

            
            if filename.lower().endswith(('.tif', '.tiff')):
                
                temp_path = f"temp_{filename}"
                with open(temp_path, 'wb') as f:
                    f.write(file_data)

                try:
                    with rasterio.open(temp_path) as src:
                        
                        image_data = src.read() 
                        
                        if image_data.ndim == 3:
                            image_display = image_data.transpose(1, 2, 0)
                        elif image_data.ndim == 2: 
                             image_display = image_data 
                        else:
                            raise ValueError(f"Unexpected array dimensions: {image_data.ndim}")

                        if image_display.dtype != np.uint8:
                            print(f"  Scaling {filename} from {image_display.dtype} to uint8")
                            image_display = scale_to_uint8(image_display)
                        
                        if image_display.ndim == 2 or image_display.shape[2] == 1:
                            image_display_bgr = cv2.cvtColor(image_display, cv2.COLOR_GRAY2BGR)
                        elif image_display.shape[2] == 4: 
                            image_display_bgr = cv2.cvtColor(image_display, cv2.COLOR_RGBA2BGR)
                        elif image_display.shape[2] == 3: 
                             
                             image_display_bgr = image_display
                        else:
                            raise ValueError(f"Unsupported band count for display: {image_display.shape[2]}")

                        
                        is_success, buffer = cv2.imencode(".jpg", image_display_bgr)
                        if not is_success:
                            raise ValueError("Could not encode image to JPEG")

                        image_base64 = base64.b64encode(buffer).decode('utf-8')
                        encoded_image_data_uri = f"data:image/jpeg;base64,{image_base64}"

                        
                        image_metadata = {
                            'original_width': src.width,
                            'original_height': src.height,
                            'original_bands': src.count,
                            'original_dtype': str(src.dtypes[0]), 
                            'crs': str(src.crs),
                            'transform': src.transform.to_gdal()
                        }
                finally:
                    
                    os.remove(temp_path)

            
            elif filename.lower().endswith(('.png', '.jpg', '.jpeg', '.bmp')):
                 try:
                    image = Image.open(io.BytesIO(file_data))
                    
                    image_array = np.array(image)

                    if image_array.ndim == 2: 
                        image_display_bgr = cv2.cvtColor(image_array, cv2.COLOR_GRAY2BGR)
                    elif image_array.shape[2] == 4: 
                        image_display_bgr = cv2.cvtColor(image_array, cv2.COLOR_RGBA2BGR)
                    elif image_array.shape[2] == 3: 
                         image_display_bgr = cv2.cvtColor(image_array, cv2.COLOR_RGB2BGR) 
                    else:
                         raise ValueError(f"Unsupported channels: {image_array.shape}")

                    
                    is_success, buffer = cv2.imencode(".jpg", image_display_bgr)
                    if not is_success:
                        raise ValueError("Could not encode image to JPEG")

                    
                    image_base64 = base64.b64encode(buffer).decode('utf-8')
                    encoded_image_data_uri = f"data:image/jpeg;base64,{image_base64}"

                    
                    image_metadata = {
                        'original_width': image.width,
                        'original_height': image.height,
                        'original_bands': len(image.getbands()),
                        'original_format': image.format
                    }
                 except Exception as img_err:
                    print(f"Error processing standard image {filename}: {img_err}")
                    continue 

            
            if encoded_image_data_uri:
                message = {
                    'image_id': filename,
                    
                    'metadata': {**metadata, **image_metadata},
                    'image_data': encoded_image_data_uri 
                }
                yield message
            else:
                 print(f"Skipping file {filename} as it's not a supported/processed image type.")

        except Exception as e:
            print(f"Error processing file {filename}: {str(e)}", exc_info=True) 
            
            temp_path = f"temp_{filename}"
            if os.path.exists(temp_path):
                 try:
                    os.remove(temp_path)
                 except OSError:
                    pass 
            continue


if __name__ == "__main__":
    producer = KafkaProducer(
        bootstrap_servers=['localhost:29092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        batch_size=32*1024,
        max_request_size=104857600,
        buffer_memory=335544320,     
        linger_ms=100
        
    )

    print("Starting producer...")
    message_count = 0
    for image_message in stream_data_as_encoded_image():
        try:
            producer.send('uav.tiles.tiff', image_message)
            message_count += 1
            print(f"Sent image data for {image_message['image_id']} ({message_count})")
        except Exception as send_err:
            print(f"Error sending message for {image_message.get('image_id', 'unknown')} to Kafka: {send_err}")
            

    print(f"Finished sending {message_count} messages. Flushing producer...")
    producer.flush() 
    print("Closing producer.")
    producer.close()