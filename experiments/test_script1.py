import os
import time
import configparser
from minio import Minio
import ssl
from minio.error import S3Error
import urllib3
from urllib3 import PoolManager
 
# Custom insecure HTTP client for development ONLY
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning) #only for dev
context = ssl.create_default_context()
context.check_hostname = False
context.verify_mode = ssl.CERT_NONE
http_client = PoolManager(ssl_context=context)

# Load config
config = configparser.ConfigParser()
config.read('config.ini')

endpoint     = config['minio']['endpoint']
access_key   = config['minio']['access_key']
secret_key   = config['minio']['secret_key']
secure       = config.getboolean('minio', 'secure')
data_folder  = os.path.expanduser(config['paths']['data_folder'])
download_dir = os.path.expanduser(config['paths']['download_folder'])
bucket_name = 'genomic-data'

# Connect to MinIO
client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure, http_client=http_client)

# Create bucket if it doesn't exist
if not client.bucket_exists(bucket_name):
    client.make_bucket(bucket_name)
    print(f"Created bucket: {bucket_name}")
else:
    print(f"Bucket '{bucket_name}' already exists.")

# Upload all .fastq files 
for filename in os.listdir(data_folder):
    if not filename.endswith(".fastq"):
        continue

    local_path = os.path.join(data_folder, filename)
    print(f"Uploading {filename}...")
    
    # Track upload time
    start = time.time()
    client.fput_object(bucket_name, filename, local_path)
    print(f"  Uploaded in {time.time() - start:.2f}s")

# List objects in bucket
print("\nFiles in bucket:")
for obj in client.list_objects(bucket_name):
    print(f"  {obj.object_name} ({obj.size} bytes)")

# Download one test file
test_file = "SRR10362244_1.fastq"
download_path = os.path.join(download_dir, f"downloaded_{test_file}")

print(f"\nDownloading {test_file}...")

start = time.time()
client.fget_object(bucket_name, test_file, download_path)
print(f"Downloaded to {download_path} in {time.time() - start:.2f}s")
