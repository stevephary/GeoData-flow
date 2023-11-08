'''Compress and decompress a JSON object'''

import zlib
import gzip
import json, base64


def json_zip_writer(j, target_key):
    f = gzip.open(target_key, 'wb')
    f.write(json.dumps(j).encode('utf-8'))
    f.close()
    except Exception as e:
        print(f"Error while saving data to {filename}: {e}")


def json_zip(data):

    # return base64.b64encode(zlib.compress(json.dumps(j).encode('utf-8'))).decode('ascii')
    return zlib.compress(json.dumps(data).encode('utf-8'))


def json_unzip(data, insist=True):

    try:
        # j = zlib.decompress(base64.b64decode(j))
        data = zlib.decompress(data)
    except:
        raise RuntimeError("Could not decode/unzip the contents")

    try:
        data = json.loads(data)
    except:
        raise RuntimeError("Could interpret the unzipped contents")

    return data