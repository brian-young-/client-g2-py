import argparse
import json
import numpy as np
import base64
import gzip

from rawsensor_wire import RawSensorRowWire 

parser = argparse.ArgumentParser(description='Motusi - next generation streaming client.')
parser.add_argument('dataset_file') 
parser.add_argument('--alignment-frames', action='store_true', help='Stream alignment frames from dataset_file')


def send_row_definition():
    t = RawSensorRowWire()
    print("SEND", "WS Message", "row_definition")
    print("SEND", "RawSensorReadings")
    print("SEND", t.SPEC_COL_COUNT)
    spec = t.header_spec()
    print("SEND", len(spec[0]), spec[0])
    print("SEND", len(spec[1]), spec[1])

def alignment_frame_to_sensor_row(frame):
    """
    Convert alignment frame to the wire sensor row format.
    """
    result = []
    result.append(frame['timestamp'])
    for sensor_index, sensor in enumerate(frame_sensor_order):
        print(sensor_index, sensor)
        for column in reading_types:
            quaternions = np.float32(frame['readings'][sensor_index])
            result.extend(list(quaternions))
            accelerations = np.float32(frame['accelerations'][sensor_index])
            result.extend(accelerations)
            angular_velocities = np.float32(frame['angularVelocities'][sensor_index])
            result.extend(angular_velocities)

    print('row len', len(result))
    return result


def main():


    args = parser.parse_args()
    #print("aframes", args.alignment_frames)

    with open(args.dataset_file) as json_file:
        dataset = json.load(json_file)

    send_row_definition()
   
    row = RawSensorRowWire()
    
    t1 = t2 = 0
    
    if(args.alignment_frames):
        frames = dataset['dataset']['alignment']['frames']
        jsonSize = 0
        binSize = 0
        csvSize = 0
        for frame in frames:
            if t1 == 0:
                t1 = frame["timestamp"]
            t2 = frame["timestamp"]
            
            row.from_aligment_frame(frame)
            jsonSize += len(json.dumps(frame))
            
            bin = row.to_bytes()
            encoded = base64.b64encode(bin)
            binSize += len(encoded)
            
            csv = row.to_csv()
            csvSize += len(csv)
    #print("Dataset loaded from", dataset['dataset'].keys())
    print("Duration", (t2 - t1)/1000, "s")
    print("JSON size", jsonSize)
    print("BIN size", binSize)
    print("CSV size", csvSize)

if __name__ == "__main__":
    main()