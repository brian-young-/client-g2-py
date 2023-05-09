import numpy as np
import struct
import io

"""
A class for working with the raw sensor data on the wire.
"""
class RawSensorRowWire:

    """ frame_sensor_order 
    The order of the sensors in the frame as saved in the dataset json file. 
    DON'T CHANGE THIS ORDER!
    """

    SPEC_ROW_COUNT = 2
    SPEC_COL_COUNT = 101

    SPEC_NAME_ROW = 0
    SPEC_TYPE_ROW = 1

    MAX_HEADER_NAME_LEN = 24

    frame_sensor_order = [
        "armr_upp",
        "armr_low",
        "arml_upp",
        "arml_low",
        "thorasic_upp",
        "legr_upp",
        "legr_low",
        "legl_upp",
        "legl_low",
        "lumbar_low",
    ]

    reading_types = [
        "quaternion",
        "acceleration",
        "angular_velocity",
    ]

    column_type_size = {
        "timestamp": "uint64",
        "quaternion": "float32",
        "acceleration": "float32",
        "angular_velocity": "float32",
    }

    sensor_readings = [
        "q_w",
        "q_x",
        "q_y",
        "q_z",
        "acc_x",
        "acc_y",
        "acc_z",
        "angv_x",
        "angv_y",
        "angv_z",
        ]
    
    def __init__(self):
        self.columns = []
        pass

    def type_from_name(self, name):
        if name == "timestamp":
            return self.column_type_size["timestamp"]
        if "q_" in name:
            return self.column_type_size["quaternion"]
        if "acc_" in name:
            return self.column_type_size["acceleration"]
        if "angv_" in name:
            return self.column_type_size["angular_velocity"]
        return None

    def header_spec(self):
        """
        Return the header spec for the sensor row format.

        The header spec is a 2d numpy array,
        first row is list of column names,
        second row is list of column types.
        """
        result = np.chararray((self.SPEC_ROW_COUNT, self.SPEC_COL_COUNT), itemsize=self.MAX_HEADER_NAME_LEN)
        index = 0
        result[self.SPEC_NAME_ROW][index] = "timestamp"
        result[self.SPEC_TYPE_ROW][index] = self.column_type_size["timestamp"]
        index = index + 1
        for sensor_index, sensor in enumerate(RawSensorRowWire.frame_sensor_order):
            for reading_index, column in enumerate(RawSensorRowWire.sensor_readings):
                name = sensor + "_" + column
                result[self.SPEC_NAME_ROW][index] = name
                result[self.SPEC_TYPE_ROW][index] = self.type_from_name(name)
                index = index + 1

        return result

    def from_aligment_frame(self, frame):
        """
        Convert alignment frame to a list of dat in the wire sensor row format.
        """
        self.columns = []
        self.columns.append(np.uint64(frame['timestamp']))
        for sensor_index, sensor in enumerate(RawSensorRowWire.frame_sensor_order):
            quaternions = np.float32(frame['readings'][sensor_index])
            self.columns.extend(list(quaternions))
            if 'accelerations' in frame:
                accelerations = np.float32(frame['accelerations'][sensor_index])
                self.columns.extend(accelerations)
            else:
                self.columns.extend([np.float32(0.0), np.float32(0.0), np.float32(0.0)])

            if 'angularVelocities' in frame:
                angular_velocities = np.float32(frame['angularVelocities'][sensor_index])
                self.columns.extend(angular_velocities)
            else:
                self.columns.extend([np.float32(0.0), np.float32(0.0), np.float32(0.0)])
    
    def to_csv(self):
        return ','.join(map(str, self.columns))
    
    def to_bytes(self):
        s = io.BytesIO()

        for el in self.columns:
            if isinstance(el, np.float32):
                b = struct.pack('!f', el)
                s.write(b)
            elif isinstance(el, np.uint32):
                b = struct.pack('!I', el)
                s.write(b)
            elif isinstance(el, np.uint64):
                b = struct.pack('!Q', el)
                s.write(b)
            else:
                raise Exception("Unhandled element type", type(el))
        
        s.seek(0)
        return s.read()


        
        