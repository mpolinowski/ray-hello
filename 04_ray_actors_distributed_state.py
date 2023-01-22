import ray

@ray.remote

class ReadSensor(object):
    def __init__(self):
        self.value = 0
    def get_value(self):
        self.value += 1
        return f"Current value is: {self.value}"

# create an actor instance
sensor_reading = ReadSensor.remote()

# call the actor multiple times
print(ray.get(sensor_reading.get_value.remote()))
print(ray.get(sensor_reading.get_value.remote()))
print(ray.get(sensor_reading.get_value.remote()))
print(ray.get(sensor_reading.get_value.remote()))
print(ray.get(sensor_reading.get_value.remote()))