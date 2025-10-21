#include "powersensor3/src/wrapper/wrapper.hpp"
#include "PowerSensor.hpp"
#include <stdexcept>

namespace powersensor3_rs {

std::unique_ptr<PowerSensor> create(const rust::Str device) {
    try {
        std::string cpp_device = std::string(device);
        return std::make_unique<PowerSensor>(cpp_device);
    } catch (const std::exception& e) {
        throw std::runtime_error(e.what());
    }
}

std::unique_ptr<State> read(const PowerSensor& sensor) {
    try {
        return std::make_unique<State>(sensor.read());
    } catch (const std::exception& e) {
        throw std::runtime_error(e.what());
    }
}

double calculate_watts(const State& start, const State& end, int pair_id) {
    return Watt(start, end, pair_id);
}

rust::String get_sensor_name(const PowerSensor& sensor, int sensor_id) {
    if (sensor_id < 0) {
        throw std::runtime_error("Invalid sensor id");
    }
    return rust::String(sensor.getPairName(sensor_id));
}

}