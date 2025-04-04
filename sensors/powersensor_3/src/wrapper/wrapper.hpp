#pragma once

#include "PowerSensor.hpp"
#include <memory>
#include <rust/cxx.h>

namespace powersensor3_rs {
using namespace PowerSensor3;

struct MeasurementResult {
    double seconds;
    double joules;
    double watts;
};

std::unique_ptr<PowerSensor> create(const rust::Str device);
std::unique_ptr<State> read(const PowerSensor& sensor);
double calculate_watts(const State& start, const State& end, int pair_id);
rust::String get_sensor_name(const PowerSensor& sensor, int sensor_id);

}