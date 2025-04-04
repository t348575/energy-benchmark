#pragma once

#include <pmt.h>
#include <memory>
#include <rust/cxx.h>

using namespace pmt;

enum class SensorType: uint8_t;

std::unique_ptr<PMT> create(const SensorType sensor);
std::unique_ptr<State> read(PMT& device);
double watts(const State& first, const State& second, const int sensor_id);
rust::String get_sensor_name(PMT& device, const int sensor_id);
