#include "pmt/src/wrapper/wrapper.hpp"
#include "pmt/src/lib.rs.h"
#include <stdexcept>
#include <string>

std::unique_ptr<PMT> create(const SensorType sensor) {
	switch (sensor) {
		case SensorType::RAPL:
			return rapl::Rapl::Create();
		default:
			throw std::runtime_error("Unknown sensor type");
	}
}

std::unique_ptr<State> read(PMT& device) {
	return std::make_unique<State>(device.Read());
}

rust::String get_sensor_name(PMT& device, const int sensor_id) {
	if (sensor_id < 0) {
		throw std::runtime_error("Invalid sensor id");
	}
	return rust::String(device.Read().name(sensor_id));
}

double Joules(const State &firstState, const State &secondState, const int pairID) {
    if (pairID > firstState.NrMeasurements()) {
		std::runtime_error("Invalid pairID" + std::to_string(pairID) + ", maximum value is " + std::to_string(firstState.NrMeasurements()));
    }

    if (pairID >= 0) {
		return secondState.joules(pairID) - firstState.joules(pairID);
    }

    double joules = 0;
    for (int i = 0; i < secondState.NrMeasurements(); i++) {
		joules += secondState.joules(i);
    }

    for (int i = 0; i < firstState.NrMeasurements(); i++) {
		joules -= firstState.joules(i);
    }
    return joules;
}

double Watt(const State &firstState, const State &secondState, const int pairID) {
	return Joules(firstState, secondState, pairID) /
		PMT::seconds(firstState, secondState);
}

double watts(const State& first, const State& second, const int sensor_id) {
	return Watt(first, second, sensor_id);
}
