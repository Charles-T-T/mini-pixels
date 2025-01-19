//
// Created by liyu on 12/23/23.
//

#include "vector/TimestampColumnVector.h"
#include <cstdint>
#include <ctime>
#include <iomanip>

TimestampColumnVector::TimestampColumnVector(int precision, bool encoding)
    : ColumnVector(VectorizedRowBatch::DEFAULT_SIZE, encoding) {
    TimestampColumnVector(VectorizedRowBatch::DEFAULT_SIZE, precision,
                          encoding);
}

TimestampColumnVector::TimestampColumnVector(uint64_t len, int precision,
                                             bool encoding)
    : ColumnVector(len, encoding) {
    this->precision = precision;
    posix_memalign(reinterpret_cast<void **>(&this->times), 64,
                   len * sizeof(long));
}

void TimestampColumnVector::close() {
    if (!closed) {
        ColumnVector::close();
        if (encoding && this->times != nullptr) {
            free(this->times);
        }
        this->times = nullptr;
    }
}

void TimestampColumnVector::print(int rowCount) {
    throw InvalidArgumentException("not support print longcolumnvector.");
    //    for(int i = 0; i < rowCount; i++) {
    //        std::cout<<longVector[i]<<std::endl;
    //		std::cout<<intVector[i]<<std::endl;
    //    }
}

TimestampColumnVector::~TimestampColumnVector() {
    if (!closed) {
        TimestampColumnVector::close();
    }
}

void *TimestampColumnVector::current() {
    if (this->times == nullptr) {
        return nullptr;
    } else {
        return this->times + readIndex;
    }
}

/**
 * Set a row from a value, which is the days from 1970-1-1 UTC.
 * We assume the entry has already been isRepeated adjusted.
 *
 * @param elementNum
 * @param days
 */
void TimestampColumnVector::set(int elementNum, long ts) {
    if (elementNum >= writeIndex) {
        writeIndex = elementNum + 1;
    }
    times[elementNum] = ts;
    // TODO: isNull
}

void TimestampColumnVector::add(std::string &value) {
    // transform string into timestamp
    std::tm tm = {};
    std::istringstream ss(value);
    ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
    if (ss.fail()) {
        throw std::invalid_argument(
            "Invalid timestamp string format. Expected: YYYY-MM-DD HH:MM:SS");
    }
    time_t timestamp = std::mktime(&tm);
    if (timestamp == -1) {
        throw std::runtime_error("Failed to convert time to timestamp.");
    }

    // transform timestamp into long, then add
    add(static_cast<long>(timestamp));
}

void TimestampColumnVector::add(int64_t value) {
    if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }
    // int index = writeIndex++;
    // times[index] = value;
    // isNull[index] = false;
    set(writeIndex++, value);
}

void TimestampColumnVector::ensureSize(uint64_t size, bool preserveData) {
    ColumnVector::ensureSize(size, preserveData);
    if (length < size) {
        long *oldTimes = times;
        posix_memalign(reinterpret_cast<void **>(&times), 32,
                       size * sizeof(int64_t));
        if (preserveData) {
            std::copy(oldTimes, oldTimes + length, times);
        }
        delete[] oldTimes;
        memoryUsage += (long)sizeof(long) * (size - length);
        resize(size);
    }
}
