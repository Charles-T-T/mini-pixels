//
// Created by yuly on 06.04.23.
//

#include "vector/DateColumnVector.h"

#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <iomanip>

DateColumnVector::DateColumnVector(uint64_t len, bool encoding)
    : ColumnVector(len, encoding) {
    posix_memalign(reinterpret_cast<void **>(&dates), 32,
                   len * sizeof(int32_t));
    memoryUsage += (long)sizeof(int) * len;
}

void DateColumnVector::close() {
    if (!closed) {
        if (encoding && dates != nullptr) {
            free(dates);
        }
        dates = nullptr;
        ColumnVector::close();
    }
}

void DateColumnVector::print(int rowCount) {
    for (int i = 0; i < rowCount; i++) {
        std::cout << dates[i] << std::endl;
    }
}

DateColumnVector::~DateColumnVector() {
    if (!closed) {
        DateColumnVector::close();
    }
}

/**
 * Set a row from a value, which is the days from 1970-1-1 UTC.
 * We assume the entry has already been isRepeated adjusted.
 *
 * @param elementNum
 * @param days
 */
void DateColumnVector::set(int elementNum, int days) {
    if (elementNum >= writeIndex) {
        writeIndex = elementNum + 1;
    }
    dates[elementNum] = days;
    // TODO: isNull
}

void *DateColumnVector::current() {
    if (dates == nullptr) {
        return nullptr;
    } else {
        return dates + readIndex;
    }
}

void DateColumnVector::add(std::string &value) {
    // transform string to date
    std::tm date = {};
    std::istringstream ss(value);
    ss >> std::get_time(&date, "%Y-%m-%d");
    if (ss.fail()) {
        throw std::invalid_argument(
            "Invalid date format. Expected: YYYY-MM-DD");
    }

    // transform date to int
    // FIXME: throw error "Invalid date" if date before 1970
    std::time_t time = std::mktime(&date);
    if (time == -1) {
        throw std::runtime_error("Failed to convert time to timestamp.");
    }
    int dayCount = static_cast<int>(time / (60 * 60 * 24)) + 1;

    // add result
    add(dayCount);
}

void DateColumnVector::add(bool value) { add(value ? 1 : 0); }

void DateColumnVector::add(int64_t value) {
    if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }
    int index = writeIndex++;
    dates[index] = value;
    isNull[index] = false;
}

void DateColumnVector::add(int value) {
    if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }
    int index = writeIndex++;
    dates[index] = value;
    isNull[index] = false;
}

void DateColumnVector::ensureSize(uint64_t size, bool preserveData) {
    if (length < size) {
        int *oldDates = dates;
        posix_memalign(reinterpret_cast<void **>(&dates), 32,
                       size * sizeof(int32_t));
        if (preserveData) {
            std::copy(oldDates, oldDates + length, dates);
        }
        delete[] oldDates;
        memoryUsage += (long)sizeof(int) * (size - length);
        resize(size);
    }
}
