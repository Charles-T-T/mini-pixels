//
// Created by yuly on 05.04.23.
//

#include "vector/DecimalColumnVector.h"
#include "duckdb/common/types/decimal.hpp"
#include <cstdint>
// #include <boost/multiprecision/cpp_dec_float.hpp>

/**
 * The decimal column vector with precision and scale.
 * The values of this column vector are the unscaled integer value
 * of the decimal. For example, the unscaled value of 3.14, which is
 * of the type decimal(3,2), is 314. While the precision and scale
 * of this decimal are 3 and 2, respectively.
 *
 * <p><b>Note: it only supports short decimals with max precision
 * and scale 18.</b></p>
 *
 * Created at: 05/03/2022
 * Author: hank
 */

DecimalColumnVector::DecimalColumnVector(int precision, int scale, bool encoding): ColumnVector(VectorizedRowBatch::DEFAULT_SIZE, encoding) {
    DecimalColumnVector(VectorizedRowBatch::DEFAULT_SIZE, precision, scale, encoding);
}

DecimalColumnVector::DecimalColumnVector(uint64_t len, int precision, int scale,
                                         bool encoding)
    : ColumnVector(len, encoding) {
    // decimal column vector has no encoding so we don't allocate memory to
    // this->vector
    this->vector = nullptr;
    this->precision = precision;
    this->scale = scale;

    using duckdb::Decimal;
    if (precision <= Decimal::MAX_WIDTH_INT16) {
        physical_type_ = PhysicalType::INT16;
        posix_memalign(reinterpret_cast<void **>(&vector), 32,
                       len * sizeof(int16_t));
        memoryUsage += (uint64_t)sizeof(int16_t) * len;
    } else if (precision <= Decimal::MAX_WIDTH_INT32) {
        physical_type_ = PhysicalType::INT32;
        posix_memalign(reinterpret_cast<void **>(&vector), 32,
                       len * sizeof(int32_t));
        memoryUsage += (uint64_t)sizeof(int32_t) * len;
    } else if (precision <= Decimal::MAX_WIDTH_INT64) {
        physical_type_ = PhysicalType::INT64;
        memoryUsage += (uint64_t)sizeof(uint64_t) * len;
    } else if (precision <= Decimal::MAX_WIDTH_INT128) {
        physical_type_ = PhysicalType::INT128;
        memoryUsage += (uint64_t)sizeof(uint64_t) * len;
    } else {
        throw std::runtime_error(
            "Decimal precision is bigger than the maximum supported width");
    }
}

void DecimalColumnVector::close() {
    if (!closed) {
        ColumnVector::close();
        if (physical_type_ == PhysicalType::INT16 ||
            physical_type_ == PhysicalType::INT32) {
            free(vector);
        }
        vector = nullptr;
    }
}

void DecimalColumnVector::print(int rowCount) {
//    throw InvalidArgumentException("not support print Decimalcolumnvector.");
    for(int i = 0; i < rowCount; i++) {
        std::cout<<vector[i]<<std::endl;
    }
}

DecimalColumnVector::~DecimalColumnVector() {
    if(!closed) {
        DecimalColumnVector::close();
    }
}

void * DecimalColumnVector::current() {
    if(vector == nullptr) {
        return nullptr;
    } else {
        return vector + readIndex;
    }
}

int DecimalColumnVector::getPrecision() {
	return precision;
}


int DecimalColumnVector::getScale() {
	return scale;
}

void DecimalColumnVector::add(std::string &value) {
        // check for valid decimal format 
    size_t dotPos = value.find('.');
    std::string integerPart = value.substr(0, dotPos);
    std::string fractionalPart = (dotPos != std::string::npos) ? value.substr(dotPos + 1) : "";

    long unscaledValue;  // result

    if (integerPart.length() == precision) {
        unscaledValue = stol(integerPart);
        if (!fractionalPart.empty()) {
            int roundDigit = fractionalPart[0] - '0';
            if (roundDigit >= 5) {
                unscaledValue++;
            }
        }
        add(static_cast<int64_t>(unscaledValue));
        return;
    }
    if (integerPart.length() > precision) {
        throw std::overflow_error("Decimal value exceeds specified precision: integer part too long.");
    }

    // handle precision and scale
    std::string combined;
    if (!fractionalPart.empty()) {
        if (fractionalPart.length() > static_cast<size_t>(scale)) {
            // truncate extra fractional digits
            int roundDigit = fractionalPart[scale] - '0';
            fractionalPart = fractionalPart.substr(0, scale); 
            long fractionalNum = stol(fractionalPart);
            if (roundDigit >= 5) {
                fractionalNum++;
            }
            fractionalPart = std::to_string(fractionalNum);
        } else {
            fractionalPart.append(scale - fractionalPart.length(), '0'); // pad with zeros
        }
    } else {
        fractionalPart.insert(0, scale, '0');
    }
    combined = integerPart + fractionalPart;
    
    // use long to save decimal result
    try {
        unscaledValue = std::stol(combined);
    } catch (const std::out_of_range&) {
        throw std::overflow_error("Decimal value exceeds range of long.");
    }

    // add result
    add(static_cast<int64_t>(unscaledValue)); 
}

void DecimalColumnVector::add(int64_t value) {
    if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }
    int index = writeIndex++;
    vector[index] = value;
    isNull[index] = false;
}

void DecimalColumnVector::ensureSize(uint64_t size, bool preserveData) {
    ColumnVector::ensureSize(size, preserveData);
    if (length < size) {
        long *oldTimes = vector;
        posix_memalign(reinterpret_cast<void **>(&vector), 32,
                       size * sizeof(int64_t));
        if (preserveData) {
            std::copy(oldTimes, oldTimes + length, vector);
        }
        delete[] oldTimes;
        memoryUsage += (long)sizeof(long) * (size - length);
        resize(size);
    }
}
