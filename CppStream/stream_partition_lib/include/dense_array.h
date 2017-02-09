#pragma once
#include <cstdint>
#include <algorithm>
#include <memory>
#include <limits.h>

/*
* Implementation taken from https://github.com/iwiwi/hyperloglog-hip
*
*/
template<size_t RegisterSizeInBits, typename Value = uint8_t>
class dense_array
{
public:
	typedef Value value_type;

	dense_array(size_t number_of_registers)
		: data(new value_type[data_length(number_of_registers)]()) 
	{
		number_of_registers_ = number_of_registers;
	}

	dense_array(const dense_array<RegisterSizeInBits, Value>& other)
	{
		number_of_registers_ = other.number_of_registers_;
		data = std::unique_ptr<value_type[]>(new value_type[data_length(number_of_registers_)]());
		for (size_t i = 0; i < data_length(number_of_registers_); ++i)
		{
			data[i] = other.data[i];
		}
	}

	void init()
	{
		value_type* for_deletion_ = nullptr;
		for_deletion_ = data.release();
		if (for_deletion_ != nullptr)
			delete[] for_deletion_;
		data = std::unique_ptr<value_type[]>(new value_type[data_length(number_of_registers_)]());
	}

	value_type get(size_t position) const 
	{
		const size_t b = position * register_size_in_bits();
		const size_t i1 = b / value_size_in_bits();
		const size_t o1 = b - i1 * value_size_in_bits();
		const size_t n1 = value_size_in_bits() - o1;
		value_type v = data[i1] >> o1;

		if (n1 > register_size_in_bits())
		{
			v &= (value_type(1) << register_size_in_bits()) - 1;
		}
		else if (n1 < register_size_in_bits())
		{
			const size_t i2 = i1 + 1;
			const size_t n2 = register_size_in_bits() - n1;
			v |= (data[i2] & ((value_type(1) << n2) - 1)) << n1;
		}
		return v;
	}

	void set(size_t position, value_type val)
	{
		const size_t b = position * register_size_in_bits();
		const size_t i1 = b / value_size_in_bits();
		const size_t o1 = b - i1 * value_size_in_bits();
		const size_t n1 = std::min(value_size_in_bits() - o1, register_size_in_bits());
		data[i1] &= value_type(-1) ^ (((value_type(1) << n1) - 1) << o1);
		data[i1] |= val << o1;

		if (n1 < register_size_in_bits()) {
			const size_t i2 = i1 + 1;
			const size_t n2 = register_size_in_bits() - n1;
			data[i2] &= value_type(-1) ^ ((value_type(1) << n2) - 1);
			data[i2] |= val >> n1;
		}
	}

	void copy(const dense_array<RegisterSizeInBits, Value>& o)
	{
		number_of_registers_ = o.number_of_registers_;
		value_type* for_deletion_ = nullptr;
		for_deletion_ = data.release();
		if (for_deletion_ != nullptr) 
			delete[] for_deletion_;
		data = std::unique_ptr<value_type[]>(new value_type[data_length(number_of_registers_)]());
		for (size_t i = 0; i < data_length(number_of_registers_); ++i)
		{
			data[i] = o.data[i];
		}
	}

private:

	size_t number_of_registers_; 

	std::unique_ptr<value_type[]> data;

	static constexpr size_t register_size_in_bits()
	{
		return RegisterSizeInBits;
	}

	static constexpr size_t value_size_in_bits()
	{
		return sizeof(Value) * CHAR_BIT;
	}

	static constexpr size_t data_length(size_t number_of_registers)
	{
		return (number_of_registers * register_size_in_bits() + value_size_in_bits() - 1) / value_size_in_bits();
	}

	static_assert(std::is_unsigned<value_type>::value, "Value should be an unsigned integral type.");

	static_assert((sizeof(value_type) * CHAR_BIT) >= RegisterSizeInBits, "Value should be at least RegisterSizeInBits bits long.");
};

template<typename Value>
class dense_array_primitive
{
public:
	typedef Value value_type;

	dense_array_primitive(size_t size) : data(new value_type[size]()) { size_ = size; }
	virtual ~dense_array_primitive() {}

	value_type get(size_t pos) const
	{
		return data[pos];
	}

	void set(size_t pos, value_type val)
	{
		data[pos] = val;
	}

	void copy(const dense_array_primitive<Value>& o)
	{
		size_ = o.size_;
		value_type* for_deletion = nullptr;
		for_deletion = data.release();
		if (for_deletion != nullptr) 
			delete[] for_deletion;
		data = std::unique_ptr<value_type[]>(new value_type[size_]());
		memcpy(data, o.data, sizeof(value_type) * size_);
	}

private:
	std::unique_ptr<value_type[]> data;
	size_t size_;
};

template<>
class dense_array<8, uint8_t> : public dense_array_primitive<uint8_t>
{
public:
	dense_array(size_t size) : dense_array_primitive(size) {}
};

template<>
class dense_array<16, uint16_t> : public dense_array_primitive<uint16_t> 
{
public:
	dense_array(size_t size) : dense_array_primitive(size) {}
};

template<>
class dense_array<32, uint32_t> : public dense_array_primitive<uint32_t>
{
public:
	dense_array(size_t size) : dense_array_primitive(size) {}
};

template<>
class dense_array<64, uint64_t> : public dense_array_primitive<uint64_t> 
{
public:
	dense_array(size_t size) : dense_array_primitive(size) {}
};