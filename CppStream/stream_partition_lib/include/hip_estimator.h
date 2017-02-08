#pragma once
#include "dense_array.h"
#include "bit_util.h"

//#if defined(_MSC_VER)
//
//#include <intrin.h>
//#pragma intrinsic(_BitScanForward)
//#pragma intrinsic(_BitScanForward64)
//
//#define tzcnt16(x)	__mvc_ctz(x)
//#define	tzcnt32(x)	__mvc_ctz(x)
//#define	tzcnt64(x)	__mvc_ctz_64(x)
//#define lzcnt8(x)	__lzcnt16(x) - CHAR_BIT
//#define lzcnt16(x)	__lzcnt16(x)
//#define lzcnt32(x)	__lzcnt(x)
//#define lzcnt64(x)	__lzcnt64(x)
//
//#define popcnt64(x)	__popcnt64(x)
//
//inline uint32_t __mvc_ctz(uint32_t value)
//{
//	unsigned long mask;
//	_BitScanForward(&mask, value);
//	return mask;
//}
//
//inline uint64_t __mvc_ctz_64(uint64_t value)
//{
//	unsigned long mask;
//	_BitScanForward64(&mask, value);
//	return mask;
//}
//#else	// defined(_MSC_VER)
//#define tzcnt16(x)	__builtin_ctz(x)
//#define tzcnt32(x)	__builtin_ctz(x)
//#define	tzcnt64(x)	__builtin_ctz(x)
//#define	lzcnt8(x)	__builtin_clz(x) - 24
//#define lzcnt16(x)	__builtin_clz(x) - 16
//#define lzcnt32(x)	__builtin_clz(x)
//#define lzcnt64(x)	__builtin_clzl(x)
//#define popcnt64(x)	__builtin_popcountll(x)
//#endif // !defined(_MSC_VER_)

/*
 * Implementation taken from https://github.com/iwiwi/hyperloglog-hip
 * 
 */
template<typename Key, typename Hash = std::hash<Key>, int RegisterSizeInBits = 5>
class hip_estimator
{
public:
	typedef Key key_type;
	
	typedef Hash hash_type;

	hip_estimator(size_t num_bucket_bits = 12)
		: num_bucket_bits_(num_bucket_bits), M(1 << num_bucket_bits), c_(0), 
	    s_(1 << num_bucket_bits) {}

	hip_estimator(const hip_estimator<Key, Hash, RegisterSizeInBits>& other)
		: num_bucket_bits_(other.num_bucket_bits_), M(other.M), c_(other.c_), s_(other.s_) {}

	void init()
	{
		c_ = 0;
		s_ = 1 << num_bucket_bits_;

	}

	void insert(const key_type &v)
	{
		static constexpr uint64_t num_register_bits = RegisterSizeInBits;
		static constexpr uint64_t register_limit = (uint64_t(1) << num_register_bits) - 1;

		const uint64_t h = hash_(v) * magic1() + magic2();
		const uint64_t h0 = h & ((uint64_t(1) << num_bucket_bits_) - 1); // isolate register bits
		const uint64_t h1 = h >> num_bucket_bits_; // isolate bucket value

		const uint64_t b_old = M.get(h0);
		const uint64_t b_new = h1 == 0 ? register_limit : std::min(register_limit, uint64_t(1 + tzcnt64(h1)));

		if (b_new > b_old)
		{
			M.set(h0, b_new);
			c_ += 1.0 / (s_ / (uint64_t(1) << num_bucket_bits_));
			s_ -= 1.0 / (uint64_t(1) << b_old);
			if (b_new < register_limit)
			{
				s_ += 1.0 / (uint64_t(1) << b_new);
			}
		}
	}

	size_t imitate_insert(const key_type &v) const 
	{
		static constexpr uint64_t num_register_bits = RegisterSizeInBits;
		static constexpr uint64_t register_limit = (uint64_t(1) << num_register_bits) - 1;

		const uint64_t h = hash_(v) * magic1() + magic2();
		const uint64_t h0 = h & ((uint64_t(1) << num_bucket_bits_) - 1); // isolate register bits
		const uint64_t h1 = h >> num_bucket_bits_; // isolate bucket value

		const uint64_t b_old = M.get(h0);
		const uint64_t b_new = h1 == 0 ? register_limit : std::min(register_limit, uint64_t(1 + tzcnt64(h1)));

		double c_copy_ = c_;

		if (b_new > b_old)
		{
			c_copy_ += 1.0 / (s_ / (uint64_t(1) << num_bucket_bits_));
		}
		return round(c_copy_);
	}

	size_t count() const 
	{
		return round(c_);
	}

	void copy(const hip_estimator<Key, Hash, RegisterSizeInBits>& o)
	{
		M.copy(o.M);
		c_ = o.c_;
		s_ = o.s_;
	}

private:
	const size_t num_bucket_bits_;

	dense_array<RegisterSizeInBits> M;
	
	double c_, s_;
	
	hash_type hash_;

	static constexpr uint64_t magic1()
	{
		return 9223372036854775837ULL;
	}

	static constexpr uint64_t magic2()
	{
		return 1234567890123456789ULL;
	}
};