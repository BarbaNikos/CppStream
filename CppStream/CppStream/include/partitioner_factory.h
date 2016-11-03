#pragma once

#ifndef _PARTITIONER_H_
#include "partitioner.h"
#endif // !_PARTITIONER_H_

#ifndef ROUND_ROBIN_PARTITIONER_H_
#include "round_robin_partitioner.h"
#endif // !ROUND_ROBIN_PARTITIONER_H_

#ifndef HASH_FLD_PARTITIONER_H_
#include "hash_fld_partitioner.h"
#endif // !HASH_FLD_PARTITIONER_H_

#ifndef PKG_PARTITIONER_H_
#include "pkg_partitioner.h"
#endif // !PKG_PARTITIONER_H_

#ifndef CA_PARTITION_LIB_H_
#include "ca_partition_lib.h"
#endif // !CA_PARTITION_LIB_H_

#ifndef PARTITIONER_FACTORY_H_
class PartitionerFactory
{
public:
	static Partitioner* generate_copy(const std::string& partitioner_identifier, const Partitioner* prototype)
	{
		if (partitioner_identifier.compare("sh") == 0)
		{
			return new RoundRobinPartitioner(*(RoundRobinPartitioner*)prototype);
		}
		else if (partitioner_identifier.compare("fld") == 0)
		{
			return new HashFieldPartitioner(*(HashFieldPartitioner*)prototype);
		}
		else if (partitioner_identifier.compare("pk") == 0)
		{
			return new PkgPartitioner(*(PkgPartitioner*)prototype);
		}
		else if (partitioner_identifier.compare("ca_naive") == 0 || partitioner_identifier.compare("la_naive") == 0)
		{
			return new CaPartitionLib::CA_Exact_Partitioner(*(CaPartitionLib::CA_Exact_Partitioner*)prototype);
		}
		else if (partitioner_identifier.compare("ca_aff_naive") == 0)
		{
			return new CaPartitionLib::CA_Exact_Aff_Partitioner(*(CaPartitionLib::CA_Exact_Aff_Partitioner*)prototype);
		}
		else if (partitioner_identifier.compare("ca_hll") == 0 || partitioner_identifier.compare("la_hll") == 0)
		{
			return new CaPartitionLib::CA_HLL_Partitioner(*(CaPartitionLib::CA_HLL_Partitioner*)prototype);
		}
		else if (partitioner_identifier.compare("ca_aff_hll") == 0)
		{
			return new CaPartitionLib::CA_HLL_Aff_Partitioner(*(CaPartitionLib::CA_HLL_Aff_Partitioner*)prototype);
		}
	}
};
#endif // !PARTITIONER_FACTORY_H_
