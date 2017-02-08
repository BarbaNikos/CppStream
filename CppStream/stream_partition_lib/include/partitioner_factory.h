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

#ifndef NAIVE_SHED_FLD_PARTITIONER_H_
#include "naive_shed_fld.h"
#endif // !NAIVE_SHED_FLD_PARTITIONER_H_

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
	//static Partitioner* generate_copy(const std::string& partitioner_identifier, const Partitioner* prototype)
	//{
	//	if (partitioner_identifier.compare("sh") == 0)
	//	{
	//		return new RoundRobinPartitioner(*(RoundRobinPartitioner*)prototype);
	//	}
	//	else if (partitioner_identifier.compare("fld") == 0)
	//	{
	//		return new HashFieldPartitioner(*(HashFieldPartitioner*)prototype);
	//	}
	//	else if (partitioner_identifier.compare("naive_shed_fld") == 0)
	//	{
	//		return new NaiveShedFieldPartitioner(*(NaiveShedFieldPartitioner*)prototype);
	//	}
	//	else if (partitioner_identifier.compare("pk") == 0)
	//	{
	//		return new PkgPartitioner(*(PkgPartitioner*)prototype);
	//	}
	//	else if (partitioner_identifier.compare("ca_naive") == 0 || partitioner_identifier.compare("la_naive") == 0)
	//	{
	//		//return new CaPartitionLib::CA_Partitioner(*(CaPartitionLib::CA_Partitioner*)prototype);
	//	}
	//	else if (partitioner_identifier.compare("ca_aff_naive") == 0)
	//	{
	//		//return new CaPartitionLib::AN_Partitioner(*(CaPartitionLib::AN_Partitioner*)prototype);
	//	}
	//	else if (partitioner_identifier.compare("ca_hll") == 0 || partitioner_identifier.compare("la_hll") == 0)
	//	{
	//		//return new CaPartitionLib::CHLL_Partitioner(*(CaPartitionLib::CHLL_Partitioner*)prototype);
	//	}
	//	else if (partitioner_identifier.compare("ca_aff_hll") == 0)
	//	{
	//		//return new CaPartitionLib::AHLL_Partitioner(*(CaPartitionLib::AHLL_Partitioner*)prototype);
	//	}
	//	else
	//	{
	//		return nullptr;
	//	}
	//}
};
#endif // !PARTITIONER_FACTORY_H_
