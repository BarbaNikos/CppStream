#pragma once
#include <cstring>
#include <fstream>
#include <sstream>
#include <unordered_map>
#include <vector>

#ifndef TPCH_STRUCT_LIB_H_
#include "tpch_struct_lib.h"
#endif  // !TPCH_STRUCT_LIB_H_

#ifndef TPCH_UTIL_H_
#define TPCH_UTIL_H_

namespace Experiment {
namespace Tpch {
class DataParser {
 public:
  static void parse_tpch_part(const std::string& f, std::vector<part>& buffer);
  static void parse_tpch_supplier(const std::string& f,
                                  std::vector<supplier>& buffer);
  static void parse_tpch_partsupp(const std::string& f,
                                  std::vector<partsupp>& buffer);
  static void parse_tpch_customer(const std::string& f,
                                  std::vector<customer>& buffer);
  static void parse_tpch_q3_customer(const std::string& f,
                                     std::vector<q3_customer>& buffer);
  static void parse_tpch_lineitem(const std::string& f,
                                  std::vector<lineitem>& buffer);
  static void parse_tpch_order(const std::string& f,
                               std::vector<order>& buffer);
  static void parse_tpch_region(const std::string& f,
                                std::vector<region>& buffer);
  static void parse_tpch_nation(const std::string& f,
                                std::vector<nation>& buffer);
  static void parse_all_files_test(const std::string& f);
};

class TableJoinUtil {
 public:
  static void hash_join_lineitem_order(
      const std::vector<lineitem>& lineitem_table,
      const std::vector<order>& order_table,
      std::vector<lineitem_order>& result);
  static void hash_join_customer_lineitem_order(
      const std::vector<customer>& customer_table,
      const std::vector<lineitem>& lineitem_table,
      const std::vector<order>& order_table,
      std::vector<customer_lineitem_order>& result);
};
}
}
#endif  // TPCH_UTIL_H_

inline void Experiment::Tpch::DataParser::parse_tpch_part(
    const std::string& f, std::vector<part>& buffer) {
  std::string line;
  std::ifstream file(f);
  if (!file.is_open()) {
    std::cout << "failed to open file.\n";
    exit(1);
  }
  while (getline(file, line)) {
    Tpch::part part;
    part.parse(line, '|');
    buffer.push_back(part);
  }
  file.close();
  buffer.shrink_to_fit();
}

inline void Experiment::Tpch::DataParser::parse_tpch_supplier(
    const std::string& f, std::vector<supplier>& buffer) {
  std::string line;
  std::ifstream file(f);
  if (!file.is_open()) {
    std::cout << "failed to open file.\n";
    exit(1);
  }
  while (getline(file, line)) {
    Tpch::supplier supplier;
    supplier.parse(line, '|');
    buffer.push_back(supplier);
  }
  file.close();
  buffer.shrink_to_fit();
}

inline void Experiment::Tpch::DataParser::parse_tpch_partsupp(
    const std::string& f, std::vector<partsupp>& buffer) {
  std::string line;
  std::ifstream file(f);
  if (!file.is_open()) {
    std::cout << "failed to open file.\n";
    exit(1);
  }
  while (getline(file, line)) {
    Tpch::partsupp partsupp;
    partsupp.parse(line, '|');
    buffer.push_back(partsupp);
  }
  file.close();
  buffer.shrink_to_fit();
}

inline void Experiment::Tpch::DataParser::parse_tpch_customer(
    const std::string& f, std::vector<customer>& buffer) {
  std::string line;
  std::ifstream file(f);
  if (!file.is_open()) {
    std::cout << "failed to open file.\n";
    exit(1);
  }
  while (getline(file, line)) {
    Tpch::customer customer;
    customer.parse(line, '|');
    buffer.push_back(customer);
  }
  file.close();
  buffer.shrink_to_fit();
}

inline void Experiment::Tpch::DataParser::parse_tpch_q3_customer(
    const std::string& f, std::vector<q3_customer>& buffer) {
  std::string line;
  std::ifstream file(f);
  if (!file.is_open()) {
    std::cout << "failed to open file.\n";
    exit(1);
  }
  while (getline(file, line)) {
    Tpch::customer customer;
    customer.parse(line, '|');
    buffer.push_back(Tpch::q3_customer(customer));
  }
  file.close();
  buffer.shrink_to_fit();
}

inline void Experiment::Tpch::DataParser::parse_tpch_lineitem(
    const std::string& f, std::vector<lineitem>& buffer) {
  std::string line;
  std::ifstream file(f);
  if (!file.is_open()) {
    std::cout << "failed to open file.\n";
    exit(1);
  }
  while (getline(file, line)) {
    Tpch::lineitem line_item;
    line_item.parse(line, '|');
    buffer.push_back(line_item);
  }
  file.close();
  buffer.shrink_to_fit();
}

inline void Experiment::Tpch::DataParser::parse_tpch_order(
    const std::string& f, std::vector<order>& buffer) {
  std::string line;
  std::ifstream file(f);
  if (!file.is_open()) {
    std::cout << "failed to open file.\n";
    exit(1);
  }
  while (getline(file, line)) {
    Tpch::order order;
    order.parse(line, '|');
    buffer.push_back(order);
  }
  file.close();
  line.shrink_to_fit();
}

inline void Experiment::Tpch::DataParser::parse_tpch_region(
    const std::string& f, std::vector<region>& buffer) {
  std::string line;
  std::ifstream file(f);
  if (!file.is_open()) {
    std::cout << "failed to open file.\n";
    exit(1);
  }
  while (getline(file, line)) {
    Tpch::region region;
    region.parse(line, '|');
    buffer.push_back(region);
  }
  file.close();
  buffer.shrink_to_fit();
}

inline void Experiment::Tpch::DataParser::parse_tpch_nation(
    const std::string& f, std::vector<nation>& buffer) {
  std::string line;
  std::ifstream file(f);
  if (!file.is_open()) {
    std::cout << "failed to open file.\n";
    exit(1);
  }
  while (getline(file, line)) {
    Tpch::nation nation;
    nation.parse(line, '|');
    buffer.push_back(nation);
  }
  file.close();
  buffer.shrink_to_fit();
}

inline void Experiment::Tpch::DataParser::parse_all_files_test(
    const std::string& f) {
  std::string nation_file = f + "\\nation.tbl";
  std::string region_file = f + "\\region.tbl";
  std::string part_file = f + "\\part.tbl";
  std::string supplier_file = f + "\\supplier.tbl";
  std::string partsupp_file = f + "\\partsupp.tbl";
  std::string customer_file = f + "\\customer.tbl";
  std::string orders_file = f + "\\orders.tbl";
  std::vector<nation> nation_table;
  std::vector<region> region_table;
  std::vector<part> part_table;
  std::vector<supplier> supplier_table;
  std::vector<partsupp> partsupp_table;
  std::vector<customer> customer_table;
  std::vector<order> orders_table;
  parse_tpch_nation(nation_file, nation_table);
  parse_tpch_region(region_file, region_table);
  parse_tpch_part(part_file, part_table);
  parse_tpch_supplier(supplier_file, supplier_table);
  parse_tpch_partsupp(partsupp_file, partsupp_table);
  parse_tpch_customer(customer_file, customer_table);
  parse_tpch_order(orders_file, orders_table);
  std::cout << "|NATION| = " << nation_table.size()
            << ", |REGION| = " << region_table.size()
            << ", |PART| = " << part_table.size()
            << ", |SUPPLIER| = " << supplier_table.size()
            << ", |PARTSUPP| = " << partsupp_table.size()
            << ", |CUSTOMER| = " << customer_table.size()
            << ", |ORDERS| = " << orders_table.size() << "\n";
}

inline void Experiment::Tpch::TableJoinUtil::hash_join_lineitem_order(
    const std::vector<lineitem>& lineitem_table,
    const std::vector<order>& order_table,
    std::vector<lineitem_order>& result) {
  // first build hash index on orders (which is the smaller table)
  std::unordered_map<uint32_t, Experiment::Tpch::order> order_index;
  for (auto it = order_table.cbegin(); it != order_table.cend(); ++it) {
    order_index[it->o_orderkey] = *it;
  }
  for (auto li_it = lineitem_table.cbegin(); li_it != lineitem_table.cend();
       ++li_it) {
    std::unordered_map<uint32_t, Tpch::order>::const_iterator o_it =
        order_index.find(li_it->l_order_key);
    result.push_back(Experiment::Tpch::lineitem_order(o_it->second, *li_it));
  }
  order_index.clear();
}

inline void Experiment::Tpch::TableJoinUtil::hash_join_customer_lineitem_order(
    const std::vector<customer>& customer_table,
    const std::vector<lineitem>& lineitem_table,
    const std::vector<order>& order_table,
    std::vector<customer_lineitem_order>& result) {
  // first build hash index on customers (which is the smallest table)
  std::unordered_map<uint32_t, Experiment::Tpch::customer> customer_index;
  for (std::vector<Tpch::customer>::const_iterator it = customer_table.cbegin();
       it != customer_table.cend(); ++it) {
    customer_index[it->c_custkey] = *it;
  }
  // second build hash index on orders (which is the smaller table)
  std::unordered_map<uint32_t, Experiment::Tpch::order> order_index;
  for (auto it = order_table.cbegin(); it != order_table.cend(); ++it) {
    order_index[it->o_orderkey] = *it;
  }
  for (auto li_it = lineitem_table.cbegin(); li_it != lineitem_table.cend();
       ++li_it) {
    std::unordered_map<uint32_t, Tpch::order>::const_iterator o_it =
        order_index.find(li_it->l_order_key);
    std::unordered_map<uint32_t, Tpch::customer>::const_iterator c_it =
        customer_index.find(o_it->second.o_custkey);
    result.push_back(Experiment::Tpch::customer_lineitem_order(
        c_it->second, *li_it, o_it->second));
  }
  customer_index.clear();
  order_index.clear();
}
