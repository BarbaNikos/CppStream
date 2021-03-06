#pragma once
#include <cinttypes>
#include <cstring>
#include <iostream>
#include <sstream>
#include <vector>

#ifndef TPCH_STRUCT_LIB_H_
#define TPCH_STRUCT_LIB_H_

namespace Experiment {
namespace Tpch {
typedef struct date_str {
  date_str() {}
  date_str(uint16_t year, uint8_t month, uint8_t day)
      : year(year), month(month), day(day) {}
  date_str(const date_str& obj) {
    year = obj.year;
    month = obj.month;
    day = obj.day;
  }
  ~date_str() {}
  date_str& operator=(const date_str& o) {
    if (this != &o) {
      year = o.year;
      month = o.month;
      day = o.day;
    }
    return *this;
  }
  bool operator==(const date_str& date_1) const {
    return (year == date_1.year && month == date_1.month && day == date_1.day);
  }
  bool operator<=(const date_str& date_1) const {
    if (year <= date_1.year) {
      return true;
    } else if (year == date_1.year && month <= date_1.month) {
      return true;
    } else if (year == date_1.year && month == date_1.month &&
               day <= date_1.day) {
      return true;
    } else {
      return false;
    }
  }
  bool operator<(const date_str& date_1) const {
    if (year < date_1.year) {
      return true;
    } else if (year == date_1.year && month < date_1.month) {
      return true;
    } else if (year == date_1.year && month == date_1.month &&
               day < date_1.day) {
      return true;
    } else {
      return false;
    }
  }
  std::string to_string() const {
    std::stringstream ss;
    ss << static_cast<unsigned int>(year) << "-"
       << static_cast<unsigned int>(month) << "-"
       << static_cast<unsigned int>(day);
    return ss.str();
  }
  uint16_t year;
  uint8_t month;
  uint8_t day;
} date;

typedef struct part_str {
  part_str() : p_name(), p_type(), p_comment() {}
  part_str(const part_str& o)
      : p_partkey(o.p_partkey),
        p_name(o.p_name),
        p_type(o.p_type),
        p_size(o.p_size),
        p_comment(o.p_comment) {
    memcpy(p_mfgr, o.p_mfgr, 25 * sizeof(char));
    memcpy(p_brand, o.p_brand, 10 * sizeof(char));
    memcpy(p_container, o.p_container, 10 * sizeof(char));
  }
  ~part_str() {}
  part_str& operator=(const part_str& o) {
    if (this != &o) {
      p_partkey = o.p_partkey;
      p_name = o.p_name;
      memcpy(p_mfgr, o.p_mfgr, 25 * sizeof(char));
      memcpy(p_brand, o.p_brand, 10 * sizeof(char));
      p_type = p_type;
      p_size = o.p_size;
      memcpy(p_container, o.p_container, 10 * sizeof(char));
      p_comment = o.p_comment;
    }
    return *this;
  }
  void parse(const std::string& s, const char delim) {
    std::stringstream str_stream(s);
    std::string token;
    std::vector<std::string> tokens;
    while (getline(str_stream, token, delim)) tokens.push_back(token);
    p_partkey = std::stoi(tokens[0]);
    p_name = tokens[1];
    memset(p_mfgr, 0, 25 * sizeof(char));
    if (tokens[2].length() < 25)
      memcpy(p_mfgr, tokens[2].c_str(), tokens[2].length());
    else
      memcpy(p_mfgr, tokens[2].c_str(), 25 * sizeof(char));
    memset(p_brand, 0, 10 * sizeof(char));
    if (tokens[3].length())
      memcpy(p_brand, tokens[3].c_str(), tokens[3].length());
    else
      memcpy(p_brand, tokens[3].c_str(), 10 * sizeof(char));
    p_type = tokens[4];
    p_size = std::stoi(tokens[5]);
    memset(p_container, 0, 10 * sizeof(char));
    if (tokens[6].length())
      memcpy(p_container, tokens[6].c_str(), tokens[6].length());
    else
      memcpy(p_container, tokens[6].c_str(), 10 * sizeof(char));
    p_retailprice = std::stof(tokens[7]);
    p_comment = tokens[8];
  }
  uint32_t p_partkey;
  std::string p_name;
  char p_mfgr[25];   // fixed text, size 25
  char p_brand[10];  // fixed text, size 10
  std::string p_type;
  uint32_t p_size;
  char p_container[10];  // fixed text, size 10
  float p_retailprice;
  std::string p_comment;
} part;

typedef struct supplier_str {
  supplier_str() : s_address(), s_comment() {}
  supplier_str(const supplier_str& o)
      : s_suppkey(o.s_suppkey),
        s_address(o.s_address),
        s_nationkey(o.s_nationkey),
        s_acctbal(o.s_acctbal),
        s_comment(o.s_comment) {
    memcpy(s_name, o.s_name, 25 * sizeof(char));
    memcpy(s_phone, o.s_phone, 15 * sizeof(char));
  }
  ~supplier_str() {}
  supplier_str& operator=(const supplier_str& o) {
    if (this != &o) {
      s_suppkey = o.s_suppkey;
      memcpy(s_name, o.s_name, 25 * sizeof(char));
      s_address = o.s_address;
      s_nationkey = o.s_nationkey;
      memcpy(s_phone, o.s_phone, 15 * sizeof(char));
      s_acctbal = o.s_acctbal;
      s_comment = o.s_comment;
    }
    return *this;
  }
  void parse(const std::string& s, const char delim) {
    std::stringstream str_stream(s);
    std::string token;
    std::vector<std::string> tokens;
    while (getline(str_stream, token, delim)) tokens.push_back(token);
    s_suppkey = std::stoi(tokens[0]);
    memset(s_name, 0, 25 * sizeof(char));
    if (tokens[1].length() < 25)
      memcpy(s_name, tokens[1].c_str(), tokens[1].length());
    else
      memcpy(s_name, tokens[1].c_str(), 25 * sizeof(char));
    s_address = tokens[2];
    s_nationkey = std::stoi(tokens[3]);
    memset(s_phone, 0, 15 * sizeof(char));
    if (tokens[4].length() < 15)
      memcpy(s_phone, tokens[4].c_str(), tokens[4].length());
    else
      memcpy(s_phone, tokens[4].c_str(), 15 * sizeof(char));
    s_acctbal = std::stof(tokens[5]);
    s_comment = tokens[6];
  }
  uint32_t s_suppkey;
  char s_name[25];  // fixed text, size 25
  std::string s_address;
  uint32_t s_nationkey;
  char s_phone[15];  // fixed text, size 15
  float s_acctbal;
  std::string s_comment;
} supplier;

typedef struct partsupp_str {
  partsupp_str() : ps_comment() {}
  partsupp_str(const partsupp_str& o)
      : ps_partkey(o.ps_partkey),
        ps_suppkey(o.ps_suppkey),
        ps_availqty(o.ps_availqty),
        ps_supplycost(o.ps_supplycost),
        ps_comment(o.ps_comment) {}
  ~partsupp_str() {}
  partsupp_str& operator=(const partsupp_str& o) {
    if (this != &o) {
      ps_partkey = o.ps_partkey;
      ps_suppkey = o.ps_suppkey;
      ps_availqty = o.ps_availqty;
      ps_supplycost = o.ps_supplycost;
      ps_comment = o.ps_comment;
    }
    return *this;
  }
  void parse(const std::string& s, const char delim) {
    std::stringstream str_stream(s);
    std::string token;
    std::vector<std::string> tokens;
    while (getline(str_stream, token, delim)) tokens.push_back(token);
    ps_partkey = std::stoi(tokens[0]);
    ps_suppkey = std::stoi(tokens[1]);
    ps_availqty = std::stoi(tokens[2]);
    ps_supplycost = std::stof(tokens[3]);
    ps_comment = tokens[4];
  }
  uint32_t ps_partkey;
  uint32_t ps_suppkey;
  int32_t ps_availqty;
  float ps_supplycost;
  std::string ps_comment;
} partsupp;

typedef struct customer_str {
  customer_str() : c_name(), c_address(), c_comment() {}
  customer_str(const customer_str& o)
      : c_custkey(o.c_custkey),
        c_address(o.c_address),
        c_nationkey(o.c_nationkey),
        c_acctbal(o.c_acctbal),
        c_comment(o.c_comment) {
    memcpy(c_phone, o.c_phone, 15 * sizeof(char));
    memcpy(c_mktsegment, o.c_mktsegment, 10 * sizeof(char));
  }
  ~customer_str() {}
  customer_str& operator=(const customer_str& o) {
    if (this != &o) {
      c_custkey = o.c_custkey;
      c_name = o.c_name;
      c_address = o.c_address;
      c_nationkey = o.c_nationkey;
      memcpy(c_phone, o.c_phone, 15 * sizeof(char));
      c_acctbal = o.c_acctbal;
      memcpy(c_mktsegment, o.c_mktsegment, 10 * sizeof(char));
      c_comment = o.c_comment;
    }
    return *this;
  }
  void parse(const std::string& s, const char delim) {
    std::stringstream str_stream(s);
    std::string token;
    std::vector<std::string> tokens;
    while (getline(str_stream, token, delim)) tokens.push_back(token);
    c_custkey = std::stoi(tokens[0]);
    c_name = tokens[1];
    c_address = tokens[2];
    c_nationkey = std::stoi(tokens[3]);
    memset(c_phone, 0, 15 * sizeof(char));
    if (tokens[4].length() < 15)
      memcpy(c_phone, tokens[4].c_str(), tokens[4].length());
    else
      memcpy(c_phone, tokens[4].c_str(), 15 * sizeof(char));
    c_acctbal = std::stof(tokens[5]);
    memset(c_mktsegment, 0, 10 * sizeof(char));
    if (tokens[6].length() < 10)
      memcpy(c_mktsegment, tokens[6].c_str(), tokens[6].length());
    else
      memcpy(c_mktsegment, tokens[6].c_str(), 10 * sizeof(char));
    c_comment = tokens[7];
  }
  uint32_t c_custkey;
  std::string c_name;
  std::string c_address;
  uint32_t c_nationkey;
  char c_phone[15];  // fixed text, size 15
  float c_acctbal;
  char c_mktsegment[10];  // fixed text, size 10
  std::string c_comment;
} customer;

typedef struct q3_customer_str {
  q3_customer_str() {}
  q3_customer_str(const customer& c) : c_custkey(c.c_custkey) {
    memcpy(c_mktsegment, c.c_mktsegment, 10 * sizeof(char));
  }
  ~q3_customer_str() {}
  uint32_t c_custkey;
  char c_mktsegment[10];
} q3_customer;

typedef struct order_str {
  order_str() : o_comment() {}
  order_str(const order_str& o)
      : o_orderkey(o.o_orderkey),
        o_custkey(o.o_custkey),
        o_orderstatus(o.o_orderstatus),
        o_totalprice(o.o_totalprice),
        o_orderdate(o.o_orderdate),
        o_shippriority(o.o_shippriority),
        o_comment(o.o_comment) {
    memcpy(o_orderpriority, o.o_orderpriority, 15 * sizeof(char));
    memcpy(o_clerk, o.o_clerk, 15 * sizeof(char));
  }
  ~order_str() {}
  order_str& operator=(const order_str& o) {
    if (this != &o) {
      o_orderkey = o.o_orderkey;
      o_custkey = o.o_custkey;
      o_orderstatus = o.o_orderstatus;
      o_totalprice = o.o_totalprice;
      o_orderdate = o.o_orderdate;
      memcpy(o_orderpriority, o.o_orderpriority, 15 * sizeof(char));
      memcpy(o_clerk, o.o_clerk, 15 * sizeof(char));
      o_shippriority = o.o_shippriority;
      o_comment = o.o_comment;
    }
    return *this;
  }
  void parse(const std::string& s, const char delim) {
    std::stringstream str_stream(s);
    std::string token;
    std::vector<std::string> tokens;
    while (getline(str_stream, token, delim)) tokens.push_back(token);
    o_orderkey = std::stoi(tokens[0]);
    o_custkey = std::stoi(tokens[1]);
    o_orderstatus = tokens[2][0];
    o_totalprice = std::stof(tokens[3]);
    // order-date
    std::vector<std::string> order_date_tokens;
    std::stringstream str_stream_1(tokens[4]);
    while (getline(str_stream_1, token, '-'))
      order_date_tokens.push_back(token);
    o_orderdate.year = std::stoi(order_date_tokens[0]);
    o_orderdate.month = std::stoi(order_date_tokens[1]);
    o_orderdate.day = std::stoi(order_date_tokens[2]);
    memset(o_orderpriority, 0, 15 * sizeof(char));
    if (tokens[5].length() < 15)
      memcpy(o_orderpriority, tokens[5].c_str(), tokens[5].length());
    else
      memcpy(o_orderpriority, tokens[5].c_str(), 15 * sizeof(char));
    memset(o_clerk, 0, 15 * sizeof(char));
    if (tokens[6].length() < 15)
      memcpy(o_clerk, tokens[6].c_str(), tokens[6].length());
    else
      memcpy(o_clerk, tokens[6].c_str(), 15 * sizeof(char));
    o_shippriority = std::stoi(tokens[7]);
    o_comment = tokens[8];
  }
  std::string to_string() const {
    std::stringstream ss;
    ss << o_orderkey << "," << o_custkey << "," << o_orderstatus << ","
       << o_totalprice << "," << o_orderdate.to_string() << ","
       << o_shippriority;
    return ss.str();
  }
  uint32_t o_orderkey;
  uint32_t o_custkey;
  char o_orderstatus;
  float o_totalprice;
  Tpch::date o_orderdate;
  char o_orderpriority[15];  // fixed text, size 15
  char o_clerk[15];          // fixed text, size 15
  uint32_t o_shippriority;
  std::string o_comment;
} order;

typedef struct lineitem_str {
  lineitem_str() : l_comment() {}
  lineitem_str(const lineitem_str& o)
      : l_order_key(o.l_order_key),
        l_part_key(o.l_part_key),
        l_supp_key(o.l_supp_key),
        l_linenumber(o.l_linenumber),
        l_quantity(o.l_quantity),
        l_extendedprice(o.l_extendedprice),
        l_discount(o.l_discount),
        l_tax(o.l_tax),
        l_returnflag(o.l_returnflag),
        l_linestatus(o.l_linestatus),
        l_shipdate(o.l_shipdate),
        l_commitdate(o.l_commitdate),
        l_receiptdate(o.l_receiptdate),
        l_comment(o.l_comment) {
    memcpy(l_shipinstruct, o.l_shipinstruct, 25 * sizeof(char));
    memcpy(l_shipmode, o.l_shipmode, 10 * sizeof(char));
  }
  ~lineitem_str() {}
  lineitem_str& operator=(const lineitem_str& o) {
    if (this != &o) {
      l_order_key = o.l_order_key;
      l_part_key = o.l_part_key;
      l_supp_key = o.l_supp_key;
      l_linenumber = o.l_linenumber;
      l_quantity = o.l_quantity;
      l_extendedprice = o.l_extendedprice;
      l_discount = o.l_discount;
      l_tax = o.l_tax;
      l_returnflag = o.l_returnflag;
      l_linestatus = o.l_linestatus;
      l_shipdate = o.l_shipdate;
      l_commitdate = o.l_commitdate;
      l_receiptdate = o.l_receiptdate;
      memcpy(l_shipinstruct, o.l_shipinstruct, 25 * sizeof(char));
      memcpy(l_shipmode, o.l_shipmode, 10 * sizeof(char));
      l_comment = o.l_comment;
    }
    return *this;
  }
  void parse(const std::string& s, const char delim) {
    std::stringstream str_stream(s);
    std::string token;
    std::vector<std::string> tokens;
    while (getline(str_stream, token, delim)) tokens.push_back(token);
    l_order_key = std::stoi(tokens[0]);
    l_part_key = std::stoi(tokens[1]);
    l_supp_key = std::stoi(tokens[2]);
    l_linenumber = std::stoi(tokens[3]);
    l_quantity = std::stof(tokens[4]);
    l_extendedprice = std::stof(tokens[5]);
    l_discount = std::stof(tokens[6]);
    l_tax = std::stof(tokens[7]);
    l_returnflag = tokens[8][0];
    l_linestatus = tokens[9][0];
    // ship-date
    std::vector<std::string> ship_date_tokens;
    std::stringstream str_stream_1(tokens[10]);
    while (getline(str_stream_1, token, '-')) ship_date_tokens.push_back(token);
    l_shipdate.year = std::stoi(ship_date_tokens[0]);
    l_shipdate.month = std::stoi(ship_date_tokens[1]);
    l_shipdate.day = std::stoi(ship_date_tokens[2]);
    // commit-date
    std::vector<std::string> commit_date_tokens;
    std::stringstream str_stream_2(tokens[11]);
    while (getline(str_stream_2, token, '-'))
      commit_date_tokens.push_back(token);
    l_commitdate.year = std::stoi(commit_date_tokens[0]);
    l_commitdate.month = std::stoi(commit_date_tokens[1]);
    l_commitdate.day = std::stoi(commit_date_tokens[2]);
    // receipt-date
    std::vector<std::string> receipt_date_tokens;
    std::stringstream str_stream_3(tokens[12]);
    while (getline(str_stream_3, token, '-'))
      receipt_date_tokens.push_back(token);
    l_receiptdate.year = std::stoi(receipt_date_tokens[0]);
    l_receiptdate.month = std::stoi(receipt_date_tokens[1]);
    l_receiptdate.day = std::stoi(receipt_date_tokens[2]);
    memset(l_shipinstruct, 0, 25 * sizeof(char));
    if (tokens[13].length() < 25)
      memcpy(l_shipinstruct, tokens[13].c_str(), tokens[13].length());
    else
      memcpy(l_shipinstruct, tokens[13].c_str(), 25 * sizeof(char));
    memset(l_shipmode, 0, 0 * sizeof(char));
    if (tokens[14].length() < 10)
      memcpy(l_shipmode, tokens[14].c_str(), tokens[14].length());
    else
      memcpy(l_shipmode, tokens[14].c_str(), 10 * sizeof(char));
    l_comment = tokens[15];
  }
  std::string to_string() const {
    std::stringstream ss;
    ss << l_order_key << "," << l_part_key << "," << l_supp_key << ","
       << l_linenumber << "," << l_quantity << "," << l_extendedprice << ","
       << l_discount << "," << l_tax << "," << l_returnflag << ","
       << l_linestatus;
    return ss.str();
  }
  uint32_t l_order_key;
  uint32_t l_part_key;
  uint32_t l_supp_key;
  int32_t l_linenumber;
  float l_quantity;
  float l_extendedprice;
  float l_discount;
  float l_tax;
  char l_returnflag;
  char l_linestatus;
  Tpch::date l_shipdate;
  Tpch::date l_commitdate;
  Tpch::date l_receiptdate;
  char l_shipinstruct[25];  // fixed text size 25
  char l_shipmode[10];      // fixed text size 10
  std::string l_comment;
} lineitem;

typedef struct lineitem_order_str {
  lineitem_order_str() {}
  lineitem_order_str(const order& o, const lineitem& l)
      : _order(o), _lineitem(l) {}
  lineitem_order_str(const lineitem_order_str& o)
      : _order(o._order), _lineitem(o._lineitem) {}
  ~lineitem_order_str() {}
  lineitem_order_str& operator=(const lineitem_order_str& o) {
    if (this != &o) {
      _order = o._order;
      _lineitem = o._lineitem;
    }
    return *this;
  }
  std::string to_string() const {
    return _lineitem.to_string() + "," + _order.to_string();
  }
  order _order;
  lineitem _lineitem;
} lineitem_order;

typedef struct customer_lineitem_order_str {
  customer_lineitem_order_str() {}
  customer_lineitem_order_str(const customer& c, const lineitem& l,
                              const order& o)
      : _customer(c), _lineitem(l), _order(o) {}
  customer_lineitem_order_str(const customer_lineitem_order_str& o)
      : _customer(o._customer), _lineitem(o._lineitem), _order(o._order) {}
  ~customer_lineitem_order_str() {}
  customer_lineitem_order_str& operator=(const customer_lineitem_order_str& o) {
    if (this != &o) {
      _customer = o._customer;
      _lineitem = o._lineitem;
      _order = o._order;
    }
    return *this;
  }
  customer _customer;
  lineitem _lineitem;
  order _order;
} customer_lineitem_order;

typedef struct nation_str {
  nation_str() : n_comment() {}
  nation_str(const nation_str& o)
      : n_nationkey(o.n_nationkey),
        n_regionkey(o.n_regionkey),
        n_comment(o.n_comment) {
    memcpy(n_name, o.n_name, 25 * sizeof(char));
  }
  ~nation_str() {}
  nation_str& operator=(const nation_str& o) {
    if (this != &o) {
      n_nationkey = o.n_nationkey;
      memcpy(n_name, o.n_name, 25 * sizeof(char));
      n_regionkey = o.n_regionkey;
      n_comment = o.n_comment;
    }
    return *this;
  }
  void parse(const std::string& s, const char delim) {
    std::stringstream str_stream(s);
    std::string token;
    std::vector<std::string> tokens;
    while (getline(str_stream, token, delim)) tokens.push_back(token);
    n_nationkey = std::stoi(tokens[0]);
    memset(n_name, 0, 25 * sizeof(char));
    if (tokens[1].length() < 25)
      memcpy(n_name, tokens[1].c_str(), tokens[1].length());
    else
      memcpy(n_name, tokens[1].c_str(), 25 * sizeof(char));
    n_regionkey = std::stoi(tokens[2]);
    n_comment = tokens[3];
  }
  uint32_t n_nationkey;
  char n_name[25];  // fixed size
  uint32_t n_regionkey;
  std::string n_comment;
} nation;

typedef struct region_str {
  region_str() : r_comment() {}
  region_str(const region_str& o)
      : r_regionkey(o.r_regionkey), r_comment(o.r_comment) {
    memcpy(r_name, o.r_name, 25 * sizeof(char));
  }
  ~region_str() {}
  region_str& operator=(const region_str& o) {
    if (this != &o) {
      r_regionkey = o.r_regionkey;
      memcpy(r_name, o.r_name, 25 * sizeof(char));
      r_comment = o.r_comment;
    }
    return *this;
  }
  void parse(const std::string& s, const char delim) {
    std::stringstream str_stream(s);
    std::string token;
    std::vector<std::string> tokens;
    while (getline(str_stream, token, delim)) tokens.push_back(token);
    r_regionkey = std::stoi(tokens[0]);
    memset(r_name, 0, 25 * sizeof(char));
    if (tokens[1].length() < 25)
      memcpy(r_name, tokens[1].c_str(), tokens[1].length());
    else
      memcpy(r_name, tokens[1].c_str(), 25 * sizeof(char));
    r_comment = tokens[2];
  }
  uint32_t r_regionkey;
  char r_name[25];  // fixed size
  std::string r_comment;
} region;
}
}
#endif  // !TPCH_STRUCT_LIB_H_
