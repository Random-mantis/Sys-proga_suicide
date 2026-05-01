#ifndef SYS_PROG_BS_PLUS_TREE_H
#define SYS_PROG_BS_PLUS_TREE_H

#include "../../b_plus_tree/include/b_plus_tree.h"

template <typename tkey, typename tvalue, comparator<tkey> compare = std::less<tkey>, std::size_t t = 5>
using BSP_tree = BP_tree<tkey, tvalue, compare, t>;

#endif
