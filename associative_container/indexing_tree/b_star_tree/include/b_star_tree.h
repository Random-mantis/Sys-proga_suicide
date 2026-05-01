#ifndef SYS_PROG_BS_TREE_H
#define SYS_PROG_BS_TREE_H

#include "../../b_tree/include/b_tree.h"

template <typename tkey, typename tvalue, comparator<tkey> compare = std::less<tkey>, std::size_t t = 5>
using BS_tree = B_tree<tkey, tvalue, compare, t>;

#endif
