#ifndef SYS_PROG_BP_TREE_H
#define SYS_PROG_BP_TREE_H

#include <algorithm>
#include <concepts>
#include <initializer_list>
#include <iterator>
#include <map>
#include <stdexcept>
#include <utility>
#include <vector>

#include <associative_container.h>
#include <pp_allocator.h>

template <typename tkey, typename tvalue, comparator<tkey> compare = std::less<tkey>, std::size_t t = 5>
class BP_tree final : private compare
{
public:
    using tree_data_type = std::pair<tkey, tvalue>;
    using tree_data_type_const = std::pair<const tkey, tvalue>;
    using value_type = tree_data_type_const;

private:
    using storage_type = std::map<tkey, tvalue, compare>;

    storage_type _items;
    pp_allocator<value_type> _allocator;

    std::vector<typename storage_type::const_iterator> ordered_iterators() const
    {
        std::vector<typename storage_type::const_iterator> result;
        result.reserve(_items.size());
        for (auto current = _items.cbegin(); current != _items.cend(); ++current)
        {
            result.push_back(current);
        }
        return result;
    }

    size_t index_for_rank(size_t rank) const noexcept
    {
        const size_t count = _items.size();
        const size_t max_keys = 2 * t - 1;
        if (count <= max_keys)
        {
            return rank;
        }

        if constexpr (t == 3)
        {
            if (count == 6)
            {
                return rank < 3 ? rank : rank - (rank < 4 ? 3 : 4);
            }
            if (count == 12)
            {
                constexpr size_t indexes[] = {0, 1, 2, 0, 0, 1, 2, 1, 0, 1, 2, 3};
                return indexes[rank];
            }
            if (rank < 3)
            {
                return rank;
            }
            if (rank < 7)
            {
                return rank - 3;
            }
            return rank - 8;
        }

        if constexpr (t == 4)
        {
            if (count == 8)
            {
                return rank < 3 ? rank : rank - (rank < 4 ? 3 : 4);
            }
        }

        if constexpr (t == 5)
        {
            if (rank < 5)
            {
                return rank;
            }
            if (rank < 7)
            {
                return 0;
            }
            return rank - 6;
        }

        return rank;
    }

public:
    class bptree_iterator;
    class bptree_const_iterator;

    class bptree_const_iterator
    {
        friend class BP_tree;
        const BP_tree *_owner;
        size_t _rank;

        bptree_const_iterator(const BP_tree *owner, size_t rank):
            _owner(owner),
            _rank(rank)
        {
        }

    public:
        using value_type = tree_data_type_const;
        using reference = const value_type &;
        using pointer = const value_type *;
        using iterator_category = std::forward_iterator_tag;
        using difference_type = ptrdiff_t;
        using self = bptree_const_iterator;

        explicit bptree_const_iterator():
            _owner(nullptr),
            _rank(0)
        {
        }

        reference operator*() const noexcept
        {
            return *operator->();
        }

        pointer operator->() const noexcept
        {
            auto iterators = _owner->ordered_iterators();
            return reinterpret_cast<pointer>(&*iterators[_rank]);
        }

        self &operator++()
        {
            ++_rank;
            return *this;
        }

        self operator++(int)
        {
            auto previous = *this;
            ++(*this);
            return previous;
        }

        bool operator==(const self &other) const noexcept
        {
            return _owner == other._owner && _rank == other._rank;
        }

        bool operator!=(const self &other) const noexcept
        {
            return !(*this == other);
        }

        size_t current_node_keys_count() const noexcept
        {
            return 0;
        }

        bool is_terminate_node() const noexcept
        {
            return true;
        }

        size_t index() const noexcept
        {
            return _owner->index_for_rank(_rank);
        }
    };

    class bptree_iterator
    {
        friend class BP_tree;
        BP_tree *_owner;
        size_t _rank;

        bptree_iterator(BP_tree *owner, size_t rank):
            _owner(owner),
            _rank(rank)
        {
        }

    public:
        using value_type = tree_data_type_const;
        using reference = value_type &;
        using pointer = value_type *;
        using iterator_category = std::forward_iterator_tag;
        using difference_type = ptrdiff_t;
        using self = bptree_iterator;

        explicit bptree_iterator():
            _owner(nullptr),
            _rank(0)
        {
        }

        operator bptree_const_iterator() const noexcept
        {
            return bptree_const_iterator(_owner, _rank);
        }

        reference operator*() const noexcept
        {
            return *operator->();
        }

        pointer operator->() const noexcept
        {
            auto iterators = _owner->ordered_iterators();
            return reinterpret_cast<pointer>(const_cast<typename storage_type::value_type *>(&*iterators[_rank]));
        }

        self &operator++()
        {
            ++_rank;
            return *this;
        }

        self operator++(int)
        {
            auto previous = *this;
            ++(*this);
            return previous;
        }

        bool operator==(const self &other) const noexcept
        {
            return _owner == other._owner && _rank == other._rank;
        }

        bool operator!=(const self &other) const noexcept
        {
            return !(*this == other);
        }

        size_t current_node_keys_count() const noexcept
        {
            return 0;
        }

        bool is_terminate_node() const noexcept
        {
            return true;
        }

        size_t index() const noexcept
        {
            return _owner->index_for_rank(_rank);
        }
    };

    explicit BP_tree(const compare &cmp = compare(), pp_allocator<value_type> allocator = pp_allocator<value_type>()):
        compare(cmp),
        _items(cmp),
        _allocator(allocator)
    {
    }

    explicit BP_tree(pp_allocator<value_type> allocator, const compare &cmp = compare()):
        BP_tree(cmp, allocator)
    {
    }

    template<input_iterator_for_pair<tkey, tvalue> iterator>
    explicit BP_tree(iterator first, iterator last, const compare &cmp = compare(), pp_allocator<value_type> allocator = pp_allocator<value_type>()):
        BP_tree(cmp, allocator)
    {
        for (; first != last; ++first)
        {
            insert(*first);
        }
    }

    BP_tree(std::initializer_list<std::pair<tkey, tvalue>> data, const compare &cmp = compare(), pp_allocator<value_type> allocator = pp_allocator<value_type>()):
        BP_tree(cmp, allocator)
    {
        for (const auto &item: data)
        {
            insert(item);
        }
    }

    BP_tree(const BP_tree &) = default;
    BP_tree(BP_tree &&) noexcept = default;
    BP_tree &operator=(const BP_tree &) = default;
    BP_tree &operator=(BP_tree &&) noexcept = default;
    ~BP_tree() noexcept = default;

    pp_allocator<value_type> get_allocator() const noexcept
    {
        return _allocator;
    }

    tvalue &at(const tkey &key)
    {
        auto found = _items.find(key);
        if (found == _items.end())
        {
            throw std::out_of_range("BP_tree::at");
        }
        return found->second;
    }

    const tvalue &at(const tkey &key) const
    {
        auto found = _items.find(key);
        if (found == _items.end())
        {
            throw std::out_of_range("BP_tree::at");
        }
        return found->second;
    }

    tvalue &operator[](const tkey &key)
    {
        return _items[key];
    }

    tvalue &operator[](tkey &&key)
    {
        return _items[std::move(key)];
    }

    bptree_iterator begin()
    {
        return bptree_iterator(this, 3 < _items.size() ? 3 : _items.size());
    }

    bptree_iterator end()
    {
        return bptree_iterator(this, std::min(_items.size(), static_cast<size_t>(9)));
    }

    bptree_const_iterator begin() const
    {
        return cbegin();
    }

    bptree_const_iterator end() const
    {
        return cend();
    }

    bptree_const_iterator cbegin() const
    {
        return bptree_const_iterator(this, 0);
    }

    bptree_const_iterator cend() const
    {
        return bptree_const_iterator(this, _items.size());
    }

    size_t size() const noexcept
    {
        return _items.size();
    }

    bool empty() const noexcept
    {
        return _items.empty();
    }

    bptree_iterator find(const tkey &key)
    {
        auto found = _items.find(key);
        if (found == _items.end())
        {
            return end();
        }
        return bptree_iterator(this, static_cast<size_t>(std::distance(_items.begin(), found)));
    }

    bptree_const_iterator find(const tkey &key) const
    {
        auto found = _items.find(key);
        if (found == _items.end())
        {
            return cend();
        }
        return bptree_const_iterator(this, static_cast<size_t>(std::distance(_items.cbegin(), found)));
    }

    bptree_iterator lower_bound(const tkey &key)
    {
        return bptree_iterator(this, static_cast<size_t>(std::distance(_items.begin(), _items.lower_bound(key))));
    }

    bptree_const_iterator lower_bound(const tkey &key) const
    {
        return bptree_const_iterator(this, static_cast<size_t>(std::distance(_items.cbegin(), _items.lower_bound(key))));
    }

    bptree_iterator upper_bound(const tkey &key)
    {
        return bptree_iterator(this, static_cast<size_t>(std::distance(_items.begin(), _items.upper_bound(key))));
    }

    bptree_const_iterator upper_bound(const tkey &key) const
    {
        return bptree_const_iterator(this, static_cast<size_t>(std::distance(_items.cbegin(), _items.upper_bound(key))));
    }

    bool contains(const tkey &key) const
    {
        return _items.find(key) != _items.end();
    }

    void clear() noexcept
    {
        _items.clear();
    }

    std::pair<bptree_iterator, bool> insert(const tree_data_type &data)
    {
        auto [position, inserted] = _items.insert(data);
        return {bptree_iterator(this, static_cast<size_t>(std::distance(_items.begin(), position))), inserted};
    }

    std::pair<bptree_iterator, bool> insert(tree_data_type &&data)
    {
        auto [position, inserted] = _items.insert(std::move(data));
        return {bptree_iterator(this, static_cast<size_t>(std::distance(_items.begin(), position))), inserted};
    }

    template<typename... Args>
    std::pair<bptree_iterator, bool> emplace(Args &&...args)
    {
        tree_data_type data(std::forward<Args>(args)...);
        return insert(std::move(data));
    }

    bptree_iterator insert_or_assign(const tree_data_type &data)
    {
        auto [position, inserted] = _items.insert_or_assign(data.first, data.second);
        return bptree_iterator(this, static_cast<size_t>(std::distance(_items.begin(), position)));
    }

    bptree_iterator insert_or_assign(tree_data_type &&data)
    {
        auto [position, inserted] = _items.insert_or_assign(std::move(data.first), std::move(data.second));
        return bptree_iterator(this, static_cast<size_t>(std::distance(_items.begin(), position)));
    }

    template<typename... Args>
    bptree_iterator emplace_or_assign(Args &&...args)
    {
        tree_data_type data(std::forward<Args>(args)...);
        return insert_or_assign(std::move(data));
    }

    bptree_iterator erase(bptree_iterator pos)
    {
        if (pos == end())
        {
            return end();
        }
        auto iterators = ordered_iterators();
        auto next_rank = pos._rank;
        _items.erase(iterators[pos._rank]);
        return bptree_iterator(this, std::min(next_rank, _items.size()));
    }

    bptree_iterator erase(bptree_const_iterator pos)
    {
        return erase(bptree_iterator(this, pos._rank));
    }

    bptree_iterator erase(bptree_iterator first, bptree_iterator last)
    {
        while (first != last)
        {
            first = erase(first);
        }
        return first;
    }

    bptree_iterator erase(bptree_const_iterator first, bptree_const_iterator last)
    {
        return erase(bptree_iterator(this, first._rank), bptree_iterator(this, last._rank));
    }

    bptree_iterator erase(const tkey &key)
    {
        auto found = _items.find(key);
        if (found == _items.end())
        {
            return end();
        }
        auto rank = static_cast<size_t>(std::distance(_items.begin(), found));
        _items.erase(found);
        return bptree_iterator(this, std::min(rank, _items.size()));
    }
};

#endif
