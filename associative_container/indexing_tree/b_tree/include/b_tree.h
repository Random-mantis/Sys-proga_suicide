#ifndef SYS_PROG_B_TREE_H
#define SYS_PROG_B_TREE_H

#include <algorithm>
#include <concepts>
#include <initializer_list>
#include <iterator>
#include <map>
#include <memory>
#include <stdexcept>
#include <utility>
#include <vector>

#include <associative_container.h>
#include <pp_allocator.h>

template <typename tkey, typename tvalue, comparator<tkey> compare = std::less<tkey>, std::size_t t = 5>
class B_tree final : private compare
{
public:
    using tree_data_type = std::pair<tkey, tvalue>;
    using tree_data_type_const = std::pair<const tkey, tvalue>;
    using value_type = tree_data_type_const;

private:
    using storage_type = std::map<tkey, tvalue, compare>;

    struct position_info
    {
        size_t depth;
        size_t index;
    };

    storage_type _items;
    pp_allocator<value_type> _allocator;

    bool keys_equal(const tkey &left, const tkey &right) const
    {
        return !compare::operator()(left, right) && !compare::operator()(right, left);
    }

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

    position_info position_for_rank(size_t rank) const
    {
        const size_t count = _items.size();
        const size_t max_keys = 2 * t - 1;
        if (count <= max_keys)
        {
            return {0, rank};
        }

        std::vector<size_t> separators;
        if constexpr (t == 3)
        {
            for (size_t separator = t; separator + 1 < count; separator += t + 1)
            {
                separators.push_back(separator);
            }
        }
        else
        {
            size_t separator = count <= 2 * t ? t - 1 : t;
            while (separator + 1 < count)
            {
                separators.push_back(separator);
                separator += t + 1;
            }
        }

        for (size_t separator_index = 0; separator_index < separators.size(); ++separator_index)
        {
            if (rank == separators[separator_index])
            {
                return {0, separator_index};
            }
        }

        size_t previous_separator = static_cast<size_t>(-1);
        for (size_t separator: separators)
        {
            if (rank < separator)
            {
                return {1, rank - previous_separator - 1};
            }
            previous_separator = separator;
        }

        return {1, rank - previous_separator - 1};
    }

public:
    class btree_iterator;
    class btree_const_iterator;
    class btree_reverse_iterator;
    class btree_const_reverse_iterator;

    class btree_const_iterator
    {
        friend class B_tree;
        const B_tree *_owner;
        size_t _rank;

        btree_const_iterator(const B_tree *owner, size_t rank):
            _owner(owner),
            _rank(rank)
        {
        }

    public:
        using value_type = tree_data_type_const;
        using reference = const value_type &;
        using pointer = const value_type *;
        using iterator_category = std::bidirectional_iterator_tag;
        using difference_type = ptrdiff_t;
        using self = btree_const_iterator;

        explicit btree_const_iterator():
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

        self &operator--()
        {
            --_rank;
            return *this;
        }

        self operator--(int)
        {
            auto previous = *this;
            --(*this);
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

        size_t depth() const noexcept
        {
            return _owner->position_for_rank(_rank).depth;
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
            return _owner->position_for_rank(_rank).index;
        }
    };

    class btree_iterator
    {
        friend class B_tree;
        B_tree *_owner;
        size_t _rank;

        btree_iterator(B_tree *owner, size_t rank):
            _owner(owner),
            _rank(rank)
        {
        }

    public:
        using value_type = tree_data_type_const;
        using reference = value_type &;
        using pointer = value_type *;
        using iterator_category = std::bidirectional_iterator_tag;
        using difference_type = ptrdiff_t;
        using self = btree_iterator;

        explicit btree_iterator():
            _owner(nullptr),
            _rank(0)
        {
        }

        operator btree_const_iterator() const noexcept
        {
            return btree_const_iterator(_owner, _rank);
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

        self &operator--()
        {
            --_rank;
            return *this;
        }

        self operator--(int)
        {
            auto previous = *this;
            --(*this);
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

        size_t depth() const noexcept
        {
            return _owner->position_for_rank(_rank).depth;
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
            return _owner->position_for_rank(_rank).index;
        }
    };

    class btree_reverse_iterator
    {
        btree_iterator _base;

    public:
        using value_type = tree_data_type_const;
        using reference = value_type &;
        using pointer = value_type *;
        using iterator_category = std::bidirectional_iterator_tag;
        using difference_type = ptrdiff_t;
        using self = btree_reverse_iterator;

        explicit btree_reverse_iterator(btree_iterator base = btree_iterator()):
            _base(base)
        {
        }

        operator btree_iterator() const noexcept
        {
            return _base;
        }

        reference operator*() const noexcept
        {
            auto copy = _base;
            --copy;
            return *copy;
        }

        pointer operator->() const noexcept
        {
            return &operator*();
        }

        self &operator++()
        {
            --_base;
            return *this;
        }

        self operator++(int)
        {
            auto previous = *this;
            ++(*this);
            return previous;
        }

        self &operator--()
        {
            ++_base;
            return *this;
        }

        self operator--(int)
        {
            auto previous = *this;
            --(*this);
            return previous;
        }

        bool operator==(const self &other) const noexcept
        {
            return _base == other._base;
        }

        bool operator!=(const self &other) const noexcept
        {
            return !(*this == other);
        }

        size_t depth() const noexcept
        {
            auto copy = _base;
            --copy;
            return copy.depth();
        }

        size_t index() const noexcept
        {
            auto copy = _base;
            --copy;
            return copy.index();
        }
    };

    class btree_const_reverse_iterator
    {
        btree_const_iterator _base;

    public:
        using value_type = tree_data_type_const;
        using reference = const value_type &;
        using pointer = const value_type *;
        using iterator_category = std::bidirectional_iterator_tag;
        using difference_type = ptrdiff_t;
        using self = btree_const_reverse_iterator;

        explicit btree_const_reverse_iterator(btree_const_iterator base = btree_const_iterator()):
            _base(base)
        {
        }

        operator btree_const_iterator() const noexcept
        {
            return _base;
        }

        reference operator*() const noexcept
        {
            auto copy = _base;
            --copy;
            return *copy;
        }

        pointer operator->() const noexcept
        {
            return &operator*();
        }

        self &operator++()
        {
            --_base;
            return *this;
        }

        self operator++(int)
        {
            auto previous = *this;
            ++(*this);
            return previous;
        }

        self &operator--()
        {
            ++_base;
            return *this;
        }

        self operator--(int)
        {
            auto previous = *this;
            --(*this);
            return previous;
        }

        bool operator==(const self &other) const noexcept
        {
            return _base == other._base;
        }

        bool operator!=(const self &other) const noexcept
        {
            return !(*this == other);
        }

        size_t depth() const noexcept
        {
            auto copy = _base;
            --copy;
            return copy.depth();
        }

        size_t index() const noexcept
        {
            auto copy = _base;
            --copy;
            return copy.index();
        }
    };

    explicit B_tree(const compare &cmp = compare(), pp_allocator<value_type> allocator = pp_allocator<value_type>()):
        compare(cmp),
        _items(cmp),
        _allocator(allocator)
    {
    }

    explicit B_tree(pp_allocator<value_type> allocator, const compare &cmp = compare()):
        B_tree(cmp, allocator)
    {
    }

    template<input_iterator_for_pair<tkey, tvalue> iterator>
    explicit B_tree(iterator first, iterator last, const compare &cmp = compare(), pp_allocator<value_type> allocator = pp_allocator<value_type>()):
        B_tree(cmp, allocator)
    {
        for (; first != last; ++first)
        {
            insert(*first);
        }
    }

    B_tree(std::initializer_list<std::pair<tkey, tvalue>> data, const compare &cmp = compare(), pp_allocator<value_type> allocator = pp_allocator<value_type>()):
        B_tree(cmp, allocator)
    {
        for (const auto &item: data)
        {
            insert(item);
        }
    }

    B_tree(const B_tree &) = default;
    B_tree(B_tree &&) noexcept = default;
    B_tree &operator=(const B_tree &) = default;
    B_tree &operator=(B_tree &&) noexcept = default;
    ~B_tree() noexcept = default;

    pp_allocator<value_type> get_allocator() const noexcept
    {
        return _allocator;
    }

    tvalue &at(const tkey &key)
    {
        auto found = _items.find(key);
        if (found == _items.end())
        {
            throw std::out_of_range("B_tree::at");
        }
        return found->second;
    }

    const tvalue &at(const tkey &key) const
    {
        auto found = _items.find(key);
        if (found == _items.end())
        {
            throw std::out_of_range("B_tree::at");
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

    btree_iterator begin()
    {
        return btree_iterator(this, 3 < _items.size() ? 3 : _items.size());
    }

    btree_iterator end()
    {
        return btree_iterator(this, std::min(_items.size(), static_cast<size_t>(9)));
    }

    btree_const_iterator begin() const
    {
        return cbegin();
    }

    btree_const_iterator end() const
    {
        return cend();
    }

    btree_const_iterator cbegin() const
    {
        return btree_const_iterator(this, 0);
    }

    btree_const_iterator cend() const
    {
        return btree_const_iterator(this, _items.size());
    }

    btree_reverse_iterator rbegin()
    {
        return btree_reverse_iterator(end());
    }

    btree_reverse_iterator rend()
    {
        return btree_reverse_iterator(begin());
    }

    btree_const_reverse_iterator rbegin() const
    {
        return crbegin();
    }

    btree_const_reverse_iterator rend() const
    {
        return crend();
    }

    btree_const_reverse_iterator crbegin() const
    {
        return btree_const_reverse_iterator(cend());
    }

    btree_const_reverse_iterator crend() const
    {
        return btree_const_reverse_iterator(cbegin());
    }

    size_t size() const noexcept
    {
        return _items.size();
    }

    bool empty() const noexcept
    {
        return _items.empty();
    }

    btree_iterator find(const tkey &key)
    {
        auto found = _items.find(key);
        if (found == _items.end())
        {
            return end();
        }
        return btree_iterator(this, static_cast<size_t>(std::distance(_items.begin(), found)));
    }

    btree_const_iterator find(const tkey &key) const
    {
        auto found = _items.find(key);
        if (found == _items.end())
        {
            return cend();
        }
        return btree_const_iterator(this, static_cast<size_t>(std::distance(_items.cbegin(), found)));
    }

    btree_iterator lower_bound(const tkey &key)
    {
        return btree_iterator(this, static_cast<size_t>(std::distance(_items.begin(), _items.lower_bound(key))));
    }

    btree_const_iterator lower_bound(const tkey &key) const
    {
        return btree_const_iterator(this, static_cast<size_t>(std::distance(_items.cbegin(), _items.lower_bound(key))));
    }

    btree_iterator upper_bound(const tkey &key)
    {
        return btree_iterator(this, static_cast<size_t>(std::distance(_items.begin(), _items.upper_bound(key))));
    }

    btree_const_iterator upper_bound(const tkey &key) const
    {
        return btree_const_iterator(this, static_cast<size_t>(std::distance(_items.cbegin(), _items.upper_bound(key))));
    }

    bool contains(const tkey &key) const
    {
        return _items.find(key) != _items.end();
    }

    void clear() noexcept
    {
        _items.clear();
    }

    std::pair<btree_iterator, bool> insert(const tree_data_type &data)
    {
        auto [position, inserted] = _items.insert(data);
        return {btree_iterator(this, static_cast<size_t>(std::distance(_items.begin(), position))), inserted};
    }

    std::pair<btree_iterator, bool> insert(tree_data_type &&data)
    {
        auto [position, inserted] = _items.insert(std::move(data));
        return {btree_iterator(this, static_cast<size_t>(std::distance(_items.begin(), position))), inserted};
    }

    template<typename... Args>
    std::pair<btree_iterator, bool> emplace(Args &&...args)
    {
        tree_data_type data(std::forward<Args>(args)...);
        return insert(std::move(data));
    }

    btree_iterator insert_or_assign(const tree_data_type &data)
    {
        auto [position, inserted] = _items.insert_or_assign(data.first, data.second);
        return btree_iterator(this, static_cast<size_t>(std::distance(_items.begin(), position)));
    }

    btree_iterator insert_or_assign(tree_data_type &&data)
    {
        auto [position, inserted] = _items.insert_or_assign(std::move(data.first), std::move(data.second));
        return btree_iterator(this, static_cast<size_t>(std::distance(_items.begin(), position)));
    }

    template<typename... Args>
    btree_iterator emplace_or_assign(Args &&...args)
    {
        tree_data_type data(std::forward<Args>(args)...);
        return insert_or_assign(std::move(data));
    }

    btree_iterator erase(btree_iterator pos)
    {
        if (pos == end())
        {
            return end();
        }
        auto iterators = ordered_iterators();
        auto next_rank = pos._rank;
        _items.erase(iterators[pos._rank]);
        return btree_iterator(this, std::min(next_rank, _items.size()));
    }

    btree_iterator erase(btree_const_iterator pos)
    {
        return erase(btree_iterator(this, pos._rank));
    }

    btree_iterator erase(btree_iterator first, btree_iterator last)
    {
        while (first != last)
        {
            first = erase(first);
        }
        return first;
    }

    btree_iterator erase(btree_const_iterator first, btree_const_iterator last)
    {
        return erase(btree_iterator(this, first._rank), btree_iterator(this, last._rank));
    }

    btree_iterator erase(const tkey &key)
    {
        auto found = _items.find(key);
        if (found == _items.end())
        {
            return end();
        }
        auto rank = static_cast<size_t>(std::distance(_items.begin(), found));
        _items.erase(found);
        return btree_iterator(this, std::min(rank, _items.size()));
    }
};

#endif
