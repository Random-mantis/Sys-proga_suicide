#include <algorithm>
#include <cstddef>
#include <limits>
#include <list>
#include <memory_resource>
#include <mutex>
#include <new>
#include <stdexcept>
#include <utility>
#include <vector>

struct buddy_block;

#include "../include/allocator_buddies_system.h"

struct buddy_block
{
    size_t offset;
    size_t size;
    bool occupied;
};

namespace
{
    struct buddy_state
    {
        std::pmr::memory_resource *parent = nullptr;
        allocator_with_fit_mode::fit_mode mode = allocator_with_fit_mode::fit_mode::first_fit;
        size_t size = 0;
        void *arena = nullptr;
        mutable std::mutex mutex;
        std::list<buddy_block> blocks;
    };

    size_t next_power_of_two(size_t value)
    {
        if (value <= 1)
        {
            return 1;
        }

        if (value > (size_t{1} << (std::numeric_limits<size_t>::digits - 1)))
        {
            throw std::bad_alloc();
        }

        --value;
        for (size_t shift = 1; shift < sizeof(size_t) * 8; shift <<= 1)
        {
            value |= value >> shift;
        }

        return value + 1;
    }

    size_t min_block_size()
    {
        return next_power_of_two(sizeof(void *) + 1);
    }

    buddy_state *state(void *trusted) noexcept
    {
        return static_cast<buddy_state *>(trusted);
    }

    char *arena_begin(buddy_state *allocator_state) noexcept
    {
        return static_cast<char *>(allocator_state->arena);
    }

    bool is_better_block(
        allocator_with_fit_mode::fit_mode mode,
        const buddy_block &candidate,
        const buddy_block &selected) noexcept
    {
        switch (mode)
        {
            case allocator_with_fit_mode::fit_mode::first_fit:
                return false;
            case allocator_with_fit_mode::fit_mode::the_best_fit:
                return candidate.size < selected.size;
            case allocator_with_fit_mode::fit_mode::the_worst_fit:
                return candidate.size > selected.size;
        }

        return false;
    }

    void merge_buddies(std::list<buddy_block> &blocks, std::list<buddy_block>::iterator block)
    {
        for (;;)
        {
            const size_t buddy_offset = block->offset ^ block->size;
            auto buddy = std::find_if(blocks.begin(), blocks.end(), [&](const buddy_block &candidate)
            {
                return !candidate.occupied &&
                       candidate.size == block->size &&
                       candidate.offset == buddy_offset;
            });

            if (buddy == blocks.end())
            {
                return;
            }

            const size_t merged_offset = std::min(block->offset, buddy->offset);
            const size_t merged_size = block->size * 2;
            block = blocks.erase(block);
            blocks.erase(buddy);

            auto insert_at = std::find_if(blocks.begin(), blocks.end(), [&](const buddy_block &candidate)
            {
                return candidate.offset > merged_offset;
            });

            block = blocks.insert(insert_at, {merged_offset, merged_size, false});
        }
    }
}

allocator_buddies_system::allocator_buddies_system(
    size_t space_size,
    std::pmr::memory_resource *parent_allocator,
    allocator_with_fit_mode::fit_mode allocate_fit_mode)
{
    const size_t arena_size = next_power_of_two(space_size);
    if (arena_size < min_block_size())
    {
        throw std::logic_error("allocator_buddies_system: memory area is too small");
    }

    auto *parent = parent_allocator == nullptr ? std::pmr::get_default_resource() : parent_allocator;
    auto *allocator_state = new buddy_state;

    try
    {
        allocator_state->parent = parent;
        allocator_state->mode = allocate_fit_mode;
        allocator_state->size = arena_size;
        allocator_state->arena = parent->allocate(arena_size, alignof(std::max_align_t));
        allocator_state->blocks.push_back({0, arena_size, false});
        _trusted_memory = allocator_state;
    }
    catch (...)
    {
        if (allocator_state->arena != nullptr)
        {
            allocator_state->parent->deallocate(allocator_state->arena, allocator_state->size, alignof(std::max_align_t));
        }
        delete allocator_state;
        throw;
    }
}

allocator_buddies_system::allocator_buddies_system(const allocator_buddies_system &other)
{
    auto *source = state(other._trusted_memory);
    if (source == nullptr)
    {
        _trusted_memory = nullptr;
        return;
    }

    std::scoped_lock lock(source->mutex);
    auto *allocator_state = new buddy_state;

    try
    {
        allocator_state->parent = source->parent;
        allocator_state->mode = source->mode;
        allocator_state->size = source->size;
        allocator_state->arena = allocator_state->parent->allocate(allocator_state->size, alignof(std::max_align_t));
        std::copy_n(static_cast<char *>(source->arena), source->size, static_cast<char *>(allocator_state->arena));
        allocator_state->blocks = source->blocks;
        _trusted_memory = allocator_state;
    }
    catch (...)
    {
        if (allocator_state->arena != nullptr)
        {
            allocator_state->parent->deallocate(allocator_state->arena, allocator_state->size, alignof(std::max_align_t));
        }
        delete allocator_state;
        throw;
    }
}

allocator_buddies_system &allocator_buddies_system::operator=(const allocator_buddies_system &other)
{
    if (this != &other)
    {
        allocator_buddies_system copy(other);
        std::swap(_trusted_memory, copy._trusted_memory);
    }

    return *this;
}

allocator_buddies_system::allocator_buddies_system(allocator_buddies_system &&other) noexcept:
    _trusted_memory(std::exchange(other._trusted_memory, nullptr))
{
}

allocator_buddies_system &allocator_buddies_system::operator=(allocator_buddies_system &&other) noexcept
{
    if (this != &other)
    {
        this->~allocator_buddies_system();
        _trusted_memory = std::exchange(other._trusted_memory, nullptr);
    }

    return *this;
}

allocator_buddies_system::~allocator_buddies_system()
{
    auto *allocator_state = state(_trusted_memory);
    if (allocator_state == nullptr)
    {
        return;
    }

    allocator_state->parent->deallocate(allocator_state->arena, allocator_state->size, alignof(std::max_align_t));
    delete allocator_state;
    _trusted_memory = nullptr;
}

[[nodiscard]] void *allocator_buddies_system::do_allocate_sm(size_t size)
{
    auto *allocator_state = state(_trusted_memory);
    if (allocator_state == nullptr)
    {
        throw std::bad_alloc();
    }

    std::scoped_lock lock(allocator_state->mutex);
    const size_t requested_size = std::max(min_block_size(), next_power_of_two(size == 0 ? 1 : size));

    auto selected = allocator_state->blocks.end();
    for (auto current = allocator_state->blocks.begin(); current != allocator_state->blocks.end(); ++current)
    {
        if (!current->occupied && current->size >= requested_size)
        {
            if (selected == allocator_state->blocks.end())
            {
                selected = current;
                if (allocator_state->mode == allocator_with_fit_mode::fit_mode::first_fit)
                {
                    break;
                }
            }
            else if (is_better_block(allocator_state->mode, *current, *selected))
            {
                selected = current;
            }
        }
    }

    if (selected == allocator_state->blocks.end())
    {
        throw std::bad_alloc();
    }

    while (selected->size > requested_size)
    {
        selected->size /= 2;
        allocator_state->blocks.insert(std::next(selected), {selected->offset + selected->size, selected->size, false});
    }

    selected->occupied = true;
    return arena_begin(allocator_state) + selected->offset;
}

void allocator_buddies_system::do_deallocate_sm(void *at)
{
    if (at == nullptr)
    {
        return;
    }

    auto *allocator_state = state(_trusted_memory);
    if (allocator_state == nullptr)
    {
        throw std::logic_error("allocator_buddies_system: allocator is empty");
    }

    std::scoped_lock lock(allocator_state->mutex);
    const auto *address = static_cast<const char *>(at);
    const auto *begin = arena_begin(allocator_state);
    const auto *end = begin + allocator_state->size;
    if (address < begin || address >= end)
    {
        throw std::logic_error("allocator_buddies_system: invalid pointer");
    }

    const auto offset = static_cast<size_t>(address - begin);
    for (auto current = allocator_state->blocks.begin(); current != allocator_state->blocks.end(); ++current)
    {
        if (current->offset == offset && current->occupied)
        {
            current->occupied = false;
            merge_buddies(allocator_state->blocks, current);
            return;
        }
    }

    throw std::logic_error("allocator_buddies_system: invalid pointer");
}

bool allocator_buddies_system::do_is_equal(const std::pmr::memory_resource &other) const noexcept
{
    return this == &other;
}

inline void allocator_buddies_system::set_fit_mode(
    allocator_with_fit_mode::fit_mode mode)
{
    auto *allocator_state = state(_trusted_memory);
    if (allocator_state == nullptr)
    {
        return;
    }

    std::scoped_lock lock(allocator_state->mutex);
    allocator_state->mode = mode;
}

std::vector<allocator_test_utils::block_info> allocator_buddies_system::get_blocks_info() const noexcept
{
    try
    {
        auto *allocator_state = state(_trusted_memory);
        if (allocator_state == nullptr)
        {
            return {};
        }

        std::scoped_lock lock(allocator_state->mutex);
        return get_blocks_info_inner();
    }
    catch (...)
    {
        return {};
    }
}

std::vector<allocator_test_utils::block_info> allocator_buddies_system::get_blocks_info_inner() const
{
    auto *allocator_state = state(_trusted_memory);
    std::vector<allocator_test_utils::block_info> result;
    result.reserve(allocator_state->blocks.size());

    for (const auto &block: allocator_state->blocks)
    {
        result.push_back({.block_size = block.size, .is_block_occupied = block.occupied});
    }

    return result;
}

allocator_buddies_system::buddy_iterator allocator_buddies_system::begin() const noexcept
{
    auto *allocator_state = state(_trusted_memory);
    if (allocator_state == nullptr || allocator_state->blocks.empty())
    {
        return buddy_iterator();
    }

    return buddy_iterator(_trusted_memory, allocator_state->blocks.begin(), allocator_state->blocks.end());
}

allocator_buddies_system::buddy_iterator allocator_buddies_system::end() const noexcept
{
    auto *allocator_state = state(_trusted_memory);
    if (allocator_state == nullptr)
    {
        return buddy_iterator();
    }

    return buddy_iterator(_trusted_memory, allocator_state->blocks.end(), allocator_state->blocks.end());
}

allocator_buddies_system::buddy_iterator::buddy_iterator() noexcept:
    _trusted_memory(nullptr),
    _it(),
    _end()
{
}

allocator_buddies_system::buddy_iterator::buddy_iterator(
    void *trusted_memory,
    std::list<buddy_block>::iterator it,
    std::list<buddy_block>::iterator end) noexcept:
    _trusted_memory(trusted_memory),
    _it(it),
    _end(end)
{
}

bool allocator_buddies_system::buddy_iterator::operator==(const buddy_iterator &other) const noexcept
{
    return _trusted_memory == other._trusted_memory && _it == other._it;
}

bool allocator_buddies_system::buddy_iterator::operator!=(const buddy_iterator &other) const noexcept
{
    return !(*this == other);
}

allocator_buddies_system::buddy_iterator &allocator_buddies_system::buddy_iterator::operator++() & noexcept
{
    if (_it != _end)
    {
        ++_it;
    }

    return *this;
}

allocator_buddies_system::buddy_iterator allocator_buddies_system::buddy_iterator::operator++(int) noexcept
{
    auto previous = *this;
    ++(*this);
    return previous;
}

size_t allocator_buddies_system::buddy_iterator::size() const noexcept
{
    return _it != _end ? _it->size : 0;
}

bool allocator_buddies_system::buddy_iterator::occupied() const noexcept
{
    return _it != _end ? _it->occupied : false;
}

void *allocator_buddies_system::buddy_iterator::operator*() const noexcept
{
    return _it != _end ? const_cast<buddy_block *>(&(*_it)) : nullptr;
}
