#include "../include/allocator_boundary_tags.h"

#include <algorithm>
#include <limits>
#include <new>
#include <stdexcept>

namespace
{
    using fit_mode = allocator_with_fit_mode::fit_mode;

    constexpr size_t align_up(size_t value, size_t alignment) noexcept
    {
        return (value + alignment - 1) / alignment * alignment;
    }

    constexpr size_t parent_offset = 0;
    constexpr size_t fit_mode_offset =
        align_up(parent_offset + sizeof(std::pmr::memory_resource *), alignof(fit_mode));
    constexpr size_t space_size_offset =
        align_up(fit_mode_offset + sizeof(fit_mode), alignof(size_t));
    constexpr size_t mutex_offset =
        align_up(space_size_offset + sizeof(size_t), alignof(std::mutex));
    constexpr size_t allocator_metadata_area_size =
        align_up(mutex_offset + sizeof(std::mutex), alignof(std::max_align_t));

    constexpr size_t block_size_offset = 0;
    constexpr size_t block_prev_size_offset = sizeof(size_t);
    constexpr size_t block_owner_offset = block_prev_size_offset + sizeof(void *);
    constexpr size_t block_reserved_offset = block_owner_offset + sizeof(void *);
    constexpr size_t block_header_size = block_reserved_offset + sizeof(void *);
    constexpr size_t min_block_size = block_header_size;

    char *as_char(void *ptr) noexcept
    {
        return static_cast<char *>(ptr);
    }

    const char *as_char(const void *ptr) noexcept
    {
        return static_cast<const char *>(ptr);
    }

    std::pmr::memory_resource *&parent_allocator_ref(void *trusted) noexcept
    {
        return *reinterpret_cast<std::pmr::memory_resource **>(as_char(trusted) + parent_offset);
    }

    fit_mode &fit_mode_ref(void *trusted) noexcept
    {
        return *reinterpret_cast<fit_mode *>(as_char(trusted) + fit_mode_offset);
    }

    size_t &space_size_ref(void *trusted) noexcept
    {
        return *reinterpret_cast<size_t *>(as_char(trusted) + space_size_offset);
    }

    const size_t &space_size_ref(const void *trusted) noexcept
    {
        return *reinterpret_cast<const size_t *>(as_char(trusted) + space_size_offset);
    }

    std::mutex &mutex_ref(void *trusted) noexcept
    {
        return *reinterpret_cast<std::mutex *>(as_char(trusted) + mutex_offset);
    }

    char *blocks_begin(void *trusted) noexcept
    {
        return as_char(trusted) + allocator_metadata_area_size;
    }

    const char *blocks_begin(const void *trusted) noexcept
    {
        return as_char(trusted) + allocator_metadata_area_size;
    }

    char *blocks_end(void *trusted) noexcept
    {
        return blocks_begin(trusted) + space_size_ref(trusted);
    }

    const char *blocks_end(const void *trusted) noexcept
    {
        return blocks_begin(trusted) + space_size_ref(trusted);
    }

    size_t total_allocation_size(void *trusted) noexcept
    {
        return allocator_metadata_area_size + space_size_ref(trusted);
    }

    size_t &block_size_ref(void *block) noexcept
    {
        return *reinterpret_cast<size_t *>(as_char(block) + block_size_offset);
    }

    const size_t &block_size_ref(const void *block) noexcept
    {
        return *reinterpret_cast<const size_t *>(as_char(block) + block_size_offset);
    }

    size_t &prev_size_ref(void *block) noexcept
    {
        return *reinterpret_cast<size_t *>(as_char(block) + block_prev_size_offset);
    }

    const size_t &prev_size_ref(const void *block) noexcept
    {
        return *reinterpret_cast<const size_t *>(as_char(block) + block_prev_size_offset);
    }

    void *&owner_ref(void *block) noexcept
    {
        return *reinterpret_cast<void **>(as_char(block) + block_owner_offset);
    }

    const void *owner_value(const void *block) noexcept
    {
        return *reinterpret_cast<void *const *>(as_char(block) + block_owner_offset);
    }

    void *&reserved_ref(void *block) noexcept
    {
        return *reinterpret_cast<void **>(as_char(block) + block_reserved_offset);
    }

    bool block_occupied_ref(const void *trusted, const void *block) noexcept
    {
        return owner_value(block) == trusted;
    }

    void write_block(void *block, size_t size, bool occupied, void *owner) noexcept
    {
        block_size_ref(block) = size;
        owner_ref(block) = occupied ? owner : nullptr;
        reserved_ref(block) = nullptr;
    }

    void fix_next_prev_size(void *trusted, void *block) noexcept
    {
        char *next = as_char(block) + block_size_ref(block);
        if (next < blocks_end(trusted))
        {
            prev_size_ref(next) = block_size_ref(block);
        }
    }

    bool block_is_valid(void *trusted, void *block) noexcept
    {
        const char *begin = blocks_begin(trusted);
        const char *end = blocks_end(trusted);
        const char *candidate = as_char(block);
        if (candidate < begin || candidate + min_block_size > end)
        {
            return false;
        }

        const size_t size = block_size_ref(block);
        return size >= min_block_size &&
               candidate + size <= end &&
               (candidate == begin || prev_size_ref(block) != 0);
    }

    size_t required_block_size(size_t user_size)
    {
        if (user_size > std::numeric_limits<size_t>::max() - block_header_size)
        {
            throw std::bad_alloc();
        }

        return block_header_size + user_size;
    }

    void construct_trusted_memory(void *trusted, size_t space_size, std::pmr::memory_resource *parent, fit_mode mode)
    {
        parent_allocator_ref(trusted) = parent == nullptr ? std::pmr::get_default_resource() : parent;
        fit_mode_ref(trusted) = mode;
        space_size_ref(trusted) = space_size;
        new (as_char(trusted) + mutex_offset) std::mutex();
        write_block(blocks_begin(trusted), space_size_ref(trusted), false, nullptr);
        prev_size_ref(blocks_begin(trusted)) = 0;
    }

    void destroy_trusted_memory(void *trusted) noexcept
    {
        if (trusted == nullptr)
        {
            return;
        }

        auto *parent = parent_allocator_ref(trusted);
        const size_t allocation_size = total_allocation_size(trusted);
        mutex_ref(trusted).~mutex();
        parent->deallocate(trusted, allocation_size, alignof(std::max_align_t));
    }

    void *next_block(void *trusted, void *block) noexcept
    {
        char *next = as_char(block) + block_size_ref(block);
        return next == blocks_end(trusted) ? nullptr : next;
    }

    void *previous_block(void *trusted, void *block) noexcept
    {
        if (block == blocks_begin(trusted))
        {
            return nullptr;
        }

        const size_t previous_size = prev_size_ref(block);
        char *previous = as_char(block) - previous_size;
        return previous < blocks_begin(trusted) ? nullptr : previous;
    }

    bool candidate_is_better(void *candidate, void *current, fit_mode mode) noexcept
    {
        if (current == nullptr)
        {
            return true;
        }

        return (mode == fit_mode::the_best_fit && block_size_ref(candidate) < block_size_ref(current)) ||
               (mode == fit_mode::the_worst_fit && block_size_ref(candidate) > block_size_ref(current));
    }

    void *find_suitable_block(void *trusted, size_t requested_size)
    {
        void *best = nullptr;
        const auto mode = fit_mode_ref(trusted);
        for (char *current = blocks_begin(trusted); current < blocks_end(trusted); current += block_size_ref(current))
        {
            if (!block_is_valid(trusted, current))
            {
                throw std::logic_error("allocator_boundary_tags: corrupted block boundary");
            }

            if (!block_occupied_ref(trusted, current) && block_size_ref(current) >= requested_size)
            {
                if (mode == fit_mode::first_fit)
                {
                    return current;
                }

                if (candidate_is_better(current, best, mode))
                {
                    best = current;
                }
            }
        }

        return best;
    }

    void retarget_copied_blocks(void *destination) noexcept
    {
        for (char *current = blocks_begin(destination); current < blocks_end(destination); current += block_size_ref(current))
        {
            if (block_occupied_ref(destination, current))
            {
                owner_ref(current) = destination;
            }
        }
    }
}

allocator_boundary_tags::~allocator_boundary_tags()
{
    destroy_trusted_memory(_trusted_memory);
}

allocator_boundary_tags::allocator_boundary_tags(allocator_boundary_tags &&other) noexcept:
    _trusted_memory(other._trusted_memory)
{
    other._trusted_memory = nullptr;
}

allocator_boundary_tags &allocator_boundary_tags::operator=(allocator_boundary_tags &&other) noexcept
{
    if (this != &other)
    {
        destroy_trusted_memory(_trusted_memory);
        _trusted_memory = other._trusted_memory;
        other._trusted_memory = nullptr;
    }

    return *this;
}

allocator_boundary_tags::allocator_boundary_tags(
    size_t space_size,
    std::pmr::memory_resource *parent_allocator,
    allocator_with_fit_mode::fit_mode allocate_fit_mode)
{
    if (space_size < min_block_size)
    {
        throw std::logic_error("allocator_boundary_tags: memory area is too small");
    }
    if (space_size > std::numeric_limits<size_t>::max() - allocator_metadata_area_size - alignof(std::max_align_t) + 1)
    {
        throw std::bad_alloc();
    }

    auto *parent = parent_allocator == nullptr ? std::pmr::get_default_resource() : parent_allocator;
    const size_t aligned_space_size = space_size;
    const size_t allocation_size = allocator_metadata_area_size + aligned_space_size;

    _trusted_memory = parent->allocate(allocation_size, alignof(std::max_align_t));
    try
    {
        construct_trusted_memory(_trusted_memory, aligned_space_size, parent, allocate_fit_mode);
    }
    catch (...)
    {
        parent->deallocate(_trusted_memory, allocation_size, alignof(std::max_align_t));
        _trusted_memory = nullptr;
        throw;
    }
}

[[nodiscard]] void *allocator_boundary_tags::do_allocate_sm(size_t size)
{
    std::scoped_lock lock(mutex_ref(_trusted_memory));
    const size_t requested_size = required_block_size(size);
    void *block = find_suitable_block(_trusted_memory, requested_size);
    if (block == nullptr)
    {
        throw std::bad_alloc();
    }

    const size_t available_size = block_size_ref(block);
    const size_t rest_size = available_size - requested_size;
    if (rest_size >= min_block_size)
    {
        const size_t previous_size = prev_size_ref(block);
        write_block(block, requested_size, true, _trusted_memory);
        prev_size_ref(block) = previous_size;
        void *rest = as_char(block) + requested_size;
        write_block(rest, rest_size, false, nullptr);
        prev_size_ref(rest) = requested_size;
        fix_next_prev_size(_trusted_memory, rest);
    }
    else
    {
        const size_t previous_size = prev_size_ref(block);
        write_block(block, available_size, true, _trusted_memory);
        prev_size_ref(block) = previous_size;
        fix_next_prev_size(_trusted_memory, block);
    }

    return as_char(block) + block_header_size;
}

void allocator_boundary_tags::do_deallocate_sm(void *at)
{
    if (at == nullptr)
    {
        return;
    }

    std::scoped_lock lock(mutex_ref(_trusted_memory));
    void *block = as_char(at) - block_header_size;
    if (!block_is_valid(_trusted_memory, block) ||
        !block_occupied_ref(_trusted_memory, block) ||
        owner_value(block) != _trusted_memory)
    {
        throw std::logic_error("allocator_boundary_tags: block does not belong to this allocator");
    }

    const size_t previous_size = prev_size_ref(block);
    write_block(block, block_size_ref(block), false, nullptr);
    prev_size_ref(block) = previous_size;

    if (void *previous = previous_block(_trusted_memory, block);
        previous != nullptr && block_is_valid(_trusted_memory, previous) && !block_occupied_ref(_trusted_memory, previous))
    {
        write_block(previous, block_size_ref(previous) + block_size_ref(block), false, nullptr);
        block = previous;
    }

    if (void *next = next_block(_trusted_memory, block);
        next != nullptr && block_is_valid(_trusted_memory, next) && !block_occupied_ref(_trusted_memory, next))
    {
        write_block(block, block_size_ref(block) + block_size_ref(next), false, nullptr);
    }
    fix_next_prev_size(_trusted_memory, block);
}

inline void allocator_boundary_tags::set_fit_mode(allocator_with_fit_mode::fit_mode mode)
{
    std::scoped_lock lock(mutex_ref(_trusted_memory));
    fit_mode_ref(_trusted_memory) = mode;
}

std::vector<allocator_test_utils::block_info> allocator_boundary_tags::get_blocks_info() const
{
    std::scoped_lock lock(mutex_ref(_trusted_memory));
    return get_blocks_info_inner();
}

allocator_boundary_tags::boundary_iterator allocator_boundary_tags::begin() const noexcept
{
    return boundary_iterator(_trusted_memory, blocks_begin(_trusted_memory), block_occupied_ref(_trusted_memory, blocks_begin(_trusted_memory)));
}

allocator_boundary_tags::boundary_iterator allocator_boundary_tags::end() const noexcept
{
    return boundary_iterator(_trusted_memory, blocks_end(_trusted_memory), false);
}

std::vector<allocator_test_utils::block_info> allocator_boundary_tags::get_blocks_info_inner() const
{
    std::vector<allocator_test_utils::block_info> result;

    for (auto it = begin(); it != end(); ++it)
    {
        result.push_back({ .block_size = it.size(), .is_block_occupied = it.occupied() });
    }

    return result;
}

allocator_boundary_tags::allocator_boundary_tags(const allocator_boundary_tags &other)
{
    std::scoped_lock lock(mutex_ref(other._trusted_memory));

    auto *parent = parent_allocator_ref(other._trusted_memory);
    const size_t allocation_size = total_allocation_size(other._trusted_memory);
    _trusted_memory = parent->allocate(allocation_size, alignof(std::max_align_t));

    try
    {
        parent_allocator_ref(_trusted_memory) = parent;
        fit_mode_ref(_trusted_memory) = fit_mode_ref(other._trusted_memory);
        space_size_ref(_trusted_memory) = space_size_ref(other._trusted_memory);
        new (as_char(_trusted_memory) + mutex_offset) std::mutex();
        std::copy_n(blocks_begin(other._trusted_memory), space_size_ref(other._trusted_memory), blocks_begin(_trusted_memory));
        retarget_copied_blocks(_trusted_memory);
    }
    catch (...)
    {
        parent->deallocate(_trusted_memory, allocation_size, alignof(std::max_align_t));
        _trusted_memory = nullptr;
        throw;
    }
}

allocator_boundary_tags &allocator_boundary_tags::operator=(const allocator_boundary_tags &other)
{
    if (this == &other)
    {
        return *this;
    }

    allocator_boundary_tags copy(other);
    std::swap(_trusted_memory, copy._trusted_memory);
    return *this;
}

bool allocator_boundary_tags::do_is_equal(const std::pmr::memory_resource &other) const noexcept
{
    return this == &other;
}

bool allocator_boundary_tags::boundary_iterator::operator==(const allocator_boundary_tags::boundary_iterator &other) const noexcept
{
    return _occupied_ptr == other._occupied_ptr && _trusted_memory == other._trusted_memory;
}

bool allocator_boundary_tags::boundary_iterator::operator!=(const allocator_boundary_tags::boundary_iterator &other) const noexcept
{
    return !(*this == other);
}

allocator_boundary_tags::boundary_iterator &allocator_boundary_tags::boundary_iterator::operator++() & noexcept
{
    if (_trusted_memory == nullptr || _occupied_ptr == blocks_end(_trusted_memory))
    {
        return *this;
    }

    _occupied_ptr = as_char(_occupied_ptr) + block_size_ref(_occupied_ptr);
    if (_occupied_ptr == blocks_end(_trusted_memory))
    {
        _occupied = false;
    }
    else
    {
        _occupied = block_occupied_ref(_trusted_memory, _occupied_ptr);
    }

    return *this;
}

allocator_boundary_tags::boundary_iterator &allocator_boundary_tags::boundary_iterator::operator--() & noexcept
{
    if (_trusted_memory == nullptr || _occupied_ptr == blocks_begin(_trusted_memory))
    {
        return *this;
    }

    if (_occupied_ptr == blocks_end(_trusted_memory))
    {
        void *last = blocks_begin(_trusted_memory);
        while (as_char(last) + block_size_ref(last) < blocks_end(_trusted_memory))
        {
            last = as_char(last) + block_size_ref(last);
        }
        _occupied_ptr = last;
    }
    else if (void *previous = previous_block(_trusted_memory, _occupied_ptr); previous != nullptr)
    {
        _occupied_ptr = previous;
    }

    _occupied = block_occupied_ref(_trusted_memory, _occupied_ptr);
    return *this;
}

allocator_boundary_tags::boundary_iterator allocator_boundary_tags::boundary_iterator::operator++(int)
{
    auto old = *this;
    ++(*this);
    return old;
}

allocator_boundary_tags::boundary_iterator allocator_boundary_tags::boundary_iterator::operator--(int)
{
    auto old = *this;
    --(*this);
    return old;
}

size_t allocator_boundary_tags::boundary_iterator::size() const noexcept
{
    return _trusted_memory == nullptr || _occupied_ptr == blocks_end(_trusted_memory)
        ? 0
        : block_size_ref(_occupied_ptr);
}

bool allocator_boundary_tags::boundary_iterator::occupied() const noexcept
{
    return _occupied;
}

void *allocator_boundary_tags::boundary_iterator::operator*() const noexcept
{
    return _occupied_ptr;
}

allocator_boundary_tags::boundary_iterator::boundary_iterator():
    _occupied_ptr(nullptr),
    _occupied(false),
    _trusted_memory(nullptr)
{
}

allocator_boundary_tags::boundary_iterator::boundary_iterator(void *trusted):
    _occupied_ptr(trusted == nullptr ? nullptr : blocks_begin(trusted)),
    _occupied(trusted != nullptr && block_occupied_ref(trusted, blocks_begin(trusted))),
    _trusted_memory(trusted)
{
}

allocator_boundary_tags::boundary_iterator::boundary_iterator(void *trusted, void *current, bool occupied):
    _occupied_ptr(current),
    _occupied(occupied),
    _trusted_memory(trusted)
{
}

void *allocator_boundary_tags::boundary_iterator::get_ptr() const noexcept
{
    return _occupied_ptr;
}
