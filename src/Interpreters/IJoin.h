#pragma once

#include <memory>
#include <vector>

#include <Core/Names.h>
#include <Core/Block.h>
#include <Columns/IColumn.h>
#include <base/logger_useful.h>

namespace DB
{

class Block;
struct ExtraBlock;
using ExtraBlockPtr = std::shared_ptr<ExtraBlock>;

class TableJoin;
class NotJoinedBlocks;
class IBlocksStream;
using IBlocksStreamPtr = std::shared_ptr<IBlocksStream>;

class IJoin;
using JoinPtr = std::shared_ptr<IJoin>;

class IJoin
{
public:
    virtual ~IJoin() = default;

    virtual const TableJoin & getTableJoin() const = 0;

    /// Add block of data from right hand of JOIN.
    /// @returns false, if some limit was exceeded and you should not insert more data.
    virtual bool addJoinedBlock(const Block & block, bool check_limits = true) = 0; /// NOLINT

    /* Some initialization may be required before joinBlock() call.
     * It's better to done in in constructor, but left block exact structure is not known at that moment.
     * TODO: pass correct left block sample to the constructor.
     */
    virtual void initialize(const Block& /* left_sample_block */) {}

    virtual void checkTypesOfKeys(const Block & block) const = 0;

    /// Join the block with data from left hand of JOIN to the right hand data (that was previously built by calls to addJoinedBlock).
    /// Could be called from different threads in parallel.
    virtual void joinBlock(Block & block, std::shared_ptr<ExtraBlock> & not_processed) = 0;

    /// Set/Get totals for right table
    virtual void setTotals(const Block & block) = 0;
    virtual const Block& getTotals() const { return totals; }

    virtual size_t getTotalRowCount() const = 0;
    virtual size_t getTotalByteCount() const = 0;
    virtual bool alwaysReturnsEmptySet() const = 0;

    /// StorageJoin/Dictionary is already filled. No need to call addJoinedBlock.
    /// Different query plan is used for such joins.
    virtual bool isFilled() const { return false; }

    // That can run FillingRightJoinSideTransform parallelly
    virtual bool supportParallelJoin() const { return false; }
    virtual bool supportTotals() const { return true; }

    /// Peek next stream of delayed joined blocks.
    virtual IBlocksStreamPtr getDelayedBlocks() { return nullptr; }
    virtual bool hasDelayedBlocks() const { return false; }

    virtual IBlocksStreamPtr
        getNonJoinedBlocks(const Block& left_sample_block, const Block& result_sample_block, UInt64 max_block_size) const = 0;

private:
    Block totals;
};

class IBlocksStream
{
public:
    /// Returns empty block on EOF
    Block next()
    {
        if (finished)
            return {};

        if (Block res = nextImpl())
            return res;

        finished = true;
        return {};
    }

    virtual ~IBlocksStream() = default;

    bool isFinished() const { return finished; }

protected:
    virtual Block nextImpl() = 0;

    std::atomic_bool finished{ false };

};

}
