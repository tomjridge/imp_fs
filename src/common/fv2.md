# Additional notes for File_impl_v2.

We have multiple different objects that may asynchronously flush
to disk. How do we know that the assembly is crash-safe?

The file objects are:
    - the file origin block
    - the used list
    - the block index map
    - the data blocks
    
    
In addition we have:
    - the global freelist
    - the block device itself (writes may commit out of order)
    
We first assume that all the file objects are on top of the same block
device. The freelist may be on a different block device. Actually, the
freelist's correctness is handled separately, so let's concentrate on
the file objects.

Additionally, only one thread holds the file lock at any time. So
during a single file operation there is no concurrent interference.

The used list is crash consistent. Although writes may hit persistent
storage asynchronously and out of order, we use barriers (within the
used list implementation) to ensure that the writes are ordered
correctly. However, the used list does NOT sync its state. So, we know
only that the persistent state of the used list is some (previous)
state consistent with the barriers.

All blocks that are allocated (for the data blocks, and the block
index map) are allocated via the used list. Before these are written,
there is a barrier on the used list, to ensure that these blocks are
correctly recorded in the used list's persistent state. (Better: to
ensure that any of these allocated blocks that are written on disk,
are written after they are recorded in the used list.)

Side note: reasoning with barriers is similar to reasoning with sync
(in a way to be made precise and formal soonish!), which is why I
sometimes resort to shorthand statements like that above, rather than
the more verbose "if this is written, then this is written previously"
etc.

Currently, the block index map is uncached, so writes go directly to
disk.

Finally, when we update the file origin block, we first flush the used
list (the blk index map is not cached and so doesn't need to be
flushed), before updating the origin block and issuing a barrier on
the block device.

If we want to sync the file, we can simply flush it (including the
origin block) and then sync the block device. But this is the only
point at which we need to sync (and we hope that the block device
handles barriers efficiently).
    
    
