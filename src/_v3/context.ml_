(** File system calls correspond to an lwt thread execution. The
   context is per-thread, and keeps track of eg resources (such as
   locks that have been acquired). *)


(* available to a thread at runtime *)

type ('res,'t) ctxt_ops = {
  acquire             : 'res -> (unit,'t)m;
  release             : 'res -> (unit,'t)m;
  release_all         : unit -> (unit,'t)m;
  assert_no_resources : unit -> unit
}

(** acquire obtains the lock on a resource, and updates the context to
   record this *)

module type T = sig
  type t

  type 'a loc (* like the locations in operational semantics, used to model reference names *)

  val loc_create: 'a -> ('a loc,t)m (* with a name? *)
  val loc_get: 'a loc -> ('a,t)m
  val loc_set: 'a loc -> 'a -> (unit,t)m
  val loc_delete: 'a loc -> (unit,t)m
  val loc_stats: unit -> (int,t)m (* how many are active *)


  type ctxt (* = { id:int; resources:res set } *)

  type ctxt_id

  (* global operations; there is a single global pool of ctxts *)
  val ctxt_create : unit -> (ctxt_id,t) m (* like loc_create, but starts off with an empty ctxt *)
  val get_ctxt: ctxt_id -> (ctxt,t)m
  val set_ctxt: ctx_id:ctxt_id -> ctxt:ctxt -> (unit,t)m

  val ctxt_delete : ctxt_id -> (unit,t)m
  (** checks that there are no resources associated with the ctxt, then removes it from the system *)

  type res = Fs_origin | Lock of int



end



(** {2 Front-end cache entries} *)

(** We want this to be as independent of the backend as possible. For
   Gom, file, directory and symlink it is fairly clear what the
   generic cache should be. What is not so clear is what the interface
   should be to the implementation-specific cache. *)

type 'a maybe_dirty = 'a * bool

type ('a,'b) map_cache

type file_extra = { fd: Unix.file_descr }

(** per-object locks *)
module Lock_layer = struct
  type res_id

  (** these operations are per thread, ie there is an implicit thread
     id available *)
  type 't lock_ops = {
    acquire     : res_id -> (unit,'t)m;
    acquire_all : res_id list -> (unit,'t)m;
    release     : res_id -> (unit,'t)m;
    release_all : unit -> (unit,'t)m;
  }
    

end


(** implementation independent cache *)
module Cache_1 = struct

  type ('fid,'did,'sid) cache_key = 
    | G
    | F of 'fid
    | D of 'did
    | S of 'sid

  type blk_off = { blk_off:int }

  type ('blk_id,'blk,'gom_key,'gom_value,'dir_k,'dir_v,'fid,'did,'sid) cache_entry = 
    | Gom of ('gom_key,'gom_value)map_cache 

    | File of {
        times           : Times.times maybe_dirty;
        file_data_cache : (blk_off,'blk) map_cache;
      }
    
    | Dir of { 
        parent     : 'did maybe_dirty; 
        times      : Times.times maybe_dirty;
        dir_cache  : ('dir_k,'dir_v)map_cache
      }  

    | Symlink of str_256 maybe_dirty
end

(** the lower object layer, no caching *)
module Lower_layer = struct

  (* can't we just treat these as the non-cached versions of these
     objects? can't we just deal with the raw unlocked operations? *)

  
  type ('k,'v,'t) map_ops_small = {
    find   : 'k -> ('v option,'t)m;
    insert : 'k -> 'v -> (unit,'t)m;
    delete : 'k -> (unit,'t)m;
  }

(*
  type ('blk_id,'gom_key,'gom_value,'fid,'did,'sid) cache_entry = 
    | Gom of { 
        gom_origin     : 'blk_id;
        gom_btree_root : 'blk_id; 
      }

    | File of { 
        file_origin : 'blk_id;
        file_extra  : file_extra
      }

    | Dir of { 
        origin     : 'blk_id; 
        btree_root : 'blk_id maybe_dirty; 
      }  (* ops? *)

    | Symlink of { 
        origin     : 'blk_id;
      }
*)

  type ('k,'v,'t) gom_ops = ('k,'v,'t)map_ops_small

  type ('k,'v,'t) file_ops = unit
    (* { pread; pwrite; truncate; get_sz; set_times; get_times }, but
       we want to cache on the block data block layer; so the
       implementation of file at cache layer should use a lower laye
       rfile object which... actually perhaps it makes more sense to
       have a cache at the block layer as well, but also track which
       blocks a file owns so that we can sync those when the file
       itsel is synced; so we need a block layer cache? but at least
       for directories it was beneficial to have an in-mem kv cache;
       so perhaps we do want a kv cache on file and no block cache? in
       this case, we need an implementation of file that is easily
       cachable as an array of blocks; this is reasonably easy to do -
       for pread we can read the backing block if necessary and return
       a fraction of that; for pwrite, again we can read the backing
       block and then allow the pwrite; so do we need to have wrappers
       round the object operations, that also take account of the
       cache? yes *)

end


(* Really, we want to hold both types of cache info in a single global
   cache (the Gom is probably held separately). We want to expose
   simple functions which we can use to implement the vn_generic
   interface... something like "acquire these resources, do this,
   release these resources". And maybe the "do this" has asserts that
   check that the resources involved are owned by the thread.

   Perhaps it is worth thinking about which operations require more
   than one object:

   - resolve_path requires access to all objects on the path, but only
   via hand-over-hand locking (but this may cause deadlock... so note
   that we don't really need hand-over-hand locking);

   - rename is the obvious problem case; in order to do the rename
   (and the various checks such as subdir) we need to hold potentially
   many locks at once

   - everything else looks fine:

   - file_ops are specific to a single file

   - mkdir needs the parent, but the child isn't created initially, so
   only one lock required


   FIXME path resolution should utilise operations which lock each
   object individually, and unlock after they have been used

*)
