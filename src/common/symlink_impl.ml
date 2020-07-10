(** Simple implementation of symlinks, as a str_256 written to a blk
   (we currently ignore times on symlinks) *)


module Symlink_block = struct
  type t = Str_256.str_256[@@deriving bin_io]
  let max_sz = 259
end

let bp_mshlr : _ bp_mshlr = (module Symlink_block)

let ba_mshlr = bp_mshlrs#ba_mshlr ~mshlr:bp_mshlr

type ('blk_id,'blk,'t) symlink_factory = <
  write_symlink : 
    blk_dev_ops : ('blk_id,'blk,'t) blk_dev_ops -> 
    blk_id      : 'blk_id -> 
    contents    : str_256 -> 
    (unit,'t)m;

  read_symlink : 
    blk_dev_ops : ('blk_id,'blk,'t) blk_dev_ops -> 
    blk_id      : 'blk_id -> 
    (str_256,'t)m;

  with_:
    blk_dev_ops : ('blk_id,'blk,'t) blk_dev_ops -> 
    freelist_ops : ('blk_id,'t) blk_allocator_ops -> 
    <    
      create_symlink : str_256 -> ('blk_id,'t)m;
    >
>

open Shared_ctxt

let example : (r,buf,t) symlink_factory = 
  let blk_sz = blk_sz |> Blk_sz.to_int in
  let ba_mshlr = ba_mshlr ~buf_sz:blk_sz in
  let module M = (val ba_mshlr) in
  let write_symlink ~blk_dev_ops ~blk_id ~contents =
      blk_dev_ops.write ~blk_id ~blk:(M.marshal contents)
  in
  object
    method write_symlink = write_symlink
    method read_symlink ~blk_dev_ops ~blk_id =
      blk_dev_ops.read ~blk_id >>= fun blk -> 
      return (M.unmarshal blk)
    method with_ ~blk_dev_ops ~freelist_ops = object
      method create_symlink contents = 
        freelist_ops.blk_alloc () >>= fun blk_id ->
        write_symlink ~blk_dev_ops ~blk_id ~contents >>= fun () ->
        return blk_id
    end
end
