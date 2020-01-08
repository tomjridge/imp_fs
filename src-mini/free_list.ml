(** Free list

This supersedes the free list in fs_shared.

*)

(**

At the highest level, a free list could be just a set (finite or
   infinite) of blk_ids.

We probably want to avoid representing each blk_id, so we have a
   blk_id beyond which all blks are free.

Each object (file, directory...) has an associated list of blks in
   use. When a file is freed, we want to return this list to the free
   list in the most efficient way possible.



A free list contains the following:

- an (optional) open range of the form [n,infinity), indicating that
   blocks n upwards are free (there may additionally be a limit on the
   total number of blocks) - a list of free-blk-lists - or do we want
   to mutate the blks so we just have a huge list of free blks?  - a
   transient (in-memory) list of free blks that can be persisted with
   a sync operation

*)

type blk_id  = int (* a blk_id corresponding to a blk storing data of type 'a *)


(** Notion of a blk_id stored on disk: either a direct blk_id, or an "indirect" blk_id, i.e., a pointer to a blk which contains ... *)

type block = 
   | Blk1 of blks 
   | Blk2 of blks

and blks = { blks: blk_id list; prev: blk_id option }


(** In the freelist, a list formed from Blk1 blks is associated with
   each file. The freelist itself contains a list of Blk2s, where each
   pointed-to blk is the head of a blk1 list. *)


module Free_list = struct

  (** A singly-linked on-disk list of blks; conceptually this is just a list (or set) of blk_ids *)
  type on_disk_blk = {
    blks         : blks;
    backing_blk  : blk_id;
  }

  type free_list = {
    free_from    : blk_id option;
    on_disk_root : blk_id; (* blk_id of the head of the blk2 list *)
    transient    : blk_id list;  (* not part of the on-disk freelist *)
  }

end
open Free_list



module Store = struct

  module M = Map.Make(struct type t = blk_id let compare: t -> t -> int = Pervasives.compare end)
  include M
  type store = block M.t
end  


module Example_store = struct
  (* store from blk 0 *)
  
  (* blk0 is the root of the freelist *)
  let blk0 = Blk2 { blks=[10;20]; prev=None; }
  let blk10 = Blk1 { blks=[11;12;13]; prev=None }
  let blk20 = Blk1 { blks=[21;23]; prev=None } 

  let store = 
    [(0,blk0);
     (10,blk10);
     (* 11, 12 and 13 are free *)
     (20,blk20)
     (* 21,23 are free *)
    ]
    |> List.to_seq
    |> Store.of_seq

  let dest_blk1 = function Blk1 blk1 -> blk1 | _ -> failwith "Expecting a Blk1"
  let dest_blk2 = function Blk2 blk2 -> blk2 | _ -> failwith "Expecting a Blk2"

  (* we return the explicitly free blkids, not including the blks that
     makeup the freelist itself *)
  let store_to_frees ~store ~root = 
    (* root is a ptr to a blk2 *)
    let acc : blk_id list = [] in
    let rec f = function
      | Blk1{blks;prev} -> f1 {blks;prev}
      | Blk2{blks;prev} -> f2 {blks;prev}
    and f1 {blks;prev} = (
        acc:=blks::!acc;
        match prev with 
        | None -> ()
        | Some blk_id -> Store.find blk_id store |> dest_blk1 |> f1)
    and f2 {blks;prev} = (
        (* blks contains ids which point to Blk1s *)
        blks |> List.iter (fun blk_id -> Store.find blk_id store |> dest_blk1 |> f1);
        (* and recurse on the prev *)
        match prev with
        | None -> ()
        | Some blk_id -> Store.find blk_id store |> dest_blk2 |> f2

      

end
