(** An implementation of directories on top of sqlite, for v3.

To implement directories, we need to implement the interface:

type ('k,'v,'t,'did) lower_ops = {
  get_meta  : unit -> ('did * times,'t)m;
  (** returns parent and times *)

  find      : 'k -> ('v option,'t) m;

  exec      : ('k,'v,'t,'did) exec_ops -> (unit,'t)m;

  opendir   : unit -> (('k,'v,'t)Ls.ops,'t)m;

  (* sync : unit -> (unit,'t)m; NOTE we assume the lower layer already
     handles these synchronously *)
}

We implement this without a cache: all operations go directly to the database.

Let's assume that each directory has a unique (int) identifier did. 

Then we can have a single table dir_entries:

(did, name, entry)

Indexed by did,name (name is unique of course, for a particular did)

find, insert and delete are fine.

For parent and times, we can have another table (did,parent,times). 

opendir creates a reference which pulls upto some limit of elts from the db, and stores the max name so we can resume later on ls_step.

FIXME what is the distinction between flush and sync at the lower layer? since these both force dirty changes to db, there isn't any

 *)

open Printf

module Times = Minifs_intf.Times

include struct
  open Dv3
  open Op
  type ('k,'v,'t,'did) dir_ops = ('k,'v,'t,'did) lower_ops = {
    get_meta  : unit -> ('did * times,'t)m;

    find      : 'k -> ('v option,'t) m;

    exec      : ('k,'v,'t,'did) exec_ops -> (unit,'t)m;

    opendir   : unit -> (('k,'v,'t)Ls.ops,'t)m;

  }
end

type times = Times.times

type did

let did_to_int: did -> int = failwith ""
let int_to_did: int -> did = failwith ""

type 'a cached_update = Some_ of 'a | Deleted

type db = Sqlite3.db

type dir_entry

(** convert to string/blob *)
let dir_entry_to_string: dir_entry -> string = failwith ""
let string_to_dir_entry: string -> dir_entry = failwith ""


(* NOTE no caching at this level *)

(* SQL commands to create a db; dir_entries and dir_meta *)
let create_db = {|
  DROP TABLE IF EXISTS 'dir_entries';
  DROP TABLE IF EXISTS 'dir_meta';

  CREATE TABLE 'dir_entries' (did INTEGER, name VARCHAR(255), dir_entry BLOB, PRIMARY KEY (did,name));
  /* CREATE UNIQUE INDEX 'index1' on 'dir_entries' (did,name); */

  CREATE TABLE 'dir_meta' (did INTEGER, parent INTEGER, atim DOUBLE, mtim DOUBLE) PRIMARY KEY did;
  /* CREATE UNIQUE INDEX 'index2' on 'dir_meta' (did); */
|}

open Sqlite3
open Tjr_monad.With_lwt

let assert_ok rc = assert(rc = Rc.OK)

[@@@warning "-27"]

(* FIXME we need to be sure where we are caching, and if the caching
   is worth it; at the moment, we already have generic directory
   caching in the layer above this one 

   We expect that the generic caching will call 
*)
let make_dir_ops ~db ~did = 

  (* statements *)
  let get_meta = prepare db {| SELECT parent,atim,mtim FROM dir_meta WHERE did=? |} in
  let insert = prepare db {| INSERT INTO dir_entries VALUES (?,?,?) ON DUPLICATE KEY UPDATE |} in
  let delete = prepare db {| DELETE FROM dir_entries WHERE (did,name) = (?,?) |} in
  let set_parent = prepare db {| UPDATE dir_meta SET parent=? WHERE did = ? |} in
  let set_times = prepare db {| UPDATE dir_meta SET (atim,mtim)=(?,?) WHERE did = ? |} in
  let find = prepare db {| SELECT dir_entry from dir_entries WHERE (did,name) = (?,?) |} in

  let did = did_to_int did in
  let get_meta () = 
    let stmt = get_meta in
    assert_ok (reset stmt);    
    assert_ok (bind_int stmt 1 did);
    step stmt |> fun rc -> 
    assert(rc=Rc.DONE || (print_endline (Rc.to_string rc); false));
    (column_int stmt 1 |> int_to_did, Times.{atim=column_double stmt 2;mtim=column_double stmt 3})
    |> return
  in
  let find k : (dir_entry option,_)m = 
    let stmt = find in
    assert_ok (reset stmt);    
    assert_ok (bind_int stmt 1 did);
    assert_ok (bind_text stmt 2 k);
    step stmt |> fun rc -> 
    assert(rc=Rc.ROW || rc=Rc.DONE || (print_endline (Rc.to_string rc); false));
    match rc=Rc.DONE with
    | true -> 
      (* no results *)
      return None
    | false -> 
      assert(rc=Rc.ROW);
      (* NOTE from the uniqueness of did,name there should not be any more entries *)
      let de = column_blob stmt 1 in
      Some (de |> string_to_dir_entry)
      |> return
  in
  let exec (eops: _ Dv3.Op.exec_ops) = 
    let (tag,ops) = eops in
    assert(tag=With_sync); (* FIXME remove alternative *)
    assert_ok (exec db "BEGIN TRANSACTION");
    begin
      ops |> List.iter (fun op -> 
          match op with 
          | Dv3.Op.Insert(k,v) ->
            let stmt = insert in
            assert_ok (reset stmt);
            assert_ok (bind_int stmt 1 did);
            assert_ok (bind_text stmt 2 k);
            assert_ok (bind_blob stmt 3 (v |> dir_entry_to_string));
            step stmt |> fun rc -> 
            assert(rc=Rc.DONE);
            ()
          | Delete k -> 
            let stmt = delete in
            assert_ok (reset stmt);
            assert_ok (bind_int stmt 1 did);
            assert_ok (bind_text stmt 2 k);
            step stmt |> fun rc -> 
            assert(rc=Rc.DONE);
            ()          
          | Set_parent p -> 
            let stmt = set_parent in
            assert_ok (reset stmt);
            assert_ok (bind_int stmt 1 (p|>did_to_int));
            assert_ok (bind_int stmt 2 did);
            step stmt |> fun rc -> 
            assert(rc=Rc.DONE);
          | Set_times times -> 
            let stmt = set_times in
            assert_ok (reset stmt);
            assert_ok (bind_double stmt 1 times.atim);
            assert_ok (bind_double stmt 2 times.mtim);
            assert_ok (bind_int stmt 3 did);
            step stmt |> fun rc -> 
            assert(rc=Rc.DONE))
    end;
    assert_ok (exec db "END TRANSACTION");
    return ()
  in
  
  let opendir () = 
    failwith ""
  in

  { get_meta; find; exec; opendir }




