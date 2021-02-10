(** An implementation of directories on top of sqlite, for v3.

To implement directories, we need to implement the interface:

{[
type ('k,'v,'t,'did) lower_ops = {
  get_meta  : unit -> ('did * times,'t)m;
  (** returns parent and times *)

  find      : 'k -> ('v option,'t) m;

  exec      : ('k,'v,'t,'did) exec_ops -> (unit,'t)m;

  opendir   : unit -> (('k,'v,'t)Ls.ops,'t)m;

  (* sync : unit -> (unit,'t)m; NOTE we assume the lower layer already
     handles these synchronously *)
}
]}

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

[@@@warning "-33"]

open Printf

module Times = Minifs_intf.Times

module List = struct
  include List
  let hd_opt xs = if xs = [] then None else Some (List.hd xs)
end

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


type 'a cached_update = Some_ of 'a | Deleted

type db = Sqlite3.db

(* NOTE no caching at this level *)

(* SQL commands to create a db; dir_entries and dir_meta *)
let create_db_stmt = {|
  DROP TABLE IF EXISTS 'dir_entries';
  DROP TABLE IF EXISTS 'dir_meta';

  CREATE TABLE 'dir_entries' (did INTEGER, name VARCHAR(255), dir_entry BLOB, PRIMARY KEY (did,name));
  /* CREATE UNIQUE INDEX 'index1' on 'dir_entries' (did,name); */

  CREATE TABLE 'dir_meta' (did INTEGER, parent INTEGER, atim DOUBLE, mtim DOUBLE, PRIMARY KEY (did));
  /* CREATE UNIQUE INDEX 'index2' on 'dir_meta' (did); */
|}


open Sqlite3
open Tjr_monad.With_lwt

let assert_ok rc = assert(rc = Rc.OK)

[@@@warning "-27"]

let readdir_limit = 1000
let _ = assert(readdir_limit > 0)



module Make(S:sig
    type did

    val did_to_int: did -> int 
    val int_to_did: int -> did 

    type dir_entry

    (** convert to string/blob *)
    val dir_entry_to_string: dir_entry -> string
    val string_to_dir_entry: string -> dir_entry 
  end) = struct
  open S

  (* FIXME we need to be sure where we are caching, and if the caching
     is worth it; at the moment, we already have generic directory
     caching in the layer above this one 

     We expect that the generic caching will call 
  *)
  let make_dir_ops ~db ~did = 

    (* statements *)
    let get_meta = prepare db {| SELECT parent,atim,mtim FROM dir_meta WHERE did=? |} in
    let insert = prepare db {| INSERT OR REPLACE INTO dir_entries VALUES (?,?,?) |} in
    let delete = prepare db {| DELETE FROM dir_entries WHERE (did,name) = (?,?) |} in
    let set_parent = prepare db {| UPDATE dir_meta SET parent=? WHERE did = ? |} in
    let set_times = prepare db {| UPDATE dir_meta SET (atim,mtim)=(?,?) WHERE did = ? |} in
    let find = prepare db {| SELECT dir_entry from dir_entries WHERE (did,name) = (?,?) |} in
    (* initial opendir - pull n entries; include did and name to try to force use of pk *)
    let opendir_1 = prepare db {|
    SELECT did,name,dir_entry FROM dir_entries 
    WHERE did=? 
    ORDER BY name 
    LIMIT ? |} 
    in
    (* subsequent readdirs - pull next n entries, using last name from previous *)
    let opendir_2 = prepare db {| 
    SELECT did,name,dir_entry FROM dir_entries 
    WHERE did=? AND name > ? 
    ORDER BY name 
    LIMIT ? |}
    in
    let did = did_to_int did in
    let get_meta () = 
      let stmt = get_meta in
      assert_ok (reset stmt);    
      assert_ok (bind_int stmt 1 did);
      step stmt |> fun rc -> 
      assert(rc=Rc.DONE || (print_endline (Rc.to_string rc); false));
      (* NOTE bind starts from 1, cols from 0 *)
      (column_int stmt 0 |> int_to_did, Times.{atim=column_double stmt 1;mtim=column_double stmt 2})
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
        let de = column_blob stmt 0 in
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

    (* read entries after (not including) from_name; return results in reverse name order *)
    let readdir ~from_name = 
      let stmt = opendir_2 in
      assert_ok (reset stmt);
      assert_ok (bind_int stmt 1 did);
      assert_ok (bind_text stmt 2 from_name);
      assert_ok (bind_int stmt 3 readdir_limit);
      [] |> iter_k (fun ~k xs ->         
          step stmt |> fun rc -> 
          assert(rc=Rc.ROW || rc=Rc.DONE || (print_endline (Rc.to_string rc); false));
          match rc=Rc.DONE with
          | true -> xs
          | false -> 
            let name = column_text stmt 1 in
            let de = column_blob stmt 2 in
            k ( (name,de|>string_to_dir_entry)::xs))
      |> fun rows -> 
      rows (* in reverse order *)
    in    

    let opendir () = 
      let stmt = opendir_1 in
      assert_ok (reset stmt);
      assert_ok (bind_int stmt 1 did);
      assert_ok (bind_int stmt 2 readdir_limit);
      [] |> iter_k (fun ~k xs ->         
          step stmt |> fun rc -> 
          assert(rc=Rc.ROW || rc=Rc.DONE || (print_endline (Rc.to_string rc); false));
          match rc=Rc.DONE with
          | true -> xs
          | false -> 
            let name = column_text stmt 1 in
            let de = column_blob stmt 2 in
            k ( (name,de|>string_to_dir_entry)::xs))
      |> fun rows -> 
      (* NOTE stateful interface *)
      let current_max_name = ref (match rows with [] -> None | (name,de)::_ -> Some name) in
      let current_rows = ref (List.rev rows) in
      let finished () = !current_rows = [] in
      let kvs = fun () -> return !current_rows in
      let step = fun () -> 
        match finished () with
        | true -> return ()
        | false -> 
          assert(!current_max_name <> None);
          let from_name = dest_Some(!current_max_name) in
          readdir ~from_name |> fun rows -> 
          match rows with
          | [] -> (
              current_max_name := None; 
              current_rows := []; 
              return ())
          | _ -> (
              current_max_name := Some(List.hd rows |> fst); 
              current_rows := List.rev rows; 
              return ())
      in
      (* let close () = return () in *)
      let ops = Dv3.Ls.{kvs;step;is_finished=(fun () -> return (finished()))} in
      return ops
    in

    { get_meta; find; exec; opendir }

end



module Test() = struct

  module Made = Make(struct 

      type did = int (* for testing *)

      let did_to_int: did -> int = fun x -> x
      let int_to_did: int -> did = fun x -> x

      type dir_entry = string (* for testing *)

      (** convert to string/blob *)
      let dir_entry_to_string: dir_entry -> string = fun s -> s
      let string_to_dir_entry: string -> dir_entry = fun s -> s
    end)

  open Made

  let db = db_open "sqlite_dir_test.db"

  let _ = 
    assert_ok (exec db create_db_stmt)

  let dir_ops = make_dir_ops ~db ~did:1

  let { get_meta; find; exec; opendir } = dir_ops

  open Dv3.Op

  let test_program = 
    (* NOTE if we don't batch delta ops, but submit individually, this is very slow *)
    let delta = 10_000 in
    1 |> iter_k (fun ~k i -> 
        match i > 100_000 with
        | true -> return ()
        | false -> 
          let xs = List_.mk_range ~min:i ~max:(i+delta) ~step:1 in
          let xs = xs |> List.map (fun i -> Insert("filename"^(string_of_int i),"FIXME")) in
          exec (With_sync,xs) >>= fun () -> 
          k (i+delta))
    >>= fun () -> 
    (* now print out the results *)
    let total = ref 0 in
    opendir () >>= fun ops -> 
    () |> iter_k (fun ~k () -> 
        ops.is_finished () >>= function
        | true -> return ()
        | false -> 
          ops.kvs () >>= fun kvs -> 
          total := !total + List.length kvs;
          ops.step() >>= fun () -> 
          k ())
    >>= fun () ->         
    printf "Read %d entries\n%!" !total;
    return ()
  
end



