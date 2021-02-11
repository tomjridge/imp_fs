(** Implement the lower_ops interface from gom_v3, using sqlite *)

[@@@warning "-33"]

open Printf
open Gom_v3

let tbl = "gom"

(* SQL commands to create a db; dir_entries and dir_meta *)
let create_db_stmt = sprintf {|
  DROP TABLE IF EXISTS '%s';
  CREATE TABLE '%s' (id INTEGER, value BLOB, PRIMARY KEY (id));
|} tbl tbl

open Sqlite3
open Tjr_monad.With_lwt

let assert_ok rc = assert(rc = Rc.OK)

open Gom_v3.Op

type ('k,'v,'t) lower_ops = {
  get_max: unit -> 'k;
  (** for debugging *)

  alloc_n: int -> ('k list,'t)m;
  (** blocking *)

  find : 'k -> ('v option,'t) m;

  exec : ('k,'v) exec_ops -> (unit,'t) m;
  (** exec is assumed synchronous - all caching done in the layer above *)
}

module Make(S:sig
    type k = int[@@warning "-34"]
    type v

    val v_to_string: v -> string
    val string_to_v: string -> v
  end) = struct
  open S

  (** Statements *)


  let make ~db ~tbl = 
    let find = prepare db @@ sprintf {| SELECT id,value from '%s' WHERE id = ? |} tbl in
    let insert = prepare db @@ sprintf {| INSERT OR REPLACE INTO '%s' VALUES (?,?) |} tbl in
    let delete = prepare db @@ sprintf {| DELETE FROM '%s' WHERE id = ? |} tbl in

    (* if the table is empty, max returns null; so we use coalesce to
       provide a default value *)
    let get_max = prepare db @@ sprintf {| SELECT coalesce(max(id),0) FROM '%s' |} tbl in
    
    let get_max () = 
      let stmt = get_max in
      assert_ok (reset stmt);    
      step stmt |> fun rc -> 
      assert(rc=Rc.ROW || (print_endline (Rc.to_string rc); false));
      let max = column_int stmt 0 in
      max
    in

    let alloc_n n = 
      let m = get_max () in
      let xs = List_.mk_range ~min:(m+1) ~max:(m+n+1) ~step:1 in
      return xs
    in

    let find k = 
      (* copied from sqlite_dir *)
      let stmt = find in
      assert_ok (reset stmt);    
      assert_ok (bind_int stmt 1 k);
      step stmt |> fun rc -> 
      assert(rc=Rc.ROW || rc=Rc.DONE || (print_endline (Rc.to_string rc); false));
      match rc=Rc.DONE with
      | true -> 
        (* no results *)
        return None
      | false -> 
        assert(rc=Rc.ROW);
        (* NOTE from the uniqueness of did,name there should not be any more entries *)
        column_blob stmt 1 |> string_to_v |> fun x -> Some x |> return
    in

    let exec ops =
      assert_ok (exec db "BEGIN TRANSACTION");
      begin
        ops |> List.iter (fun op -> 
            match op with 
            | Insert(k,v) ->
              let stmt = insert in
              assert_ok (reset stmt);
              assert_ok (bind_int stmt 1 k);
              assert_ok (bind_blob stmt 2 (v |> v_to_string));
              step stmt |> fun rc -> 
              assert(rc=Rc.DONE);
              ()
            | Delete k -> 
              let stmt = delete in
              assert_ok (reset stmt);
              assert_ok (bind_int stmt 1 k);
              step stmt |> fun rc -> 
              assert(rc=Rc.DONE);
              ())
      end;
      assert_ok (exec db "END TRANSACTION");
      return ()
    in

    let _ = exec in
      
    { get_max; alloc_n; find; exec }
          
      
    
end


module Test() = struct

  module Made = Make(struct 
      type k = int
      type v = string

      let v_to_string = fun x -> x
      let string_to_v = fun x -> x
    end)

  open Made

  let db = db_open "sqlite_gom_test.db"

  let _ = 
    assert_ok (exec db create_db_stmt)

  let gom_ops = make ~db ~tbl

  let { get_max; alloc_n; find; exec } = gom_ops

  let test_program = 
    (* NOTE if we don't batch delta ops, but submit individually, this is very slow *)
    let delta = 10_000 in
    1 |> iter_k (fun ~k i -> 
        match i > 100_000 with
        | true -> return ()
        | false -> 
          let xs = List_.mk_range ~min:i ~max:(i+delta) ~step:1 in
          let xs = xs |> List.map (fun i -> Insert(i,"FIXME")) in
          exec xs >>= fun () -> 
          k (i+delta))
    >>= fun () -> 
    get_max () |> fun n -> 
    Printf.printf "Max id: %d\n%!" n;
    return ()

end
