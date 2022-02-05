(** Instantiate Sqlite_dir with standard v3 types *)

(* NOTE trivial functor because Sqlite_dir.Make creates mutable state *)
module Make() = struct
  include Sqlite_dir.Make(struct
      type did = V3_base_types.did
      let did_to_int = fun x -> x
      let int_to_did = fun x -> x
      type dir_entry = V3_level2.S1.dir_entry
      let dir_entry_to_string de = 
        de |> V3_level2.S1.dir_entry_to_yojson |> Yojson.Safe.to_string
      let string_to_dir_entry s = 
        s |> Yojson.Safe.from_string |> V3_level2.S1.dir_entry_of_yojson |> function 
        | Ok x -> x
        | Error e -> failwith e (* shouldn't happen of course *)
    end)
end
