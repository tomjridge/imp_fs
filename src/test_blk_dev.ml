(** A test blk dev which keeps track of which writes occurred when and
   can write all intermediate states to a directory of files (numbered
   0...) *)

open Shared_ctxt

module M = Map.Make(struct type t = r let compare = Stdlib.compare end)

type map = Bigstring.t M.t

(** A list of block writes *)
type trace = {
  mutable writes : (r*string) list;
  mutable map    : map;
  mutable dirty  : bool;
}

let empty_trace () = { writes=[]; map=M.empty; dirty=true }  

let blk_sz' = (Blk_sz.to_int blk_sz)


let marshal_fn ~dirname = dirname^"/trace.marshal"

(** Construct the blk dev, backed by the dir *)
let make ~dirname =
  Printf.printf "Using directory %s for trace\n%!" dirname;
  let t0 = empty_trace () in
  let write ~blk_id ~blk = 
    Bigstring.to_string blk |> fun blk' -> 
    t0.writes <- (blk_id,blk')::t0.writes;
    t0.map <- M.add blk_id blk t0.map; 
    t0.dirty <- true;
    return ()
  in
  let read ~blk_id = 
    M.find_opt blk_id t0.map |> function
    | None -> 
      Printf.printf "%s: WARNING! attempt to read unwritten block %d\n%!" __FILE__ (B.to_int blk_id);
      Bigstring.make blk_sz' 'x' |>
      return
    | Some x -> return x
  in
  let write_versions_to_dir () = 
    ignore(Sys.command ("mkdir -p "^dirname));
    let trace = List.rev t0.writes in

    (* Write the entire trace to dirname/trace.marshal *)
    (Marshal.to_string t0 [] |> 
     Tjr_file.write_string_to_file ~fn:(marshal_fn ~dirname));

    (* Write versions to dirname/0,1,... *)
    (0,Bytes.empty,trace) |> iter_k (fun ~k (n,file,tr) ->
        match tr with
        | [] -> (t0.dirty <- false;())
        | (blk_id,blk)::tr -> 
          (* maybe resize *)
          let file = 
            let newlen = blk_sz' * (B.to_int blk_id+1) in
            match Bytes.length file < newlen with
            | true -> 
              let file' = Bytes.create newlen in
              Bytes.blit file 0 file' 0 (Bytes.length file);
              file'
            | false -> file
          in
          Bytes.blit_string blk 0 file (blk_sz' * B.to_int blk_id) blk_sz';
          Tjr_file.write_string_to_file ~fn:(dirname^"/"^(string_of_int n)) 
            (Bytes.to_string file);
          k (n+1,file,tr))
  in
  (* In case of an exception, make sure we actually write the trace *)
  Stdlib.at_exit (fun () -> if t0.dirty then write_versions_to_dir ());
  object
    method blk_dev_ops = { read; write; blk_sz; write_many=(fun _ -> failwith __LOC__) }
    method write_versions_to_dir = write_versions_to_dir
    method close () = (write_versions_to_dir (); return ())
    method t0 = t0
  end           

let max_ () =
  Sys.readdir "." |> fun arr -> 
  let max_ = ref 0 in
  arr |> Array.iter (fun fn -> 
      try (max_ := max !max_ (int_of_string fn)) with _ -> ());
  !max_


(* by default, use a directory whose number is bigger than all previous... *)
let make () =
  let max_ = max_ () in
  make ~dirname:(max_ |> fun x-> x+1 |> string_of_int)


let restore () = 
  (* directory we restore from *)
  let max_ = max_ () in  
  make () |> fun trace ->
  let fn = marshal_fn ~dirname:(string_of_int max_) in
  Printf.printf "%s: restoring from %s\n%!" __FILE__ fn;
  Tjr_file.read_file fn |> fun s -> 
  Marshal.from_string s 0 |> fun (x:trace) -> 
  trace#t0.writes <- x.writes;
  trace#t0.map <- x.map;
  trace#t0.dirty <- true;
  trace
  
