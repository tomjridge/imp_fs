(** Free list

This is a little experiment in modelling the free list as a list of
   blk_ids, and a list of operations (similar to the pcache).

*)


type blk_id  = {blk_id:int}

let mk_id blk_id = {blk_id}

type root_blk = Root_blk of { free_list: blk_id; op_list: blk_id }

type v = {value:int}

let mk_v value = {value}

type op = Free of v | Alloc of v

type blk = 
    | Free_blk of v list * blk_id option
    | Op_blk of op list * blk_id option

module M = Map.Make(struct type t = blk_id let compare: t -> t -> int = Pervasives.compare end)
type store = blk M.t

type state = {
  store          : store;
  free_list_root : blk_id;
  free_list_head : blk_id;
  op_list_root   : blk_id;
  op_list_head   : blk_id;
  transient      : v list;
}

let initial_state = 
  let free_list_root = {blk_id=0} in
  let free_list_head = {blk_id=5} in

  let op_list_root = {blk_id=1} in
  let op_list_head = {blk_id=2} in
  {
  store = [
    (free_list_root,Free_blk([10;11;12;13;14;15;16;17] |> List.map mk_v,Some free_list_head));
    (free_list_head,Free_blk([18;19;20]|>List.map mk_v,None));

    (op_list_root,Op_blk([],Some op_list_head));
    (op_list_head,Op_blk([],None));
  ] |> List.to_seq |> M.of_seq;
  free_list_root;
  free_list_head;
  op_list_root;
  op_list_head;
  transient    = [];
}

[@@@ocaml.warning "-8-27"]

let append_op ~store ~op_list_head ~free_blk_id ~op = 
  (* read the blk, check if we can fit; if not, allocate a blk (without appending to the op list) and write the new op+the new alloc in the new blk *)
  M.find op_list_head store |> fun (Op_blk(ops,nxt)) -> 
  match List.length ops < 5 with 
  | true -> (
      (* allocate in the head blk *)
      let store = M.add op_list_head (Op_blk(ops@[op],nxt)) store in
      store,Some free_blk_id  (* return unused free_blk *)
    )
  | false -> (
      store |> M.add free_blk_id (Op_blk([op],None))
      |> M.add op_list_head (Op_blk(ops,Some free_blk_id)) |> fun store -> 
      store,None)

let _ = append_op


(* requires |s.transient|<=1; ensures |...|>=2; perhaps we should just set a min>=2 for the transients?  *)
let fill_transient s = 
  assert(List.length s.transient <= 1);
  s.store |> M.find s.free_list_root |> fun (Free_blk(vs,Some nxt)) ->
  (* FIXME what if |vs|<=1? need to keep going *)
  let old_blk = s.free_list_root in
  (* if free_list is persisted, we can also free old_blk... this can be an action post root blk persist *)
  let transient = vs@s.transient in
  assert(List.length vs >= 2);
  {s with free_list_root=nxt; transient},old_blk
  
let alloc s = 
  (* ensure s.transient is not empty *)
  begin
    match s.transient with
    | [] -> (s |> fill_transient |> fun (s,old_blk) -> (s,Some old_blk))
    | _ -> s,None
  end 
  |> fun (s,_old_blk) ->

  match s.transient with 
  | [] -> failwith "impossible"
  | v1::v2::transient -> 
    let op = Alloc v1 in
    (* NOTE cast from value to blk_id; NOTE transient assumed non-empty here as well... but why FIXME? *)
    let free_blk_id = v2 |> fun {value} -> {blk_id=value} in
    let s = {s with transient } in

    (* now add op to the op list *)
    append_op ~store:s.store ~op_list_head:s.op_list_head ~free_blk_id ~op |> fun (store,maybe_free_blk) -> 
    let s = {s with transient=(
        match maybe_free_blk with 
        | None -> s.transient 
        | Some {blk_id} -> s.transient@[{value=blk_id}]) } (* NOTE cast from blk_id to value *)
    in
    `Allocated v1,`New_store s

(* FIXME or do we need to record a free blk after we persist the root blk ? *)


(* test scenarios:

- non-empty transient
  - free space in op_list_head (simple alloc, and op append)
  - no space in op_list_head, use head of transient as a free blk to extend op_list
- empty transient
  - get free_list_head blk and fill transient, then continue as above

FIXME we also need to incorporate root blk, and post-root additional free of prefix from free_list_root

*)



