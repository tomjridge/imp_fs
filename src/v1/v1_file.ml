(** Implement file data (not metadata, apart from size) by using files
   on an underlying filesystem.

Don't open.

*)

open Printf
open Tjr_monad.With_lwt
open Shared_ctxt

let dont_log = true

module File_ops = struct
  open Call_specific_errors

  type 'fid file_ops = {
    (* fn: 'fid -> string; *)
    pread: fid:'fid -> foff:int -> len:int -> buf:buf -> boff:int -> 
      ((int,pread_err)result, t) m;
    pwrite: fid:'fid -> foff:int -> len:int -> buf:buf -> boff:int -> 
      ((int,pwrite_err)result, t) m;
    truncate: fid:'fid -> int -> (unit,t)m;
    get_sz: fid:'fid -> unit -> (int,t)m;
    create: fid:'fid -> (unit,t)m;
  }
end
include File_ops

module Make(S:sig
    type fid
    val fid_to_int: fid -> int
  end) = struct
  open S

  let fn fid = sprintf "./tmp/v1_files/%d" (fid|>fid_to_int)

  let get_fd : fid:fid -> foff:int -> (Lwt_unix.file_descr,t)m = fun ~fid ~foff ->
    let default_file_perm = Tjr_file.default_create_perm in
    (from_lwt Lwt_unix.(openfile (fn fid) [O_RDWR] default_file_perm)) >>= fun fd ->
    (from_lwt Lwt_unix.(lseek fd foff SEEK_SET)) >>= fun (_:int) ->
    return fd

  let close fd = (from_lwt Lwt_unix.(close fd))

  let pread ~fid ~foff ~len ~buf ~boff = 
    get_fd ~fid ~foff >>= fun fd ->
    let bs = Bytes.create (buf_ops.buf_length buf - boff) in
    (from_lwt Lwt_unix.(read fd bs boff len)) >>= fun (n:int) ->
    Bigstring.blit_of_bytes bs 0 buf boff n;
    close fd >>= fun () ->
    return (Ok n)

  let pwrite ~fid ~foff ~len ~buf ~boff = 
    assert(dont_log || 
           (printf "pwrite 1: fid=%d foff=%d len=%d boff=%d \n%!" 
              (fid|>fid_to_int) foff len boff; true));
    get_fd ~fid ~foff >>= fun fd ->
    assert(dont_log || (printf "pwrite 2\n%!";true));
    let bs = Bigstring.to_bytes buf in  (* FIXME don't need whole buf *)
    assert(dont_log || (printf "pwrite 3\n%!";true));
    (from_lwt Lwt_unix.(write fd bs boff len)) >>= fun n ->
    Printf.printf "pwrite 4\n%!";
    assert(n=len); (* FIXME *)
    Bigstring.blit_of_bytes bs boff buf boff n;
    close fd >>= fun () ->
    assert(dont_log || (printf "pwrite 5\n%!";true));
    return (Ok n)

  let truncate ~fid len = 
    (from_lwt Lwt_unix.(truncate (fn fid) len))

  let get_sz ~fid () =
    (from_lwt Lwt_unix.(stat (fn fid))) >>= fun st -> 
    return st.st_size

  (* NOTE no times - this is data only *)
  let create ~fid = 
    let perm = Tjr_file.default_create_perm in
    (* FIXME we assume it doesn't already exist *)
    from_lwt Lwt_unix.(openfile (fn fid) [O_CREAT;O_RDWR] perm) >>= fun fd ->
    from_lwt Lwt_unix.(close fd) >>= fun () ->
    return ()

  let file_ops = { pread; pwrite; truncate; get_sz; create }

end

